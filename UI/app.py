import os
import sys
import random
import string
import datetime
sys.path.append('../bin/')
import traceback
import functionsForUI as fn
import email_content_extraction as ece
import logging
# from functionsForUI import validateCredentials,exportExcelToJSON,getSingleValueMap,getMultiValueMap,readFrame,uiDriverFunction,adminScreenDriverFunction,commitAdminMapping
from flask import Flask, render_template, json, request,jsonify,send_file,send_from_directory
from werkzeug.utils import secure_filename
import encrypt as en
import constants
import pdfExtraction as pdfExt
import pandas as pd
import waitress
import zipfile
from flask_pymongo import PyMongo
import gridfs
import io
from time import perf_counter
app = Flask(__name__)

import requests
import socket
import collections
from bson import json_util

app.config["MONGO_URI"] = constants.mongoUri
mongo = PyMongo(app)
fs = gridfs.GridFS(mongo.db)

logging.getLogger().setLevel(logging.DEBUG)

# This is the path to the upload directory
app.config['UPLOAD_FOLDER'] = '../input/'

# These are the extension that we are accepting to be uploaded
# These are the extension that we are accepting to be uploaded
app.config['ALLOWED_EXTENSIONS'] = set([".htm",".html",".pdf"])

# For a given file, returns whether it's an allowed type or not
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in app.config['ALLOWED_EXTENSIONS']

# header value change
@app.after_request
def apply_caching(response):
    response.headers["Server"] = ""
    #response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' cdnjs.cloudflare.com; img-src 'self'; style-src 'self' 'unsafe-inline' fonts.googleapis.com cdnjs.cloudflare.com; font-src 'self' fonts.gstatic.com cdnjs.cloudflare.com; form-action 'self';"
    response.headers["Content-Security-Policy"] = "default-src 'self' blob: unsafe-eval; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://fontawesome.com/; font-src 'self' https://fonts.gstatic.com data;script-src 'self' 'unsafe-inline' 'unsafe-eval'"
    response.headers["X-XSS-Protection"] = "1"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "deny"
    response.headers["Cache-Control"] = 'no-store,no-cache'
    response.headers['Access-Control-Request-Method'] = 'POST'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return response

def head(method):
    if method=="HEAD":
        return 'Method not allowed', 405

@app.route('/')
def main():
    return render_template('starter.html')

@app.route('/signin/',methods=['POST'])
def signUp():
    decyptedData = en.decrypt(request.data, en.key)
    userData = json.loads(decyptedData)
    _name = userData['name']
    _password = userData['password']
    return fn.validateCredentials(_name,_password)

@app.route('/signout/',methods=['POST'])
def signOut():
    decData = en.decrypt(request.data, en.key)
    decData = json.loads(decData)
    if decData:
        role = decData['role']
        name = decData['name']
        fn.adminSignout(role, name)
        print('Signed out successfully')
    return 'success'

@app.route('/getDelegates',methods=['GET','POST'])
def getDelegates():
    if (request.method == 'GET' or request.method =='POST'):
        providerData = fn.getAllProviders();
        return jsonify(providerData)
    else:
        return head(request.method)

@app.route('/uploadFiles/upload',methods=['GET','POST'])
def uploadCombine():
    allowed_chars = ''.join((string.ascii_lowercase, string.ascii_uppercase, string.digits))
    unique_id = (''.join(random.choice(allowed_chars) for _ in range(32))) +'_'+ str(datetime.datetime.now().timestamp())
    if (request.method == 'GET' or request.method =='POST'):
        files = request.files
        if files:
            filenames = []
            try:
                for file in files:
                    print('allowing file')
                    filename = secure_filename(files[file].filename)
                    print("filename while uploading:",filename)
                    fNmUniq, fileExt = os.path.splitext(filename)
                    fNmUniq = fNmUniq +'_'+unique_id + fileExt
                    if fileExt=='.pdf':
                        files[file].save(os.path.join(app.config['UPLOAD_FOLDER'], fNmUniq))
                        print(fNmUniq + " successfully saved on disk")
                    else:
                        fn.saveToGridFS(mongo, fNmUniq, files[file], 'inputFile')
                        print(fNmUniq + " successfully saved on mongodb")
                    filenames.append(fNmUniq)
                return json.dumps({'filename':filenames,'token':unique_id,'fNmUniq':fNmUniq}), 200
            except Exception as e:
                return 'Invalid Template', 500
        return 'error', 500
    else:
        return head(request.method)



@app.route('/extractEmail/<filename>',methods=['POST'])
def extractEmail(filename):
    if filename:
        filename = secure_filename(filename)
        print("email to process:",filename)
        data = ece.main(filename)
        print("Email Data:",data)
        return jsonify(data)

@app.route('/getInputFile/<filename>',methods=['POST'])
def getInputFile(filename):
    stMain = perf_counter()
    userName = request.json['userName']
    headers = request.json['headers']
    unique_id,fNmUniq=headers['token'],headers['fNmUniq']
    if filename:
        startProcessTimestamp = str(datetime.datetime.now())
        ############## creating log for excel files ######################
        logData = {"filename": [filename],
                   "token": unique_id,
                   "username": userName,
                   "fileType": "excel",
                   "startProcessTimestamp": startProcessTimestamp,
                   "processCompleteFlag": False}
        constants.handler.insert_one_document('logging', logData)
        ###################################################################
        fileObj=fn.retrieveFileFromMongo(fNmUniq,mongo.db,fs)
        st=perf_counter()
        fObj=fn.saveExlFileSnapshot(fNmUniq,fileObj) #this operation saves a snspshot onto disk
        fn.logPerf(st,'saveExlFileSnapshot')
        st=perf_counter()
        headerData,workbook = fn.exportExcelToJSON(fObj)
        fn.logPerf(st, 'exportExcelToJSON')
        # print("headerFields:",headerData)
        # excelData = fn.displayInput(fNmUniq,headerData)
        sheetNames=workbook.sheet_names()
        st = perf_counter()
        excelData = fn.displayInputNew(fObj,headerData,sheetNames)
        fn.logPerf(st, 'displayInputNew')
        fileObj.close()
        fObj.close()
        # print("Input Data:",excelData)

        # providerData = fn.getAllProviders();
        data = {
        "excelData": excelData,
        "headers": headerData,
        'unique_id': unique_id
        # 'AllDelegates': providerData
        }
        fn.logPerf(stMain,'getInputFile',ovrRide=True)
        return jsonify(data)

@app.route('/setHeaderRows',methods=['POST'])
def setHeaderRows():
    stMain = perf_counter()
    unique_id,fNmUniq=request.json['token'],request.json['fNmUniq']
    headerRows = request.json['headervalues']
    filename = request.json['filename']
    unique_id = request.json['token']
    delegateName = request.json.get('delegateCode', None)
    delegateStatus = request.json.get('delegateStatus', None)
    # print("token in get delegate:",unique_id)
    fileObj=fn.retrieveFileFromMongo(fNmUniq,mongo.db,fs)
    _diDFs=fn.createDiDFs(headerRows['tabDict'],fNmUniq,fileObj)
    sheet_with_header_detail = fn.readFrameWrapper(fNmUniq, headerRows, unique_id,mongo,fileObj,_diDFs)
    fileObj.close()
    sheet_with_header_detail = fn.getMappingwithMatch(sheet_with_header_detail, delegateName, delegateStatus)
    ############## updating logs with delegateCode, nTabs and tabInfo ###########
    logData = constants.handler.find_one_document('logging',{"token":unique_id})
    logData['delegateCode'] = delegateName if delegateName else ""
    logData['nTabs'] = len(sheet_with_header_detail)
    logData['tabInfo'] = [{"tabName":tab, "originalMatchPercent":tabData['matchPer'], "tabProcessed":False} for tab, tabData in sheet_with_header_detail.items()]
    constants.handler.replace_one_document('logging', logData)
    ##############################################################################
    fn.logPerf(stMain,'setHeaderRows',ovrRide=True)
    return jsonify(sheet_with_header_detail)

@app.route('/getProviderMappings/',methods=['POST'])
def getProvMap():
    headerRows = request.json['headervalues']
    filename = request.json['filename']
    unique_id = request.json['token']
    data = fn.readFrameWrapper(filename, headerRows, unique_id)
    providerMap = fn.readProvMapping(request.json['delegateCode'])
    return json.dumps(providerMap)

@app.route('/getSvMappingFields/',methods=['POST'])
def getSvMappingFields():
    headers = request.json['headers']
    token = headers['token']
    sheetname = request.json['selectedSheet']
    print("token in single value:",token)
    delegate = request.json['delegate']
    if delegate == 'new':
        delegate = None
    # data = fn.diDFsFromMongo(token,mongo,fs)
    # data = data[sheetname].fillna('')
    data=pd.read_json(headers['mappingData'][sheetname]['dfSnapshot'], dtype=str).fillna('')
    singleValueMap = fn.getSingleValueMap(data,delegate)
    #print("InptSvMap:",singleValueMap)
    return json.dumps(singleValueMap)

@app.route('/getMvMappingFields/',methods=['POST'])
def getMvMappingFields():
    headers = request.json['headers']
    sheetname = request.json['sheetname']
    userSvMappings = {
        'sv' : request.json['sv']
    }
    token = request.json['token']
    delegate = request.json['delegate']
    if delegate == 'new':
        delegate = None
    #print("usersvmappings:",userSvMappings)
    # data = fn.diDFsFromMongo(token,mongo,fs)
    # data = data[sheetname].fillna('')
    data = pd.read_json(headers['mappingData'][sheetname]['dfSnapshot'], dtype=str).fillna('')
    multiValueMap = fn.getMultiValueMap(data,userSvMappings,delegate)
    #print("InptMvMap:",multiValueMap)
    sampleData = fn.sotDataToJson(data)
    #print("sampleData:",sampleData)
    mappings = { 'multiValueMap' : multiValueMap, 'sampleData' : sampleData}
    return json.dumps(mappings)

@app.route('/setMvMappingFields/',methods=['POST'])
def setMvMappingFields():
    stMain=perf_counter()
    prevData = request.json
    # for non delegated PDFs, request.json['mappings'] contains mapped sv,mv data
    # for non delegated PDFs, request.json['headers']['htmlData'] contains html
    userMapping = request.json['mappings']
    username = request.json['username']
    print("User:",username)
    headerValues = request.json['headers']
    filename = headerValues['filename']
    print("filename:",filename)
    token = headerValues['token']
    # print("token:",token)
    fileType = headerValues['fileType']
    ###################updating logs only for excel files #####################
    if fileType == 'attachmentFile':
        logData = constants.handler.find_one_document('logging',{'token':token})
        logData['businessType'] = headerValues['delType']
        for ix,tabDi in enumerate(logData['tabInfo']):
            if userMapping[tabDi['tabName']]['matchPer'] == 'Customised':
                logData['tabInfo'][ix].update({'remappedFlag':True})
            else:
                logData['tabInfo'][ix].update({'remappedFlag': False})
        constants.handler.replace_one_document('logging',logData)
    ###########################################################################
    if fileType == 'nonDelPdf':  # save data on mongodb for training
        filename = ', '.join(filename)
        fileTrainFlag, fileWiseTrainData = fn.toTrain(userMapping)
        for eachFile, flag in fileTrainFlag.items():
            if flag:  # flag=True implies save data
                constants.handler.insert_one_document('trainData',
                                                      {"htmlData": request.json['headers']['htmlData'][eachFile],
                                                       "userMapping": fileWiseTrainData[eachFile],
                                                       "timeStamp": str(datetime.datetime.now()),
                                                       "filename": eachFile})
    # status = headerValues['delegateStatus']
    # if status == 'new':
    #     delegate = None
    prevData, userMapping = fn.fileTypeDriver(token, prevData, userMapping, fileType)
    sysActionsJson=fn.compileActions(userMapping, token, filename,mongo,fs)

    # reconsolation report-(to get the input npi data and their count in Input sot )
    inputNpidata = fn.inputsotnpi(userMapping, token, filename, mongo, fs)

    log_Data = {"filename": filename,
                "token": token,
                "username": username,
                "fileType": "excel",
                "inputNpiData": inputNpidata,
                "timestamp": str(datetime.datetime.now())
                }

    # store the log_data in reconcilation collection(specially created for reconciliation report)
    constants.handler.insert_one_document('reconciliation', log_Data)

    # data = fn.uiDriverFunction(userMapping,token,filename,username,provName=delegate)
    prevData['ff'] = False
    jsonForActionScreen={'sysActionsJson':sysActionsJson,'previousScrData':prevData}

    clonned_db = constants.handler.db.get_collection('Cloned_data')
    # not present & 1st time  (saving this record for fetch SOT API)
    if clonned_db.count_documents({"Filename": filename, "Token": token}) == 0:
        jsonForActionScreen.update(
            {"Filename": filename, "Token": token, "Type": "Original", "Timestamp": str(datetime.datetime.now())})
        clonned_db.insert_one(jsonForActionScreen)
        # Saving original dflen in log collection
        logData = constants.handler.find_one_document('logging', {"token": token})
        for ix, di in enumerate(logData['tabInfo']):
            if di['tabName'] in jsonForActionScreen.get("previousScrData").get("mappings"):
                logData['tabInfo'][ix].update({'original_df_len': jsonForActionScreen.get("previousScrData").get(
                    "mappings").get(di['tabName']).get("dfLen"), "row_index": []})
                constants.handler.replace_one_document('logging', logData)

    sys_data = jsonForActionScreen.get("sysActionsJson").get("data")
    if clonned_db.count_documents({"Filename": filename, "Token": token, "Type": "Cloned"}) > 0 or clonned_db.count_documents(
            {"Filename": filename, "Token": token, "Type": "Decloned"}) > 0 or clonned_db.count_documents(
            {"Filename": filename, "Token": token, "Type": "FetchSOT"}) > 0:
        db_jsonForActionScreen = \
            clonned_db.find({"Filename": filename, "Token": token}).sort([("Timestamp", -1)])[
                0]  ## db last record is fetching
        for k, v in sys_data.items():
            if k in db_jsonForActionScreen.get("sysActionsJson").get("data"):
                sys_data[k] = db_jsonForActionScreen.get("sysActionsJson").get("data").get(k)

    fn.logPerf(stMain, 'setMvMappingFields', ovrRide=True)
    # return json.dumps(jsonForActionScreen)
    return json.loads(json_util.dumps(jsonForActionScreen))

def cloned_db_data(filename,token,previousScrData,sysActionsJson,cloned_type):
    clonned_db = constants.handler.db.get_collection('Cloned_data')
    check_d = clonned_db.count_documents(
        {"Filename": filename, "Token": token})
    if check_d != 0:
        final_dict = {}
        final_dict.update({"Filename": filename, "Token": token, "Type": cloned_type,
                           "Timestamp": str(datetime.datetime.now()), "previousScrData": previousScrData,
                           "sysActionsJson": sysActionsJson})
        clonned_db.insert_one(final_dict)
        return final_dict
    return {"Sucessfull"}

## data cloning API
@app.route('/multiTransDataClone/', methods=['GET', 'POST'])
def multiTransDataClone():
    headers = request.json.get('headers')
    username = request.json.get('username')
    filename = headers.get('filename')
    token = headers['token']
    cloning_data = request.json.get('cloningData')
    clone_flag = cloning_data.get('cloneflag')
    payload_data = cloning_data.get('req_data')
    sheet_data = cloning_data.get('sheetdata')  # for multiple sheets data for global save
    data = fn.diDFsFromMongo(token, mongo,fs)  ## returns the existing sheetwise data as dataframes in dictionary object7

    previousScrData =payload_data.get('previousScrData')
    sysActionsJson = payload_data.get('sysActionsJson')
    clonned_db = constants.handler.db.get_collection('Cloned_data')

    print("token : ", token)
    try:
        if (request.method == 'POST' and clone_flag == True):
            #print("Before Cloning, DB sheetwise data statistics: ")
            #for sheet, sheet_content in data.items():
            #    print("Length of ", sheet, "is : ", len(sheet_content))
            time1 = time2 = 0
            time1 = datetime.datetime.now()
            for sheetname, row_index in sheet_data.items():
                if sheetname and row_index:
                    org_sheet_data = data[sheetname]
                    df = org_sheet_data
                    counter = collections.Counter(row_index)
                    row_index_dict = dict(counter)
                    #print("sheetname : ", sheetname)
                    for index, freq in row_index_dict.items():
                        #print("index :", index)
                        #print("freq : ", freq)
                        #clonedf = org_sheet_data.iloc[index, :]
                        #df = df.append([org_sheet_data.iloc[index,:]] * freq,ignore_index=False)
                        df = df._append([org_sheet_data.iloc[index]] * freq, ignore_index=False)
                    df.sort_index(inplace=True)
                    df.reset_index(drop=True, inplace=True)
                    data[sheetname] = df
                    row_index_dict["Timestamp"] = str(datetime.datetime.now())
                    row_index_dict["Type"] = "Cloned"
                    constants.handler.db.get_collection('Logs').update_one(
                        {"tabInfo.tabName": sheetname, "token": token}, {
                            "$push": {"tabInfo.$.row_index": json.loads(
                                json_util.dumps(row_index_dict))}})
            fn.diDFsToMongo(data, token, mongo)
            time2 = datetime.datetime.now()
            print("Time taken to clone: ", str(time2 - time1))
            ## to fetch and verify the updated contents
            updated_data = fn.diDFsFromMongo(token, mongo, fs)
            #print("After Cloning, DB sheetwise data statistics: ")
            #for sheet, sheet_content in updated_data.items():
            #    print("Length of ", sheet, "is : ", len(sheet_content))

            filename = headers.get("fNmUniq")
            cloned_type = "Cloned"
            final_dict = cloned_db_data(filename, token, previousScrData, sysActionsJson, cloned_type)
            return json.loads(json_util.dumps(final_dict))
    except Exception as e:
        print("----CLONENED EXCEPTION---", e)
        return {"Error": "Clone Failed"},500
    ## data cloning API ends --original

    ## decloning API
    try:
        if (request.method == 'POST' and clone_flag == False):

            #print("Before decloning, DB sheetwise data statistics: ")
            #for sheet, sheet_content in data.items():
            #    print("Length of ", sheet, "is : ", len(sheet_content))
            time1 = time2 = 0
            time1 = datetime.datetime.now()
            for sheetname, row_index in sheet_data.items():
                if sheetname and row_index:
                    #print("sheet name for declonning the data: ", sheetname)
                    df = data[sheetname] ## df is the data fetched from the sheet after clone
                    #print("Length of sheet data before declone : ", len(sheet_content))
                    bs = df.duplicated(keep='first')
                    org_data = df[~bs] ##org_data is the original SOT data w.r.t the specific excel sheet
                    org_index_lst = list(org_data.index)
                    intersect_lst = list(set(row_index) & set(org_index_lst))
                    if intersect_lst:
                        #print("There is an intersection of indices, do not delete the original data")
                        return ("Select only clonned rows")
                    else:
                        #print("There is no intersection of clonned and original index list,can proceed to declone the rows")
                        decloned_df = df.drop(labels=row_index, axis=0)
                        decloned_df = decloned_df.reset_index(drop=True)
                        #print("Length of sheet data after declone : ", len(decloned_df))
                        data[sheetname] = decloned_df
                        fn.diDFsToMongo(data, token, mongo)
                        time2 = datetime.datetime.now()
                        print("Time taken to declone: ", str(time2 - time1))

                        ## declone rowindex-freq
                        df = decloned_df
                        row_index_dict = {}
                        dup_rows_df = df[df.duplicated(keep=False)]
                        if not dup_rows_df.empty:
                            dfgroup = dup_rows_df.groupby(list(dup_rows_df))
                            #print(dfgroup)
                            #keys = dfgroup.groups.keys()  ## key as a grouped duplicate rows data
                            values = dfgroup.groups.values()  ## values as a list of duplicated rows index
                            for item in values:
                                #print(item)
                                row_index_dict[str(item[0])] = len(item) - 1
                                #print("\n")
                            #import pdb;pdb.set_trace()
                            #print("after declone the row index-freq dict:\n", row_index_dict)
                            row_index_dict["Timestamp"] = str(
                                datetime.datetime.now())
                            row_index_dict["Type"] = "Decloned"
                            constants.handler.db.get_collection('Logs').update_one(
                                {"tabInfo.tabName": sheetname, "token": token}, {
                                    "$push": {"tabInfo.$.row_index": row_index_dict}})
                        else:
                            #print("After the declone, the original data has retained in the db. There are no clones")
                            row_index_dict["Timestamp"] = str(datetime.datetime.now())
                            row_index_dict["Type"] = "Decloned"
                            row_index_dict["Message"] = "After the declone, the original data has retained in the db. There are no clones"
                            constants.handler.db.get_collection('Logs').update_one(
                                {"tabInfo.tabName": sheetname, "token": token}, {
                                    "$push": {"tabInfo.$.row_index": json.loads(
                                        json_util.dumps(row_index_dict))}})
                        ##declone rowindex-freq ends

            updated_data = fn.diDFsFromMongo(token, mongo, fs)

            #print("After decloning, db statistics: ")
            #for sheet, sheet_content in updated_data.items():
            #    print("Length of ", sheet, "is : ", len(sheet_content))

            filename = headers.get("fNmUniq")
            cloned_type = "Decloned"
            final_dict = cloned_db_data(filename, token, previousScrData, sysActionsJson, cloned_type)
            return json.loads(json_util.dumps(final_dict))
    except Exception as e:
        print("----DECLONNED EXCEPTION-----",e)
        return {"Error":"Declone Failed"},500


    ## Data clone reset API (fetch original SOT)
    try:
        if (request.method == 'POST' and clone_flag == "reset_clone"):
            #print("Before resetting the clonned data, db statistics: ")
            time1 = time2 = 0
            time1 = datetime.datetime.now()
            for sheet, sheet_content in data.items():
                #print("Length of ", sheet, "is : ", len(sheet_content))
                #print("resetting the data for : ", sheet)
                df = data[sheet]
                bs = df.duplicated(keep='first')
                df = df[~bs]
                df = df.reset_index(drop=True)
                #print("Length of sheet data after reset : ", len(df))
                data[sheet] = df
            fn.diDFsToMongo(data, token, mongo)
            time2 = datetime.datetime.now()
            print("Time taken to DB reset/fetchSOT: ", str(time2 - time1))
            org_data = fn.diDFsFromMongo(token, mongo, fs)
            #print("After resetting the clonned data, DB statistics: ")
            #for sheet, sheet_content in org_data.items():
            #    print("Length of ", sheet, "is : ", len(sheet_content))

            response = clonned_db.find_one({"Token":token,"Filename":filename,"Type":"Original"},{"_id":0})
            filename = headers.get("fNmUniq")

            cloned_db_data(filename, token, response.get("previousScrData"), response.get("sysActionsJson"), "FetchSOT")
            return json.loads(json_util.dumps(response))
    except Exception as e:
        print("Fetch SOT Exception",e)
        return {"Error":"Fetch SOT Failed"},500
    return {"Message":"Sucessfull Execution"}
    ## Data clone reset API ends (fetch original SOT)


@app.route('/generateOutput/',methods=['POST'])
def processValidatedActions():
    previousScrData = request.json['previousScrData']
    validatedActions = request.json['validateActions']
    fileType = request.json['previousScrData']['headers']['fileType']
    userMapping = previousScrData['mappings']
    username = previousScrData['username']
    print("User:", username)
    headerValues = previousScrData['headers']
    flag = previousScrData['ff']
    # print("flag:", flag)
    filename = headerValues['filename']
    print("filename:", filename)
    token = headerValues['token']
    # print("token:", token)
    delegate = headerValues.get('delegateCode',None)
    # print("delegate:", delegate)
    delType=headerValues['delType']
    if fileType=='pdf':
        flag=True
    if fileType == 'nonDelPdf':  # convert filename array to string to be added in dataframe
        filename = ', '.join(filename)
    data = fn.uiDriverFunction(userMapping,token,filename,username,delType,mongo,fs,fileType,provName=delegate,validatedActions=validatedActions,ff=flag)
    return json.dumps(data)

@app.route('/setOutputData/',methods=['POST'])
def setOutputData():
    downloadFlag  = request.json['dwndFlag']
    outputDi = request.json['outputData']
    token = request.json['token']
    logData = constants.handler.find_one_document('reconciliation', {"token": token})
    if(downloadFlag):
        #logData = constants.handler.find_one_document('reconciliation', {"token": token})
        zipFNm = 'outFiles_' + token + '.zip'
        if logData['inputNpiData']:
            data,lstFileObj = fn.exportReviewedData([{k:v} for k,v in outputDi.items()], token, downloadFlag,logData)
        else:
            data, lstFileObj = fn.exportReviewedData([{k: v} for k, v in outputDi.items()], token, downloadFlag,logData)
        zipObj = io.BytesIO()
        with zipfile.ZipFile(zipObj, 'w', compression=zipfile.ZIP_DEFLATED) as zipMe:
            for templtDi,fileObj in zip(data,lstFileObj):
                arcFNm=list(templtDi.values())[0]+'.xlsx'
                zipMe.writestr(arcFNm, fileObj.read())
                fileObj.close()
        zipObj.seek(0)
        fn.saveToGridFS(mongo, zipFNm, zipObj, 'outputZipFile')
        zipObj.close()
        return zipFNm
    else:
        data,lstFileObj = fn.exportReviewedData(outputDi,token, downloadFlag,logData)
        for templtDi, fileObj in zip(data, lstFileObj):
            gfsFNm = list(templtDi.values())[0]
            fn.saveToGridFS(mongo, gfsFNm + '.xlsx', fileObj, 'outputFile')
            fileObj.close()
        return json.dumps(data)

@app.route('/providerOutput/',methods=['POST'])
def getProviderOutput():
    prevData=request.json
    username = request.json['username']
    print("User:",username)
    headerValues = request.json['headers']
    print("headers:",headerValues)
    # sotHeader = data.columns
    data = fn.readFrameWrapper(headerValues['filename'],headerValues['headervalues'],headerValues['token'])
    sotHeader = fn.readPickle(headerValues['token'])
    provMapping = fn.readProvMapping(request.json['providerName'])
    status, sheetsFailedValidation = fn.validateTemplate(sotHeader, provMapping)
    if not status:
        print('not validated')
        return json.dumps(False)
    else:
        validateActions = fn.compileActions(provMapping, headerValues['token'], headerValues['filename'])
        prevData['ff'] = True
        prevData['mappings'] = provMapping
        jsonForActionScreen = {'sysActionsJson': validateActions, 'previousScrData': prevData}
        return json.dumps(jsonForActionScreen)

@app.route('/viewMapping/',methods=['POST'])
def viewMapping():
    sheetMap = request.json
    #print("sheetMap:",sheetMap)
    # data = fn.readProvMapping(providerName)
    formatedData = fn.viewMapping(sheetMap)
    #print("ViewMapping:",formatedData)
    return json.dumps(formatedData)

@app.route('/getDwnldfile/<token>/',methods=['GET'])
def get_file(token):
    if (request.method == 'GET'):
        print("token:",token)
        # outputFile = "Output_"+token
        fileObj=fn.retrieveFileFromMongo(token+'.xlsx',mongo.db,fs,zipflag=1)
        return send_file(fileObj, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', download_name=token+'.xlsx', as_attachment=True)
    else:
        return head(request.method)

@app.route('/getDwnldAllfile/<token>/',methods=['GET'])
def get_zipFile(token):
    if (request.method == 'GET'):
        print("token:",token)
        fileObj=fn.retrieveFileFromMongo(token,mongo.db,fs,zipflag=1)
        return send_file(fileObj,download_name=token, as_attachment=True)
    else:
        return head(request.method)

@app.route('/adminMap/',methods=['POST'])
def getAdminMappings():
    unMapData,userMappingIds,totalCount = fn.adminScreenDriverFunction()
    totalCount = totalCount-len(userMappingIds)
    data = {
        "unMapData":unMapData,
        "userMapIds":userMappingIds,
        "totalCount":totalCount
    }
    print("AdminMap:",unMapData)
    return json.dumps(data)

@app.route('/setAdminMap/',methods=['POST'])
def setAdminMappings():
    data = request.json["adminMap"]
    userMapIds = request.json["userMapIds"]
    print("setAdminMap:",data)
    fn.commitAdminMapping(data,userMapIds)
    return json.dumps("success")

@app.route('/setExceptionData/',methods=['POST'])
def setExceptions():
    data = request.json['data']
    username = request.json['username']
    print("setExceptionData:",data)
    fn.writeUserMappingToDisk(data,username,"SpecialtyException")
    return json.dumps("success")

@app.route('/exceptions/',methods=['POST'])
def getExceptions():
    specData = fn.getCsvToJson("specialty_exceptions")
    userSpecData = fn.adminSpecExceptDriverFunction()
    data = {'specData' : specData,'userSpecData' : userSpecData}
    print("Specialty exceptions:",data)

    return json.dumps(data)

@app.route('/setAdminExceptionData/',methods=['POST'])
def setAdminExceptions():
    data = request.json
    print("setAdminExceptionData:",data)
    cols = ["specialty_name_in_sot","specialty_name_in_taxonomy_grid","specialty_code","degree"]
    fn.getJsonToCsv("specialty_exceptions", data, cols)
    return json.dumps("success")

@app.route('/uploadGridFiles/upload/',methods=['GET','POST'])
def uploadNDB():
    if (request.method == 'GET' or  request.method =='POST'):
        file = request.files['file']
        type = request.form['type']
        print("type selected:",type)
        print('NDBfilename: ' + file.filename)
    else:
        return head(request.method)

@app.route('/svAdminMap/',methods=['POST'])
def getSvAdminMappings():
    data = fn.svAdminMapping()
    print("svAdminMap:",data)
    return json.dumps(data)

@app.route('/setSvAdminMap/',methods=['POST'])
def setSvAdminMappings():
    data = request.json
    print("setSvAdminMap:",data)
    fn.commitSvAdminMapping(data)
    return json.dumps("success")

@app.route('/mvAdminMap/',methods=['POST'])
def getMvAdminMappings():
    data = fn.mvAdminMapping()
    print("mvAdminMap:",data)
    return json.dumps(data)

@app.route('/setMvAdminMap/',methods=['POST'])
def setMvAdminMappings():
    data = request.json
    print("setMvAdminMap:",data)
    fn.commitMvAdminMapping(data)
    return json.dumps("success")

@app.route('/getAdminProvMap/',methods=['POST'])
def getAdminProvMap():
    data = fn.diffProvMapping()
    print("ProvDifferences:",data)
    data = fn.viewDiff(data)
    print("ProviderMap:",data)
    return json.dumps(data)

@app.route('/setProvUserMap/',methods=['POST'])
def setProvUserMap():
    data = request.json
    print("setProvUserMap:",data)
    fn.commitAdminActionsProvMapping(data)
    return json.dumps("success")

@app.route('/getJSON/',methods=['POST'])
def getJSON():
    decryptFile = en.decrypt(request.data, en.key)
    filename = json.loads(decryptFile)['filename']
    data = fn.getFileJSON(filename)
    encryptJson = en.encrypt(json.dumps(data), en.key)
    return encryptJson

@app.route('/addNewUser/',methods=['POST'])
def addNewUser():
    data = en.decrypt(request.data, en.key)
    data = json.loads(data)
    print("new user:",data)
    fn.addCredentials(data)
    return 'success'

@app.route('/setJSON/',methods=['POST'])
def setDataList():
    decyptedData = en.decrypt(request.data, en.key)
    decyptedData = json.loads(decyptedData)
    data = json.loads(decyptedData['data'])
    filename = decyptedData['filename']
    filemapping = filename.split('.json')[0]
    print("data:",data)
    fn.setJSONData(data,filemapping)
    return 'success'

@app.route('/setProvider/',methods=['POST'])
def setProvider():
    data = request.json
    print("data:",data)
    fn.updateProviderStatus(data)
    return 'success'

@app.route('/getCsvToJson/',methods=['POST'])
def getCsvToJson():
    filename = (request.json['filename']).split('.csv')[0]
    filemapping = constants.esConfig["index_input_files"][filename]
    print("filename",filename)
    data = fn.getCsvToJson(filemapping)
    #print("getMasterDeg:",data)
    return json.dumps(data)

@app.route('/getJsonToCsv/',methods=['POST'])
def getJsonToCsv():
    data = request.json['data']
    filename = (request.json['filename']).split('.csv')[0]
    filemapping = constants.esConfig["index_input_files"][filename]
    cols = request.json['cols']
    #print("setMasterDeg:",data)
    print("filename:",filename)
    #print("cols:",cols)
    fn.getJsonToCsv(filemapping,data,cols)
    return json.dumps("success")

########### pdf routes ##############################################################

@app.route('/extractPDF/',methods=['POST'])
def extractPDF():
    startProcessTimestamp = str(datetime.datetime.now())
    filename = request.json['filename']
    fNm = os.path.splitext(filename)[0]
    provider = request.json['delegateCode']
    userName = request.json['username']
    allowed_chars = ''.join((string.ascii_lowercase, string.ascii_uppercase, string.digits))
    token = (''.join(random.choice(allowed_chars) for _ in range(32))) + str(datetime.datetime.now()).replace(':', '-')
    ############# creating log for delegated pdfs######################
    logData = {"filename": [filename],
               "token": token,
               "username": userName,
               "fileType": "pdf",
               "businessType": "delegate",
               "startProcessTimestamp": startProcessTimestamp,
               "processCompleteFlag": False,
               "delegateCode": provider,
               "nTabs":1,
               "tabInfo":[{"tabName":"pdfTab","remapped":False, "originalMatchPercent":0,"tabProcessed":False}]}
    constants.handler.insert_one_document('logging', logData)
    #####################################################
    pdfExistingStatus, allJsons = pdfExt.getAllJsonsMongoDB(provider)
    originalPageDi, modifiedPageDi, modifiedPageTextlineDi, origPagebbox, pagebbox = pdfExt.newExtractXmlPageDi(fNm, token)
    uiAllJsons = pdfExt.jsonBackendToUiWrapper(provider)
    pageDiOut = {'originalPageDi':originalPageDi,
                 'modifiedPageDi':modifiedPageDi,
                 'origPagebbox':origPagebbox,
                 'pagebbox':pagebbox}
    pdfData = {}
    headerData = {}
    if pdfExistingStatus:
        try:
            diDFs = pdfExt.processPdfWrapper(token, allJsons, modifiedPageDi, originalPageDi, pagebbox, origPagebbox)
            df = diDFs.get('pdfTab')
            headerData = {'sheetcount':1, 'tabDict':[{'sheetname':'pdfTab', 'startIndex':1, 'TaxIDColumnName':'TAX_ID'}]}
            dfDataLst = df.values.tolist()
            dfDataLst.insert(0,list(df.columns.values))
            pdfData = {'pdfTab':dfDataLst}
        except:
            traceback.print_exc()
    data = {
        "pdfData": pdfData,
        "headers": headerData,
        'unique_id': token,
        "pageDi" : pageDiOut,
        "uiAllJsons":uiAllJsons
    }
    return jsonify(data)

#pdf extraction routes
@app.route('/openPDFFile/<fileName>',methods=['GET'])
def openPDFFile(fileName):
    print(fileName)
    return send_from_directory('../input/', fileName)

@app.route('/sendPDFInfo/',methods=['POST'])
def getPdfInfo():
    fNm = os.path.splitext(request.json['filename'])[0]
    token = '0' # request.json['token']
    pdfInfoJson = request.json['pdfInfo']
    pageDiData = json.loads(request.json['pageDi'], object_hook=fn.int_keys)
    pdfSections, recordDi, recordToPdfPg, pagebbox = pdfExt.extractPdfInfoWrapper(pageDiData['modifiedPageDi'], pageDiData['pagebbox'], pdfInfoJson)
    data = {'pdfSections': pdfSections, 'recordDi': recordDi, 'recordToPdfPg': recordToPdfPg, 'pagebbox': pagebbox}
    return jsonify(data)

# added to test extraction using single value
# @app.route('/svSampleRun/',methods=['POST'])
# def svSampleRun():
#     singleValueJson = pdfExt.jsonUiToBackend(request.json['singleValue'], 'singleValue')
#     pdfInfoData = json.loads(request.json['pdfInfoData'], object_hook=fn.int_keys)
#     out = pdfExt.sampleRunSV(singleValueJson, pdfInfoData['recordDi'], pdfInfoData['pagebbox'], pdfInfoData['pdfSections'], False)
#     # out = {'DOB':"gdgdh"}
#     print out
#     return json.dumps(out)

#implemented to set margin..
# @app.route('/setMargin/',methods=['POST'])
# def setMargin():
#     pdfInfoJson = request.json['pdfInfoJson']
#     pdfInfoData = json.loads(request.json['pdfInfoData'], object_hook=fn.int_keys)
#     singleValueJson = pdfExt.jsonUiToBackend(request.json['singleValue'], 'singleValue')
#     modifiedData = request.json['modifiedData']
#     expectedVal = {di['key']:di['val'] for di in modifiedData}
#     xMargin, yMargin = pdfExt.setMarginSV(expectedVal, singleValueJson, pdfInfoData['recordDi'], pdfInfoData['pagebbox'], pdfInfoData['pdfSections'])
#     pdfInfoJson['xMargin'] = xMargin
#     pdfInfoJson['yMargin'] = yMargin
#     return json.dumps(pdfInfoJson)


@app.route('/getExistingPDFJson/', methods=['POST'])
def getExistingJson():
    fNm = os.path.splitext(request.json['filename'])[0]
    provider = request.json['delegateCode']
    allowed_chars = ''.join((string.lowercase, string.uppercase, string.digits))
    token = (''.join(random.choice(allowed_chars) for _ in range(32))) + str(datetime.datetime.now()).replace(':', '-')
    data = pdfExt.jsonBackendToUiWrapper(provider)
    #print(data)
    return jsonify(data)

@app.route('/sendAllJson/', methods=['POST'])
def getUiPdfMapping():
    pdfMapping = request.json['pdfMapping']
    data = pdfExt.jsonUiToBackendWrapper(pdfMapping)
    #print(data)
    #delegate code
    mongoDB_document = {}
    mongoDB_document["Provider"] = request.json['headers']['delegateCode']
    mongoDB_document["Type"] = "ProviderPdf"
    mongoDB_document["Doc"] = data
    constants.handler.replace_one_document(mongoDB_document["Type"], mongoDB_document)
    return "success"

# Non DelgetPdfData routes
@app.route('/getPdfData/<filename>',methods=['GET'])
def getPdfData(filename):
    return  send_from_directory('../input/',filename,max_age=0),filename

@app.route('/getHtmlData/<filename>',methods=['POST'])
def getHtmlData(filename):
    startProcessTimestamp = str(datetime.datetime.now())
    # generating token
    userName =  request.data.decode('utf-8')
    filename = filename.split(',')
    allowed_chars = ''.join((string.ascii_lowercase, string.ascii_uppercase, string.digits))
    unique_id = (''.join(random.choice(allowed_chars) for _ in range(32))) + str(datetime.datetime.now()).replace(':','-')
    ################## creating log for non delegated pdfs ####################
    logData = {"filename": filename,
               "token": unique_id,
               "username": userName,
               "fileType": "pdf",
               "businessType": "nonDelegate",
               "startProcessTimestamp": startProcessTimestamp,
               "processCompleteFlag": False,
               "delegateCode": "",
               "nTabs": 1,
               "tabInfo": [{"tabName": "pdfTab", "remapped": False, "originalMatchPercent": 0,"tabProcessed":False}]
               }
    constants.handler.insert_one_document('logging', logData)
    ###############################################################################
    # Api call
    htmlData = {eachFile: fn.callPdfToHtmlApi(unique_id, os.path.splitext(eachFile)[0]) for eachFile in filename}
    data = {
        "htmlData": htmlData,
        "unique_id":unique_id
    }
    return  json.dumps(data)


@app.route('/getNonDelData/',methods=['GET'])
def getNonDelData():
    # getting single value and multi value json for UI display
    df = pd.DataFrame()
    svMapJson = [fn.getSingleValueMap(df)]
    mvMapJson = fn.getMultiValueMap(df,{'sv':{}})
    data ={
        "sampleSvMvMapJson": {"sv": svMapJson, "mv": mvMapJson},
        "records": {"sv": svMapJson, "mv": mvMapJson}
    }
    return json.dumps(data)

@app.route('/getLogData/',methods=['GET'])
def getLogData():
    logDf = fn.createLogDf()
    userData = constants.handler.find_documents('credentials')[0]['credentials']
    users = [val['userName'] for key, val in list(userData.items()) if (val['role'] == 'user')]
    data = {
        "logData" : logDf,
        "users":users
    }
    return json.dumps(data)

@app.route('/getNextMappings/<userMapIds>',methods=['GET'])
def getNextMappings(userMapIds):
    userMapIds = userMapIds.split(',')
    if(len(userMapIds)>0):
        fn.backupOneUserMapping("UserMapping",userMapIds)
    return "success"


if __name__ == "__main__":
    # waitress.serve(app.run(host='0.0.0.0', port=8060, threaded=True,ssl_context = (constants.crtFile,constants.keyFile)))
    waitress.serve(app.run(host='0.0.0.0', port=8059, threaded=True,ssl_context = (constants.crtFile,constants.keyFile)))
    #waitress.serve(app.run(host='0.0.0.0', port=8059, threaded=True))

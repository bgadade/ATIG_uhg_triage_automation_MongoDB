import pandas as pd
import columnTrees as ct
import datetime
import usaddress as ua
from collections import OrderedDict
import re
import elastic
import constants
from dateutil import parser
import calendar
import string
import traceback
import numpy as np
from nltk.metrics import edit_distance
import copy
from classify_request_type import classify
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from dateutil.relativedelta import relativedelta
import json
from nameparser import HumanName
from commonFiles import diImport
# standards = "../config/Standards.json"
# standardsDict = utils.readFile(standards, type="json")

def dropColumns(df,lstCols):
    commonCols=list(set(df.columns).intersection(set(lstCols)))
    df=df.drop(commonCols, axis=1)
    return df

def merge_delimiter_separated_rows(df, argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']

    trans_type = df[inpCol].values.tolist()

    trans_type_rows = trans_type[0][0].split("||")
    t2 = []
    for i in trans_type_rows:
        t2.extend(i.split(','))

    t2 = [ele.strip() for ele in t2]
    transaction_types = list(set(t2))

    df[outCol] = ', '.join([i for i in transaction_types if i != ''])

    return df


def handle_multiple_row_transaction(df, argDi):

    df = df.fillna('')
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']

    if len(df[inpCol]) > 1:
        new_val = df[inpCol].values.tolist()

        new_val = [', '.join(j) for j in new_val if j != []]

        new_val = ' || '.join(new_val)
    else:
        new_val = df[inpCol]
        # print new_val
    df[outCol] = new_val

    return df


def wrapper_classify(df, argDi):

    df = df.fillna('')
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']

    # print df[column], len(column)
    df[outCol] = df[inpCol].map(classify)
    return df

def wrapper_transaction_type(df, argDi):

    if argDi['prec'] == 'True':
        df = wrapper_transaction_type_precedence(df, argDi)
        return df

    df = df.fillna('')
    out_file = argDi['inputCol'][0]
    out_tab = argDi['inputCol'][1]
    out_sent = argDi['inputCol'][2]
    outCol = argDi['outputCol']


    df[outCol] = df[out_file].apply(lambda x: str(x).split(",")) + \
                 df[out_tab].apply(lambda x: str(x).split(",")) + \
                 df[out_sent].apply(lambda x: str(x).split(","))

    df[outCol] = df[outCol].apply(lambda x : ', '.join([i for i in list(set(x)) if i != '']))
    # lambda x: ', '.join(list(set(x))) if x else "")

    return df


def wrapper_transaction_type_precedence(df, argDi):
    df = df.fillna('')
    out_file = argDi['inputCol'][0]
    out_tab = argDi['inputCol'][1]
    out_sent = argDi['inputCol'][2]
    outCol = argDi['outputCol']

    for i, row in df.iterrows():
        # print row[out_sent]
        if row[out_sent]:
            df.loc[i, outCol] = row[out_sent]
        elif row[out_tab]:
            df.loc[i, outCol] = row[out_tab]
        else:
            df.loc[i, outCol] = row[out_file]

    return df

def mapMVGeneralColumns(inputColumns, mvMappingJson,parentCategory,mappedInpCols):
    mvMappedOPColsDict = {}  # renameColumns expects dictionary
    missingColumns = []  # missing columns in the input file after checking with columns.json
    identifiedColumns = []
    mvMappedOPCols = []
    mvMappedOPColsList = []
    addset = []
    count = 0
    flag = False
    filterdMappingJson={key:value for key,value in list(mvMappingJson.items()) if value['PARENT_CATEGORY'] == parentCategory and value['DATAFRAME_TYPE'] == "Normalize"}
    asIsMappingJson={key:value for key,value in list(mvMappingJson.items()) if value['PARENT_CATEGORY'] == parentCategory and value['DATAFRAME_TYPE'] == "AsIs"}
    tag = ''
    for col in inputColumns:
        if col in mappedInpCols:
            continue
        break_innermost=0

        for key, value in list(filterdMappingJson.items()):
            for i in value["Column_Type"]:
                if col in [item.lower() for item in i["Input_Column"]]:
                    if filterdMappingJson[key]["flag"] == 1 and len(addset) > 0:
                        count = count + 1
                        mvMappedOPColsList.append(addset)
                        tag = i["Tag"]
                        flag = True
                        addset = []

                    if tag == '':
                        tag = i["Tag"]

                    ostr = tag + '@' + key + '@' + str(count)
                    if key not in [item.split('@')[1] for item in addset]:
                        mvMappedOPCols.append(key)
                        mvMappedOPColsDict[col.lower()] = ostr
                        # identifiedColumns.append(i["Tag"] + '@' + key + '@' + str(count))
                        addset.append(ostr)
                        flag = False
                        # loc = inputDF.columns.get_loc(col)
                        break_innermost=1
                        break
            if break_innermost:
                break
        for k , v in list(asIsMappingJson.items()):
            if col in [item for item in v["Input_Column"] if item not in list(mvMappedOPColsDict.keys())]:
                mvMappedOPCols.append(k)
                mvMappedOPColsDict[col.lower()] = k
    mvUnmappedOPCols=[col for col in list(filterdMappingJson.keys()) if col not in mvMappedOPCols]
    mvUnmappedInpCols=[col for col in inputColumns  if col not in list(mvMappedOPColsDict.keys())]
    print('mvMappedOPColsDict:',mvMappedOPColsDict)

    if not flag and len(addset) > 0:
        mvMappedOPColsList.append(addset)

    return mvMappedOPColsDict,mvMappedOPColsList,mvUnmappedOPCols,mvUnmappedInpCols


def mapMVUnorderedColumns(inputColumns, mvMappingJson,parentCategory, mappSotCols):
    mvMappedOPColsDict = {}  # renameColumns expects dictionary
    mvMappedOPCols = []
    filterdMappingJson={key:value for key,value in list(mvMappingJson.items()) if value['PARENT_CATEGORY'] == parentCategory and value['DATAFRAME_TYPE'] == "Normalize"}
    asIsMappingJson={key:value for key,value in list(mvMappingJson.items()) if value['PARENT_CATEGORY'] == parentCategory and value['DATAFRAME_TYPE'] == "AsIs"}
    mapDi={}
    for col in inputColumns:
        if col in mappSotCols:
            continue
        for key, value in list(filterdMappingJson.items()):
            for i in value["Column_Type"]:
                if col in [item.lower() for item in i["Input_Column"]]:
                    diKey = key
                    if diKey not in mapDi:
                        mapDi.update({diKey:[]})
                    mapDi[diKey].append((i["Tag"], col))
                    break
            else:
                continue
            break


        for k , v in list(asIsMappingJson.items()):
            if col in [item for item in v["Input_Column"] if item not in list(mvMappedOPColsDict.keys())]:
                mvMappedOPCols.append(k)
                mvMappedOPColsDict[col.lower()] = k
    print(mapDi)
    setDi = {}
    for k, v in list(mapDi.items()):
        for idx, item in enumerate(v):
            map = item[0] + '@' + k + '@' + str(idx)
            mvMappedOPColsDict[item[1]] = map
            if idx not in setDi:
                setDi.update({idx: []})
            setDi[idx].append(map)
    print(setDi)
    mvMappedOPColsList = [v for k,v in sorted(setDi.items())]
    print('mvMappedOPColsList:',mvMappedOPColsList)
    mvMappedOPCols=[key for key in list(mapDi.keys())]
    mvUnmappedOPCols=[col for col in list(filterdMappingJson.keys()) if col not in mvMappedOPCols]
    mvUnmappedInpCols=[col for col in inputColumns  if col not in list(mvMappedOPColsDict.keys())]
    print('mvMappedOPColsDict:',mvMappedOPColsDict)

    return mvMappedOPColsDict,mvMappedOPColsList,mvUnmappedOPCols,mvUnmappedInpCols

def mvAddressNormalization(frame,mvAddressSets,parentCategory,lstAsIs,mvMappings):
    fdf = pd.DataFrame()
    # drvColList =['DERIVED_'+ lval for lval in lstAsIs]
    drvColList =['DERIVED_ADDRESS_INDICATOR','DERIVED_ADDRESS_TYPE']
    # drvTypeCol='DERIVED_'+parentCategory.upper()+'_TYPE'
    # indicatorCol = parentCategory.upper() + '_INDICATOR'
    # TypeCol = parentCategory.upper() + '_TYPE'
    for address_set in mvAddressSets:
        # tdf = frame[address_set].drop_duplicates()
        # tdf = frame[['ADDRESS_TYPE','ADDRESS_INDICATOR']+address_set]
        tdf = frame[address_set]
        tdf = tdf.replace(r'^\s+|\s+$', '', regex=True).replace('', np.nan)
        tdf = tdf.replace(r'\s\s+', ' ', regex=True).replace('', np.nan)
        sbst = [col for col in frame[address_set].columns if
                'ADDRESS_LINE_1' in col or 'ADDRESS_LINE_2' in col or 'ADDRESS_STATE' in col or 'ADDRESS_CITY' in col or 'ADDRESS_ZIP' in col]
        tdf = tdf.dropna(subset=sbst, how='all')

        # print tdf
        for index, value in tdf.iterrows():
            # tdict = {}
            tdict = {key: None for key, val in list(mvMappings.items()) if val['PARENT_CATEGORY'] == parentCategory}
            for col in tdf.columns:  # Todo: include include all columns in output.
                # if col in lstAsIs:
                #     tdict[col]=value[col]
                #     continue
                # print value[col]
                x = col.split('@')
                tdict['order']=x[-1]
                # print x[1]
                x1=x[0].split('_')
                if len(x1)==2 and len(drvColList) == 2:
                    tdict[drvColList[0]] = x1[0]
                    tdict[drvColList[1]] = x1[1]
                elif len(x[0].split('_'))==1 and len(drvColList) == 2:
                    tdict[drvColList[0]] = ''
                    tdict[drvColList[1]] = x1[0]

                tdict[x[1]] = value[col]
                # print x
            fdf = fdf._append(tdict, ignore_index=True)
            #            for k in tdict.keys():
            #                fdf.loc[index,k] = tdict[k]

            #    print fdf
    # if TypeCol in fdf.columns:
    try:
        fdf = fdf.drop_duplicates()
    except:
        tdict = {key: None for key, val in list(mvMappings.items()) if val['PARENT_CATEGORY'] == parentCategory}
        tdict.update({item: None for item in drvColList+['order']})
        fdf = fdf._append(tdict, ignore_index=True)
        pass
    fdf = fdf.reset_index()
    return fdf

def mvGeneralNormalization(frame,mvAddressSets,parentCategory,lstAsIs,mvMappings):
    fdf = pd.DataFrame()
    # drvColList =['DERIVED_'+ lval for lval in lstAsIs]
    drvColList = ['DERIVED_'+parentCategory.upper()+'_INDICATOR']
    # drvTypeCol = 'DERIVED_' + parentCategory.upper() + '_TYPE'
    # indicatorCol = parentCategory.upper() + '_INDICATOR'
    # TypeCol = parentCategory.upper() + '_TYPE'
    for address_set in mvAddressSets:
        # tdf = frame[address_set].drop_duplicates()
        tdf=frame[address_set]
        tdf = tdf.replace(r'^\s+|\s+$', '', regex=True).replace('', np.nan)
        tdf = tdf.dropna(how='all')

        # print tdf
        for index, value in tdf.iterrows():
            tdict = {key: None for key, val in list(mvMappings.items()) if val['PARENT_CATEGORY'] == parentCategory}
            for col in tdf.columns:  # Todo: include include all columns in output.
                # if col in lstAsIs:
                #     tdict[col]=value[col]
                #     continue
                # print value[col]
                x = col.split('@')
                tdict[parentCategory+'_order'] = x[-1]
                # print x[1]
                if len(drvColList) >= 1:
                    tdict[drvColList[0]] = x[0]
                tdict[x[1]] = value[col]
                # print x
            fdf = fdf._append(tdict, ignore_index=True)
            #            for k in tdict.keys():
            #                fdf.loc[index,k] = tdict[k]

            #    print fdf
    try:
        fdf = fdf.drop_duplicates()
    except:
        tdict = {key: None for key, val in list(mvMappings.items()) if val['PARENT_CATEGORY'] == parentCategory}
        tdict.update({item:None for item in drvColList+[parentCategory+'_order']})
        fdf=fdf._append(tdict, ignore_index=True)
        pass
    #todo: logic for updating original address_type and address_indicator column based on the values of the derived one
    fdf = fdf.reset_index()
    return fdf

def combineMVMapping(dictMVMappings):
    mvMappedOPColsDict = {}
    mvMappedOPColsList = []
    mvUnmappedOPCols = []
    mvUnmappedInpCols = []

    for key, lst in list(dictMVMappings.items()):
        mvMappedOPColsDict.update(lst[0])
        mvMappedOPColsList += lst[1]
        mvUnmappedOPCols += lst[2]
        mvUnmappedInpCols += lst[3]
    mvUnmappedOPCols=list(set(mvUnmappedOPCols))
    mvUnmappedInpCols=[col for col in list(set(mvUnmappedInpCols)) if col not in list(mvMappedOPColsDict.keys())]

    return mvMappedOPColsDict,mvMappedOPColsList,mvUnmappedOPCols,mvUnmappedInpCols

def mvMapColumns(inputColumns,mvMappingJson,functionMappings):
    dictMVMappings={}
    mappedSotCols = []
    for parentCategory in list(functionMappings.keys()):
        # if parentCategory != 'Address':
        #     continue
        pCatMapping={parentCategory: list(
            eval(functionMappings[parentCategory]['mapping'])(inputColumns, mvMappingJson, parentCategory,mappedSotCols))}
        dictMVMappings.update(pCatMapping)
        mappedSotCols = mappedSotCols + list(pCatMapping[parentCategory][0].keys())
    # dictMVMappings = adoptOrphanSets(dictMVMappings,'Address',1)
    diOrphanColumns={'Address':[('DIR_IND',),('ELECTRONIC_COMM_1', 'ELECTRONIC_COMM_2'),('AGE_LIMIT', 'MIN_AGE', 'MAX_AGE'),('WORKING_DAYS','WORKING_HOURS','BREAK_HOURS'),('MONDAY_START_TIME', 'MONDAY_END_TIME', 'TUESDAY_START_TIME', 'TUESDAY_END_TIME', 'WEDNESDAY_START_TIME', 'WEDNESDAY_END_TIME', 'THURSDAY_START_TIME', 'THURSDAY_END_TIME', 'FRIDAY_START_TIME', 'FRIDAY_END_TIME', 'SATURDAY_START_TIME', 'SATURDAY_END_TIME', 'SUNDAY_START_TIME', 'SUNDAY_END_TIME')]}
    dictMVMappings = adoptOrphanColumns(dictMVMappings,'Address',diOrphanColumns['Address'])
    return dictMVMappings

def concatdictNdf(dictNdf):
    mvFrames = []
    for key, value in dictNdf.items():
        mvFrames.append(value)
    concatedMVFrames = pd.concat(mvFrames, axis=1)
    return concatedMVFrames

def getFinalDegreeDF(dictNdf):
    degreeDF = dictNdf["Degree"]
    credDF = dictNdf["Credential"]
    titleDF = dictNdf["Title"]

    if not degreeDF.empty and "DEGREE" in degreeDF.columns and not degreeDF["DEGREE"].isnull().all():
        dictNdf["FinalDegree"] = degreeDF
        return dictNdf
    elif not titleDF.empty and "TITLE" in titleDF.columns and not titleDF["TITLE"].isnull().all():
        dictNdf["FinalDegree"] = titleDF.rename(columns={'TITLE': 'DEGREE', 'PRIMARY_TITLE_IND': 'DEGREE_INDICATOR', 'DERIVED_TITLE_INDICATOR': 'DERIVED_DEGREE_INDICATOR','Title_order':'Degree_order'})
        dictNdf["FinalDegree"]["ROW_NUM"] = titleDF["ROW_NUM"]
        dictNdf["FinalDegree"]["ROW_COUNT"] = titleDF["ROW_COUNT"]
        return dictNdf
    elif not credDF.empty and "CREDENTIAL" in credDF.columns and not credDF["CREDENTIAL"].isnull().all():
        dictNdf["FinalDegree"] =  credDF.rename(columns={'CREDENTIAL': 'DEGREE', 'PRIMARY_CREDENTIAL_IND': 'DEGREE_INDICATOR', 'DERIVED_CREDENTIAL_INDICATOR': 'DERIVED_DEGREE_INDICATOR','Credential_order':'Degree_order'})
        dictNdf["FinalDegree"]["ROW_NUM"] = credDF["ROW_NUM"]
        dictNdf["FinalDegree"]["ROW_COUNT"] = credDF["ROW_COUNT"]
        return dictNdf
    else:
        dictNdf["FinalDegree"] = pd.DataFrame(data={"DEGREE":[''], "DEGREE_INDICATOR":[''], "DERIVED_DEGREE_INDICATOR": [''], "ROW_NUM":[0], "ROW_COUNT":[1],'Degree_order':[0]}).reset_index()
        return dictNdf

def mvNormalizeColumns(splittedFrame,functionMappings,dictMVMappings,mvMappings):
    # normalizedMVFrames=[]
    # tupple = splittedFrame
        # print type(tupple),len(tupple)
    dictNdf={}
    for parentCategory in list(functionMappings.keys()):
        # if parentCategory!='Address':
        #     continue
        lstAsIs = [(val["order"], key) for key, val in list(mvMappings.items()) if
                   val['PARENT_CATEGORY'] == parentCategory and val['DATAFRAME_TYPE'] == 'AsIs' and key in
                   list(dictMVMappings[parentCategory][0].values())]
        lstAsIs.sort()
        lstAsIs = [l[1] for l in lstAsIs]

        ndf = eval(functionMappings[parentCategory]['normalization'])(splittedFrame, dictMVMappings[parentCategory][1],parentCategory, lstAsIs,mvMappings)
        ndf["ROW_NUM"] = ndf.index
        ndf["ROW_COUNT"] = len(ndf.index)
        dictNdf.update({parentCategory: ndf})
        # normalizedMVFrames.append(dictNdf)
        # dictNdf = concatdictNdf(dictNdf)

    dictNdf = getFinalDegreeDF(dictNdf)
    return dictNdf

def var_ParseAddress(row, concatAdd):
    row = row.fillna('')
    AddJson = eval(row[concatAdd])
    zipCode = AddJson["zip_code"]
    # if AddJson["zip_code"]:
    #     if "-" in zipCode:
    #         zipCode = zipCode.split("-")[0]
    #     if len(zipCode) > 5:
    #         zipCode = zipCode[:5]
    #     else:
    #         zipCode = zipCode.rjust(5,'0')#(5 - len(zipCode)) * '0' + zipCode


    AddressString = AddJson.get('address1', '') + "," + AddJson.get('address2', '') + "," + AddJson.get('city', '') + "," + AddJson.get('state', '') + "," + zipCode
    tagMapping = {
        'Recipient': 'address1', 'AddressNumber': 'address1', 'AddressNumberPrefix': 'address1', 'AddressNumberSuffix': 'address1', 'StreetName': 'address1', 'StreetNamePreDirectional': 'address1', 'StreetNamePreModifier': 'address1',
        'StreetNamePreType': 'address1',
        'StreetNamePostDirectional': 'address1',
        'StreetNamePostModifier': 'address1',
        'StreetNamePostType': 'address1',
        'CornerOf': 'address1',
        'IntersectionSeparator': 'address1',
        'LandmarkName': 'address1',
        'USPSBoxGroupID': 'address1',
        'USPSBoxGroupType': 'address1',
        'USPSBoxID': 'address1',
        'USPSBoxType': 'address1',
        'BuildingName': 'address2',
        'OccupancyType': 'address2',
        'OccupancyIdentifier': 'address2',
        'SubaddressIdentifier': 'address2',
        'SubaddressType': 'address2',
        'PlaceName': 'city',
       'StateName': 'state',
        'ZipCode': 'zip_code',
    }
    # AddressString = AddressString.translate(None, string.punctuation)
    AddressString = re.sub(' +', ' ', AddressString)
    AddDict = {'address1':'', 'address2':'',  'city':'', 'state':'', 'zip_code':''}
    try:
        OrderDict = ua.tag(AddressString, tag_mapping = tagMapping)
    except Exception as e:
        AddJson["address1"] = ErrorMessage([], ["E", "Unparsed Address", AddJson["address1"]])
        return AddJson["address1"] + ";" + AddJson["address2"] + ";" + AddJson["city"] + ";" + AddJson["state"] + ";" + zipCode

    for val in OrderDict:
        if type(val) is OrderedDict:
            for key in val:
                if key != 'recipient':
                    AddDict[key] = val[key]

    # zip = AddDict["zip_code"]
    if AddDict["zip_code"]:
        zipCode = AddDict["zip_code"]

    result = AddDict["address1"] + ";" + AddDict["address2"] + ";" + AddDict["city"] + ";" + AddDict["state"] + ";" + zipCode
    return result

####################################################### multivalue columns derivation #############################################

def var_isInCol(row, col):
    if row[col] and not row[col].isspace():
        return "Y"
    else:
        return "N"

def var_isInSot(row, degCol):
    return var_isInCol(row, degCol)

def var_getHospitalName(row, hospital_name):
    return row[hospital_name]

def var_isInNdbGrid(row, NdbDegCol):
    return var_isInCol(row, NdbDegCol)

def cleanDF(df, argDi):
    col = argDi['inputCol']
    # outputCol = argDi['outputCol']
    blankVallst = argDi['standardsKeys']
    blankVallst = constants.standardsDict[blankVallst[0]]

    # deaSeries = df[deaCol]
    for char in blankVallst:
        df[col] = df[col].fillna('').str.lower().replace(char, "")
    return df

def cleanDeaDF(df, argDi):
    return cleanDF(df, argDi)

def cleanSVDF(df, argDi):
    return cleanDF(df, argDi)

def cleanCdsCsr(df, argDi):
    return cleanAddressDir(df, argDi)

def cleanBoardDF(df, argDi):
    # argDi['inputCol'] = argDi['inputCol'][0]
        return cleanAddressDir(df, argDi)

def cleanHospDF(df, argDi):
    # argDi['inputCol'] = argDi['inputCol'][0]
        return cleanAddressDir(df, argDi)

def cleanNewPatients(df, argDi):
    # argDi['inputCol'] = argDi['inputCol'][0]
    if argDi['val']:
        return cleanAddressDir(df, argDi)

def cleanNameField(df, argDi):
    df = df.fillna('')
    col_lname = argDi['inputCol']
    lastName = argDi['col1']
    firstName = argDi['col2']
    middleName = argDi['col3']
    fnameMnameMap=argDi['fnameMnameMap']

    splitComma = df[col_lname][0].split(",")
    splitSpace = df[col_lname][0].split()

    if len(splitComma) > 1 and not df[fnameMnameMap][0]:
        if len(splitComma) >= 3:
            df[lastName][0] = splitComma[0]
            df[firstName][0] = splitComma[1]
            df[middleName][0] = splitComma[2]
        else:
            df[lastName][0] = splitComma[0]
            df[firstName][0] = splitComma[1]
    elif len(splitSpace) > 1 and not df[fnameMnameMap][0]:
        if len(splitSpace) >= 3:
            df[lastName][0] = splitSpace[2]
            df[firstName][0] = splitSpace[0]
            df[middleName][0] = splitSpace[1]
        else:
            df[lastName][0] = splitSpace[1]
            df[firstName][0] = splitSpace[0]

    return df

def var_finalGender(df, cleanGender, npiGender, clarify ):
    if (df[cleanGender].upper() == "M" or df[cleanGender].upper() == "F"):
        return df[cleanGender]
    elif(df[cleanGender].upper() != "M" and df[cleanGender].upper() != "F"):
        if df[npiGender] == '' or str(df[npiGender]).isspace() or df[npiGender] is None:
            return ErrorMessage([], ["c", "whether the gender is correct or not", df[cleanGender] ])
        else:
            return df[npiGender]


def cleanProvType(df, argDi):
    df = df.fillna('')
    col = argDi['inputCol']
    val1 = argDi['val1']
    val2 = argDi['val2']
    val3 = argDi['val3']
    val4 = argDi['val4']
    pcpProvCase = argDi['standardsKeys']
    pcpProvType = constants.standardsDict[pcpProvCase[0]]
    pcpProvSpecialCase = constants.standardsDict[pcpProvCase[1]]
    provHosp = constants.standardsDict[pcpProvCase[2]]

    for char in provHosp:
        if df[col][0] and df[col][0].lower() == char:  # = df[col].fillna('').str.lower().replace(char, val1)
            df[col][0] = val4
            return df
    # deaSeries = df[deaCol]
    for char in pcpProvType:
        if df[col][0] and df[col][0].lower() == char: # = df[col].fillna('').str.lower().replace(char, val1)
            df[col][0] = val1
            return df

    for char in pcpProvSpecialCase:
        if df[col][0] and df[col][0].lower() == char: # = df[col].fillna('').str.lower().replace(char, val1)
            df[col][0] = val3
            return df

    if df[col][0]:
        df[col][0] = val2

    return df

def cleanAddressDir(df, argDi):
    deaCol = argDi['inputCol']
    val = argDi['val']
    # outputCol = argDi['outputCol']
    dir_y_n = argDi['standardsKeys']
    dir_y_n =constants.standardsDict[dir_y_n[0]]

    # deaSeries = df[deaCol]
    for char in dir_y_n:
        df[deaCol] = df[deaCol].fillna('').str.lower().replace(char.lower(), val)
    return df

def cleanChangeType(df, argDi):
    changeCol = argDi['inputCol']
    val = argDi['val']
    outputCol = argDi['outputCol']
    standKey = argDi['standardsKeys']
    changeType =constants.standardsDict[standKey[0]]

    for char in changeType:
        df[outputCol] = df[changeCol].fillna('').str.lower().replace(char.lower(), val)
    return df

def var_dirInd(row, dirIndCol, addTypeCol):
    if row[addTypeCol] and (row[addTypeCol].lower() == 'plsv' or row[addTypeCol].lower() == 'combo') and not row[dirIndCol]:
        return "y"
    elif row[addTypeCol].lower() == 'bill':
        return ""
    else:
        return row[dirIndCol]

def var_dirIndForSpec(row, specInd):
    if row[specInd]:
        return row[specInd]
    else:
        return "Y"

def taxIdClean(row, taxIdColName):
    return ''.join(re.findall(r'\b\d+\b', row[taxIdColName].strip()))

def degreePunctuationClean(df,argDi):
    # DegValue = row[degreeColName]
    # replace_punctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
    # return str(DegValue).translate(replace_punctuation).strip()
    degreeColName=argDi["inputCol"][0]
    outputCol=argDi["outputCol"]
    for idx,row in df.iterrows():
        if row[degreeColName]:
            astr = row[degreeColName].strip()
            df.loc[idx,outputCol]=astr.replace('.','').split("-")[0]
        else:
            df.loc[idx, outputCol]=""
    return df

def var_isMasterMidLevelDegreeLookup(df, argDi):
    degree = argDi["inputCol"][0]
    outputCol = argDi["outputCol"]
    # degree = row[degree].upper()
    for idx, row in df.iterrows():
        degCol = row[degree].upper()
        bestMatch = elastic.gridLookup('master_degree', degCol, 0, {}, **{'match_fields': ['degree']})
        if not bestMatch:
            bestMatchAlt = elastic.gridLookup('master_degree', degCol, 1, {}, **{'match_fields': ['degree']})
            if bestMatchAlt:
                if (len(bestMatchAlt['degree']) == len(degCol) and bestMatchAlt['degree'].upper() == degCol) or (len(bestMatchAlt['degree']) != len(degCol) and (bestMatchAlt['degree'].upper() in degCol.upper())):
                    bestMatch = bestMatchAlt

        #Todo: lookup for isMasterDegree and isMidlevel.
        if bestMatch:
            df.loc[idx, outputCol] = bestMatch["mid_level_indicator"] + "|" + bestMatch["master_list_indicator"] + "|" + bestMatch["degree"]
        else:
            df.loc[idx, outputCol] = ""
    return df

def var_OverwriteDeg(df, argDi):
    cleanDeg = argDi["inputCol"][0]
    lkpdegree = argDi["inputCol"][1]
    outputCol = argDi["outputCol"]
    for idx, row in df.iterrows():
        if row[lkpdegree]:
            val = row[lkpdegree].split("|")[2]
            if row[cleanDeg] != val:
                df.loc[idx, outputCol] = val
    return df

def var_getProviderAdd(row, svdf, provColName):
    return svdf[provColName][0]

def var_getLanguageJson(row, langLst):
    #This function can take more languages than 6 because of the code below. However any other parameter -
    #-other than language may cause conflict
    #LangParam = [i for i in list(var_getLanguageJson.__code__.co_varnames[:var_getLanguageJson.__code__.co_argcount]) if not isinstance(eval(i), pd.core.series.Series)]
    LangValue = " ".join([row[x] for x in langLst])
    replace_punctuation = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    text = str(LangValue).translate(replace_punctuation).strip()
    allLang = text.split()
    rtrnDict = {}
    count = 1
    for l in allLang:
        if len(l) <= 3:
            bestMatch = elastic.gridLookup('language_code', l, 0, **{'match_fields': ["code"]})
        else:
            bestMatch = elastic.gridLookup('language_code', l, 1, **{'match_fields': ["language_description"]})
        if bestMatch:
            if bestMatch["code"] != 'ENG':
                rtrnDict[count] = bestMatch["code"]
            else:
                continue
        else:
            rtrnDict[count] = ErrorMessage([], ["E", "Unable to map to a Lang Code", l])
        count = count + 1
    return str(rtrnDict)


def var_getLangValue(row, landictColName, langNumber):
    langDict = eval(row[landictColName])
    return langDict.get(langNumber,"")


def var_getLangSpokenWrittenValue(row, langInpColName, langColName, provColName):
    if row[langColName] and row[langColName] != "nan" and row[langColName] != "" and not row[langColName].isspace():
        if row[langColName] == row[provColName]:
            return "B"
        if row[langInpColName] and row[langInpColName] != "nan":
            return row[langInpColName]
        else:
            return "S"
    else:
        return ""

def var_cosmosNum(row, specDF, midLevSup, primcosprovCol, supercosprovCol, isMidLevel, midLevSupSpec):
    if row[isMidLevel] == "Y" and not (row[midLevSupSpec]):
        return ""

    primSpecDF = pd.DataFrame()
    if row[midLevSup]:
        return row[supercosprovCol].rjust(4, "0") #XXXX
        # primSpecDF = specDF[specDF["PRIMARY_SPECIALITY_IND"] == "SUPERVISING"]
    else:
        return row[primcosprovCol].rjust(4, "0")
        # primSpecDF = specDF[specDF["PRIMARY_SPECIALITY_IND"] == "P"]

    # if not primSpecDF.empty:
    #     bestMatch = elastic.gridLookup('ndb_taxonomy', primSpecDF["SPECIALITY"], 1, **{'match_fields': ["prov_type_name"]})
    #     if bestMatch is not None:
    #         return bestMatch["cos_cred"]

def var_clnName(row, lastName, firstName, middleName, primDegree, nameSuffix, pcpInd):
    if row[pcpInd] == "PCP":
        primDeg = row[primDegree]
        middleName = row[middleName]
        if middleName:
            middleName=middleName[0]
        else:
            middleName = ""

        if primDeg :
            ans= row[firstName] + " " + middleName + " " + row[lastName] + " " + row[nameSuffix] + " " + primDeg
        else:
            ans = row[firstName] + " " + middleName + " " + row[lastName] + " " + row[nameSuffix]
        ans = re.sub('[^a-zA-Z0-9 \n\.]', ' ', ans)
        ans = re.sub(r' $', '',re.sub(r' +', ' ', ans))
        return ans
    else:
        return ""
        return ""

def var_isMidLevel(row, specisMidlvl, degisMidLvl):#specDF, midlevNdBSpec, ndbSpecCol):
    # primDegDf = degDF[degDF["var_primaryDegreeInd"] == "P"]
    # if not primDegDf.empty:
    #     primDeg = primDegDf["DEGREE"][0]
    #     bestMatch = elastic.gridLookup('ndb_taxonomy', primDeg, 1, **{'match_fields': ["ndb_deg"]})
    #     if bestMatch is not None:
    if row[specisMidlvl] == 'Y' or row[degisMidLvl] == 'Y':
        return "Y"
    else:
        return "N"
    # if not specDF.empty:
    #     try:
    #         if int(row[ndbSpecCol]) in midlevNdBSpec:
    #             return "Y"
    #         else:
    #             return "N"
    #     except ValueError:
    #         return "N"
    # else:
    #     return "N"


def var_isMidLevelDegAndSpec(row, specisMidlvl, degisMidLvl):
    if (row[specisMidlvl] == 'Y' and row[degisMidLvl] != 'Y'):
        return "N"
    else:
        return "Y"

def var_getFinalDegree(row, degreeCredTitle, isMidLevel):
    if row[degreeCredTitle]:
        if row[isMidLevel] == 'Y':
            return row[degreeCredTitle]
        else:
            return ErrorMessage([], ["c", "Degree is not midLevel", row[degreeCredTitle]])


def var_getPrimarySpecialty(row, specdf, specprimCol):
    result = specdf[specdf[specprimCol] == 'P']
    if not result.empty:
        return result["SPECIALITY"][0]
    else:
        return ""

def var_getSupervisorSpecialty(row, specdf, specprimCol):
    result = specdf[specdf[specprimCol] == 'SUPER']
    if not result.empty:
        return result["SPECIALITY"][0]
    else:
        return ""

def var_midLevelSupervisorSpeciality(row, isMidLevel, suprSplCol):
    if row[isMidLevel] == "Y":
        if row[suprSplCol]:
            return row[suprSplCol].rjust(3, "0")
        else:
            return ErrorMessage([], ["c", "Provider is midlevel and no supervising spec", row[suprSplCol] ])
    else:
        return ""

def var_midLevelSupervisorSpecialityName(row, isMidLevel, suprSplCol):
    if row[isMidLevel] == "Y":
        if row[suprSplCol]:
            return row[suprSplCol]
        else:
            return ErrorMessage([], ["c", "Provider is midlevel and no supervising spec", row[suprSplCol]])
    else:
        return ""

def var_midLevelSupervisorPhysician(row, midLevelSpecialty, supervisingPhysician, isMidLevel):
    if (row[isMidLevel] == "Y"):
        if row[supervisingPhysician]:
            return row[supervisingPhysician]
        else:
            if row[midLevelSpecialty]:
                return ""
            else:
                return ErrorMessage([], ["C", "Missing Value" + "", ""])
    else:
        return ""

def var_otherDFDetails(row, df, specprimCol, fltr, detailColName):
    if not df.empty:
        result = df[df[specprimCol] == fltr]
        if not result.empty:
            return result[str(detailColName)].iloc[0]
        else:
            return ""

def var_getSpecDetails(row, specdf, specprimCol, fltr, detailColName):
    return var_otherDFDetails(row, specdf, specprimCol, fltr, detailColName)


def var_NDBForPCPCalculation(row, superNdbSpecCode, primNdbSpecCode):
    if row[superNdbSpecCode]:
        return row[superNdbSpecCode] + ";S"
    else:
        return row[primNdbSpecCode] + ";P"

def placeHolder(row):
    return ""

def Type(row):
    return "T"

def var_getFinalPcp(row, derivedPcpInd, provType, specCodeForPcp):
    # ["'PCP_IND'", "'PROVIDER_TYPE'", "'var_isMidLevel'", "'OUT_MID_LEVEL_SUPERVISOR_SPECIALITY'"]
    specCode, specInd = row[specCodeForPcp].split(";")
    pcpInd = row[derivedPcpInd]
    provType = row[provType]
    if specCode and specCode in ["250", "251", "230"]:
        return provType

    if specInd == "S" and not specCode.isdigit():
        return ErrorMessage([], ["c", "Confirm PCP indicator - Supervising Specialty Missing", provType])
    # if row[isMidLevel] == "Y":
    #     spec = row[midLevSpec]
    #     bestMatch = elastic.gridLookup('ndb_taxonomy', spec, 1, {},**{'match_fields': ['prov_type_name'], 'operator': 'and'})
    #     if bestMatch and bestMatch["specialty_name"]:
    #         if bestMatch["ndb_spec"] in ["250", "251"]:
    #             return provType
    #         return "PCP"
    #     else:
    #         return "SPEC"
    # pcpInd = row[derivedPcpInd]
    # provType = row[provType]
    if pcpInd == provType:
        return pcpInd
    elif provType == 'SPECIAL_CASE':
        if pcpInd == "PCP":
            return ErrorMessage([], ["c", "Confirm PCP indicator", provType])
        else:
            return pcpInd
    elif pcpInd == "SPEC" and provType == "PCP":
        return ErrorMessage([], ["c", "Confirm PCP indicator", provType ])
    elif pcpInd == "PCP" and provType == "SPEC":
        return provType
    elif not provType and pcpInd:
        # if pcpInd == "SPEC":
        return pcpInd
        # return ErrorMessage([], ["c", "sot doesnt have pcp ind, ", "derived pcp ind->" + pcpInd])
    else:
        return pcpInd

def var_getDegDetails(row, degdf, degprimCol, fltr, detailColName):
    return var_otherDFDetails(row, degdf, degprimCol, fltr, detailColName)

def var_getAddDetails(row, df, addIndCol, fltr1, addTypeCol, fltr2, fltr3, detailColName):
    if not df.empty:
        result = df[(df[addIndCol].str.lower() == fltr1) & (df[addTypeCol].str.lower() == (fltr2 or fltr3))]
        if not result.empty:
            return result[str(detailColName)].iloc[0]
        else:
            return ""
    # return var_otherDFDetails(row, addf, addprimCol, fltr, detailColName)

# def var_getAddDetails(row, adddf, specprimCol, fltr, detailColName):
#     var_otherDFDetails(row, adddf, specprimCol, fltr, detailColName)


def validateString(str1, str2):
    dist = edit_distance(str1.lower(), str2.lower())
    if dist <= 2:
        return str2
    else:
        return ""

def lastName(row, lastName, errMsg, npiLastName):
    if row[npiLastName]:
        res = validateString(row[lastName], row[npiLastName])
        if res and res != "":
            return res
        else:
            return ErrorMessage([], ["E", errMsg, row[lastName] + ", " + row[npiLastName]])
    else:
        return row[lastName]

def middleName(row, npi, middleName):
    if row[middleName]:
        row[middleName]=re.sub(r'[^\w]', '', row[middleName])
        return row[middleName].upper()
    else:
        return ""


def var_pcpHBPSpec(row, pcpSpec):
    return row[pcpSpec]
    # primSpecDF = pd.DataFrame()
    # if not specDF.empty:
    #     primSpecDF = specDF[specDF[primSplInd] == "Primary"]
    #
    # if not primSpecDF.empty:
    # if row[pcpSpec] == "PCP":
    #     return "PCP"
    # # elif row[hbpSoley] == "Y":
    # #     return "HBP"
    # else:
    #     return "SPEC"
    # else:
    #     return None

def var_finalSpec(row, inSot, boardSpec, boardExSpec, sotExSpec, specCol):
    if row[inSot] == "Y":
        if row[sotExSpec]:
            return row[sotExSpec]
        else:
            return row[specCol]
    else:
        if row[boardSpec]:
            if row[boardExSpec]:
                return row[boardExSpec]
            return row[boardSpec]
        else:
            return ""

def var_pcpSpec(row, primSpec, isMidlevel, superSpec):
    spec = ''
    if row[isMidlevel] == 'Y':
        spec = row[superSpec]
    else:
        spec = row[primSpec]
    found = 0
    bestMatch = elastic.gridLookup('pcp_vs_specialist', spec, 1, {}, **{'match_fields': ['specialty_name'],'operator':'and'})
    if not bestMatch:
        spec = re.sub("[\(\[].*?[\)\]]", "", spec).strip()
        bestMatch = elastic.gridLookup('pcp_vs_specialist', spec, 1, {}, **{'match_fields': ['specialty_name'], 'operator': 'and'})
        if not bestMatch:
            CompleteIndex = elastic.gridLookup('pcp_vs_specialist', '', 0, {}, **{'match_all':True})
            lstOfIndexString = [(x["_source"]["specialty_name"].split()) for x in CompleteIndex["hits"]["hits"]]
            for x in lstOfIndexString:
                llen = len(x)
                count = 0
                for wrd in x:
                    if wrd not in spec:
                        break
                    else:
                        count += 1
                if count == llen:
                    found = 1
                    break


    if (bestMatch is not None and bestMatch["specialty_name"]) or found == 1:
        return "PCP"
    else:
        return "SPEC"
#
# def getPCP(spec, pcp,ndbspec):
#     bestMatch = elastic.gridLookup('pcp_vs_specialist', spec, 1)
#     if bestMatch is not None and bestMatch["specialty_name"]:
#         return "Y"
#     else:
#         return "N"
    # for key, value in pcp.iteritems():
    #     if key.strip() == spec.strip():
    #         if (value[key] == ndbspec):##Todo: if value of Speciality code from JSON = value of specialty code in Specialty datframe
    #             return 1
    #         else:
    #             return 0

def var_fillTaxonomy(row, primSplCol, primNdbSpecCol, sotTaxonomy):
    #todo: implemet nucc gird
    matchFound = False
    primNdbSpec = row[primNdbSpecCol]
    primSpl = row[primSplCol]
    if row["NPI_TAXONOMY_PRIMARY"]:
        npiPrimTax = row["NPI_TAXONOMY_PRIMARY"].split("|")
        for val in npiPrimTax:
            s = val.split(":")
            if int(s[1]) == int(primNdbSpec):
                matchFound = True
                taxonomy = s[0]
                break

    if matchFound:
        return taxonomy
    else:
        customQuery = {"nucc_description": {'search_string': str(primSpl), 'bool_param': 'must', 'fuzziness': 3},
                       "ndb_specialty_code": {'search_string': str(primNdbSpec), 'bool_param': 'must', 'fuzziness': 1}}
        bestMatch = elastic.gridLookup('nucc', str(primSpl) + " " + str(primNdbSpec), 1, customQuery,**{'match_fields': ["nucc_description", "ndb_specialty_code"]})
        if not bestMatch:
            customQuery = {"ndb_description": {'search_string': str(primSpl), 'bool_param': 'must', 'fuzziness': 3},
                           "ndb_specialty_code": {'search_string': str(primNdbSpec), 'bool_param': 'must',
                                                  'fuzziness': 1}}
            bestMatch = elastic.gridLookup('nucc', str(primSpl) + " " + str(primNdbSpec), 1, customQuery,
                                           **{'match_fields': ["ndb_description", "ndb_specialty_code"]})

        # bestMatch = elastic.gridLookup('nucc', str(primSpl) + " " + str(primNdbSpec), 1,**{'match_fields': ["nucc_description", "ndb_specialty_code"]})
        if bestMatch and bestMatch["nucc_taxonomy_code"]:
            return bestMatch["nucc_taxonomy_code"]
        else:
            return row[sotTaxonomy]


        #
        # if row["TAXONOMY"] and not row["TAXONOMY"].isspace():
        #     return row["TAXONOMY"]
        # else:
        #     return row["NPI_TAXONOMY_PRIMARY"]


def isPresentInHBP(primSpecialty):
    if primSpecialty:
        # elastic.gridLookup('pcp_vs_specialist', spec, 1, {}, **{'match_fields': ['specialty_name'], 'operator': 'and'})
        bestMatch = elastic.gridLookup('hbp_speciality', primSpecialty, 1, {}, **{'match_fields': ['specialty_name'], 'operator': 'and'})
        if bestMatch:
            return "Y"
        else:
            return "N"
    else:
        return "N"

def var_hbpSolelyInHospital(row, diColMapping, primSpec):
    if "HBP_SOLELY_IN_HOSPITAL" in list(diColMapping.keys()):
        if row["HBP_SOLELY_IN_HOSPITAL"] == "Y" or row["HBP_SOLELY_IN_HOSPITAL"] == "N":
            return row["HBP_SOLELY_IN_HOSPITAL"]
        else:
            return isPresentInHBP(row[primSpec])
    else:
        return ""

def primarySpecialityName(row, ndbSpecName):
    if row[ndbSpecName]:
        return row[ndbSpecName]

def primarySpecialityOrSpecialityCode(row, inSot, ndbSpec, exSpecCol, boardSpec, svdf, npiPrimSpec):
    #case-1
    if row[ndbSpec]:
        return row[ndbSpec].rjust(3, "0")

    #case-2
    if svdf[npiPrimSpec][0]:
        return ErrorMessage([], ["C", "spec is empty in sot and npi registry", "NPI Spec is empty"])
    elif row[inSot] == "N" and (not row[boardSpec]):
        return ErrorMessage([], ["R", "spec and board-spec are empty", svdf[npiPrimSpec][0]])
    elif row[exSpecCol] and (not row[ndbSpec]):
        return ErrorMessage([], ["R", "spec is not avail in Exception list and NDB Grid", svdf[npiPrimSpec][0]])
    # else:
    #     return ErrorMessage([], ["R", "spec and board-spec are empty and not in Exception list and NDB Grid", svdf[npiPrimSpec][0]])

    # if row[ndbSpec]:
    #     bestMatch = None  #spec exception list, elastic.gridLookup('master_degree', row[specCol], 0, **{'match_fields': ['degree']})
    #     if bestMatch and bestMatch["specialty"]:
    #         if svdf[npiPrimSpec][0]:
    #             return ErrorMessage([], ["R", "spec is in exception list", svdf[npiPrimSpec][0] ])
    #         else:
    #             return ErrorMessage([], ["C", "spec is in exception list", "NPI Spec is empty"])
    # else:
    #     if svdf[npiPrimSpec][0]:
    #         return ErrorMessage([], ["R", "spec is empty or not avail in NDB", svdf[npiPrimSpec][0]])
    #     else:
    #         return ErrorMessage([], ["C", "spec is empty or not avail in NDB", "NPI Spec is empty"])


def var_primSecSpecalityInd(row, isDegMidLevel, isSpecMidLevel, indColName, primidentifier):
    # if row[isDegMidLevel] == "Y" and row[isSpecMidLevel] == "N" and row[indColName].lower() <> primidentifier.lower():
    #     return "supervising"
    # else:
    return row[indColName]
    # nrows = len(specDF.index)
    # if nrows > 1:
    #     return "Send for clarification(multiple prim/sec ind)"
    # else:
    #     if row["PRIMARY_SPECIALITY_IND"]:
    #         return row["PRIMARY_SPECIALITY_IND"]

def var_DegreeSpec(row, degreeMidLevelCol, degreeColName, degMasterInd):
    if row[degreeMidLevelCol] == "Y" and row[degMasterInd] == "Y":
        if row[degreeColName].upper() == 'NP':
            return "Nurse Practitioner"

        bestMatch = elastic.gridLookup('mid_specialty', row[degreeColName], 0, **{'match_fields': ['ndb_deg']})
        if bestMatch:
            return bestMatch["prov_type_name"]
        else:
            return ""

def var_fillPrimaryDegreeInd(row, degreeIndColName):
    if row['DEGREE'] or row["ROW_COUNT"] == 1:
        return row[degreeIndColName]
    else:
        return ""

def var_getBoardSpec(row, inSot, boardDF, boardInd, boardSpec, spec):
    if row[inSot] == "N":
        primBoardDF = boardDF[boardDF[boardInd].fillna('').str.lower() == "primary"]
        if not primBoardDF.empty:
            return primBoardDF[boardSpec][0]
        else:
            return boardDF[boardSpec][0]
    # else:
    #     return row[spec]

def var_exceptionSpec(row, spec,degForLkp):
    spec = row[spec]
    if spec:
        bestMatch =lookupSpecialtyExceptions(spec,degForLkp)
        # bestMatch = elastic.gridLookup('specialty_exceptions', spec, 1, {}, **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
        if bestMatch:
            return bestMatch["specialty_name_in_taxonomy_grid"]
        # else:
        #     return spec

# def var_matchSpecWithDeg(row, degDF, degFinalInd, cleanDegCol):
#     degDF2 = degDF[degFinalInd == "Primary"]
#
#     degree = None
#     if not degDF2.empty:
#         degree = degDF2[cleanDegCol][0]
#     else:
#         degree = degDF[cleanDegCol][0]
#
#     if degree:
#         bestMatch = elastic.gridLookup('specialty_exceptions', degree, 1, {}, **{'match_fields': ['degree']})
#         if bestMatch:
#             return bestMatch["specialty_name_in_taxonomy_grid"]

def var_isMidLevelSpec(row, finalSpec):
    specval = row[finalSpec]
    if len(specval.split()) > 1:
        bestMatch = elastic.gridLookup('mid_specialty', specval, 1,**{'match_fields': ['prov_type_name'], 'operator': 'and'})
    else:
        bestMatch = elastic.gridLookup('mid_specialty', specval, 1, **{'match_fields': ['prov_type_name'], 'operator': 'and', 'restrict': True})

    if bestMatch:
        return "Y"
    else:
        return "N"

def var_DefaultMidLevelSpec(row, finalSpecCol, specMidLevelind, degMidLevelind, degMasterInd, degreeSpec, finalInd):
    if row[specMidLevelind] == 'N' and row[degMidLevelind] == 'Y' and row[finalInd] == "Primary":
        superspec = row[finalSpecCol]
        if row[degMasterInd] == "N":
            row[finalSpecCol] = 'Nurse Practitioner'
        else:
            row[finalSpecCol] = row[degreeSpec]
        return superspec
    else:
        return ""

def var_getFinalSuperSpec(row, altsupervisorSpecialty, supervisorSpecialty):
    if row[supervisorSpecialty]:
        return row[supervisorSpecialty]
    else:
        return row[altsupervisorSpecialty]

def var_getFinalSuperSpecCode(row, supervisorSpecialtyCode, altSuperSpecialtyCosCred):
    row = row.fillna('')
    if row[supervisorSpecialtyCode]:
        return row[supervisorSpecialtyCode]
    elif row[altSuperSpecialtyCosCred] and row[altSuperSpecialtyCosCred].split("|")[0]:
        return row[altSuperSpecialtyCosCred].split("|")[0]
    else:
        return ""

def var_getFinalSuperSpecName(row, supervisorSpecialtyName, altSuperSpecialtyName):
    row = row.fillna('')
    if row[supervisorSpecialtyName]:
        return row[supervisorSpecialtyName]
    elif row[altSuperSpecialtyName] and row[altSuperSpecialtyName].split("|")[4]:
        return row[altSuperSpecialtyName].split("|")[4]
    else:
        return ""


def var_getFinalSuperCosCred(row, SuperSpecialtyCosCred, altSuperSpecialtyCosCred):
    row = row.fillna('')
    if row[SuperSpecialtyCosCred]:
        return row[SuperSpecialtyCosCred]
    else:
        if row[altSuperSpecialtyCosCred]:
            return row[altSuperSpecialtyCosCred].split("|")[3]
        else:
            return ''

def var_getFinalSuperCosProv(row, SuperSpecialtyCosProv, altSuperSpecialtyCosProv):
    row = row.fillna('')
    if row[SuperSpecialtyCosProv]:
        return row[SuperSpecialtyCosProv]
    else:
        if row[altSuperSpecialtyCosProv]:
            return row[altSuperSpecialtyCosProv].split("|")[2]
        else:
            return ''

# def var_isMasterMidLevelDegreeLookup(row, degree):
#     degree = row[degree].upper()
#     bestMatch = elastic.gridLookup('master_degree', degree, 0, {}, **{'match_fields': ['degree']})
#     if not bestMatch:
#         bestMatchAlt = elastic.gridLookup('master_degree', degree, 1, {}, **{'match_fields': ['degree']})
#         if bestMatchAlt:
#             if (len(bestMatchAlt['degree']) == len(degree) and bestMatchAlt['degree'].upper() == degree) or (len(bestMatchAlt['degree']) <> len(degree) and (bestMatchAlt['degree'].upper() in degree.upper())):
#                 bestMatch = bestMatchAlt
#
#     #Todo: lookup for isMasterDegree and isMidlevel.
#     if bestMatch:
#         return bestMatch["mid_level_indicator"] + "|" + bestMatch["master_list_indicator"] + "|" + bestMatch["degree"]
#     else:
#         return ""

def var_isMidLevelDeg(row, lkpdegree):
    val = ''
    if row[lkpdegree]:
        val = row[lkpdegree].split("|")[0]

    if val and val.lower() == "yes":
        return "Y"
    else:
        return "N"

# def var_OverwriteDeg(row, cleanDeg, lkpdegree):
#     val = ''
#     if row[lkpdegree]:
#         val = row[lkpdegree].split("|")[2]
#
#     if row[cleanDeg] == val:
#         return row[cleanDeg]
#     else:
#         return val

def var_isInMasterDegList(row, lkpdegree):
    val = ''
    if row[lkpdegree]:
        val = row[lkpdegree].split("|")[1]

    if val  and val.lower() == "yes":
        return "Y"
    else:
        return "N"

def var_getDegreeFromNdbGrid(row, specdf, finalDegreeCol, specdfprimcol, degreeColName, degreeIndCol, degreeInd):
    if not specdf.empty:
        if row[finalDegreeCol] == '' or str(row[finalDegreeCol]).isspace():
            if not specdf[specdf[specdfprimcol] == 'Primary'].empty:
                if row[degreeIndCol] == degreeInd:
                    return specdf[specdf[specdfprimcol] == 'Primary'][degreeColName].iloc[0]
                else:
                    return row[finalDegreeCol]
            else:
                return row[finalDegreeCol]
        else:
            return row[finalDegreeCol]

def var_markDeletion(row, degreeind, degreecol):
    if row[degreeind] != 'Primary':
        if row[degreecol] == '' or not row[degreecol]:
            return 'Y'
        else:
            return ""
    else:
        return ""

def var_getDegreeCredTitle(row, svdf, degreeCol, inMasterDegreeList, isMidLevlDegree):
    #var_getDegreeFromNdbGrid -- not used anymore??
    if row[degreeCol]:
        # if row[inMasterDegreeList] == "Yes":
        #     return row[degreeCol]
        if row[isMidLevlDegree] == "Y" and row[inMasterDegreeList] == "N": #and row[primSpec] and row[primSpecInNdbGrid] == "N":
            #consider it as nurse practitioner
            return "NP"
        elif row[inMasterDegreeList] == "Y": #and row[isMidLevlDegree] == "Y" and (not row[primSpec]):
            return row[degreeCol]
        else:
            return svdf["NPI_CREDENTIAL_PRIMARY"][0]
    else:
        return svdf["NPI_CREDENTIAL_PRIMARY"][0] #var_getDegreeFromNdbGrid(row, specdf, degreeCol, specdfprimcol)
###########################################ADDED BY ROHAN ###############################################

def ErrorMessage(lst, Input):

    if not len(lst) >= 1:
        Type = Input[0]
        Message = Input[1]
        Inp = Input[2]
    else:
        Type = lst[0].populate(Input)
        Message = lst[1].populate(Input)
        Inp = lst[2].populate(Input)

    intrimStr = str(Message) + "|" + "Input: " + str(Inp)
    if Type == "R":
        return "Reject: " + intrimStr
    elif Type == "E":
        return "Error: " + intrimStr
    else:
        return "Clarify: " + intrimStr

def var_extractDate(row, col):
    try:
        parser.parse(str(row[col]))
    except:
        possibleFormats = [r"\d{1}/\d{1}/\d{2}", r"\d{2}/\d{1}/\d{2}", r"\d{1}/\d{2}/\d{2}", r"\d{2}/\d{2}/\d{2}"]
        for ptrn in possibleFormats:
            match = re.findall(ptrn, row[col])
            if match and match[0]:
                return match[0]
            return row[col]
    return row[col]

def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def var_isBlankDate(row, ColumnName):
    if row[ColumnName]:
        return "N"
    else:
        return "Y"
def var_splitDate(row, colName):
    match = re.search(r'(\d+/\d+/\d+)', row[colName])
    if match:
        dateAfterSplit = match.group(1)
        return dateAfterSplit
    else:
        return row[colName]

def var_parseDate(row, ColumnName, coltyp='C', deaNumCol = ""):
    if row[ColumnName] =='ignore':
        return "ignore"
    if row[ColumnName] == '' or str(row[ColumnName]).isspace() or row[ColumnName] is None:
        if coltyp == 'DEA' and row[deaNumCol]:
            return "12/31/9999"
        else:
            return ""
    # if coltyp == 'DEA':
    #     if row[deaNumCol]:
    #         try:
    #             parseVal = parser.parse(str(row[ColumnName]), default=datetime.datetime(9999, 12, 31))
    #             cleansedDate = str(parseVal.month) + "/" + str(parseVal.day) + "/" + str(parseVal.year)
    #             return cleansedDate
    #         except ValueError as e:
    #             return "" #""Error: " + e.message + "| Input: " + row[ColumnName]
    #     else:
    #         return ""
    else:
        if coltyp == 'C':
            val = row[ColumnName]
            if is_float(val):
                val = int(float(val))

            try:
                # import os
                # base, ext = os.path.splitext(str(val))
                # parseVal = parser.parse((str(base)), default=datetime.datetime(1, 1, 1))
                # parseVal =parser.parse((str(val))[0:11], default=datetime.datetime(1, 1, 1))
                parseVal = parser.parse((str(val)), default=datetime.datetime(1, 1, 1))
                cleansedDate = str(parseVal.month).rjust(2, "0") + "/" + str(parseVal.day).rjust(2, "0") + "/" + str(parseVal.year).rjust(4, "0")
                return cleansedDate
            except ValueError as e:
                if row[ColumnName] == "":
                    return row[ColumnName]
                return "Error: " + e.message + "| Input: " + row[ColumnName]
        else:
            try:
                dt = parser.parse(str(row[ColumnName]))
            except ValueError as e:
                if ColumnName in ["BOARD_CERT_DATE", "BOARD_CERT_EXPIRATION_DATE"]:
                    return str(row[ColumnName]).upper()
                # if row[ColumnName].lower == ""
                return "Error: " + e.message + "| Input: " + row[ColumnName]

            month = dt.month
            if len(str(row[ColumnName])) <= 4:
                month = 12

            year = dt.year
            lastday = calendar.monthrange(year, month)
            parseVal = parser.parse(str(row[ColumnName]), default=datetime.datetime(4, month, lastday[1])).date()
            cleansedDate = str(parseVal.month).rjust(2, "0") + "/" + str(parseVal.day).rjust(2, "0") + "/" + str(parseVal.year).rjust(4, "0")
            return cleansedDate


def var_standardiseString(row, inputCol, applyList):
    for st in applyList:
        if row[inputCol].lower() in [v.lower() for v in constants.standardsDict[st]]:
            return st
    return row[inputCol].lower()


def var_mergeDerivedWithInput(row, inputCol, derivedCol):
    row = row.fillna('')
    if row[inputCol] == '' or str(row[inputCol]).isspace():
        return row[derivedCol]
    else:
        return row[inputCol]

def rowLvlFuncFromDfLayer(df,argDi):
    lstInputCol=argDi["inputCol"]
    outputCol=argDi["outputCol"]
    for idx,row in df.iterrows():
        df.loc[idx,outputCol]=eval(argDi['funcName'])(*[row]+lstInputCol)
    return df


def var_addOrderInd(row, addOrderCol, mergeAddCol):
    try:
        # diNumber={"1":["first","1st"],"2":["second","2nd"],"3":["third","3rd"],"4":["fourth","4th"],"5":["fifth","5th"]}
        try:
            val = int(row[mergeAddCol])
        except:
            lstMatch = re.findall("\\b\d\\b",row[mergeAddCol])
            if lstMatch:
                val=int(lstMatch[0])
            else:
                for stdKey in ["1","2","3","4","5"]:
                    if re.search('\\b'+'\\b|\\b'.join(constants.standardsDict[stdKey])+'\\b',row[mergeAddCol],re.IGNORECASE):
                        val=int(stdKey)
                        break
        if val == 1:
            return "P"
        elif val > 1:
            return "S"

    except Exception as e:
        return row[mergeAddCol]

def var_specialityFinalIndicator(row, Message, cleanColName):
    if row[cleanColName] == '' or str(row[cleanColName]).isspace() or str(row[cleanColName]).lower() == 'general':
        if row["ROW_NUM"] == 0:
            return "Primary"
        else:
            return "Secondary" #ErrorMessage([], ["C", Message, ""]) #ToDo: Check if this needs to be done here or in Field level logic
    else:
        return row[cleanColName]

def var_degreeFinalIndicator(df, argDi):
    cleanColName=argDi["inputCol"][0]
    outputCol=argDi["outputCol"]
    for idx,row in df.iterrows():
        if row[cleanColName] == '' or str(row[cleanColName]).isspace() or str(row[cleanColName]).lower() == 'general':
            if row["ROW_NUM"] == 0:
                df.loc[idx,outputCol]="Primary"
            else:
                df.loc[idx, outputCol] ="Secondary"
        else:
            df.loc[idx, outputCol] =row[cleanColName]
    return df

def var_finalColIndicator(df, argDi):
    return var_degreeFinalIndicator(df, argDi)
#
# def var_boardFinalIndicator(row, cleanColName):
#     if row[cleanColName] == '' or str(row[cleanColName]).isspace() or str(row[cleanColName]).lower() == 'general':
#         if row["ROW_NUM"] == 0:
#             return "P"
#         else:
#             return "S"
#     else:
#         return row[cleanColName]

def updateSpecialtyDf(specDF, boardDF):
    ## This function merges the Board dataframe and Speciality dataframe
    tdf = boardDF[['BOARD_SPECIALITY', 'SPEC_ROW_NUM', 'ASSOCIATION_MID_LEVEL']]
    tdf.columns = ['BRD_SPECIALITY', 'ROW_NUM', 'BOARD_MID_LEVEL']

    tdf = tdf.drop_duplicates("BRD_SPECIALITY", keep="first")

    if len(tdf.index) == 1 and not tdf['BRD_SPECIALITY'][0]:
        tdf["ROW_NUM"] = 0

    specDF['SPEC_MID_LEVEL'] = ''
    specDF['SUPER_SPEC'] = ''
    mergeDF = pd.merge(specDF, tdf, how='outer', on='ROW_NUM').fillna('')
    finalDF = pd.DataFrame(columns = mergeDF.columns)
    superviserOnlyCheck = True if len(mergeDF.query("DERIVED_SPECIALITY_INDICATOR != 'SUPERVISING'").index) == 1 else False
    for index, row in mergeDF.iterrows():
        #If the Speciality is empty the we pick the value from Board Specialty
        if row['BRD_SPECIALITY'] and not row['SPECIALITY'] and row['DERIVED_SPECIALITY_INDICATOR'] != 'SUPERVISING':
            row['SPECIALITY'] = row['BRD_SPECIALITY']
            if row['ROW_NUM'] == 0 or superviserOnlyCheck: # Also Assign the DERIVED Indicator accordingly
                row['DERIVED_SPECIALITY_INDICATOR'] = 'Primary'
            else:
                row['DERIVED_SPECIALITY_INDICATOR'] = 'Secondary'
        bestMatch=lookupSpecialtyExceptions(row['SPECIALITY'],row['degForLkp'])
        # bestMatch = elastic.gridLookup('specialty_exceptions', row['SPECIALITY'], 1, {},
        #                                **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
        specExep = bestMatch["specialty_name_in_taxonomy_grid"] if bestMatch else row['SPECIALITY']
        isMidLevel = elastic.gridLookup('mid_specialty', specExep, 1, **{'match_fields': ['prov_type_name'], 'operator': 'and'})
        MidLevelInd = 'Y' if isMidLevel else 'N'
        row['SPECIALITY'] = specExep
        row['SPEC_MID_LEVEL'] = MidLevelInd

        #Special Treatment for Midlevel Board Specialty and Non Mid Level Specialty
        #Note that Midlevel Board Speciality is always associated to the either the -
        # - primary record or the First record of the speciality dataframe
        if row['SPECIALITY'] and row['BRD_SPECIALITY'] and row['BOARD_MID_LEVEL'] == 'Y' and row['SPEC_MID_LEVEL'] == 'N':
            row['SUPER_SPEC'] = row['SPECIALITY']
            row['SPECIALITY'] = row['BRD_SPECIALITY']

        finalDF.loc[index] = row
    return finalDF

def getMinDistanceFromSpec(boardDf, specDF):
    specDF = specDF.query("DERIVED_SPECIALITY_INDICATOR != 'SUPERVISING'")
    emptySpec = False
    emptyBoard = False
    if len(specDF.index) == 1 and not specDF['SPECIALITY'][0]:
        emptySpec = True
    if len(boardDf.index) == 1 and not boardDf['BOARD_SPECIALITY'][0]:
        emptyBoard = True
    ## The Below logic associates the two Speciality and Board dataframe on the basis of the NDB spec returned from the taxonomy grid
    if not emptyBoard:
        specDF = specDF.fillna('')
        boardDf = boardDf.fillna('')
        for index, row in boardDf.iterrows():
            #Lookup on Elastic search to get the NDB Spec
            bestMatch=lookupSpecialtyExceptions(row['BOARD_SPECIALITY'],row['degForLkp'])
            # bestMatch = elastic.gridLookup('specialty_exceptions', row['BOARD_SPECIALITY'], 1, {},
            #                                **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
            BoardSpec = bestMatch["specialty_name_in_taxonomy_grid"] if bestMatch else row['BOARD_SPECIALITY']
            result = lookupNDBTaxonomy(BoardSpec) #elastic.gridLookup('ndb_taxonomy', BoardSpec, 1, {}, **{'match_fields': ['prov_type_name'],'stem':True})
            isMidLevel = elastic.gridLookup('mid_specialty', BoardSpec, 1, **{'match_fields': ['prov_type_name'], 'operator': 'and'})
            MidLevelInd = 'Y' if isMidLevel else 'N'
            boardDf.loc[index, 'BOARD_SPECIALITY'] = BoardSpec
            boardDf.loc[index, 'ASSOCIATION_NDB_SPEC'] = result["ndb_spec"] if result else np.nan
            boardDf.loc[index, 'ASSOCIATION_MID_LEVEL'] = MidLevelInd



    ## Execute the same steps for Speciality dataframe
        for index, row in specDF.iterrows():
            bestMatch=lookupSpecialtyExceptions(row['SPECIALITY'],row['degForLkp'])

            # bestMatch = elastic.gridLookup('specialty_exceptions', row['SPECIALITY'], 1, {},
            #                                **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and'})
            BoardSpec = bestMatch["specialty_name_in_taxonomy_grid"] if bestMatch else row['SPECIALITY']
            result = lookupNDBTaxonomy(BoardSpec) #elastic.gridLookup('ndb_taxonomy', BoardSpec, 1, {}, **{'match_fields': ['prov_type_name'],'stem':True})
            specDF.loc[index, 'ASSOCIATION_NDB_SPEC'] = result["ndb_spec"] if result else np.nan

    ## Run the association logic to associate the two dataframes on the basis of NDB Spec
        for index, row in boardDf.iterrows():
            # SpecRowNum = specDF[specDF["ASSOCIATION_NDB_SPEC"] == row["ASSOCIATION_NDB_SPEC"]]["ROW_NUM"][0]
            SpecRowNum = np.nan
            for idx, rw in specDF.iterrows():
                if rw['ASSOCIATION_NDB_SPEC'] == row['ASSOCIATION_NDB_SPEC']:
                    SpecRowNum = rw["ROW_NUM"]
                    break
            boardDf.loc[index, 'SPEC_ROW_NUM'] = SpecRowNum

    ## If Speciality is empty Board is ordered on Indicator else its ordered on Speciality Row Number associated above
        if not emptySpec:
            boardDf = boardDf.sort_values(by=['SPEC_ROW_NUM', 'ROW_NUM'], ascending=[True, True], na_position = "last")
        else:
            boardDf = boardDf.sort_values(by=['DERIVED_BOARD_INDICATOR', 'ROW_NUM'], ascending=[True, True], na_position="last")

        boardDf =boardDf.reset_index()

    ## Special case when either of them are completely empty - Valid after above sorting
    ## very important to keep the initializing order same as below
    if emptySpec or emptyBoard:
        boardDf.loc[0, 'ASSOCIATION_NDB_SPEC'] = '999'
        boardDf.loc[0, 'ASSOCIATION_MID_LEVEL'] = 'N'
        boardDf.loc[0,'SPEC_ROW_NUM'] = 0
        return boardDf

    ## mid level case- If Board is Mid level it is always associated to the first record or the primary Speciality record
    primSpec = specDF[specDF['DERIVED_SPECIALITY_INDICATOR'].str.lower() == 'primary']
    SpecRowNum = np.nan
    if not primSpec.empty:
        SpecRowNum = list(primSpec["ROW_NUM"])[0]
        # SpecRowNum = primSpec["ROW_NUM"][0]
    else:
        SpecRowNum = specDF['ROW_NUM'].iloc[0] if not specDF.empty else np.nan
    boardDf.loc[boardDf['ASSOCIATION_MID_LEVEL'] == 'Y', 'SPEC_ROW_NUM'] = SpecRowNum
        # primSpec["ROW_NUM"][0] if not primSpec.empty else np.nan

    return boardDf


def associateSpecDf(df, specDf):
    specDf = specDf[['OUT_PRIMARYSPECIALITY_SPECIALITYCODE', 'OUT_SPECIALITY_PRIMARY_IND']]
    specDf.columns = ['BOARD_FILTER_SPEC_CODE', 'BOARD_FILTER_PRIMARY_IND']
    tdf = pd.concat([specDf, df], axis=1).fillna('')
    return tdf

def removeRowDuplicate(df, columnNm):
    return df.drop_duplicates(columnNm)

###########################################ADDED BY ROHAN ###############################################

def removeRowDf(df, qry):
    df = df.query(qry[0])
    df = df.reset_index(drop=True)
    return df


def var_getUniquePLSVStates(row, addDF, addTypeCol, addStateCol):
    #Todo: add df filter plsv unique states and get licence no for the same
    # list(addDF[addDF["MERGE_ADDRESS_TYPE"] == "PLSV"].ADDRESS_STATE.unique())
    uniquePLSVStates = list(addDF[addDF[addTypeCol].str.lower() == "plsv"].drop_duplicates([addStateCol])[addStateCol])
    return ("|").join(uniquePLSVStates)

def var_licenceNumber(row, addState, licCol, unqPLSVStates):
    # uniquePLSVStates = row[unqPLSVStates].split("|")
    licCol=row[licCol]
    if licCol: #row[addState] in uniquePLSVStates:
        # licNum = re.sub(r'[.]+', '', licCol) ## Commented as alphanumeric characters are suppossed to be added
        return licCol
    else:
        return None

def var_licenseState(row, licCol, licState, unqPLSVStates):
    if row[licCol]:
        if row[licState]:
            customQuery = {"us_state": {'search_string': row[licState], 'bool_param': 'must', 'fuzziness': 0}}
            bestMatch = elastic.gridLookup('us_states', "customQuery", 0, customQuery,
                                           **{'match_fields': ["us_state"]})
            if bestMatch and bestMatch["abbreviation"]:
                result = bestMatch["abbreviation"]
                return result
            else:
                return row[licState]
        elif row[unqPLSVStates]:
            spltList = row[unqPLSVStates].split("|")
            if row["ROW_NUM"] < len(spltList):
                return spltList[row["ROW_NUM"]]
            else:
                return ""
        else:
            return ""
    else:
        return ""

# def var_licenseState(row, licCol, licState, unqPLSVStates, diColMapping):
#     if row[licCol]:
#         if row[licState]:
#             customQuery = {"us_state": {'search_string': row[licState], 'bool_param': 'must', 'fuzziness': 0}}
#             bestMatch = elastic.gridLookup('us_states', "customQuery", 0, customQuery,
#                                            **{'match_fields': ["us_state"]})
#             if bestMatch and bestMatch["abbreviation"]:
#                 result = bestMatch["abbreviation"]
#                 return result
#             else:
#                 return row[licState]
#         elif row[licState] == "":
#             if row[licCol]:
#                 license = [k for k, v in diColMapping.iteritems() if "LICENCE_NUMBER" + "@" in k]
#                 license.sort()
#                 for i in license:
#                     return diColMapping[i][:2]
#             # for key in license:
#             #     return diColMapping[key][:2]
#         elif row[unqPLSVStates]:
#                 spltList = row[unqPLSVStates].split("|")
#                 if row["ROW_NUM"] <= len(spltList):
#                     return spltList[row["ROW_NUM"]]
#                 else:
#                     return ""
#         else:
#             return ""
#     else:
#         return ""

def var_cdsState(row, cdsState, unqPLSVStates):
    if row[cdsState]:
        return row[cdsState]
    elif row[unqPLSVStates]:
        spltList = row[unqPLSVStates].split("|")
        if row["ROW_NUM"] <= len(spltList):
            return spltList[row["ROW_NUM"]]
        else:
            return ""
    else:
        return ""

####################################################### multivalue columns derivation #############################################

def var_isMatchingWithNpiSpec(row, spec, svDF, npiTaxonomy):
    val = None
    try:
        val = svDF[npiTaxonomy][0].split("|")[0].split(":")[1]
    except:
        val = None

    if row[spec] and val:
        if row[spec] == val:
            return "Y"
        else:
            return "N"

def var_ndbDegreeLookup(row, degCol):
    result = elastic.gridLookup('ndb_taxonomy', row[degCol], 0, {} , **{'match_fields': ["ndb_deg"]})
    if result:
        return result["prov_type_name"] + "|" + result["ndb_deg"] + "|" + result["ndb_spec"] + "|" + result["cos_prov"] + "|" + result["cos_cred"] + "|" + result["ndb_rec"] + "|" + result["ur_ind"]
    else:
        return "||||||"

def var_ndbDegree(row, ndbLookupCol):
    return row[ndbLookupCol].split("|")[1]

def var_getMedicare(row, medicare):
    if row[medicare] and row[medicare] != "":
        if row[medicare][0]=="'":
            row[medicare]=row[medicare][1:]
        result= str(int(float(row[medicare]))) if re.search('^\d+?\.\d+?$',row[medicare]) else str(row[medicare]) if re.search('^[0-9A-Za-z]+$',str(row[medicare])) and not re.search('^[A-Za-z]+$',str(row[medicare])) else ''
    else:
        result=''
    return result


def deaNumber(row, deaNumberCol):
    if row["ROW_NUM"] > 1:
        return ErrorMessage([], ["R", "More than 2 dea numbers", row[deaNumberCol] ])
    else:
        return row[deaNumberCol]

def deaNumberExpirationDate(df, dea, deaExpirationDate):
    if df[dea]:
        return df[deaExpirationDate]
    else:
        return ""


def var_fillCurDelDate(row, isCurrentDelDateNotEmpty, isOrigDelDateNotEmpty, origDelDate, currentDelDate, isCurDelGreater2days):
    if row[isCurrentDelDateNotEmpty] == "N" and row[isOrigDelDateNotEmpty] == "Y":
        return row[origDelDate]
    elif row[isCurrentDelDateNotEmpty] == "Y" and row[isCurDelGreater2days] == "N":
        return row[currentDelDate]
    else:
        return row[origDelDate]

def var_fillOrigDelDate(row, isCurrentDelDateNotEmpty, isCurDelGreater2days, isOrigDelDateNotEmpty, currentDelDate, errMsg1, isCredDateReq, isCredDateGreater1day, errMsg2, origDelDate, effDate, stateExceptions, isOrigDateGreaterThanEffective, isEffectiveDateNotEmpty):
    if row[isOrigDelDateNotEmpty] == "N" and row[isCurrentDelDateNotEmpty] == "N" and row[isCredDateReq] =="1" :
        return ErrorMessage([], ["R", errMsg2, "CredDateReq-> " + row[isCredDateReq] + " and CredDateGreater1day-> " + row[isCredDateGreater1day]])
    if row[isOrigDelDateNotEmpty] == "N" or row[isCredDateGreater1day] == "Y":
        if row[isCurDelGreater2days] == "Y":
            if row[stateExceptions] == "1":
                return row[effDate]
            else:
                return ErrorMessage([], ["R","currentDelDate cannot be future date. ", "currentDelDate-> "+ row[currentDelDate]])
        if row[currentDelDate]:
            return row[currentDelDate]
        else:
            return row[effDate]

    elif (row[isOrigDelDateNotEmpty] == "Y" and row[isEffectiveDateNotEmpty] == "Y"):
        if row[isOrigDateGreaterThanEffective] == "Y":
            if row[stateExceptions] == "1":
                return row[origDelDate]
            else:
                return ErrorMessage([], ["R","OrigDelDate cannot be greater than effective date. ", "origDelDate-> "+ row[origDelDate]])
        else:
            if row[isOrigDelDateNotEmpty] == "Y":
                return row[origDelDate]
            else:
                return row[effDate]
    else:
        if row[isOrigDelDateNotEmpty] == "Y":
            return row[origDelDate]
        else:
            return row[effDate]

def var_compareDate(row, effDate, origDelDate):
    try:
        effDate = parser.parse(row[effDate])
    except:
        # no currDelDate to compare return n
        return "Y"
    try:
        origDelDate = parser.parse(row[origDelDate])
    except:
        #no origDelDate to compare return y
        return "N"

    if origDelDate > effDate:
        return "Y"
    else:
        return "N"

def var_getPrimDegree(row, degDF, degFinalInd, degreeCol):
    primDegDF =degDF[ degDF[degFinalInd].str.lower() == "primary" ]
    if not primDegDF.empty:
        return primDegDF[degreeCol][0]
    else:
        return ""

def var_board1(row, parentlist, primDeg):
    val = row[primDeg]
    if val in parentlist:
        return int(1)
    else:
        return int(0)

def var_board2(row, board1, date, boardExpDate, boardCertDate, boardCertCol):
    board1 = int(row[board1])
    # if primSpecCode and not primSpecCode.isspace():
    if board1 == 1:
        if row[boardCertCol].upper() in ["L", "C", "E"]:
            return row[boardCertCol]
        elif row[boardCertCol].upper() in ["Y", "C", "E", "X", ""]:

            a = parser.parse(date)

            b = None
            try:
                b = parser.parse(row[boardExpDate])
            except:
                print("not valid boardExpdate")

            if (a == b):
                return "L"
            elif row[boardCertDate] or row[boardExpDate]:
                return "C"
            else:
                return "N"


        elif row[boardCertCol].upper() == "N":
            try:
                dt1 = parser.parse(str(row[boardExpDate]))
                dt2 = parser.parse(str(row[boardCertDate]))
                return ErrorMessage([], ["C", "boardCert is N and boardExpDate or boardCertDate is not empty","boardExpDate -> " + str(dt1) + "boardCertDate -> " + str(dt2) ])
            except:
                return "N"
        else:
            return ErrorMessage([], ["C", "invalid board cert value","boardcert -> " + row[boardCertCol] ])
    else:
        return "X"
    # else:
    #     return ""



def var_board3(row, arg1, arg2):
    return None

def var_board4(row, arg1):
    return None

def var_board5(row, arg1):
    return None

def var_board8(row, arg1, arg2, arg3, arg4, arg5):
    return None

def var_getExtPhn1(row):
    return row["EXT_PHONE_1"].rjust(4, "0")

def var_getExtPhn2(row):
    return row["EXT_PHONE_2"].rjust(4, "0")

def var_getExtPhn3(row):
    return row["EXT_PHONE_3"].rjust(4, "0")

def var_getExtPhn4(row):
    return row["EXT_PHONE_4"].rjust(4, "0")

def var_getPhnType(row, finalAddType, phoneNumber1, phoneNumber2, phoneNumber3, phoneNumber4):
    if(row[phoneNumber1] or row[phoneNumber2] or row[phoneNumber3] or row[phoneNumber4]):
        if row[finalAddType].lower() == "cred":
            return "Bill"
        else:
            return row[finalAddType]

def var_getFinalAddInd(row):
    if row["ADDRESS_INDICATOR"] and not row["ADDRESS_INDICATOR"].isspace():
        return row["ADDRESS_INDICATOR"]
    else:
        return row["DERIVED_ADDRESS_INDICATOR"]

def var_getFinalAddType(row):
    if row["ADDRESS_TYPE"] and not row["ADDRESS_TYPE"].isspace():
        return row["ADDRESS_TYPE"]
    else:
        return row["DERIVED_ADDRESS_TYPE"]
#####################################ADDDED BY ROHAN SHILPA's Fields Integration #####################
# def var_getBillAdd(row, df, AddtypeColName, concatAddColName):
#     for index, rw in df.iterrows():
#         rw = rw.fillna('')
#         if rw[AddtypeColName].lower() == 'bill':
#             return rw[concatAddColName]
#
# def var_getMailAdd(row, df, AddtypeColName, concatAddColName):
#     for index, rw in df.iterrows():
#         rw = rw.fillna('')
#         if rw[AddtypeColName].lower() == 'mail':
#             return rw[concatAddColName]

def var_getAddType(row, df, AddtypeColName, concatAddColName, addType):
    for index, rw in df.iterrows():
        rw = rw.fillna('')
        if rw[AddtypeColName].lower() == addType.lower():
            return rw[concatAddColName]

# def var_CheckCombo(row, AddtypeColName, concatAddColName, mailAddColname):
#     if row[AddtypeColName] == 'PLSV':
#         if row[concatAddColName] == row[mailAddColname]:
#             return "Y"
#         else:
#             return "N"

def var_generalToPlsv(row, addTypeCol,ignore=None):
    if ignore:
        return row[addTypeCol]
    if row[addTypeCol].strip().lower() == "general":
        return "plsv"
    else:
        return row[addTypeCol]

def var_CheckCombo(row, df, AddtypeColName, concatAddColName, billAddColname):
    row = row.fillna('')
    billAddCol = ''

    if row[AddtypeColName].lower() == 'plsv':
        for index, rw in df.iterrows():
            rw = rw.fillna('')
            if rw[AddtypeColName].lower() == 'bill':
                billAdd = rw[concatAddColName]
                break
            else:
                billAdd = ''

        if billAdd:
            if row[concatAddColName].lower().strip() == billAdd.lower().strip():
                return "Y"
            else:
                return "N"
        else:
            return "N"

def var_IgnoreMail(row, AddtypeColName, concatAddColName, billAddColname):
    row = row.fillna('')
    if row[AddtypeColName].lower() == 'mail':
        if row[concatAddColName] == row[billAddColname]:
            return "Y"
        else:
            return "N"

def var_IgnoreCred(row, AddtypeColName, concatAddColName, billAddColname):
    row = row.fillna('')
    if row[AddtypeColName].lower() == 'cred':
        if row[concatAddColName] == row[billAddColname]:
            return "Y"
        else:
            return "N"

def directmap(row, ColName):
    return row[ColName]


def var_CheckPOBOX(row, AddtypeColName, concatAddColName):
    row = row.fillna('')
    if row[AddtypeColName].lower() == 'plsv':
        if 'BOX' in row[concatAddColName].upper() and 'PO' in row[concatAddColName].upper():
            return "Y"
        else:
            return "N"

def var_updateAddressType(row, AddtypeColName, comboCheckColName, poBoxcolName, Message):
    row = row.fillna('')

    if row[comboCheckColName] == "Y":
        return "Combo"
    elif row[comboCheckColName] != "Y" and row[poBoxcolName] == "Y":
        return ErrorMessage([], ["C", Message, row[poBoxcolName]])
    # general to plsv mapping
    # elif row[AddtypeColName].strip().lower() == "general":
    #         return "plsv"
    else:
        return row[AddtypeColName]


def var_getMinPLSVRow(row, Adddf, AddtypeColName):
    for index, rw in Adddf.iterrows():
        rw = rw.fillna('')
        if rw[AddtypeColName].lower() == 'plsv':
            return rw["ROW_NUM"]


def var_getAddressIndicator(row, AddIndColName, NewAddtypeColName, plsvMinRow, addLine1, addLine2):
    row = row.fillna('')
    if row[addLine1] or row[addLine2]:
        if row[AddIndColName] and row[AddIndColName] != '':
            return row[AddIndColName]
        elif row[NewAddtypeColName].lower() == 'bill' or row[NewAddtypeColName].lower() == 'combo':
            return "P" #p/s according to standards
        elif row[NewAddtypeColName].lower() == 'plsv' and row["ROW_NUM"] == row[plsvMinRow]:
            return "P"
        else:
            return "S"
    else:
        return ErrorMessage([], ["C", "Both Address Line1 and Line2 are empty", ""])


def var_correspondence(row, svdf, mailaddcolName,concatAddColName, NewAddtypeColName, NewAddIndColName ):
    taxId = svdf["TAX_ID"][0]
    npi = svdf["INDIVIDUAL_NPI"][0]

    if row['ADDRESS_LINE_1']:
        if row[NewAddtypeColName].lower() == "bill":
            #check bill address count for a combination of npi and tax-id
            if hasattr(var_correspondence, 'taxid') and hasattr(var_correspondence, 'npi'):
                if (var_correspondence.taxid == taxId) and (var_correspondence.npi == npi):
                    var_correspondence.billAddCount += 1
                    if var_correspondence.billAddCount > 1:
                        return ErrorMessage([], ["C", "Multiple billing address", ""])
                else:
                    var_correspondence.taxid = taxId

                    var_correspondence.npi = npi
                    var_correspondence.billAddCount = 1
            else:
                var_correspondence.taxid = taxId
                var_correspondence.npi = npi
                var_correspondence.billAddCount = 1

        row = row.fillna('')
        if row[NewAddtypeColName].lower() == "bill":
            return "P"
        elif row[NewAddtypeColName].lower() == "mail":
            return "P"
        elif row[NewAddtypeColName].lower() == "combo":
            return "P"
        elif row[mailaddcolName] == '' and row[NewAddtypeColName].lower() == 'plsv' and row[NewAddIndColName].lower() == 'p':
            return "P"
        else:
            return "S"
        var_correspondence.billAddCount = 0
    else:
        return ""

def var_setEmail_P_A(row, addDf, email1colName):
    addDf = addDf.fillna('')
    # Assumption is: that only the Palce of Service Address will have the email
    emailSet = set()
    for idx, rw in addDf.iterrows():
        if rw[email1colName] != "" and not rw[email1colName].isspace():
            emailSet.add(rw[email1colName])
    if row[email1colName] != "" and not row[email1colName].isspace():
        if len(emailSet) > 1:
            return "A"
        else:
            return "P"


def var_setEmail_P_A2(row, addDf, email2colName):
    addDf = addDf.fillna('')
    # Assumption is: that only the Palce of Service Address will have the email
    emailSet = set()
    for idx, rw in addDf.iterrows():
        if rw[email2colName] != "" and not rw[email2colName].isspace():
            emailSet.add(rw[email2colName])
    if row[email2colName] != "" and not row[email2colName].isspace():
        if len(emailSet) > 1:
            return "A"
        else:
            return "P"

def var_electronicCommType(row, email1colName, CommTypeColName):
        if row[CommTypeColName] != "" and not row[CommTypeColName].isspace():
            return row[CommTypeColName]
        else:
            if row[email1colName] != "" and not row[email1colName].isspace():
                if "@" in row[email1colName]:
                    return "E"
                else:
                    return "W"

def credDesign(row, primSpecCosCred, primDegree):
    coscred = row[primSpecCosCred]
    replace_punctuation = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
    text = str(coscred).translate(replace_punctuation).strip()
    degList = text.split()
    if len(degList) > 1:
        return row[primDegree]
    else:
        return row[primSpecCosCred]

def loc(row, loc, plsvState):

    if row[loc]:
        return row[loc]
    else:
        return row[plsvState]


def var_getPrimaryPLSVState(row, addDf, addType, addInd, addState):
    addDf = addDf.fillna('')
    for idx, rw in addDf.iterrows():
        if rw[addType].lower() == "plsv" and rw[addInd].lower() == "p" :
            return rw[addState]

def var_lkpCnSGrid(row, specDF, specCol, specIndCol, addStateCol):
    primSpec = None
    if not specDF.empty:
        primspecDF = specDF[specDF[specIndCol].str.lower() == "primary"]
        if not primspecDF.empty:
            primSpec = primspecDF[specCol].iloc[0]
    if primSpec and row[addStateCol]:
        if len(row[addStateCol]) > 2:
            customQuery = {"state": {'search_string': row[addStateCol], 'bool_param': 'must', 'fuzziness': 0},
                           "provider_specialty": {'search_string': primSpec, 'bool_param': 'must', 'fuzziness': 1}}
        else:
            customQuery = {"code": {'search_string': row[addStateCol], 'bool_param': 'must', 'fuzziness': 0},
                           "provider_specialty": {'search_string': primSpec, 'bool_param': 'must', 'fuzziness': 1}}

        result = elastic.gridLookup('c_and_s', row[addStateCol] + " " + primSpec, 1, customQuery,**{'match_fields': ["code", "provider_specialty"]})
        if result is not None:
            s = result["state"] + "|" + result["individual_enrollment_limit"] + "|" + result[
                "group_enrollment_limit"] + "|" + result["age_limit_of_patients"]
            return s
        else:
            return '||||'+row[addStateCol]+'|'+primSpec
    else:
        return '|||'

# def var_getAgeLimit(row, ageLimit, pcp, addInd, cnsLookup, addType, minAge, maxAge):
#     if row[addType].strip().lower() == "plsv" or row[addType].strip().lower() == "combo":
#         if row[ageLimit]:
#             return row[ageLimit]
#         elif row[minAge] and row[maxAge]:
#             return str(row[minAge]) + "-" + str(row[maxAge])
#         elif row[pcp] and len(row[cnsLookup].split('|')) <> 6:
#             return "CNS_" + row[cnsLookup].split("|")[3]
#         else:
#             return ""
#
# def var_formatAgeLimit(row, ageLimit):
#     if row[ageLimit]:
#
#         try:
#             ageLimits = row[ageLimit].split("-")
#             minAge = ageLimits[0].strip()
#             maxAge = ageLimits[1].strip()
#         except:
#             minAge = "0"
#             maxAge = "0"
#
#         if "cns" in minAge.lower():
#             minAge = minAge.split("_")[0] + "_" + minAge.split("_")[1].rjust(2, '0')
#         if len(minAge) < 2:
#             minAge = minAge.rjust(2, '0')
#         if len(maxAge) < 3:
#             maxAge = maxAge.rjust(3, '0')
#
#         return minAge + "-" + maxAge

def var_getAgeLimit(row, ageLimit, pcp, addInd, cnsLookup, addType, minAge, maxAge):
    if row[addType].strip().lower() == "plsv" or row[addType].strip().lower() == "combo":
        if row[pcp] and len(row[cnsLookup].split('|')) != 6:
            if row[ageLimit]:
                return row[ageLimit] + "/" + "CNS_" + row[cnsLookup].split("|")[3]
            elif row[minAge] and row[maxAge]:
                return str(row[minAge]) + "-" + str(row[maxAge]) + "/" + "CNS_" + row[cnsLookup].split("|")[3]
            else:
                return "CNS_" + row[cnsLookup].split("|")[3]
        elif row[ageLimit]:
            return row[ageLimit]
        elif row[minAge] and row[maxAge]:
            return str(row[minAge]) + "-" + str(row[maxAge])
        else:
            return ""

def var_getAgeLimitTaxIDAdd(row, ageLimit, minAge, maxAge):
    if row[ageLimit]:
        return row[ageLimit]
    elif row[minAge] and row[maxAge]:
        return str(row[minAge]) + "-" + str(row[maxAge])
    else:
        return ""

def var_getFinalAgeLimit(row, finalAgeLimit):
    ageLimit = var_formatAgeLimit(row, finalAgeLimit)
    return ageLimit

def var_formatAgeLimit(row, ageLimit):
    if row[ageLimit]:
        if '/' in row[ageLimit]:
            try:
                variousAgeLimits = row[ageLimit].split("/")
                sotAge = variousAgeLimits[0].strip()
                cnsAge = variousAgeLimits[1].strip()
                # splitting sot Age Limit
                ageLimits = sotAge.split("-")
                sotMinAge = ageLimits[0].strip()
                sotMaxAge = ageLimits[1].strip()
                #splitting CNS Age limit
                cnsAgeLimits = cnsAge.split("-")
                cnsMinAge = cnsAgeLimits[0].strip()
                cnsMaxAge = cnsAgeLimits[1].strip()
                if "cns" in cnsMinAge.lower() :
                    cnsMinAge = cnsMinAge.split("_")[0] + "_" + cnsMinAge.split("_")[1].rjust(2, '0')
                if len(sotMinAge) < 2:
                    sotMinAge = sotMinAge.rjust(2, '0')
                if len(sotMaxAge) < 3:
                    sotMaxAge = sotMaxAge.rjust(3, '0')
                if len(cnsMaxAge) < 3:
                    cnsMaxAge = cnsMaxAge.rjust(3, '0')
                return sotMinAge+ "-" + sotMaxAge + "/" + cnsMinAge + "-" + cnsMaxAge
            except:
                return row[ageLimit]
        try:
            ageLimits = row[ageLimit].split("-")
            minAge = ageLimits[0].strip()
            maxAge = ageLimits[1].strip()
        except:
            minAge = "0"
            maxAge = "0"
        if "cns" in minAge.lower():
            minAge = minAge.split("_")[0] + "_" + minAge.split("_")[1].rjust(2, '0')
        if len(minAge) < 2:
            minAge = minAge.rjust(2, '0')
        if len(maxAge) < 3:
            maxAge = maxAge.rjust(3, '0')
        return minAge + "-" + maxAge



# def var_getMinAge(row, pcp, ageLimit,cns):
#     cnslkp=row[cns]
#     if row[pcp] and len(cnslkp.split('|'))==6:
#         return ErrorMessage([], ["C", "State or Spec not Available","state -> '" + cnslkp.split('|')[4] + "' provider_specialty -> '" + cnslkp.split('|')[5]+"'"])
#     if row[pcp] and row[ageLimit]:
#         return row[ageLimit].split("-")[0]

def var_getMinAge(row, pcp, ageLimit,cns, minAge, addType):
    if row[addType].strip().lower() == "plsv" or row[addType].strip().lower() == "combo":
        cnslkp=row[cns]
        if row[minAge]:
            return row[minAge]
        elif row[ageLimit]:
            return row[ageLimit].split("-")[0]
        elif row[pcp] and len(cnslkp.split('|'))==6:
            return ErrorMessage([], ["C", "State or Spec not Available","state -> '" + cnslkp.split('|')[4] + "' provider_specialty -> '" + cnslkp.split('|')[5]+"'"])

def var_getMinAgeTaxIDAdd(row, ageLimit, minAge):
    if row[minAge]:
        return row[minAge]
    elif row[ageLimit]:
        return row[ageLimit].split("-")[0].strip()
    else:
        return ""

# def var_getMaxAge(row, pcp, ageLimit):
#     if row[pcp] and row[ageLimit]:
#         return row[ageLimit].split("-")[1]

def var_getMaxAge(row, pcp, ageLimit,cns,maxAge, addType):
    if row[addType].strip().lower() == "plsv" or row[addType].strip().lower() == "combo":
        cnslkp = row[cns]
        if row[maxAge]:
            return row[maxAge]
        elif row[ageLimit]:
            return row[ageLimit].split("-")[1]
        elif row[pcp] and len(cnslkp.split('|'))==6:
            return ErrorMessage([], ["C", "State or Spec not Available","state -> '" + cnslkp.split('|')[4] + "' provider_specialty -> '" + cnslkp.split('|')[5]+"'"])

def var_getMaxAgeTaxIDAdd(row, ageLimit, maxAge):
    if row[maxAge]:
        return row[maxAge]
    elif row[ageLimit] and len(row[ageLimit].split("-")) > 1:
        return row[ageLimit].split("-")[1].strip()
    else:
        return ""

def var_enrolmentLimit(row, pcp, npiTypeInd, cns, addType):
    if row[addType].strip().lower() == "plsv" or row[addType].strip().lower() == "combo":
        if row[pcp]:
            if npiTypeInd == "I":
                return row[cns].split("|")[1]
            else:
                return row[cns].split("|")[2]

def var_lkpPCPvsSpecGrid(row, prov_type):
    if row[prov_type] == row[prov_type]: #check whther row[ndb_spec] is not nan
       # result = elastic.gridLookup('pcp_vs_specialist', row[ndb_spec] , 0,**{'match_fields': ["ndb_specialty_code"], 'operator': 'and'})
       if row[prov_type].lower()=='pcp':
            return 1
       else:
            return 0
    else:
        return 0

# def var_imMedicaid(row, medIM):
#         return row[medIM]

def var_getFinalMedicaid(row, plsvState, medicaid):
    bestMatch = elastic.gridLookup('medicaid_numbers', row[plsvState], 0, **{'match_fields': ['state_code']})
    if bestMatch and row[medicaid] and row[medicaid].strip() != '':
        return bestMatch["provider_address_level"] + " - " + row[medicaid]
    else:
        if not bestMatch:
            return ErrorMessage([], ["C", "State not matched", "plsv State: ->" + row[plsvState]])
        if (not row[medicaid]) or (row[medicaid].strip() == ''):
            return ErrorMessage([], ["C", "Medicaid Number Missing", " medicaid No: ->" + row[medicaid]])

    # if row[iMedicaid] and row[iMedicaid] <> "":
    #     # if row[iMedicaid].lower() == "pending":
    #     #     return ""
    #     if row[iMedicaid][0]=="'":
    #         row[iMedicaid]=row[iMedicaid][1:]
    #     result= str(int(float(row[iMedicaid]))) if re.search('^\d+?\.\d+?$',row[iMedicaid]) else str(row[iMedicaid]) if re.search('^[0-9A-Za-z]+$',str(row[iMedicaid])) and not re.search('^[A-Za-z]+$',str(row[iMedicaid]))  else ''
    # elif row[medicaid] and row[medicaid] <> "":
    #     if row[medicaid][0]=="'":
    #         row[medicaid]=row[medicaid][1:]
    #     result= str(int(float(row[medicaid]))) if re.search('^\d+?\.\d+?$',row[medicaid]) else str(row[medicaid]) if re.search('^[0-9A-Za-z]+$',str(row[medicaid])) and not re.search('^[A-Za-z]+$',str(row[medicaid])) else ''
    # else:
    #     result = ""
    # return result

def var_getEnwInd(row, enwIndCol):
    if row[enwIndCol]:
        row[enwIndCol]
    else:
        return ""

def var_getNewPatients(row, newPatientsCol, finalPCPInd):
    if row[finalPCPInd].lower() == "pcp" or "spec": #requirment change for specialist provider too
        if row[newPatientsCol]:
            return row[newPatientsCol].upper()
        else:
            return "O"

def cleanBoardCert(df, argDi):
    baordCertCol = argDi['inputCol']
    possiblCertVals = constants.standardsDict[argDi['standardsKeys'][0]]
    for idx, val in enumerate(df[baordCertCol]):
        if len(val.strip()) > 1:
            for char in val.split(" "):
                if char in possiblCertVals:
                    df[baordCertCol][idx] = char
    return df

def var_getHospitalAffiliation(row, inSOT, hospital, svdf, taxId, addDf, primDeg, HospAffMandDeg):
    if row[inSOT] == "Y":
        customQuery = {"lastname": {'search_string': row[hospital], 'bool_param': 'must', 'fuzziness': 1,"operator":"and","restrict":True},
                       "taxid": {'search_string': svdf[taxId].iloc[0], 'bool_param': 'must','fuzziness': 0}}
        bestMatch = elastic.gridLookup('hospital_list', "customQuery", 1, customQuery,**{'match_fields': ["lastname", "taxid"]})
        if bestMatch and bestMatch["mpin"]:
            result = bestMatch["mpin"]
            return result
        elif not addDf[(addDf['OUT_ADDRESS_TYPE'].str.lower() == "plsv") & (addDf['OUT_ADDRESS_IND'].str.lower() == "p")]["OUT_ADDRESS_STATE"].empty:
            customQuery = {"lastname": {'search_string': row[hospital], 'bool_param': 'must', 'fuzziness': 1,"operator":"and","restrict":True},
                               "state": {'search_string': addDf[(addDf['OUT_ADDRESS_TYPE'].str.lower() == "plsv") & (addDf['OUT_ADDRESS_IND'].str.lower() == "p")]["OUT_ADDRESS_STATE"].iloc[0].upper(), 'bool_param': 'must', 'fuzziness': 1}}
            bestMatch1 = elastic.gridLookup('hospital_list', "customQuery", 1, customQuery, **{'match_fields': ["lastname", "state"]})
            if (bestMatch1 and bestMatch1["mpin"]):
                result1 = bestMatch1["mpin"]
                return result1
            elif len( addDf['OUT_ADDRESS_STATE'].unique() ) > 1:
                secPlsvAddDf = addDf[(addDf['OUT_ADDRESS_TYPE'].str.lower() == "plsv") & (addDf['OUT_ADDRESS_IND'].str.lower() == "s")]
                if not secPlsvAddDf.empty:
                    customQuery = {"lastname": {'search_string': row[hospital], 'bool_param': 'must', 'fuzziness': 1,"operator":"and","restrict":True},
                                       "state": {'search_string': secPlsvAddDf["OUT_ADDRESS_STATE"].iloc[0].upper(), 'bool_param': 'must', 'fuzziness': 0}}
                    bestMatch1 = elastic.gridLookup('hospital_list', "customQuery", 1,customQuery,**{'match_fields': ["lastname", "state"]})
                    if (bestMatch1 and bestMatch1["mpin"]):
                        result1 = bestMatch1["mpin"]
                        return result1
                    else:
                        return ErrorMessage([], ["C", "MPIN not found", "Hopital Name->" + str(row[hospital]) ])
                else:
                    return ErrorMessage([], ["C", "MPIN not found", "Hopital Name->" + str(row[hospital])])
            else:
                return ErrorMessage([], ["C", "MPIN not found", "Hopital Name->" + str(row[hospital])])
        else:
            return ErrorMessage([], ["C", "MPIN not found", "Hopital Name->" + str(row[hospital])])
    else:
        if row[primDeg] in constants.standardsDict[HospAffMandDeg]:
            return ErrorMessage([], ["C", "Hospital affiliation name is empty", ""])

def var_stateExceptions(row, state, exceptionalStates):
    # addDF = addDf.fillna('')
    if row[state].upper() in constants.standardsDict[exceptionalStates]:
        # return ErrorMessage([], ["C", "State comes under Exceptions states--> Effective Date", str(row[effectiveDate])])
        return "1"
    else:
        return ""

def var_hospInd(row, hospInd, hospital):
    if row[hospital]:
        if row[hospInd]:
            return row[hospInd]
        # elif row[derivedHospInd]:
        #     return row[derivedHospInd]
        elif row["ROW_NUM"] == 0:
            return "Primary"
        else:
            return "Secondary"
    else:
        return ""

def var_hospDir(row, svdf, ndb_spec, hospital):
    if row[hospital]:
        if str(svdf[ndb_spec][0]).strip() == "340":
        # hosplst ndb spec is 340
            return "N"
        else:
            return "Y"
    else:
        return ""

def var_hospAffStatus(row, hospAffStatus, hospital):
    if row[hospital]:
        if row[hospAffStatus].strip() == "active" or row[hospAffStatus].strip() == "ac" :
            return "AC"
        if row[hospAffStatus] and row[hospAffStatus].lower().strip() != "pending":
            bestMatch = elastic.gridLookup('hosp_aff_status', row[hospAffStatus], 2, **{'match_fields': ['abb']})
            if bestMatch and bestMatch["acronym"]:
                return bestMatch["acronym"]
            else:
                return "UNK"
        elif row[hospAffStatus].lower().strip() == "pending":
            return ErrorMessage([],
                                ["C", "hospital affliation status is pending.", "hospAffStatus->" + str(hospAffStatus)])
        else:
            return "AC"
    return ""
#####################################ADDDED BY ROHAN SHILPA's Fields Integration#####################

def var_getExtPhn1(row):
    return row["EXT_PHONE_1"]

def var_getExtPhn2(row):
    return row["EXT_PHONE_2"]

def var_getExtPhn3(row):
    return row["EXT_PHONE_3"]

def var_getExtPhn4(row):
    return row["EXT_PHONE_4"]

def var_fillPhn2(row, phn1, phn2):
    if not (row[phn1] and row[phn1] != ""):
        return phn1, row[phn2]
    else:
        return phn2, row[phn2]

def var_fillPhn3(row, phn1, phn2, phn3):
    if not (row[phn1] and row[phn1] != ""):
        return phn1, row[phn3]
    elif not (row[phn2] and row[phn2] != ""):
        return phn2, row[phn3]
    else:
        return phn3, row[phn3]

def var_fillPhn4(row, phn1, phn2, phn3, phn4):
    if not (row[phn1] and row[phn1] != ""):
        return phn1, row[phn4]
    elif not (row[phn2] and row[phn2] != ""):
        return phn2, row[phn4]
    elif not (row[phn3] and row[phn3] != ""):
        return phn3, row[phn4]
    else:
        return phn3, row[phn4]

def getFrstEmptyCol(row, phnCols):
    for col in phnCols:
        if not (row[col] and row[col] != ""):
            return col
    return None

def var_fillFaxCol(row, phn1, phn2, phn3, phn4, faxCol):
    col = getFrstEmptyCol(row, [phn1, phn2, phn3, phn4])
    if col:
        return col, row[faxCol]
    else:
        return phn4, str(row[phn4]) + ", " + str(row[faxCol])
#
# def var_fillFax(row, faxCol):
#     return "faxCol", "data"

def var_validatePhone(row, phn, phnIndex, addType):
    if row[phn] and row[phn] != "":
        symbols = ["(", ")", "-"," "]
        for sym in symbols:
            row[phn] = row[phn].replace(sym, "")
        cleanPhn = row[phn].strip()
        if len(cleanPhn) == 10:
            return cleanPhn[:3] + "-" + cleanPhn[3:6] + "-" + cleanPhn[6:10]
        elif row[phn].strip() == "":
            return ""
        else:
            return ErrorMessage([], ["C", "not equal to 10 digits", row[phn].strip()])
    elif row[addType].strip().lower() == "bill" and phnIndex == "1":
        # return ErrorMessage([], ["R", "Bill add type must have phone", row[phn] ])
        return ""
# def var_getPhnType(row):
#     return row["var_finalAddInd"]

# def var_getFinalAddInd(row, cleanAddInd):
#     return row[cleanAddInd]
#     # if row["ADDRESS_INDICATOR"] and  not str(row["ADDRESS_INDICATOR"]).isspace():
#     #     return row["ADDRESS_INDICATOR"]
#     # else:
#     #     return row["DERIVED_ADDRESS_INDICATOR"]
#
# def var_getFinalAddType(row):
#     if row["ADDRESS_TYPE"] and not str(row["ADDRESS_TYPE"]).isspace():
#         return row["ADDRESS_TYPE"]
#     else:
#         return row["DERIVED_ADDRESS_TYPE"]

def var_getAddress1(row):
    add1 = row["PARSED_ADDRESS"].split(";")[0]
    if not add1 or add1.isspace():
        return row["PARSED_ADDRESS"].split(";")[1]
    else:
        return row["PARSED_ADDRESS"].split(";")[0]

def var_getAddress2(row):
    add1 = row["PARSED_ADDRESS"].split(";")[0]
    if not add1 or add1.isspace():
        return ""
    else:
        return row["PARSED_ADDRESS"].split(";")[1]

def var_getCity(row):
    return row["PARSED_ADDRESS"].split(";")[2]

def var_fnlCity(row, parsedCity, parsedAddLine1, addCity):
    if row[addCity]:
        if row[parsedCity] == row[addCity]:
            return row[parsedCity]
        else:
            return row[addCity]
    else:
        return row[parsedCity]
    # bestMatch = elastic.gridLookup('us_cities', row[parsedCity], 0, **{'match_fields': ['city'], 'operator': 'and',
    #                                                                    'restrict': True})  # elastic.gridLookup('us_cities', row[parsedCity], 0, **{'match_fields': ["city"]})
    # if bestMatch:
    #     return row[parsedCity]
    # else:
    #     # row[parsedAddLine1] = ErrorMessage([], ["C", "Unparsed Address kindly parse manually", row[parsedAddLine1]])
    #     row[parsedAddLine1] = ErrorMessage([], ["C", "Couldn't parse Address City, Check Manually", "addressLine1 -->" + row[parsedAddLine1]])
    #     return ""

def var_getState(row):
    return row["PARSED_ADDRESS"].split(";")[3]

def var_fnlState(row, parsedState, parsedAddLine1, addCity):
    bestMatchShort = elastic.gridLookup('us_states', row[parsedState], 0,
                                   **{'match_fields': ["abbreviation"], 'operator': 'and', 'restrict': True})
    bestMatchFull = elastic.gridLookup('us_states', row[parsedState], 0,
                                        **{'match_fields': ["us_state"], 'operator': 'and', 'restrict': True})

    #In case City has been parsed as a state
    if row[parsedState] in row[addCity]:
        return ErrorMessage([], ["C", "Couldn't parse Address State", ""])

    if bestMatchShort:
        return row[parsedState]
    elif bestMatchFull:
        return bestMatchFull['abbreviation']
    else:
        if isErrorVal(row[parsedAddLine1]):
            return row[parsedState]
        else:
            return ErrorMessage([], ["C", "Couldn't parse Address State", row[parsedState]])
        # if 'Clarify' in row[parsedAddLine1]:
        #     # print row[parsedAddLine1].split(":")
        #     row[parsedAddLine1] = row[parsedAddLine1].split("-->")[1]
        #     row[parsedAddLine1] = ErrorMessage([], ["C", "Couldn't parse Address State and City, Check Manually", "addressLine1 -->" + row[parsedAddLine1]])
        # else:
        #     row[parsedAddLine1] = ErrorMessage([], ["C", "Couldn't parse Address State, Check Manually","addressLine1 -->" + row[parsedAddLine1]])
        # return ""

def var_getZipCode(row, parsedAdd):
    zip = row[parsedAdd].split(";")[4]
    if "-" in zip:
        return zip.split("-")[0]

    return row[parsedAdd].split(";")[4]

# def var_getAddPhn1(row):
#     return row["PARSED_ADDRESS"].split(";")[4]

# def var_isCredDateReq(row, specDF, addDF, primSpecInd, addInd, addType, specialty, state):
#     #Todo: read iscredRequired from RFP grid for spec and state combo
#     specDF = specDF.fillna('')
#     addDF = addDF.fillna('')
#
#     if not (addDF.empty or specDF.empty):
#         primSpec = specDF[specDF[primSpecInd] == "Primary"][specialty]
#         primPLSVAdd = addDF[(addDF[addInd].str.lower() == "Primary") &(addDF[addType].str.lower() == "plsv")][state]
#     elif not((primPLSVAdd1.empty and primSpec.empty)):
#         customQuery ={"specialty" : {'search_string' : primSpec.iloc[0],'bool_param':'must', 'fuzziness':1},
#                       "state" :{'search_string' : primPLSVAdd1.iloc[0].upper(),'bool_param':'must', 'fuzziness':0}}
#         bestMatch1 = elastic.gridLookup('rfp_grid', str(primSpec.iloc[0]) + " " + str(primPLSVAdd1.iloc[0]), 1,customQuery,**{'match_fields': ["state", "specialty"]})
#         if bestMatch1 and bestMatch1["credentialing_required"]:
#             result = bestMatch1["credentialing_required"]
#             if result.lower().find("yes") == -1:
#                 return "0"
#             else:
#                 return "1"
#
#     else:
#         return "0"
def var_isCredDateReq(row, primSpecCol, state):
    #Todo: read iscredRequired from RFP grid for spec and state combo
    # specDF = specDF.fillna('')
    # addDF = addDF.fillna('')
    primSpec = row[primSpecCol]
    primPLSVAdd = row[state]

    if not (primSpec or primPLSVAdd):
        # primSpec = row[primSpecCol]
        # primPLSVAdd = row[state]
        # if not (primPLSVAdd.empty or primSpec.empty):
        customQuery ={"specialty" : {'search_string' : primSpec,'bool_param':'must', 'fuzziness':1},
                      "state" :{'search_string' : primPLSVAdd,'bool_param':'must', 'fuzziness':0}}
        bestMatch = elastic.gridLookup('rfp_grid', " ", 1,customQuery,**{'match_fields': ["state", "specialty"]})
        if bestMatch and bestMatch["credentialing_required"]:
            result = bestMatch["credentialing_required"]
            if result.lower().find("yes") == -1:
                return "0"
            else:
                return "1"
        else:
            return "0"
        # else:
        #     return "0"
    else:
        return "0"

def enumerationDate2(row, enumDate):
    if row[enumDate]:
        return row[enumDate]
    else:
        return ErrorMessage([], ["C", "Empty enumeration date.", ""])

def var_fillReqDate(row):
    now = datetime.datetime.now()
    return str(now.month) + "/" + str(now.day) + "/" + str(now.year)

def var_ConcatAddress(row, addZip):
    row = row.fillna('')
    AddDict = {'address1': '', 'address2': '', 'city': '', 'state': '', 'zip_code': ''}
    # Concat_Address = ", ".join(row.fillna('')) #Todo: check with rohan
    AddDict['address1'] = row["ADDRESS_LINE_1"].strip()
    AddDict['address2'] = row["ADDRESS_LINE_2"].strip()
    AddDict['city'] = row["ADDRESS_CITY"].strip()
    AddDict['state'] = row["ADDRESS_STATE"].strip()
    AddDict['zip_code'] = row[addZip].strip()
    try:
        Concat_Address = row["ADDRESS_LINE_1"].strip() + "," + row["ADDRESS_LINE_2"].strip() + "," + row["ADDRESS_CITY"].strip() + "," + row["ADDRESS_STATE"].strip() + "," + row[addZip].strip()
    except:
        Concat_Address=""
    return str(AddDict)#Concat_Address

def var_lkpNdbTaxonomy(row, finalSpec):
    spec = row[finalSpec]
    if spec:
        result = lookupNDBTaxonomy(spec)
        #elastic.gridLookup('ndb_taxonomy', spec, 1, {}, **{'match_fields': ['prov_type_name']})
        if result is not None:
            specName = "Input: " + row[finalSpec] + "/" + "Output: " + result["prov_type_name"]
            s = result["ndb_spec"] + "|" + result["ndb_deg"] + "|" + result["cos_prov"] + "|" + result["cos_cred"] + "|" + specName + "|" + result["ur_ind"]
            return s
        else:
            return "" + "|" + "" + "|" + "" + "|" + "" + "|" + ""+ "|" + ""
    else:
        return "" + "|" + "" + "|" + "" + "|" + "" + "|" + ""+ "|" + ""

def var_urInd(row, primSpec):
    uriInd = row[primSpec]
    if uriInd:
        return uriInd.split("|")[5]

# def getNdbPvalue(row, colname):
#     return
    # return elastic.gridLookup('ndb_taxonomy', spec, 1)["ndb_spec"]
    # for key,value in ndb_p.iteritems():
    #     if key.strip() == spec.strip():
    #         return value["Ndb_spec"]

# def getNdbDegree(row, colname):
#     return
    # return elastic.gridLookup('ndb_taxonomy', spec, 1)["ndb_deg"]
    # for key,value in ndb_p.iteritems():
    #     if key.strip() == spec.strip():
    #         return value["Ndb Deg"]

def var_ndbSpec(row, colname,spec, inSot):
    row = row.fillna('')
    if row[inSot] == "Y":
        if len([x for x in row[colname].split("|") if x]) > 0:
            return row[colname].split("|")[0]
        else:
            return ErrorMessage([], ["c", "could not match speciality", row[spec]])
    else:
        return ErrorMessage([], ["c", "speciality not found", row[spec]])

def var_ndbSpecName(row, colname):
    row = row.fillna('')
    if len([x for x in row[colname].split("|") if x]) > 0:
        return row[colname].split("|")[4]
    else:
        return None

def var_cosCred(row, ndbLkp):
    row = row.fillna('')
    if len([x for x in row[ndbLkp].split("|") if x]) > 0:
        return row[ndbLkp].split("|")[3]
    else:
        return None

def var_cosProv(row, ndbLkp):
    row = row.fillna('')
    if len([x for x in row[ndbLkp].split("|") if x]) > 0:
        return row[ndbLkp].split("|")[2]
    else:
        return None

def var_degree(row, colname):
    row = row.fillna('')
    if len([x for x in row[colname].split("|") if x]) > 0:
        degVal = row[colname].split("|")[1]
        replace_punctuation = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
        text = str(degVal).translate(replace_punctuation).strip()
        degList = text.split()
        if len(degList) > 1:
            return ErrorMessage([], ["C", "More than one degree in NDB Grid", degVal])
        else:
            return degVal
    else:
        return None

def CreateDependenciesDf(masterDict):
    masterDict["Dependencies"] = pd.DataFrame()

def splitString(string, lstDelimsForSplit=constants.lstDelimsForHospSplit,exceptions=constants.hospitalDelimsExceptions):
    if any(ext in string for ext in exceptions):
        return [string]
    extSub = '|'.join(lstDelimsForSplit)
    regex = re.compile('(.+?)(?=' + extSub + ')(?:' + extSub + ')')
    return re.findall(regex, string)

# def splitHospitalString(string, lstDelimsForHospSplit=constants.lstDelimsForHospSplit, hospitalDelimsExceptions=constants.hospitalDelimsExceptions):
#     extSub = '|'.join(lstDelimsForHospSplit)
#     if any(ext in string for ext in hospitalDelimsExceptions):
#         return [string]
#     else:
#         regex = re.compile('(.+?)(?=' + extSub + ')(?:' + extSub + ')')
#         return re.findall(regex, string)

def normalizeAfterSplit(splitDf,splitCols):
    splitColSets = {}
    for splitCol in splitCols:
        splitColSets.update({splitCol: [col for col in list(splitDf.columns) if re.search(splitCol + '_[0-9]+', col)]})
    excludedCols = list(splitColSets.keys()) + [item for sublist in list(splitColSets.values()) for item in sublist]

    lstNormalizedRows = []
    for index, row in splitDf.iterrows():

        for i in range(max([len(value) for key, value in list(splitColSets.items())])):
            tdict = {key: value for key, value in list(dict(row).items()) if key not in excludedCols}
            tdict.update({key: None if pd.isnull(row.get(value[i] if len(value) > i else None)) else row.get(
                value[i] if len(value) > i else None) for key, value in list(splitColSets.items())})
            if len(value) == 1:
                lstNormalizedRows.append(tdict)
            elif [1 for key in list(splitColSets.keys()) if tdict[key] != None]:
                lstNormalizedRows.append(tdict)
    if lstNormalizedRows:
        dfNormalized = pd.DataFrame(lstNormalizedRows)
        return dfNormalized
    else:
        return splitDf

# def splitColumn(df,inputCol):
#     df = df.fillna('')
#     if not isinstance(inputCol,list):
#         splitCols=[inputCol]
#     else:
#         splitCols=inputCol
#     lstRows = []
#     for index, row in df.iterrows():
#         for col in splitCols:
#             if not row[col] or not isinstance(row[col],basestring):
#                 continue
#             if (col == "HOSPITAL_NAME"):
#                     splitColValues = splitHospitalString(row[col])
#                     # if len(splitColValues) > 1:
#                     # for idx, value in enumerate(splitColValues):
#                     #     row[col + '_' + str(idx)] = value
#             else:
#                     splitColValues = splitString(row[col])
#
#             for idx, value in enumerate(splitColValues):
#                 row[col + '_'+str(idx)] = value
#         lstRows.append(row)
#     splitDf = pd.DataFrame(lstRows)
#     return splitDf
#     # return normalizeAfterSplit(splitDf,splitCols)


def splitColumn(df,inputCol):
    df = df.fillna('')
    if not isinstance(inputCol,list):
        splitCols=[inputCol]
    else:
        splitCols=inputCol
    lstRows = []
    for index, row in df.iterrows():
        for col in splitCols:
            if not row[col] or not isinstance(row[col],str):
                continue
            splitColValues = splitString(row[col])
            for idx, value in enumerate(splitColValues):
                row[col + '_'+str(idx)] = value
        lstRows.append(row)
    splitDf = pd.DataFrame(lstRows)
    return splitDf

def extractIndicator(df,argDi):
    df = df.fillna('')
    tokens = []
    inputCol=argDi['inputCol']
    outputCol=argDi['outputCol']
    standardsKeys=argDi['standardsKeys']
    kwrd = ''
    for k, v in list(constants.standardsDict.items()):
        if k in standardsKeys:
            tokens += v
    strTokens = "(?:^|\s+?)(" + '|'.join(tokens) + ")(?:\s|$)"
    regex = re.compile(strTokens,re.IGNORECASE)
    for idx,row in df.iterrows():
        if not row[inputCol] or not isinstance(row[inputCol],str):
            continue
        try:
            kwrd=re.findall(regex,row[inputCol])
        except:
            print('row[inputCol]:',row[inputCol])
            continue
        if not kwrd:
            continue
        for k, v in list(constants.standardsDict.items()):
            if k in standardsKeys:
                if kwrd[0] in v:
                    df.loc[idx,outputCol]=k
                    break
    return df


def resetRowNumCount(df, prmlst):
    df["ROW_NUM"] = df.index
    df["ROW_COUNT"] = len(df.index)
    return df

def extractDayTime(line_string):
    # print line
    def func(line):
        capturedString = []
        groups = segmentWh(line)
        lst_groups = []
        for index, item in enumerate(groups):
            if not re.search('[0-9]+', item, re.IGNORECASE):
                lst_groups.append([item, '', ''])
            else:
                combined = item
                days = re.search('(.*?)([0-9].*$)', combined, re.IGNORECASE).group(1)
                time = re.search('(.*?)([0-9].*$)', combined, re.IGNORECASE).group(2)
                lst_groups.append([combined, days, time])

        for group in lst_groups:
            try:
                # print group[1]
                grpString=group[1].replace('&amp;', ',').strip()
                lstDaysIndex,captured = extractDays(grpString)
                capturedString.extend(captured)
            except:

                traceback.print_exc()
                continue
            # print lstDays
            group.append({"days": lstDaysIndex, "time": group[2]})

        lstG = []
        # print 'lst_groups:',lst_groups
        # exit(0)
        for group in lst_groups:
            try:
                # print 'group:',group
                diCleaned = {}
                diCleaned["line"] = group[0]
                diCleaned["daytime"] = group[3]
                lstG.append(diCleaned)
            except:
                continue
        return lstG,capturedString

    # lstG = {line:'MON:9am-10pm', daytime: {'days': [MON], time:'9am-10pm'}}
    lines = line_string.splitlines()

    finalLst=[]
    capturedString = []

    for line in lines:
        lst, captured = func(line)
        finalLst.extend(lst)
        capturedString.extend(captured)
    if finalLst==[]:
        finalLst=[{'line': line_string, 'daytime': {'days': [], 'time': ''}}]
    return finalLst, capturedString


def extractDays(DaysMatchedString):
    reComp = '|'.join(['\s+' + item + '\s+' for item in constants.expanders])
    reComp += '|' + '|'.join(['\s*' + item + '\s*(?!$)' for item in constants.expanders1])
    DaysMatchedString = DaysMatchedString.strip()
    if re.search(reComp, DaysMatchedString):
        lstDays = re.split(',', DaysMatchedString)
    else:
        lstDays = re.split(',|[ ]+', DaysMatchedString)
    lstDays = [item for item in lstDays if item]
    lstDaysIndex = []
    captured = []
    for day in lstDays:
        try:
            if not day:
                continue
            brk = 0
            for exp in constants.expanders + constants.expanders1:
                # print 'day:',day
                if exp in day:
                    dayIdx = [getKey(re.search(constants.wd, d, re.IGNORECASE).group(0).strip()) for d in
                              day.replace(',', '').split(exp) if d]
                    dayIdx.sort()
                    dayIdx = tuple(dayIdx)
                    # lstDaysIndex = [constants.diWeekDays[idx]['day'] for idx in range(dayIdx[0], dayIdx[1] + 1)]
                    lstDaysIndex.extend([constants.diWeekDays[idx]['day'] for idx in range(dayIdx[0], dayIdx[1] + 1)])
                    # lstDaysIndex += constants.diWeekDays[dayIdx[0]:dayIdx[1] + 1]
                    brk = 1
                    if dayIdx:
                        captured.append(DaysMatchedString)
                    break
            if brk:
                continue
            # idx = getKey(re.search(constants.wd, day, re.IGNORECASE).group(0).strip())
            # d=constants.diWeekDays[idx]['day']
            # lstDaysIndex.append(d)
            lstD = re.findall(constants.wd, day, re.IGNORECASE)
            for d in lstD:
                idx = getKey(d.strip())
                dkey = constants.diWeekDays[idx]['day']
                lstDaysIndex.append(dkey)
                captured.append(day)
        except:
            traceback.print_exc()
            continue
    return lstDaysIndex,captured



def captureTime(timeString):
    timeString=timeString.strip()
    diTime = {'startTime': "", 'endTime': ""}
    if not timeString:
        return diTime
    regex=re.compile(constants.regexTimeGroup,re.IGNORECASE)
    regex12hr=constants.regex12hr
    try:
        diTime['startTime'], diTime['endTime'] = re.search(regex, timeString).groups()
        if re.search(regex12hr,timeString):
            diTime['startTime'], diTime['endTime']='12:01am','11:59pm'
    except:
        print('\n*****************************error in capturing time from the string "{}"  *****************'.format(
            timeString))
        diTime['startTime'], diTime['endTime'] = 'error in capturing time','error in capturing time'
        pass
    return diTime

def getWorkHours(row, workHoursCol,diColMapping,workDayCol=None):
    inputWorkHours=row[workHoursCol].strip()
    inputWorkHours = standardiseWh(inputWorkHours)
    extracted,capturedString = extractDayTime(inputWorkHours)

    finalDi = {'workHours': {}}
    for dayTimeSet in extracted:
        dayTimeSet['daytime'].update(captureTime(dayTimeSet['daytime']['time'].strip()))

    regex=constants.regexTimePart
    empty_values = ['', '-']
    for idx, dayTimeSet in enumerate(extracted):
        for day in dayTimeSet['daytime']['days']:
            st=dayTimeSet['daytime']['startTime']
            et=dayTimeSet['daytime']['endTime']
            if (st and et):
                if len(re.findall(regex,st))==2 or len(re.findall(regex,et))==2:
                    finalDi['workHours'].update({day: (convertTimeFmt(str(st),'am-pm') , convertTimeFmt(str(et),'pm-pm'))})
                else:
                    finalDi['workHours'].update({day: (convertTimeFmt(str(st),'am') + '-' + convertTimeFmt(str(et),'pm'), '')})
            else:
                finalDi['workHours'].update({day: ('', '')})
            if not(finalDi['workHours'][day][0] in empty_values and finalDi['workHours'][day][1] in empty_values and not (st=='00' and et=='00')):
                capturedString.append(dayTimeSet['daytime']['time'])

    finalDi['line'] = row[workHoursCol]
    lstCapturedStrings = list(set(capturedString))
    flag = WHcapturedStringClarification(lstCapturedStrings,inputWorkHours)
    return str({"parsed": finalDi['workHours'], "clarification":flag})

def WHcapturedStringClarification(lstCapturedStrings, inputWorkHours):
    WhClrRegex = constants.regexWhClrString.format(constants.regexWhClrJoin.join([re.escape(i) for i in lstCapturedStrings]))
    inputWorkHours=re.sub(WhClrRegex,'',inputWorkHours,flags=re.IGNORECASE)

    return bool(re.search(r'[0-9a-z]+',inputWorkHours))


def getWorkHoursDayWise(row, day, index, drvWorkingHoursCol, drvSotWhCol):
    # print '**********came in getWorkHoursDayWise'
    # print '++++++++++++row,day,index,drvWorkingHoursCol,inpDayWH',row,day,index,drvWorkingHoursCol,inpDayWH
    try:
        if row[drvSotWhCol] and eval(row[drvSotWhCol])[day][index]:
            diWH = eval(row[drvSotWhCol])
            # print '*************row[inpDayWH] for {}'.format(inpDayWH),row[inpDayWH]
            # exit(0)
            return diWH[day][index]


        elif row[drvWorkingHoursCol]:
            diWH = eval(row[drvWorkingHoursCol])
            # print '*************diWH[day][index] for {}'.format(inpDayWH),diWH[day][index]
            # exit(0)
            return diWH[day][index]
    except:
        return ''

def getKey(wrd):
    for k,v in list(constants.diWeekDays.items()):
        if wrd.lower() in v['words']:
            return k

def getExtHrIndicator(row, diColmapping,_8to5Col):
    if isErrorVal(row[_8to5Col]):
        return row[_8to5Col]
    lenBlnk=0
    for col in constants.whColsExtHrInd:
        if not row[col].strip() or row[col] == '0':
            lenBlnk+=1
    wkEvening=0
    wkndEvening=0
    weekend=0
    extHrInd=''

    regexTime = re.compile(constants.regexExtHrInd)
    for startTime in constants.lstStartTime:
        try:
            if re.search(regexTime, str(row[startTime])):
                lstMatches = re.findall(constants.regexExtHrInd, str(row[startTime]))
                if len(lstMatches) == 2:
                    # tString,fmt=determineTimeFmt(lstMatches[1])
                    # _24hrFmtTime=formatTime(tString,fmt)
                    # if _24hrFmtTime:
                    #     if float(_24hrFmtTime.replace(':', '.')) > 17:
                    #         wkEvening+=1
                    floatTime=float('.'.join(lstMatches[1].split(':')[0:2]))
                    if floatTime > 5 and floatTime<12:
                        wkEvening += 1
        except:
            continue
    for startTime in ['OUT_SAT_START_TIME', 'OUT_SUN_START_TIME']:
        try:
            if re.search(regexTime, str(row[startTime])):
                weekend+=1
                lstMatches = re.findall(constants.regexExtHrInd, str(row[startTime]))
                if len(lstMatches) == 2:
                    # tString, fmt = determineTimeFmt(lstMatches[1])
                    # _24hrFmtTime = formatTime(tString, fmt)
                    # if _24hrFmtTime:
                    #     if float(_24hrFmtTime.replace(':', '.')) > 17:
                    #         wkndEvening+=1
                    floatTime=float('.'.join(lstMatches[1].split(':')[0:2]))
                    if floatTime > 5 and floatTime<12:
                        wkndEvening += 1
        except:
            continue
    if lenBlnk==len(constants.whColsExtHrInd):
        return ''
    if weekend and wkEvening:
        extHrInd='B'
    elif not weekend and wkEvening:
        extHrInd='E'
    elif weekend and not wkEvening:
        extHrInd='W'
    elif not weekend and not wkEvening:
        extHrInd='N'
    # print '****extHrInd:', extHrInd
    return extHrInd

def sortDf(df,sortParams):
    df=df.sort_values(by=eval(sortParams[0]),ascending=eval(sortParams[1]))
    df=df.reset_index(drop=True)
    return df


def updateRows(df,funcInp):
    filterCondition=eval(funcInp[0])
    colsToBeUpdated=eval(funcInp[1])
    updateValue=eval(funcInp[2])
    for col in colsToBeUpdated:
        if filterCondition is not None:
            df.loc[filterCondition,col]=updateValue
        else:
            df.loc[:, col] = updateValue
    return df

def reverseFilling(df,funcInp):
    colsToBeUpdated=eval(funcInp[2])
    algo=funcInp[1]
    print(colsToBeUpdated)
    def execute(df,value,col):
        if isinstance(value,dict):
            tupleCols=col
            if len(tupleCols) == len(list(value.keys())):
                for column, valueToBeFilled in list(value.items()):
                    df = updateRows(df, [funcInp[0], "['" + column + "']", "'" + valueToBeFilled + "'"])
        else:
            df = updateRows(df, [funcInp[0], "['" + col + "']", "'" + value + "'"])
        return df

    def lastSet(df,col):
        if isinstance(col,tuple):
            tupleCols=col
            diValues={}
            for column in tupleCols:
                for idx, row in df.iterrows():
                    # print 'idx+1 :', idx + 1 < df.shape[0]
                    if row[column] != '' and idx + 1 < df.shape[0]:
                        break
                    elif idx + 1 == df.shape[0]:
                        diValues[column]=row[column]
            return diValues
        else:
            for idx,row in df.iterrows():
                # print 'idx+1 :',idx+1<df.shape[0]
                if row[col]!='' and idx+1<df.shape[0]:
                    break
                elif row[col]!='' and idx+1==df.shape[0]:
                    valueToBeFilled=row[col]
                    return valueToBeFilled

    def lastAddrSet(df,col):
        value=lastSet(df,col)
        if value:
            return execute(df,value,col)
        return df

    def lastPLSVSet(df,col):
        df1 = df.loc[~df['DERIVED_ADDRESS_TYPE'].str.lower().isin(['bill', 'mailing',''])].sort_values(by=['order','ROW_NUM'])
        value = lastSet(df1, col)
        if value:
            return execute(df, value, col)
        return df

    diAlgo={"lastAddrSet":lastAddrSet,"lastPLSVSet":lastPLSVSet}

    for col in colsToBeUpdated:
        df=diAlgo[algo](*[df,col])

    return df



def comboCheck(df, funcInp):
    addTypeCol = funcInp[0]
    parsedAddCol = funcInp[1]
    addSeries = df[addTypeCol]
    if addSeries.unique().size == 1:
        if not [x for x in df[parsedAddCol].iloc[0].split("|") if x]:
            df[addTypeCol] = ErrorMessage([], ["R", "no combo, plsv and bill addresses are mandatory.", ""])

    if not "combo" in list(df[addTypeCol].fillna('').str.lower()):
        if not ( "plsv" in list(df[addTypeCol].fillna('').str.lower()) and "bill" in list(df[addTypeCol].fillna('').str.lower()) ):
            df[addTypeCol] = ErrorMessage([], ["R", "no combo, plsv and bill address are mandatory.", ""])

    return df
    #if combo remove billing address
def markDeleteRow(df, funcInp):
    addTypeCol = funcInp[0]
    ignoreMail = funcInp[1]
    ignoreCred = funcInp[2]
    addLine1 = funcInp[3]
    addLine2 = funcInp[4]

    df["DELETE_ROW"] = None
    if "combo" in list(df[addTypeCol].fillna('').str.lower()):
        comboDF = df[df[addTypeCol].fillna('').str.lower() == "combo"]
        df[(df[addLine1].isin(comboDF[addLine1])) & (df[addLine2].isin(comboDF[addLine2])) & (df[addTypeCol].str.lower() == "bill")]["DELETE_ROW"] = "Y"
        # df[df[addTypeCol] == "Bill"]["DELETE_ROW"] = "Y"
        # df.loc[df[addTypeCol].str.lower() == "bill", "DELETE_ROW"] = "Y"
    else:
        df.loc[df[addTypeCol].str.lower() == "bill", "DELETE_ROW"] = "N"

    for idx,row in df.iterrows():
        if row[addTypeCol].lower() != 'bill':
            if row[ignoreMail] == 'Y':
                df.loc[idx,"DELETE_ROW"] = 'Y'
            elif row[ignoreCred] == 'Y':
                df.loc[idx, "DELETE_ROW"] = 'Y'
            else:
                df.loc[idx, "DELETE_ROW"] = 'N'
    return df

def markRowForDelete(df, funcInp):
    df["DELETE_SPEC_ROW"] = ''
    specInd = funcInp[0]
    ndbLkp = funcInp[1]
    superNdbLkp = funcInp[2]

    # tdf = df[df[specInd].str.lower() == 'secondary']
    counter=-1
    for idx, row in df.iterrows():
        counter+=1
        if row[specInd].lower() == 'secondary':
            for i, r in df.iterrows():
                rowndbLkp = row[ndbLkp].split("|")[0] if len(row[ndbLkp].split("|")) > 1 else None
                rndbLkp = r[ndbLkp].split("|")[0] if len(r[ndbLkp].split("|")) > 1 else None
                rsuperNdbLkp = r[superNdbLkp].split("|")[0] if len(r[superNdbLkp].split("|")) > 1 else None
                if i != idx and ((rowndbLkp and rndbLkp and rowndbLkp == rndbLkp) or (rowndbLkp and rsuperNdbLkp and rowndbLkp == rsuperNdbLkp)):
                    df['DELETE_SPEC_ROW'].iloc[counter] = 'Y'
                    break

    return df

def deleteBillRow(df, funcInp):
    delRowCol = funcInp[0]
    df = df[df[delRowCol] != "Y"]
    df = df.reset_index()
    return df

def extractDefaultWorkHours(colName):
    dtTup=('','')
    lst=re.findall('(?:default\s*to\s*)(.*?$)', colName, re.IGNORECASE)
    if lst:
        extracted,captured=extractDayTime(lst[0])
        if captured:
            dtTup=[(','.join(item['daytime']['days']), item['daytime']['time']) for item in extracted if captured[0] in item['line']][0]
        else:
            dtTup=[(','.join(item['daytime']['days']), item['daytime']['time']) for item in extracted if (item['daytime']['time'])][0]
    return dtTup

def standardizeColumn(df,argDi):
    df = df.fillna('')
    # if argDi.get('outputCol') and argDi.get('createOutputCol'):
    #     df[argDi.get('outputCol')]=''
    lstInputCol = argDi['inputCol']
    standardsKeys = argDi['standardsKeys']
    filteredStdKeys={k:v for k,v in list(constants.standardsDict.items()) if k in standardsKeys}
    for idx, row in df.iterrows():
        for inputCol in lstInputCol:
            outputCol=argDi.get('outputCol',inputCol)
            # print 'inputCol:',inputCol
            if not row[inputCol] or not isinstance(row[inputCol], str):
                df.loc[idx, outputCol] = row[inputCol]
                continue
            for stdKey,lst in list(filteredStdKeys.items()):
                row[inputCol]=re.sub('\\b'+'\\b|\\b'.join(lst)+'\\b',stdKey,row[inputCol],flags=re.IGNORECASE)
            df.loc[idx, outputCol] = row[inputCol]
    return df

def var_addHospRow(df, svDF):
    df = df.fillna('')
    provType = svDF['PROVIDER_TYPE'][0]
    newRowIdx = int(df.index.values.max()) + 1

    if provType.lower() == "hosp":
        df.loc[newRowIdx, "SPECIALITY"] = "hospitalist"
        df.loc[newRowIdx, "SPECIALITY_INDICATOR"] = "S"
        df = df.fillna('')

    return df

def intCols(df, argDi):
    df = df.fillna('')
    inpCollst = argDi['standardsKeys'][0]
    dfname = argDi['dfname']
    inpCollst = constants.standardsDict[inpCollst]
    for col in inpCollst:
        if col in df:
            for ch in ['-', '(', ')']:
                df[col] = df[col].apply(str).fillna('').str.replace(ch, "")
            try:
                df[col] = df[col].astype(int).astype(str)
            except:
                print("int conversion failed", "col->", col, "df->", dfname, "values->", df[col])
                continue

    return df

def intCols(df, argDi):
    df = df.fillna('')
    inpCollst = argDi['standardsKeys'][0]
    dfname = argDi['dfname']
    inpCollst = constants.standardsDict[inpCollst]
    for col in inpCollst:
        if col in df:
            for ch in ['-', '(', ')']:
                df[col] = df[col].apply(str).fillna('').str.replace(ch, "")
            try:
                df[col] = df[col].astype(int).astype(str)
            except:
                print("int conversion failed", "col->", col, "df->", dfname, "values->", df[col])
                continue

    return df

def shufflePhoneNo(row, lstCols, addType):
    # print lstCols
    di1 = {}
    lstIndex = [idx for idx, tupCol in enumerate(lstCols[0:4]) if row[tupCol[0]]]
    toBeFilled = lstCols
    for idx, tupCols in enumerate(toBeFilled[0:len(lstIndex)]):
        di1[tupCols[0]] = row[toBeFilled[lstIndex[idx]][0]]
        if lstIndex[idx]!=0 and list(range(len(lstIndex)))!=lstIndex:
            di1[toBeFilled[lstIndex[idx]][0]]=''
            di1[toBeFilled[lstIndex[idx]][1]]=''
            di1[toBeFilled[lstIndex[idx]][2]]=''
        di1[tupCols[1]] = "Bill" if row[addType].lower() == "cred" else row[addType]
        try:
            di1[tupCols[2]] = int(float(str(row[toBeFilled[lstIndex[idx]][2]])))
        except:
            di1[tupCols[2]] = str(row[toBeFilled[lstIndex[idx]][2]])
    if lstIndex and re.search('[0-9]+',str(row[toBeFilled[-1][0]])):
        di1[toBeFilled[len(lstIndex)][0]] = row[toBeFilled[-1][0]]
        di1[toBeFilled[len(lstIndex)][1]] = 'Fax'
    for key in [item[1] for item in lstCols]:
        if key not in di1:
            di1.update({key: None})
    for key in [item[2] for item in lstCols]:
        if key not in di1:
            di1.update({key: None})
    return list(di1.keys()), list(di1.values())

def combineSotDayWiseWorkHours(row,diSotWH):
    diCombined = {}
    empty_values = ['', '-']
    flagclr = False
    combinedString = ''
    regex = constants.regexTimePart
    for day,tup in list(diSotWH.items()):
        st = row[tup[0]].strip()
        et = row[tup[1]].strip()

        if st or et:
            combinedString = combinedString + day + ': ' + st + ' ' + et + ' '

        st = standardiseWh(st)
        et = standardiseWh(et)

        if re.search(constants.regex247, st, re.IGNORECASE):
            st = constants.time247

        if st and not et:
                diCombined.update({day: (convertTimeFmt(str(st), 'am-pm'), "")})

        elif (st and et):
            if len(re.findall(regex,st))==2 or len(re.findall(regex,et))==2:
                diCombined.update({day: (convertTimeFmt(str(st),'am-pm') , convertTimeFmt(str(et),'pm-pm'))})
            else:
                diCombined.update({day: (convertTimeFmt(str(st),'am') + '-' + convertTimeFmt(str(et),'pm'), '')})
        else:
            diCombined.update({day: ('', '')})
        if flagclr == False:
            if diCombined[day][0] in empty_values and diCombined[day][1] in empty_values and not (
                    st == '00' and et == '00') and not (st == '' and et == ''):
                flagclr = True  # True indicates clarification is required

    return str({"parsed": diCombined, "clarification": flagclr, "combinedString": combinedString})


def deriveCorrespondanceInd(df,funcInp):
    addTypeColName, addIndColName,drvCorresIndColName=funcInp[0],funcInp[1],funcInp[2]
    df[drvCorresIndColName]=''
    df.loc[:,drvCorresIndColName]='S'
    df.loc[df[addTypeColName].str.lower() == 'bill', drvCorresIndColName] = 'P'
    if True in df[addTypeColName].str.lower().isin(['mail', 'cred', 'combo']).tolist():
        df.loc[df[addTypeColName].str.lower().isin(['cred', 'combo']) & (df[addIndColName].str.lower() == 'p'), drvCorresIndColName] = 'P'
        df.loc[df[addTypeColName].str.lower().isin(['mail']), drvCorresIndColName] = 'P'
        # df.loc[df[addTypeColName].str.lower()==, drvCorresIndColName] = 'P'
        # df.loc[df[addTypeColName].str.lower() == 'combo', drvCorresIndColName] = 'P'
    else:
        df.loc[(df[addTypeColName].str.lower() == 'plsv') & (df[addIndColName].str.lower() == 'p'), drvCorresIndColName] = 'P' #var_getAddressIndicator added Mail
    return df

def fillCorrespondanceInd(df,funcInp):
    drvCorresIndColName,sotCorresIndColName,outCorresIndColName=funcInp[0],funcInp[1],funcInp[2]
    if df['ADDRESS_LINE_1'][0]:
        for idx, row in df.iterrows():
            if row[sotCorresIndColName]:
                df.loc[idx,outCorresIndColName]=row[sotCorresIndColName]
            else:
                df.loc[idx,outCorresIndColName] = row[drvCorresIndColName]
    else:
        df[outCorresIndColName] = ""
    return df

def fillAddField(df, cols):
    inpCol, svdf, addType, addInd, outputCol = cols

    val = svdf[inpCol][0]
    indexes = df[((df[addType].str.lower() == "plsv") & (df[addInd].str.lower() == "p")) | (
    df[addType].str.lower() == "combo")].index.values

    df.loc[indexes, outputCol] = val
    # df[ ( (df[addType] == "PLSV") & (df[addInd] == "Primary") ) | (df[addInd] == "Combo") ]

    return df

def fillLoc(df, svdf):
    cols = ["FNL_MEDICAID_LOCATION", svdf, "OUT_ADDRESS_TYPE", "OUT_ADDRESS_IND", "OUT_MEDICAID_LOCATION" ]
    return fillAddField(df, cols)

def fillMedicaid(df, svdf):
    cols = ["FNL_MEDICAID", svdf, "OUT_ADDRESS_TYPE", "OUT_ADDRESS_IND", "OUT_MEDICAID"]
    return fillAddField(df, cols)

def fillTaxonomy(df, svdf):
    cols = ["FNL_TAXONOMY", svdf, "OUT_ADDRESS_TYPE", "OUT_ADDRESS_IND", "OUT_TAXONOMY"]
    return fillAddField(df, cols)

def confirmTaxID(df, taxId):
    if "blank".upper() in df[taxId]:
        return ""
    else:
        return df[taxId]


def getSingleValue(df, funcInp):
    svDf = funcInp
    df["TABNAME"] = list(svDf["TABNAME"]) * df.shape[0]
    df["FILENAME"] = list(svDf["FILENAME"]) * df.shape[0]
    return df


def associateCommCredContDf(df,funcInp):
    commDF, addTypeCol = funcInp, "OUT_ADDRESS_TYPE"
    commDF = pd.DataFrame(commDF[[c for c in commDF.columns if "OUT_CRED" in c]].iloc[0]).T
    extendCommDF = "extendCommDF"
    fltrLst = ["bill", "combo"]
    df[extendCommDF] = "0"
    commDF[extendCommDF] = "1"
    credAddDf = df[df[addTypeCol].str.lower() == "cred"]
    if credAddDf.empty:
        df.loc[df[addTypeCol].str.lower().isin(fltrLst), extendCommDF] = "1"
    else:
        df.loc[df[addTypeCol].str.lower() == "cred", extendCommDF] = "1"
    fnlDF = pd.merge(df, commDF, how='outer', on=extendCommDF).fillna('')
    return fnlDF

def validateLength(df,funcInp):
    df=df.fillna('')
    col, size, type, excepCol = funcInp
    if excepCol!=col:
        df[excepCol]=''
    for idx, row in df.iterrows():
        if isErrorVal(row[col]):
            continue
        if type == "max":
            # if df['check_taxId'].iloc[0] == '':
            #     df.loc[idx, col] = ErrorMessage([], ["C", "Tax Id not given " + "", df['check_taxId'].iloc[0]])
            if row[col] == '':
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            elif row[col] and (len(str(row[col])) > int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size greater than " + size, row[col]])
        elif type == "eql":
            if row[col] and not (len(str(row[col])) == int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size not eql to " + size, row[col]])
    return df

def validateLengthPsi(df,funcInp):
    col, size, type, excepCol = funcInp['params']
    filter=0
    if funcInp.get('filter'):
        filter=1
    if excepCol!=col:
        df[excepCol]=''
    for idx, row in df.iterrows():
        if isErrorVal(str(row[col])):
            continue
        if filter:
            if all([True if row[k] not in v else False for k,v in list(funcInp.get('filter').items())]):
                continue
        if type == "max":
            if row[col] == '':
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            elif row[col] and (len(str(row[col])) > int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["E", "size greater than " + size, row[col]])
        elif type == "eql":
            if row[col] == '':
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            elif row[col] and not (len(str(row[col])) == int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["E", "size not eql to " + size, row[col]])
    return df

def clarifyFields(df, col):
    clarifyCol = col[0]
    try:
        for idx, row in df.iterrows():
            if row[clarifyCol] == '' or str(row[clarifyCol]).isspace() or row[clarifyCol] is None or row[clarifyCol] == '00000':
                df.loc[idx, col] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        return df
    except:
        return df

# def clarifyDegree(df, col):
#     try:
#         if df[col[0]].empty:
#             df[col[0]] = ErrorMessage([], ["C", "Field not given " + "", ""])
#             return df
#         else:
#             return df
#     except:
#         return df
# {"name": "clarifyDegree","type": "var","input": ["OUT_DEGREE_PRIMARY_IND"]}


def processCombinedAddTypeInd(df,argDi):
    df = df.fillna('')
    inputCol = argDi['inputCol']
    standardsKeys = argDi['standardsKeys']
    filteredStdKeys = {k: v for k, v in list(constants.standardsDict.items()) if k in standardsKeys}
    flag = 0
    for idx, row in df.iterrows():
        # print 'inputCol:',inputCol
        if not row[inputCol] or not isinstance(row[inputCol], str):
            continue
        for stdKey, lst in list(filteredStdKeys.items()):
            if re.search('\\b' + '\\b|\\b'.join(lst) + '\\b', row[inputCol], re.IGNORECASE):
                df.loc[idx, argDi['outputCol'][0]] = 'PLSV'
                df.loc[idx, argDi['outputCol'][1]] = stdKey
                flag = 1
                break
        if not flag:
            df.loc[idx,argDi['outputCol'][0]] = row[inputCol]
    return df

def applyRegex(df,argDi):
    regexKey, inpColName=argDi['regexKey'],argDi['inputCol']
    for idx,row in df.iterrows():
        for regx in constants.regexDict[regexKey]:
            results=re.findall(regx, str(row[inpColName]))
            if results:
                df.loc[idx, inpColName]= results[0]
                # return df
            else:
                continue
    return df

def var_compareProvType(row,svDf, drvProvTypeColName):
    return svDf[drvProvTypeColName][0]
    # if svDf[sotProvTypeColName][0] and svDf[sotProvTypeColName][0]!=row[drvProvTypeColName]:
    #     if svDf[sotProvTypeColName][0].lower()=='pcp':
    #         print 'clarification'
    #         return ''
    #     else:
    #         return svDf[sotProvTypeColName][0]
    # else:
    #     return row[drvProvTypeColName]

def convertTimeFmt(string,ampm=''):
    # if len(ampm.split('-'))==2:
        regex=constants.regexTimePart
        lst=re.findall(regex,string,re.IGNORECASE)
        # print "zip(lst,ampm.split('-')):",zip(lst,ampm.split('-'))
        lstConverted=[]
        for tString,ampm in zip(lst,ampm.split('-')):
            string,fmt=determineTimeFmt(tString,ampm)
            _24hrTString=formatTime(string,fmt)
            if _24hrTString:
                ts=datetime.datetime.strptime(_24hrTString,"%H:%M")
                lstConverted.append(ts.strftime("%I:%M%p"))
            else:
                lstConverted.append('')
        return '-'.join(lstConverted)



def determineTimeFmt(string,ampm='pm'):
    ampmRegex=constants.ampmRegex
    string=string.replace('.',':')
    lstTimeParts=re.split(r'[\:]',string)
    if len(lstTimeParts)==3:
        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I:%M:%S%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I:%M:%S%p'
                string = string + ampm
            else:
                fmt = '%H:%M:%S'
    elif len(lstTimeParts)==2:
        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I:%M%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I:%M%p'
                string=string+ampm
            else:
                fmt = '%H:%M'
    elif len(lstTimeParts) == 1:
        string_temp = lstTimeParts[0]

        if len(string_temp) > 2 and not re.search(ampmRegex,string_temp,re.IGNORECASE):
            if len(string_temp) % 2 != 0:
                string_temp = '0' + string_temp

            # string_temp = str(datetime.datetime.now().date()) + " " + string_temp
            # string_temp = str(parser.parse(string_temp).time())

            for i in range(2, len(string_temp), 2):
                string_temp = string_temp[:i] + ":" + string_temp[i:]

            string, fmt = determineTimeFmt(string_temp, ampm)
            return string, fmt

        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I%p'
                string=string+ampm
            else:
                fmt = '%H'
    return string,fmt

def formatTime(string,fmt):
    ampmRegex=constants.ampmRegex
    if not string or not re.search('[0-9]+',str(string)):
        return ''
    try:
        if re.search(ampmRegex,string,re.IGNORECASE):
            # print '**',re.sub('\s+','',string),fmt
            t=datetime.datetime.strptime(re.sub('\s+','',string), fmt)
            return t.strftime('%H:%M')
        elif not re.search(ampmRegex,string,re.IGNORECASE):
            # print '**', re.sub('\s+', '', string), fmt
            t = datetime.datetime.strptime(re.sub('\s+','',string), fmt)
            return t.strftime('%H:%M')
    except:
        return ''

def adoptOrphanSets(dictMVMappings,pCat,adopter):
    dictMVMappingsCopy=copy.deepcopy(dictMVMappings)
    di=dictMVMappingsCopy[pCat][0]
    lol=dictMVMappingsCopy[pCat][1]
    idxOrphan=determineOrpanSet(lol)
    counterAdopter=lol[adopter][0].split('@')[2]
    typeAdopter=lol[adopter][0].split('@')[0]
    outpuColsAdopter=[elem.split('@')[1] for elem in lol[adopter]]
    modifiedOrphan=[(e,typeAdopter+'@'+e.split('@')[1]+'@'+counterAdopter) for e in lol[idxOrphan] if e.split('@')[1] not in outpuColsAdopter]
    if len(lol)>=2 and idxOrphan is not None:
        lol[adopter]=lol[adopter]+ [elem1 for elem0,elem1 in modifiedOrphan]
    for k,v in list(di.items()):
        if v in dict(modifiedOrphan):
            di[k]=dict(modifiedOrphan)[v]
            print(k,dict(modifiedOrphan)[v])
    lol.pop(idxOrphan)
    return dictMVMappingsCopy


def determineOrpanSet(lol):
    for idx,l in enumerate(lol):
        if not len([item for item in l if 'ADDRESS_LINE_1' in item]):
            return idx

def getBreakHours(row, breakHoursCol,diColMapping):
    lst=[]
    for strr in row[breakHoursCol].split():
        if not re.search('[0-9]\s*am|[0-9]\s*pm|\bto\b|\bthru\b',strr,re.IGNORECASE) and re.search('^[a-zA-Z\-]+$',strr) and not re.search(constants.wd,strr,re.IGNORECASE):
            pass
        else:
            lst.append(strr)
    strr=' '.join(lst)
    row[breakHoursCol] = strr
    breakHours=getWorkHours(row, breakHoursCol,diColMapping)
    return str(breakHours)


def subtractBreakHours(row, timeRangeDiColName, breakHoursColName):
    officeHours = eval(row[timeRangeDiColName])
    breakHours = eval(row[breakHoursColName])
    for k, v in list(officeHours.items()):
        if breakHours.get(k) and v[0]:
            lst1 = officeHours[k][0].split('-')
            lst2 = breakHours[k][0].split('-')
            if len(lst1) == 2 and len(lst2) == 2:
                officeHours[k] = (lst1[0] + '-' + lst2[0], lst2[1] + '-' + lst1[1])
    return str(officeHours)

def separatePhNo(row,lstPhNoColNames):
    concated='|'.join(row[col] for col in lstPhNoColNames)
    lstPhNo=re.split('/|\||;',concated)
    diPhNo=dict(list(zip(lstPhNoColNames,lstPhNo)))
    return list(diPhNo.keys()),list(diPhNo.values())

def separateIntoCols(row,lstPhNoColNames):
    concated='|'.join(row[col] for col in lstPhNoColNames)
    lstPhNo=re.split('\-|\||;',concated)
    diPhNo=dict(list(zip(lstPhNoColNames,lstPhNo)))
    return list(diPhNo.keys()),list(diPhNo.values())

def formatDate(df,inputCol):
    if not isinstance(inputCol, list):
        cols = [inputCol]
    else:
        cols = inputCol
    def fmt(x):
        try:
            dt=datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y/%m/%d')
        except:
            return x
    for col in cols:
        df[col]=df[col].map(lambda x:fmt(x))
    return df

def cleanAgeLimit(df, inpCol, outputCol, searchKeys, val, type):
    for idx,row in df.iterrows():
        numVal = re.findall(r'\d+', row[inpCol])
        for key in searchKeys:
            if row[inpCol] and key in row[inpCol].lower():
                if len(numVal) >= 1:
                    try:
                        if type == "below":
                            df.loc[idx, outputCol] = val + "-" + numVal[0]
                        else:
                            df.loc[idx, outputCol] = numVal[0] + "-" + val
                    except:
                        traceback.print_exc()
                    break
                else:
                    df.loc[idx, outputCol] = ""
            else:
                if not outputCol in row.index.values:
                    df.loc[idx,outputCol] = row[inpCol]
    return df

def belowAgeLimit(df,argDi):
    inpCol = argDi['inputCol']
    outputCol = argDi['outputCol']
    val = argDi['val']
    below = argDi['standardsKeys'][0]
    below = constants.standardsDict[below]

    return cleanAgeLimit(df, inpCol, outputCol, below, val, "below")

def aboveAgeLimit(df,argDi):
    inpCol = argDi['inputCol']
    outputCol = argDi['outputCol']
    val = argDi['val']
    abv = argDi['standardsKeys'][0]
    abv = constants.standardsDict[abv]

    return cleanAgeLimit(df, inpCol, outputCol, abv, val, "above")

def nullAgeLimits(df,argDi):
    inpCol = argDi['inputCol']
    val = argDi['val']
    blankAgeLimits = argDi['standardsKeys'][0]
    blankAgeLimits = constants.standardsDict[blankAgeLimits]

    for key in blankAgeLimits:
        df[inpCol] = df[inpCol].replace({key: val})

    return df

def lookupSpecialtyExceptions(spec,deg):
    replace_punctuation = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
    res=elastic.gridLookup('specialty_exceptions', spec, 1, {},
                       **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
    if not res:
        spec = re.sub("[\(\[].*?[\)\]]", "", spec).strip()
        res = elastic.gridLookup('specialty_exceptions', spec, 1, {},
                                 **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
        if not res:
            CompleteIndex = elastic.gridLookup('specialty_exceptions', '', 0, {}, **{'match_all':True})
            lstOfIndexString = [str(x["_source"]["specialty_name_in_sot"]).translate(replace_punctuation).strip().split() for x in CompleteIndex["hits"]["hits"]]
            newSpec = ''
            found = 0
            for x in lstOfIndexString:
                llen = len(x)
                count = 0
                for wrd in x:
                    if wrd not in spec:
                        break
                    else:
                        count += 1
                if count == llen:
                    found = 1
                    specText = str(spec).translate(replace_punctuation).strip()
                    specTokens = specText.split()
                    for t in specTokens:
                        if t in x:
                            newSpec = newSpec + ' ' + t
                    break
            if found:
                res = elastic.gridLookup('specialty_exceptions', newSpec, 1, {}, **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})

    if isinstance(res,list):
        for di in res:
            # replace_punctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
            text = str(di['degree']).translate(replace_punctuation).strip()
            degList = text.split()
            if deg==di['degree']:
                return di
            elif deg in degList:
                return di
            else:
                for dg in degList:
                    if dg.lower() in deg.lower():
                        return di

    else:
        return res

def lookupNDBTaxonomy(spec):
    res1= elastic.gridLookup('ndb_taxonomy', spec, 1, {}, **{'match_fields': ['prov_type_name'], 'operator': 'and', 'restrict': True})
    spec = re.sub("[\(\[].*?[\)\]]", "", spec).strip()
    res2 = elastic.gridLookup('ndb_taxonomy', spec, 1, {}, **{'match_fields': ['prov_type_name'], 'operator': 'and', 'restrict': True})
    specRes1 = res1['prov_type_name'] if res1 else ""
    specRes2 = res2['prov_type_name'] if res2 else ""

    spec1Dist = edit_distance(spec, specRes1) if specRes1 else 9999
    spec2Dist = edit_distance(spec, specRes2) if specRes2 else 9999

    if spec1Dist <= spec2Dist and res1:
        return res1
    elif res2:
        return res2
    else:
        return None


def getCleanDegree(df,degreeDf):
    try:
        degForLkp=degreeDf.loc[degreeDf["DEGREE_FINAL_INDICATOR"]=="Primary","CLEAN_DEGREE"].iloc[0]
        if degForLkp:
            df['degForLkp']=degForLkp
        else:
            df['degForLkp'] = ""
    except:
        df['degForLkp'] = ""
    return df

def readCombinedBoardInfo(df,argDi):
    infoType = argDi['standardsKeys']
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    def extractInfo(inpStr,outStr,regex):
        if outStr:
            return outStr
        if inpStr:
            try:
                return re.findall(regex,inpStr,re.IGNORECASE)[0]
            except:
                return ""

    if infoType in ["Effective","Expire"]:
        regex="(?:\\b{}\\b)".format('\\b|\\b'.join(constants.standardsDict[infoType])) + ".*?(\\d{1,2}/\\d{1,2}/(?:\\d{4}|\\d{2}))"
        for idx,row in df.iterrows():
            df.loc[idx,outCol]=extractInfo(row[inpCol],row[outCol],regex)
    return df

def extractNthInfo(df,argDi):
    df=df.fillna('')
    inpCol=argDi["inputCol"]
    outCol=argDi["outputCol"]
    i=eval(argDi["index"])
    for idx,row in df.iterrows():
        if row[inpCol]:
            print('row[inpCol]:',row[inpCol])
            lst=re.split('&|,',row[inpCol])
            df.loc[idx,outCol]=lst[i]
    return df

def adoptOrphanColumns(dictMVMappings,pCat,lstOrphCols):
    dictMVMappingsCopy = copy.deepcopy(dictMVMappings)
    di = dictMVMappingsCopy[pCat][0]
    lol = dictMVMappingsCopy[pCat][1]
    diOrphCol={}
    for tup in lstOrphCols:
        flag=0
        tempDiOrphCol={}
        for col in tup:
            lstMatch=[item.split('@')[0]+'@'+item.split('@')[2] for sublist in lol for item in sublist if col==item.split('@')[1]]
            # lstMatch=['BILL@1','MAILING@2']
            if re.search('PLSV|GENERAL',' '.join(lstMatch),re.IGNORECASE):
                flag=1
                break
            if lstMatch:
                tempDiOrphCol.update({col:lstMatch})
        if not flag:
            diOrphCol.update(tempDiOrphCol)
    lstTemp=[(lst[0].split('@')[0],int(lst[0].split('@')[2]),idx) for idx,lst in enumerate(lol) if re.search('PLSV|GENERAL',lst[0].split('@')[0],re.IGNORECASE)]
    sorted(lstTemp,key=lambda x: x[1],reverse=True)
    if len(lstTemp) == 0:
        return dictMVMappings
    else:
        adopterSet = lstTemp[0]

    deltaDiValues={}
        # deltaDiValues.update({item.split('@')[0]+k+item.split('@')[1]:adopterSet[0]+k+str(adopterSet[1]) for item in v})
    for k,v in list(diOrphCol.items()):
        for item in v:
            oldName=item.split('@')[0]+'@'+k+'@'+item.split('@')[1]
            newName=adopterSet[0]+'@'+k+'@'+str(adopterSet[1])
            deltaDiValues.update({oldName:newName})
            lol[adopterSet[2]].append(newName)
            idxOrphanLst=[idx for idx,lst in enumerate(lol) if lst[0].split('@')[2]==item.split('@')[1]]
            if idxOrphanLst:
                lol[idxOrphanLst[0]].remove(oldName)
    # lol=[lst for lst in lol if lst]
    if [] in lol:
        lol.remove([])
    for k,v in list(di.items()):
        if v in deltaDiValues:
            di[k]=deltaDiValues[v]
    return dictMVMappingsCopy


def getNewPatientHeader(df,diColMapping):
    lst=[v for k,v in list(diColMapping.items()) if  k=='NEW_PATIENTS']
    if lst:
        df.loc[:,'newPatientHeader']=lst[0]
    else:
        df.loc[:, 'newPatientHeader'] = ''
    return df

def standardiseNewPatients(df,argDi):
    df=df.fillna('')
    stdKey = argDi['standardsKeys']
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    headerCol=argDi['header']
    newPatients = constants.standardsDict[stdKey]
    df.loc[:, outCol]=''
    def removeNewLine(string):
        return re.sub('\r|\n', ' ',string).lower().strip()
    def compress(string):
        return re.sub('\s+','',string)
    for idx,row in df.iterrows():
        if not row[headerCol]:
            break
        if row[inpCol].lower() =='y':
            for k,v in list(newPatients['Y'].items()):
                if [1 for elem in v[0] if compress(removeNewLine(row[headerCol]))==compress(elem)]:
                    df.loc[idx,outCol]=k
        elif row[inpCol].lower()=='n':
            for k,v in list(newPatients['N'].items()):
                if [1 for elem in v[0] if compress(removeNewLine(row[headerCol])) == compress(elem)]:
                    df.loc[idx, outCol] = k
        elif row[inpCol]=='':
            for k,v in list(newPatients['Y'].items()):
                if [1 for elem in v[0] if compress(removeNewLine(row[headerCol])) == compress(elem)]:
                    df.loc[idx, outCol] = k
        elif row[inpCol]:
            for k,v in list(newPatients['Y'].items()):
                if [1 for elem in v[1] if compress(removeNewLine(row[inpCol])) == compress(elem)]:
                    df.loc[idx, outCol] = k
    return df

def dropEmptyRows(df, diColMapping):
    return df#.dropna(axis=0, subset=["COMM_CONT_TYPE", "COMM_FAX_NUMBER", "COMM_EXTENSION_NUMBER", "COMM_PHONE_NUMBER", "COMM_NAME"], how='all')


def getCommCredTypeHeader1(df, diColMapping):
    lst=[v for k,v in list(diColMapping.items()) if 'COMM_CONT_TYPE@0' in k]
    if lst:
        df.loc[0,'commCredTypeHeader']=lst[0]
    else:
        df.loc[0, 'commCredTypeHeader'] = ''
    return df

def getCommCredTypeHeader(df, diColMapping):
    orderCol = "CommCredCont_order"

    for idx, row in df.iterrows():
        currOrder = str(row[orderCol]).strip()
        contTypeCol = 'COMM_CONT_TYPE@' + currOrder
        lst=[v for k,v in list(diColMapping.items()) if contTypeCol in k]
        if lst:
            df.loc[idx,'commCredTypeHeader']=lst[0]
        else:
            df.loc[idx, 'commCredTypeHeader'] = ''
    return df

def standardiseCommCredTypeHeaders(df,argDi):
    df=df.fillna('')
    outCol=argDi['outputCol']
    headerCol=argDi['header']
    df[outCol]=''

    if df[headerCol].empty:
        df[outCol] = "R"
        return df

    for idx, row in df.iterrows():
        headerName = df[headerCol][idx].lower().strip()
        bestMatch = elastic.gridLookup('comm_types', headerName, 1, **{'match_fields': ["comm_type_abb"]})
        if bestMatch:
            commTypeAcrnm = bestMatch["comm_type_acrnm"]
            df.loc[idx, outCol] = commTypeAcrnm

    return df

def standardiseCommCredTypeValues(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    df[outCol] = ''
    for idx, row in df.iterrows():
        if len(row[inpCol].strip()) > 1:
            bestMatch = elastic.gridLookup('comm_types', row[inpCol], 1, **{'match_fields': ["comm_type_abb"]})
            if bestMatch:
                df.loc[idx, outCol] = bestMatch["comm_type_acrnm"]
            else:
                df.loc[idx, outCol] = row[inpCol].strip()
        else:
            df.loc[idx, outCol] = row[inpCol].strip()

    return df

def fnlCommCredTypeValues(df, argDi):
    drvHeadersCol = argDi['drvHeaders']
    drvValuesCol = argDi['drvValues']
    outputCol = argDi['outputCol']
    sotCol = argDi['sotCol']

    df[outputCol] = df[drvHeadersCol]
    for idx, row in df.iterrows():
        if not row[drvHeadersCol]:
            if not row[drvValuesCol]:
                df.loc[idx, outputCol] = "R"
            else:
                df.loc[idx, outputCol] = row[drvValuesCol]
        elif row[drvValuesCol] and row[drvHeadersCol] != row[drvValuesCol]:
            df.loc[idx, outputCol] = ErrorMessage([], ["C", "Predicted and Sot COMM_TYPE values are different.", "Predicted -> " + row[drvHeadersCol][idx] + " Sot Value -> " + row[sotCol][idx]])
        else:
            df.loc[idx, outputCol] = row[drvHeadersCol]

    return df


def getCommExtnFrmPhn(df, argDi):
    df = df.fillna('')
    stdKey = argDi['standardsKeys']
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    phnCol = inpCol
    extWrds = constants.standardsDict[stdKey]
    df[outCol] = ''

    for idx, row in df.iterrows():
        lst = [wrd for wrd in extWrds if wrd.lower() in row[inpCol].lower()]
        if lst:
            str1 = re.findall(lst[0].lower() + "(.*)", row[inpCol])
            ext = re.findall("[0-9]+", str1[0])
            phn = re.findall(".+?(?=" + lst[0].lower() + ")", row[inpCol])
            df.loc[idx, outCol] = ext[0]
            df.loc[idx, phnCol] = phn[0]
        else:
            df.loc[idx, outCol] = ""

    return df

def standardisePSIndi(row,inputCol,applyList):
    for stdKey in applyList:
        if re.search('\\b' + '\\b|\\b'.join(constants.standardsDict[stdKey]) + '\\b', row[inputCol], re.IGNORECASE):
            if stdKey.lower() == "primary":
                return "P"
            else:
                return "S"
    return row[inputCol]

def var_handicapAccess(row, handicapAccess):
    if row[handicapAccess]:
        return row[handicapAccess]

def var_groupName(row, groupName):
    if row[groupName]:
        return row[groupName]
    else:
        return ErrorMessage([], ["C", "Group name is not given kindly check", ""])

def var_nameOfLegalOwner(row, legalOwner):
    if row[legalOwner]:
        return row[legalOwner]

def getVerSource(row,boardNameCol):
    if row[boardNameCol]=='':
        return "PRV"
    elif row[boardNameCol]:
        customQuery={'board_name': {'bool_param': 'must', 'operator': 'and', 'fuzziness': 1, 'search_string': row[boardNameCol]}}
        elasticRes=elastic.gridLookup('ver_source','','',customQuery=customQuery)
        if elasticRes:
            return elasticRes['ver_source']

def var_getCredContactType(row, col, idx, df, diColMapping):
    if [k for k, v in diColMapping.items() if "COMM_CONT_TYPE"+ "@"+str(idx) in k]:
        if row["ROW_NUM"] == 0:
            if (idx in df.index) and (df.iloc[int(idx)][col]):
                return df.iloc[int(idx)][col]
            else:
                return "R"
        else:
            return ""
    else:
        return ""

def var_getCredName(row,name, idx, df, diColMapping):
    if [k for k, v in diColMapping.items() if "COMM_NAME" + "@" + str(idx) in k]:
        if row["ROW_NUM"] == 0:
            if (idx in df.index) and (df.iloc[int(idx)][name]):
                return df.iloc[int(idx)][name]
            else:
                return ErrorMessage([], ["C", "Contact name is empty.", ""])
        else:
            return ""
    else:
        return ""

def var_getCredCommunication(row, phnCol, faxCol, idx, df,diColMapping):
    if [k for k, v in diColMapping.items() if "COMM_FAX_NUMBER" + "@" + str(idx) in k or "COMM_PHONE_NUMBER" + "@" + str(idx) in k]:
        if row["ROW_NUM"] == 0:
            if (idx in df.index) and (df.iloc[int(idx)][phnCol]):
                return "P"
            elif (idx in df.index) and (df.iloc[int(idx)][faxCol]):
                return "F"
            else:
                return ErrorMessage([], ["C", "phone and fax are empty.", ""])
        else:
            return ""
    else:
        return ""

def var_getCredPhoneNumber(row, phnCol, faxCol, idx, df, diColMapping):
    if [k for k, v in diColMapping.items() if "COMM_FAX_NUMBER" + "@" + str(idx) in k or "COMM_PHONE_NUMBER" + "@" + str(idx) in k]:
        if row["ROW_NUM"] == 0:
            if (idx in df.index) and (df.iloc[int(idx)][phnCol]):
                return df.iloc[int(idx)][phnCol]
            elif (idx in df.index) and (df.iloc[int(idx)][faxCol]):
                return df.iloc[int(idx)][faxCol]
            else:
                return ErrorMessage([], ["C", "phone and fax are empty.", ""])
        else:
            return ""
    return ""

# def var_getCredFaxNumber(row, faxCol, phnCol):
#     if row[faxCol]:
#         return row[faxCol]
#     elif row[phnCol]:
#         return ErrorMessage([], ["C", "phone and fax are empty.", ""])


def var_getCredExtNumber(row, extNo, dervExtNo, idx, df,diColMapping):
    if [k for k, v in diColMapping.items() if "COMM_EXTENSION_NUMBER" + "@" + str(idx) in k]:
        if row["ROW_NUM"] == 0:
            if (idx in df.index) and (df.iloc[int(idx)][extNo]):
                return df.iloc[int(idx)][extNo]
            elif (idx in df.index) and (df.iloc[int(idx)][dervExtNo]):
                return df.iloc[int(idx)][dervExtNo].rjust(5, "0")
            else:
                return ""
        else:
            return ""
    else:
        return ""
# def getCommContactTypeHeader(df,diColMapping, commTypeCol):
#     lst=[v for k,v in diColMapping.items() if  k==commTypeCol]
#     if lst:
#         df.loc[:,'commContactTypeHeader']=lst[0]
#     else:
#         df.loc[:, 'commContactTypeHeader'] = ''
#     return df
def emptyDF(df, input):
    return pd.DataFrame()

def fnameMnameMap(df,diColMapping):
    df['drvFnameMnameMap']=0
    if 'NAME_FIRST' in  list(diColMapping.keys()):
        df.loc[:,'drvFnameMnameMap']=1
    return  df

def get_pra(row):
    return "MI"

def var_pcpReAssign(row, provType, derivedPcpInd, pcpReAssign):
    if row[provType]:
        if row[provType] == "PCP":
            if row[pcpReAssign] == '' or str(row[pcpReAssign]).isspace() or row[pcpReAssign] is None:
                return ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
            else:
                return row[pcpReAssign]
    elif row[derivedPcpInd] == "PCP":
        if row[pcpReAssign] == '' or str(row[pcpReAssign]).isspace() or row[pcpReAssign] is None:
            return ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        else:
            return row[pcpReAssign]
    else:
        return row[pcpReAssign]

def var_mapReasonCode(row, rsnCode, rsnDesc):
    if row[rsnCode]:
        return row[rsnCode]
    elif row[rsnDesc]:
        bestMatch = elastic.gridLookup('reason_desc_code', row[rsnDesc], 1, **{'match_fields': ['rsn_description']})
        if bestMatch:
            return bestMatch["primary_reason_code"]
        # how to return spell correct reason description
        else:
            return "21"
    else:
        return "21"

def var_mapReasonDescription(row, reasonDesc):
    if row[reasonDesc]:
        return row[reasonDesc]
    else:
        return ""

def var_action(row, action):
    return row[action]

def var_changeType(row, changeType, standardizeChange):
    if row[standardizeChange]:
            return row[standardizeChange].upper()
    elif row[changeType]:
        return row[changeType]
    else:
        return ErrorMessage([], ["C", "Please provide change type" + "", ""])

# if __name__=='__main__':
#     print elastic.gridLookup('hosp_aff_status', "deferred admitting privileges", 2, **{'match_fields': ['abb']})
#     string="Reid Hospital & Health Care Services,  Reid Health"
#     print splitHospitalString(string)
#     print splitString(string)


def hospSpecialityCode(svdf,funcInp):
    primSupSpecHBP, isHospSecSpec, isHospPrimSpec, isMidLevel, midLevSupSpec, provType,midLeveSupSpecName = funcInp
    addSpecColInd = "ADD_SPECIALITY_COL"
    svdf[addSpecColInd] = 'N'
    getHospNdb = var_getHospNDB()
    ndbSpecCode = getHospNdb['ndb_spec']
    ndbSpecName = getHospNdb['prov_type_name']
    if svdf[provType][0].lower() == "hosp":
        if (svdf[primSupSpecHBP][0] )== "Y": # or supervising spec is HBP
            pass
        elif (svdf[isHospSecSpec][0] == "Y") or (svdf[isHospPrimSpec][0] == "Y"):
            pass
        elif (svdf[isMidLevel][0] == "Y") and ((isErrorVal(svdf[midLevSupSpec][0])) or (not svdf[midLevSupSpec][0])):#check for clarification midlevesuperspec, midlevel = y
            svdf[midLevSupSpec][0] = ndbSpecCode
            svdf[midLeveSupSpecName][0] = ndbSpecName
        elif (svdf[isMidLevel][0] == "Y") and ((not isErrorVal(svdf[midLevSupSpec][0])) and svdf[midLevSupSpec][0]):
            pass
        elif svdf[isMidLevel][0] == "N":
            svdf[addSpecColInd] = "Y"
    return svdf

def var_checkSpecHosp(row,specDf,fltr,finalSpec, hospSpecName, primIndColName):
    for index, rw in specDf.iterrows():
        if rw[finalSpec].lower() == hospSpecName and rw[primIndColName] == fltr:
            return "Y"
    return "N"

def var_isPrimSpecSupSpecHBP(row,primSpec,finalSupSpec,isMidLevel):
    primSpecialty = row[primSpec]
    finalSupSpeciality = row[finalSupSpec]
    if primSpecialty or finalSupSpeciality:
        primSpecBestMatch = isPresentInHBP(primSpecialty)#elastic.gridLookup('hbp_speciality', primSpec, 0,**{'match_fields': ['specialty_code']})
        supSpecBestMatch = isPresentInHBP(finalSupSpeciality) #elastic.gridLookup('hbp_speciality', finalSupSpec, 0,**{'match_fields': ['specialty_code']})
        if row[isMidLevel]:
            return supSpecBestMatch
        else:
            return primSpecBestMatch
    return "N"

def var_getHospNDB():
    # if spec.lower()=="hospitalist":
    result = lookupNDBTaxonomy("hospitalist")
    return result

def addSpecRow(specDF,svdf):
    if svdf['ADD_SPECIALITY_COL'][0] == "Y":
        ndbSpecCode = var_getHospNDB()['ndb_spec']
        specLoc = max(specDF.index) + 1
        specDF.loc[specLoc, "OUT_NDB_SPEC_NAME"] = "hospitalist"
        specDF.loc[specLoc, "OUT_PRIMARYSPECIALITY_SPECIALITYCODE"] = ndbSpecCode
        specDF.loc[specLoc, "OUT_SPECIALITY_PRIMARY_IND"] = "Secondary"
    return specDF

def isErrorVal(str):
    return bool(re.search("reject\:|error\:|clarify\:", str, re.IGNORECASE))


def finalCosmosNum(svdf,funInp):
    cosmosNum,provType,primSupSpecHBP,isSecSpecHosp, pcpInd = funInp
    cosProvCode = var_getHospNDB()['cos_prov']
    if svdf[provType][0].lower() == "hosp":
        if svdf[primSupSpecHBP][0] == "Y":
            pass  # Todo: validate that cosprov will be empty if prim or super spec is HBP and prov type is Hosp
        else:
            svdf[cosmosNum][0] = cosProvCode     # Todo: check if this is fine or elastic lookup
    elif svdf[provType][0] == "" and svdf[pcpInd][0] == "pcp":
        if svdf[isSecSpecHosp][0] == "Y":
            svdf[cosmosNum][0] = cosProvCode #Todo: check if this is fine or elastic lookup
    return svdf

def removeRowDfPsi(df, qry):
    if qry[0] != "":
        df = df.query(qry[0])
    if df.empty:
        df=createOneRowDf(df.columns)

    df = df.reset_index(drop=True)
    return df.head(1)

# def getFinalDfPsi(masterDictList):
#     fdf = pd.DataFrame()
#     for masterDict in masterDictList:
#         colsToBeRemovedOthers = ['index', 'ROW_COUNT', 'ROW_NUM', 'level_0']
#         colsToBeRemovedFD = []
#         colsToBeRemovedSV = []
#         colsToBeRemovedBrd = colsToBeRemovedOthers + ['degForLkp']
#         masterDict = {k: v for k, v in masterDict.items() if k in ['FinalDegree', 'singleValue', 'Address','Speciality']}
#         dfs = [v.drop(colsToBeRemovedFD, axis=1) if k == 'FinalDegree'
#         else v.drop(colsToBeRemovedSV,axis=1) if k == 'singleValue'
#         else v.drop(colsToBeRemovedBrd,axis=1, errors='ignore') if k == 'Board'
#         else v.drop(colsToBeRemovedOthers, axis=1, errors='ignore') for k, v in masterDict.items()]
#         record = pd.concat(dfs, axis=1)
#         record = record.sort_index(axis=1)
#         fdf=fdf.append(record)
#     return fdf

def lkpSpecPsi(df, argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    placeHolder=argDi['placeHolder']
    i=-1
    df[outCol]=''
    for idx,row in df.iterrows():
        i+=1
        spec=row[inpCol]
        if spec:
            if isErrorVal(spec):
                s=spec
            else:
                result = elastic.gridLookup('spec_psi', spec, 1, {}, **{'match_fields': ['spec'], 'operator': 'and', 'restrict': True})
                if result is not None:
                    s = result["spec"]
                else:
                    s=ErrorMessage([], ["c", "could not match speciality", spec])
        else:
            s=placeHolder
        df.iloc[i,df.columns.get_loc(outCol)]=s
    return df

def getPractionerType(df,argDi):
    df = df.fillna('')
    name_first=argDi['inputCol'][0]
    name_last=argDi['inputCol'][1]
    name_group=argDi['inputCol'][2]
    outputCol=argDi['outputCol']
    df[outputCol]=''
    df.loc[(df[name_first]!='') & (df[name_last]!=''),outputCol]='P'
    df.loc[(((df[name_first]=='') | (df[name_last]=='')) & ~((df[name_first]=='') & (df[name_last]=='')))|((df[name_first]=='') & (df[name_last]=='') & (df[name_group]!='')),outputCol]='G'
    return df

def extractTaxonomyCode(df,argDi):
    def extract(inp):
        ptn = '(?=\\b[a-z0-9]{9}[x]\\b)((?:[a-zA-Z]+[0-9]|[0-9]+[a-zA-Z])[a-zA-Z0-9]*)'
        lstMatch=re.findall(ptn,inp,re.IGNORECASE)
        return lstMatch[0] if lstMatch else inp
    df[argDi['outputCol']]=df[argDi['inputCol']].apply(lambda x: extract(x))
    return df

def accessDeg(df,degDf):
    outputCol='var_isMidLevelDeg'
    df[outputCol]=''
    lstDegInd=list(degDf.loc[degDf['OUT_DEGREE_PRIMARY_IND'] == 'P'][outputCol])
    if lstDegInd:
        df[outputCol]=lstDegInd[0]
    return df

def accessSv(df,svDf):
    outputCol='var_finalSupervisorSpecialty'
    df[outputCol]=list(svDf[outputCol])[0]
    return df
def mergeSuperSpec(df,argDi):
    spec=argDi["inputCol"][0]
    specInd=argDi["inputCol"][1]
    degMidLevelInd=argDi["inputCol"][2]
    superSpec=argDi["inputCol"][3]
    outputCol=argDi['outputCol']
    df[outputCol]=df[spec]
    i=-1
    for idx,row in df.iterrows():
        i+=1
        if row[specInd].lower() in ['primary'] and row[degMidLevelInd]=='Y'and row[superSpec]:
            df.iloc[i,df.columns.get_loc(outputCol)]=row[superSpec]
        elif row[specInd].lower() in ['primary'] and row[degMidLevelInd] == 'Y' and not row[superSpec]:
            df.iloc[i, df.columns.get_loc(outputCol)] = ErrorMessage([], ["c", "supervising speciality is not found", ""])
    return df

def changeDegreeInd(df,argDi):
    inputCol=argDi['inputCol']
    outputCol=argDi['outputCol']
    df.loc[df[inputCol].str.lower().isin(['primary']),outputCol]='P'
    return df

def parseNameFromFullNameCol(row, fName, mName, lName, deg, diColMapping):
    if isErrorVal(row[lName]):
        return ""
    if not fName in list(diColMapping.keys()):
        parsedNameDict = parseName(row[lName], fName, mName, lName, deg)
        return json.dumps(parsedNameDict)
    else:
        return ""

def fillFrstMidLstNames(row, parsedName, col):
    if row[parsedName]:
        parsedName = json.loads(row[parsedName])
        return parsedName[col]
    return row[col]


def fillGroupName(df,argDi):
    df = df.fillna('')
    name_first=argDi['inputCol'][0]
    name_last=argDi['inputCol'][1]
    name_group=argDi['inputCol'][2]
    prac_type=argDi['inputCol'][3]
    outputCol=argDi['outputCol']
    placeHolder=argDi['placeHolder']
    df[outputCol]=''
    df.loc[(df[prac_type]=='G')& (df[name_group]!=''),outputCol]=df[name_group]
    df.loc[(df[prac_type]=='G')& (df[name_group]=='')&(df[name_first]!=''),outputCol]=df[name_first]
    df.loc[(df[prac_type]=='G')& (df[name_group]=='')&(df[name_last]!=''),outputCol]=df[name_last]
    df.loc[(df[prac_type] == 'G'), [name_first,name_last]] = placeHolder['G']
    df.loc[(df[prac_type] == 'P'), outputCol] = placeHolder['P']
    return df

def removePunctuation(text,lstIgnore=[]):
    ign = re.escape("".join(lstIgnore))
    punct=re.sub(ign,'',string.punctuation)
    replace_punctuation = str.maketrans(punct, ' ' * len(punct))
    text = str(text).translate(replace_punctuation).strip()
    return text

def cleanName(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    lstIgnore=['(',')']
    def removeAdditionalInfo(string):
        noAdd=re.sub(r'(\(.*?\))','',string)
        noPunct=removePunctuation(noAdd)
        if string!=noAdd:
            return ErrorMessage([], ["c", "the name '{}' has additional information".format(noPunct), string])
        else:
            return noPunct
    df[outCol]=df.apply(lambda row:removeAdditionalInfo(row[inpCol]),axis=1)
    return df

def lkpDegreePsi(df, argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    def lookup(inputDegree):
        if not inputDegree:
            s = ErrorMessage([], ["c", "Degree Is Empty", inputDegree])
        else:
            result = elastic.gridLookup('degree_psi', inputDegree, 0, {},
                                        **{'match_fields': ['code'], 'operator': 'and', 'restrict': True})
            if result is not None:
                s = result["code"]
            else:
                s = ErrorMessage([], ["c", "could not match degree", inputDegree])
        return s

    df[outCol] = df.apply(lambda row: lookup(row[inpCol]), axis=1)
    return df

def standardAbbreviation(clean_text, abbreviate_dict):
    for k, v in abbreviate_dict.items():
        clean_text = re.sub('\\b' + '\\b|\\b'.join(v) + '\\b', k, clean_text, flags=re.IGNORECASE)
    return clean_text

def abbreviate(text, max_len):
    stop_words = stopwords.words('english') + list(string.punctuation)
    stop_words.extend(["&"])
    text_len = len(text)
    if text_len > max_len:
        # step 1 - remove extra spaces
        clean_text = text.strip()
        while '  ' in clean_text:
            clean_text = clean_text.replace('  ', ' ')
        if len(clean_text) <= max_len:
            return clean_text.upper()

        # step added - abbreviate from standards
        abbreviate_dict = constants.standardsDict["ABBREVIATIONS"]
        stdText = standardAbbreviation(clean_text, abbreviate_dict)

        # step 2 - remove stopwords
        word_tokens = word_tokenize(stdText)
        no_stop_word_text_list = [w for w in word_tokens if not w in stop_words]
        no_stop_word_text = " ".join(no_stop_word_text_list)
        if len(no_stop_word_text) <= max_len:
            return no_stop_word_text.upper()

        # step 3 - remove vowels
        vowels = ['a', 'e', 'i', 'o', 'u']
        vowel_remove_text_list = []
        for cl in no_stop_word_text_list:
            new_cl = cl[0] + ''.join(l for l in cl[1:] if l not in vowels)
            vowel_remove_text_list.append(new_cl)
        vowel_remove_text = " ".join(vowel_remove_text_list)
        if len(vowel_remove_text) <= max_len:
            return vowel_remove_text.upper()

        # step 4 - truncate long words
        word_tokens_new = word_tokenize(vowel_remove_text)
        word_tokens_trunc = word_tokens_new[:]
        count = len(word_tokens_new)
        n = 5
        # print vowel_remove_text

        # print "************************"
        # print word_tokens_new
        # print word_tokens_trunc
        # print "************************"

        while count > 0 and n >= 1:
            long_word = max(word_tokens_trunc, key=len)
            long_word_short = long_word[:n]
            word_tokens_new = [w.replace(long_word, long_word_short) for w in word_tokens_new]
            word_tokens_trunc = word_tokens_new[:]
            trunc_text = " ".join(word_tokens_new)
            # print "trunc text is :", trunc_text
            if len(trunc_text) <= max_len:
                return trunc_text.upper()
            word_tokens_trunc.remove(long_word_short)
            count = count - 1
            if count == 0:
                n = n - 1
                count = len(word_tokens_new)
                word_tokens_trunc = word_tokens_new[:]
            # print "************************"
            # print word_tokens_new
            # print word_tokens_trunc
            # print "************************"
        if n == 0:
            return ErrorMessage([], ["c", "TOO LONG FOR ACRONYM", text])
    else:
        return text

def abbreviateCol(df,argDi):
    df = df.fillna('')
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    maxLength = int(argDi['maxLength'])
    df[outCol]=df.apply(lambda row:abbreviate(row[inpCol], maxLength),axis=1)
    return df

def effectiveDatePsi(df,argDi):
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    def diffInYears(dateString):
        d0 = datetime.datetime.strptime(dateString, '%m/%d/%Y').date()
        d1 = datetime.datetime.today().date()
        # return relativedelta(d1, d0)
        return (d1-d0).days
    def func(dateString1, dateString2):
        dateString = dateString1 if dateString1 else dateString2
        if not isErrorVal(dateString):
            if dateString:
                diff=diffInYears(dateString)
                if diff >= 365:
                    out=ErrorMessage([], ["c", "effective date is past one year", dateString])
                elif diff < -90:
                    out = ErrorMessage([], ["c", "effective date is after 90 days", dateString])
                else:
                    out=dateString
            else:
                out=ErrorMessage([], ["c", "effective date is missing", dateString])
        else:
            out=dateString
        return out

    df[outCol]=df.apply(lambda row:func(row[inpCol[0]], row[inpCol[1]]), axis=1)
    return df

def standardisePcpInd(df,argDi):
    inputCol = argDi['inputCol']
    standardsKey = argDi.get('standardsKeys')
    outputCol = argDi.get('outputCol')
    diRegex={k:"^"+"$|^".join(v)+"$" for k,v in list(constants.standardsDict[standardsKey].items())}
    def match(inpStr):
        for k, v in list(diRegex.items()):
            if re.search(v, inpStr.strip(), re.IGNORECASE):
                return k
    def func(inpStr):
        if not inpStr:
            return match("P")
        else:
            key=match(inpStr)
            if key:
                return key
            else:
                return ErrorMessage([], ["c", "PCP Indicator is non blank but could not be standardized", inpStr])

    df[outputCol]=df.apply(lambda row:func(row[inputCol]),axis=1)
    return df

def parseName(name, fName, mName, lName, deg):
    parsedName = HumanName(name)
    return {fName: parsedName['first'], mName: parsedName['middle'], lName: parsedName['last'], deg: parsedName['suffix']}

def determineDuplicates(df, argDi):
    df = df.reset_index()
    levels = argDi['levels']
    fields = argDi['fields']
    msgCol = argDi['msgCol']
    clrMsg = argDi['clarification']

    grouped = df.groupby(levels).nunique().reindex()
    query = ['{}>1'.format(elm) for elm in fields]
    query = " or ".join(query)
    lstConflictLevels = list(grouped.query(query).index)

    lstFilters = [dict(list(zip(tuple(levels), lvl))) if isinstance(lvl, tuple) else dict(list(zip(tuple(levels), (lvl,)))) for lvl
                  in lstConflictLevels]
    for di in lstFilters:
        df.loc[(df[list(di)] == pd.Series(di)).all(axis=1), msgCol] = df[msgCol].apply(
            lambda x: ErrorMessage([], ["C", clrMsg, str(x)]))
    df = df.set_index('index')
    return df

def updateRowsPsi(df,funcInp):
    filterCondition=eval(funcInp.get('filterCondition','None'))
    colsToBeUpdated=funcInp['cols']
    updateValue=funcInp.get('replaceWithValue')
    updateCondition=funcInp.get('replaceWithCondition')
    updateWithCol=funcInp.get('replaceWithCol')
    clar=funcInp.get('clar')

    for col in colsToBeUpdated:
        if col not in list(df.columns):
            df[col]=''
        if filterCondition is not None:
            if clar:
                df.loc[filterCondition, col] = ErrorMessage([], [clar, updateValue, ""])

            else:
                df.loc[filterCondition,col]=updateValue
        else:
            if updateCondition is not None:
                resolvedDf=df.loc[eval(updateCondition)]
                if not resolvedDf.empty:
                    df.loc[:, col] = resolvedDf.iloc[0][updateWithCol]
            else:
                df.loc[:, col] = updateValue
    return df


def isException(text,lstExceptions=('UNKNOWN SPECIAL PHYSICIAN','reject:','error:','clarify')):
    regex="|".join([re.escape(item) for item in lstExceptions])
    return bool(re.search(regex, text, re.IGNORECASE))

def prioritizeColValues(text,priorityList=(('reject:','error:','clarify:'),('UNKNOWN SPECIAL PHYSICIAN',))):
    for idx,tup in enumerate(priorityList):
        regex="|".join([re.escape(item) for item in tup])
        if bool(re.search(regex, text, re.IGNORECASE)):
            return idx+2
    return 1
def conditionallyDeduplicate(df,argDi):
    subst=argDi['levels']
    priorityCol=argDi['priorityCol']
    df['priority']=df[priorityCol].apply(lambda x:prioritizeColValues(x))
    subst1=subst+['priority']
    df.sort_values(subst1,inplace=True)
    df.drop_duplicates(subst,keep='first',inplace=True)
    return df.sort_index()


def createOneRowDf(cols):
    df = pd.DataFrame([{col: None for col in cols}])
    df["ROW_COUNT"]=0
    df["ROW_NUM"]=0
    return df

def lookupDegreeExceptions(degForLkp):
    res = elastic.gridLookup('degree_exceptions', degForLkp, 0, {},
                             **{'match_fields': ['degree_in_sot'], 'operator': 'and', 'restrict': True})
    return res


def var_isMidLevelSpecPsi(row, finalSpec):
    specval = row[finalSpec]
    if len(specval.split()) > 1:
        cq = {'prov_type_name': {'search_string': specval, 'bool_param': 'must', 'fuzziness': 1},
              'is_mid_level': {'search_string': 'Y', 'bool_param': 'must', 'fuzziness': 0}}
    else:
        cq = {'prov_type_name': {'search_string': specval, 'bool_param': 'must', 'fuzziness': 1, 'operator': 'and',
                                 'restrict': True},
              'is_mid_level': {'search_string': 'Y', 'bool_param': 'must', 'fuzziness': 0, 'operator': 'and'}}
    bestMatch = elastic.gridLookup('', '', 0, customQuery=cq)
    if bestMatch:
        return "Y"
    else:
        return "N"


def var_getDegreeCredTitlePsi(row, degreeCol):
    return row[degreeCol]


def var_isMidLevelDegPsi(row, lkpdegree):
    if row[lkpdegree]:
        val = row[lkpdegree].split("|")[0]
        return val

def var_finalSpecPsi(row, inSot,sotExSpec, specCol):
    if row[inSot] == "Y":
        if row[sotExSpec]:
            return row[sotExSpec]
        else:
            return row[specCol]
    else:
        return ""

def var_ndbSpecIsMidLvl(row, colname, inSot):
    row = row.fillna('')
    if row[inSot] == "Y":
        if len([x for x in row[colname].split("|") if x]) > 0:
            return row[colname].split("|")[1]
        else:
            return ""
    else:
        return ""

def var_getFinalDegreePsi(row, degreeCredTitle):
    return row[degreeCredTitle]

def var_lkpMasterSpec(row, finalSpec):
    spec = row[finalSpec]
    if spec:
        result = lookupMasterSpec(spec)
        #elastic.gridLookup('ndb_taxonomy', spec, 1, {}, **{'match_fields': ['prov_type_name']})
        if result is not None:
            # specName = "Input: " + row[finalSpec] + "/" + "Output: " + result["spec"]
            s = result["spec"] + "|" + result["is_mid_level"]
            return s
        else:
            return "" + "|" + ""
    else:
        return "" + "|" + ""


def lookupMasterSpec(spec):
    res1= elastic.gridLookup('spec_psi', spec, 1, {}, **{'match_fields': ['spec'], 'operator': 'and', 'restrict': True})
    spec = re.sub("[\(\[].*?[\)\]]", "", spec).strip()
    res2 = elastic.gridLookup('spec_psi', spec, 1, {}, **{'match_fields': ['spec'], 'operator': 'and', 'restrict': True})
    specRes1 = res1['spec'] if res1 else ""
    specRes2 = res2['spec'] if res2 else ""

    spec1Dist = edit_distance(spec, specRes1) if specRes1 else 9999
    spec2Dist = edit_distance(spec, specRes2) if specRes2 else 9999

    if spec1Dist <= spec2Dist and res1:
        return res1
    elif res2:
        return res2
    else:
        return None

def var_getDegreeFromNdbGridPsi(row, specdf, finalDegreeCol):
    return row[finalDegreeCol]

def getSpecPSI(df,argDi):
    isPSpecMidLvl=argDi['inputCol'][0]
    isPDegMidLvl=argDi['inputCol'][1]
    superSpecPsi=argDi['inputCol'][2]
    spec=argDi['inputCol'][3]
    outSpecCol=argDi['outputCol'][0]
    outSuperSpecCol=argDi['outputCol'][1]
    isEmptyCol=argDi['isEmptyCol']
    isEmptyDF=False
    if len(df.index)==1:
        isEmptyDF=determineEmptyDF(df,excludedCols=["ROW_COUNT","ROW_NUM"])
    df[outSpecCol]=''
    df[outSuperSpecCol]=''

    def deriveSpecPsiNew(row,specInd,superSpec,spec,outSpecCol,outSuperSpecCol,isEmptyCol):
        if isEmptyDF:
            row[outSpecCol]=ErrorMessage([], ["c", "speciality not found", ""])
            row[isEmptyCol]="Y"
            return row
        if row[specInd]:
            if row[specInd]=='Y':
                row[outSuperSpecCol]=row[superSpec]
                row[outSpecCol] = row[spec]
            elif row[specInd]=='N':
                row[outSpecCol]=row[spec]#if prim spec is non-midlevel then non-midlevel speciality is populated in the spec col
        else:
            row[outSpecCol]=row[spec]# if mid-level indicator is blank then error msg is populated in spec col
        return row

    df=df.apply(lambda row:deriveSpecPsiNew(row,isPSpecMidLvl,superSpecPsi,spec,outSpecCol, outSuperSpecCol,isEmptyCol),axis=1)
    return df

def var_exceptionDeg(row,degForLkp):
    deg = row[degForLkp]
    if deg:
        bestMatch =lookupDegreeExceptions(deg)
        # bestMatch = elastic.gridLookup('specialty_exceptions', spec, 1, {}, **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
        if bestMatch:
            return bestMatch["degree_in_master_list"]
        # else:
        #     return spec

def var_finalDegPsi(row, inSot,sotExDeg, degCol):
    if row[inSot] == "Y":
        if row[sotExDeg]:
            return row[sotExDeg]
        else:
            return row[degCol]
    else:
        return ""


def var_lkpMasterDeg(row, finalDeg):
    deg = row[finalDeg]
    if deg:
        return lookupMasterDeg(deg.upper())
    else:
        return "" + "|" + "|" + ""


def lookupMasterDeg(deg):
    bestMatch = elastic.gridLookup('degree_psi', deg, 0, {}, **{'match_fields': ['code']})

    if bestMatch:
        return bestMatch["is_mid_level"] + "|" + "|" + bestMatch["code"]
    else:
        return "" + "|" + "|" + ""

def var_masterDeg(row, colname,deg):
    row = row.fillna('')
    if row[deg]:
        if len([x for x in row[colname].split("|") if x]) > 0:
            return row[colname].split("|")[2]
        else:
            return ErrorMessage([], ["c", "could not match degree", row[deg]])
    else:
        return ErrorMessage([], ["c", "degree not found", row[deg]])

def var_masterDegIsMidLvl(row, colname):
    row = row.fillna('')
    if len([x for x in row[colname].split("|") if x]) > 0:
        return row[colname].split("|")[0]
    else:
        return ""


def getFirstNonMidLvlSupSpec(df,funcInp):
    outCol=funcInp['outputCol']
    updateCondition=funcInp.get('replaceWithCondition')
    updateWithCol=funcInp.get('replaceWithCol')
    inSotCol=funcInp.get('isInSOT')
    isEmptyCol=funcInp.get('IsEmptyCol')
    df[outCol]=''
    resolvedDf=df.loc[eval(updateCondition)]
    if len(resolvedDf) > 1:
        df.loc[:, outCol] = ErrorMessage([], ["c", "multiple supervising speciality found", ""])
        df.loc[:, isEmptyCol] = "N"
        return df
    if not resolvedDf.empty:
        updateValue=resolvedDf.iloc[0][updateWithCol]
        varIsInSOT = resolvedDf.iloc[0][inSotCol]
        if varIsInSOT == "Y":
            df.loc[:, outCol] = updateValue
            df.loc[:, isEmptyCol] = "N"
        else:
            df.loc[:, outCol] = ErrorMessage([], ["c", "supervising speciality not found", ""])
            df.loc[:, isEmptyCol] = "Y"
    elif resolvedDf.empty:
        df.loc[:, outCol] =ErrorMessage([], ["c", "supervising speciality not found", ""])
        df.loc[:, isEmptyCol] = "Y"
    return df


def accessChangeType(df,changeTypeDf):
    outputCol="ttype"
    df[outputCol]=''
    ttype=changeTypeDf["OUT_TRANSACTION_TYPE"].iloc[0]
    df[outputCol]=ttype
    return df


def unknownSpPhy(df,argDi):
    spec=argDi['inputCol']['spec']
    superSpec=argDi['inputCol']['superSpec']
    ttype=argDi['inputCol']['ttype']
    outSpecCol=argDi['outputCol'][0]
    outSuperSpecCol=argDi['outputCol'][1]
    isSpecEmptyCol=argDi['inputCol']['isSpecEmptyCol']
    isSuperSpecEmptyCol=argDi['inputCol']['isSuperSpecEmptyCol']
    def func(spec,isEmpty,ttype):
        if not ttype or 'TIN ADD' in ttype:
            return spec
        else:
            if isErrorVal(spec) and isEmpty=='Y':
                return 'UNKNOWN SPECIAL PHYSICIAN'
            else:
                return spec

    df[outSpecCol]=df.apply(lambda row:func(row[spec],row[isSpecEmptyCol],row[ttype]),axis=1)
    df[outSuperSpecCol]=df.apply(lambda row:func(row[superSpec],row[isSuperSpecEmptyCol],row[ttype]),axis=1)
    return df

def determineEmptyDF(df, excludedCols=None):
    if not len([v for k, v in list(dict(df.iloc[0]).items()) if k not in excludedCols and v is not None]):
        return True
    return False

def updateClarification(df,argDi):
    if argDi.get('ignore'):  # to ignore this step
        return df
    colMapping=argDi['colMapping']
    updateFrom=argDi['updateFrom']
    conditionMapping=argDi.get("conditionMapping")
    i=-1
    for idx, row in df.iterrows():
        diUpdateFrom=eval(row[updateFrom])
        i += 1
        for inpCol,outCol in list(colMapping.items()):
            if diUpdateFrom.get(inpCol):
                if conditionMapping and conditionMapping.get(inpCol):
                    if eval(conditionMapping[inpCol]):
                        df.iloc[i,df.columns.get_loc(outCol)]=diUpdateFrom[inpCol]
                    else:
                        continue
                else:
                    df.iloc[i, df.columns.get_loc(outCol)]=diUpdateFrom[inpCol]
    return df

def locateProviderInFile(df,argDi):
    levels = argDi['levels']
    fileNameCol=argDi["fileNameCol"]
    tabNameCol=argDi["tabNameCol"]
    outCol=argDi["outputCol"]
    template=argDi["template"]
    df=df.set_index(levels).join(pd.DataFrame({"tabs": df.groupby(levels)[tabNameCol].apply(lambda x: x.unique())})).reset_index()
    def fillTemplate(fname,tname):
        if isinstance(tname,np.ndarray):
            return template.format(fname,','.join(tname))
        else:
            return template.format(fname,tname)
    df[outCol]=df.apply(lambda row:fillTemplate(row[fileNameCol],row["tabs"]),axis=1)
    return df

def createIsEmptySpec(df,funcInp):
    outCol = funcInp['outputCol']
    inpCol = funcInp.get('inputCol')
    df[outCol]=df.apply(lambda row:"Y" if row[inpCol]=="N" else "N",axis=1)
    return df


def createIsEmptySuperSpec(df,funcInp):
    outCol=funcInp['outputCol']
    updateCondition=funcInp.get('replaceWithCondition')
    updateWithCol=funcInp.get('replaceWithCol')
    inpSpecCol=funcInp.get('inputSpec')
    df[outCol]=''
    resolvedDf=df.loc[eval(updateCondition)]
    if not resolvedDf.empty:
        updateValue=resolvedDf.iloc[0][updateWithCol]
        df.loc[:, outCol] = updateValue
    elif resolvedDf.empty:
        df.loc[:, outCol] ="Y"
    return df

def var_getPhoneIndicator(row, phonetypeColName, phoneNumber, phoneNumberIndex):
    row = row.fillna('')
    if row[phoneNumber]:
        if phoneNumberIndex == 1:
            return "P"
        elif row[phonetypeColName].lower() == 'fax':
            return "S"
        else:
            return "S"
    else:
        return ""

####################################################################################################
# code from roster reused
def splitZipCode(df,argDi):
    inpCol=argDi['inputCol']
    zip5Col=argDi['outputCol'][0]
    zip4Col=argDi['outputCol'][1]
    df=df.fillna('')
    def extract(row,zip,zip5Col,zip4Col):
        row[zip5Col]=''
        row[zip4Col]=''
        zipcode = row[zip]
        splitted=row[zip].split('-')
        if len(row[zip])==9:
            row[zip5Col] = row[zip][:5]
            row[zip4Col] = row[zip][5:]
        elif len(splitted)>=2:
            row[zip5Col]= splitted[0]
            row[zip4Col]= splitted[1]
        elif len(splitted)==1:
            row[zip5Col] = splitted[0]
        return row
    df=df.apply(lambda row:extract(row,inpCol,zip5Col,zip4Col),axis=1)
    return df

def standardizeZipCode(df, argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']

    i=-1
    for idx, row in df.iterrows():
        i+=1

        if row[inpCol] and row[inpCol].strip().isdigit():
            if len(row[inpCol]) < 5:
                df.iloc[i, df.columns.get_loc(outCol)] = row[inpCol].rjust(5, '0')

    return df

def updateZipCode(df,argDi):
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    df[outCol]=df.apply(lambda row:row[inpCol] if row[inpCol].strip() else row[outCol],axis=1)
    return df

# Batul : New functions tax-id template specific
def var_getZipPlus4(row, parsedAdd, zip4col):
    """get plus 4 of zip code"""
    if row[zip4col]:
        return row[zip4col]
    else:
        zip4 = row[parsedAdd].split(";")[4]
        if "-" in zip4:
            return zip4.split("-")[1]
        return ""
'''
def combineAddressLine1n2(df,argDi):
    """combines address line 1 and address line 2"""
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    df = df.fillna('')
    # df[outCol] = df[inpCol].apply(lambda x: ', '.join(x) if x!='' else ''.join(x), axis=1)
    df[outCol] = df[inpCol].apply(lambda x: ' '.join(x), axis=1)
    return df
'''
def var_deriveAddressIndicator(row, addInd, addType):
    """determine address indicator as P/S i.e. P if plsv and S for anything else"""
    # if row[addType] == "" or row[addType].lower() == "general":
    #     return row[addInd]
    if row[addType].lower() == "plsv":
        if row[addInd]:
            return row[addInd]
        else:
            return ErrorMessage([], ["C", "Address Indicator not found" + "", ""])
    else:
        return "S"

def var_getStdType(row, inp, stdKey):
    """utility for df layer function: getStdType"""
    if row[inp]:
        stdDict = constants.standardsDict[stdKey]
        for k, v in stdDict.items():
            if row[inp].lower() in [x.lower() for x in v]:
                return k
        return ErrorMessage([], ["C", "Unable to map to a Code", row[inp]])
    else:
        return ""

def getStdType(df, inp):
    """get standard value for the columns from standards dictionary using the key passed as argument"""
    df = df.fillna('')
    inputCol = inp["inputCol"]
    outputCol = inp["outputCol"]
    stdKey = inp["stdKey"]
    df[outputCol] = df.apply(lambda row: var_getStdType(row, inputCol, stdKey), axis=1)
    return df

def getSubFrameListOfDictionary(row, subFrameList, rownum):
    """ generic function for creating a list of dictionary for some
    columns like language, email, dba name and contact information"""
    colNames = subFrameList
    list_of_dictionary = []
    for i in range(1, rownum+1):
        frame = {}
        for col in colNames:
            stdCol = col + "_" + str(i)
            frame[col] = row[stdCol]
        # check for empty values in dictionary
        if not all(v=="" for v in list(frame.values())) or not (frame[colNames[0]]==""):
            list_of_dictionary.append(frame)
    if len(list_of_dictionary) == 0:
        list_of_dictionary.append(dict.fromkeys(colNames, ""))
    return str(list_of_dictionary)

def getSeparateLanguages(row, colName, col_inp):
    """separates the languages in one column to multiple in the list
    of dictionary by adding separate dictionary without loosing its
    corresponding other columns"""
    keys = colName
    inp = row[col_inp]
    newInp = []
    ignoreEnglish = [x.upper() for x in constants.standardsDict["IGNORE_LANGUAGE"]]
    flag = True
    if len(eval(inp)) == 1:
        if all(v == "" for v in eval(inp)[0][keys[0]]):
            flag = False
    if flag:
        for dicts in eval(inp):
            replace_punctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
            text = str(dicts[keys[0]]).translate(replace_punctuation).strip()
            allLang = text.split()
            new_dict = {}
            for lang in allLang:
                if lang.upper() not in ignoreEnglish: # ignoring english language
                    new_dict[keys[0]] = lang
                    new_dict[keys[1]] = dicts[keys[1]]
                    new_dict[keys[2]] = dicts[keys[2]]
                    newInp.append(new_dict)
                    new_dict = {}
        if not newInp:
            newInp = [{k:v for k, v in zip(keys, ['']*len(keys))}]
        return str(newInp)
    else:
        return inp

def combinePhoneFax(row, funInp, colInp):
    """combines phone and fax number to one column"""
    inp = row[colInp]
    funInpNew = [i for i in funInp if i!=funInp[1]]
    newInp = []
    for dicts in eval(inp):
        new_dict = dict.fromkeys(funInpNew, "")
        if dicts[funInp[0]] is not "" or dicts[funInp[1]] is not "":
            if dicts[funInp[0]] is not "":
                new_dict[funInp[0]] = dicts[funInp[0]]
                new_dict[funInp[2]] = dicts[funInp[2]]
                new_dict[funInp[3]] = dicts[funInp[3]]
                new_dict[funInp[4]] = dicts[funInp[4]]
                newInp.append(new_dict)
                new_dict = dict.fromkeys(funInpNew, "")
            if dicts[funInp[1]] is not "":
                new_dict[funInp[0]] = dicts[funInp[1]]
                new_dict[funInp[2]] = dicts[funInp[2]]
                new_dict[funInp[3]] = dicts[funInp[3]]
                new_dict[funInp[4]] = "F"
                newInp.append(new_dict)
        else:
            newInp.append(new_dict)
    return str(newInp)

def providerEmailGetSubFrameWrapper(df, argDi):
    """get list of dictionary of provider email by making use of getSubFrameListOfDictionary"""
    df = df.fillna('')
    outputCol = argDi['outputCol']
    colNames = argDi['inputCol']
    rownum = len(df)
    newDf = pd.DataFrame()
    for idx, row in df.iterrows():
        for col in colNames:
            newCol = col + "_" + str(idx+1)
            newDf.loc[0, newCol] = row[col]
    newDf = newDf.fillna('')
    for idx_,row_ in newDf.iterrows():
        listOfDictionary = getSubFrameListOfDictionary(row_, colNames, rownum)
    df.loc[:, outputCol] =  pd.Series([listOfDictionary]*len(df))
    return df


def add_mapCols(df, argDi):
    """add new columns with some default value and also maps input col to new output cols """
    df = df.fillna('')
    mapColDict = eval(argDi['mapCol'])
    defVal = argDi['defVal']
    outputCol = argDi['outputCol']

    for k,v in mapColDict.items():
        df[v] = df[k]

    for col,dval in zip(outputCol,defVal):
        df[col] = dval

    return df


def providerLangGetSubFrameWrapper(df, argDi):
    """get list of dictionary of provider's language by making use of getSubFrameListOfDictionary"""
    df = df.fillna('')
    outputCol = argDi['outputCol']
    colNames = argDi['inputCol']
    rownum = len(df)
    newDf = pd.DataFrame()
    for idx, row in df.iterrows():
        for col in colNames:
            newCol = col + "_" + str(idx+1)
            newDf.loc[0, newCol] = row[col]
    newDf = newDf.fillna('')
    for idx_,row_ in newDf.iterrows():
        listOfDictionary = getSubFrameListOfDictionary(row_, colNames, rownum)
    df.loc[:, outputCol] = pd.Series([listOfDictionary]*len(df))
    return df

def combineProvAddEmail(row, provDf, provLOD, addLOD, emailInd, colAssociationDict):
    """combines address and provider type email in one list of dictionary"""
    prov_list_of_dict = eval(provDf[provLOD][0])
    add_list_of_dict = eval(row[addLOD])
    for dicts in add_list_of_dict:
        if not all(v == "" for v in list(dicts.values())):
            dicts[emailInd] = "A"
        else:
            dicts[emailInd] = ""
    for dicts in prov_list_of_dict:
        if not all(v == "" for v in list(dicts.values())):
            dicts[emailInd] = "P"
        else:
            dicts[emailInd] = ""
        for k,v in dicts.items():
            if k in list(colAssociationDict.keys()):
                dicts[colAssociationDict[k]] = dicts.pop(k)
        if not all(v == "" for v in list(dicts.values())):
            add_list_of_dict = [dict_ for dict_ in add_list_of_dict if not all(v == "" for v in list(dict_.values()))]
            add_list_of_dict.append(dicts)
    return str(add_list_of_dict)


def combineProvLangAddrLang(row, provLangDf, provLangLOD, addrLangLOD):
    """ combines provider's language and language under address into one list of dictionary"""
    prov_lang_list_of_dict = eval(provLangDf[provLangLOD][0])
    addr_lang_list_of_dict = eval(row[addrLangLOD])

    for dicts in prov_lang_list_of_dict:
        if not all(v == "" for v in list(dicts.values())):
            addr_lang_list_of_dict = [dict_ for dict_ in addr_lang_list_of_dict if not all(v == "" for v in list(dict_.values()))]
            addr_lang_list_of_dict.append(dicts)

    return str(addr_lang_list_of_dict)


def var_getThreeLetterLanguageCode(row, lang):
    """utility for getting 3 letter language code row wise"""
    if row[lang] == "":
        return ""
    elif row[lang].lower() == "none":
        return row[lang]
    if len(row[lang]) <= 3:
        bestMatch = elastic.gridLookup('language_code', row[lang], 0, **{'match_fields': ["code"]})
    else:
        bestMatch = elastic.gridLookup('language_code', row[lang], 1,
                                       **{'match_fields': ["language_description"]})
    if bestMatch:
        return bestMatch["code"]
    else:
        return ErrorMessage([], ["C", "Unable to map to a Lang Code", row[lang]])

def getThreeLetterLanguageCode(df, funInp):
    """get 3 letter code for language"""
    lang = funInp["inputCol"]
    out_lang = funInp["outputCol"]
    df[out_lang] = df.apply(lambda row: var_getThreeLetterLanguageCode(row, lang), axis=1)
    return df


def var_validatePhoneTaxID(row, phn):
    """utility for df level function: validatePhoneDf"""
    if row[phn] and row[phn] != "" and not isErrorVal(str(row[phn])):
        symbols = ["(", ")", "-"," "]
        for sym in symbols:
            row[phn] = row[phn].replace(sym, "")
        cleanPhn = row[phn].strip()
        if len(cleanPhn) == 10:
            return cleanPhn
        elif cleanPhn == "":
            return ""
        else:
            return ErrorMessage([], ["C", "not equal to 10 digits", row[phn].strip()])
    return row[phn]

def validatePhoneDf(df, funInp):
    """check if the phone number is of 10 digits or not; if not give clarification"""
    df = df.fillna('')
    phn = funInp["inputCol"]
    out_phn = funInp["outputCol"]
    df[out_phn] = df.apply(lambda row: var_validatePhoneTaxID(row, phn), axis=1)
    return df


def electronicCommTypeDf(df, funInp):
    """finds the electronic commuunication type from electonic comm. i.e. email or web address"""
    inp = funInp["inputCol"]
    out = funInp["outputCol"]
    df[out] = df.apply(lambda row: var_electronicCommType(row, inp[0], inp[1]), axis=1)
    return df


def concat_subframes(df, funInp):
    """form sub-dataframes from list of dictionary and
    concat it to the rest of the address frame"""
    lang, phone, email, dba = funInp
    finalAddDf = pd.DataFrame()
    for idx, row in df.iterrows():
        langDf = pd.DataFrame(eval(row[lang]))
        phoneDf = pd.DataFrame(eval(row[phone]))
        emailDf = pd.DataFrame(eval(row[email]))
        dbaDf = pd.DataFrame(eval(row[dba]))

        subframe1 = pd.concat([langDf, phoneDf, emailDf, dbaDf], axis=1)
        subframe2 = row

        def transform(df, l):
            for i in range(0, l):
                yield df
        addDf = pd.DataFrame(transform(subframe2, len(subframe1)), index=range(0, len(subframe1)))
        addDfNew = pd.concat([addDf, subframe1], axis=1)
        finalAddDf = pd.concat([finalAddDf, addDfNew])
        finalAddDf = finalAddDf.reset_index(drop=True)
        finalAddDf = finalAddDf.fillna('')
    return finalAddDf

def apply_8_to_5(row, inpCols,whclrFlagCol,whCol, combinedclrFlagCol, combinedString):
    """determine if working hours are 8-5 for weekdays"""
    if row[combinedString] and row[combinedclrFlagCol]:
        return ErrorMessage([], ["C", "could not parse working hours", row[combinedString]])
    elif row[whCol] and row[whclrFlagCol]:
        return ErrorMessage([], ["C", "could not parse working hours", row[whCol]])
    count = 0
    inpCols = inpCols
    for col in inpCols[0:]:
        if row[col] == "08:00AM-05:00PM":
            count += 1
    if count == 5:
        return "True"
    else:
        return "False"

def var_getPhoneType(row, phn, phn_type, add_type):
    """utility for getPhoneType"""
    if not isErrorVal(str(row[phn])) and row[phn] != "" and not str(row[phn]).isspace() and row[phn] is not None and row[phn] != '00000':
        if row[phn_type] == "" and row[add_type] != "":
            return row[add_type]
        elif row[phn_type] == "" and row[add_type] == "":
            return ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        else:
            return row[phn_type]
    elif isErrorVal(str(row[phn])):
        return ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
    else:
        return ""

def getPhoneType(df, funInp):
    """determine the phone type if directly present else give address type as phone type"""
    df = df.fillna('')
    phn, phn_type, add_type = funInp
    df[phn_type] = df.apply(lambda row: var_getPhoneType(row, phn, phn_type, add_type), axis=1)
    return df

def validateLengthMedicaid(df,funcInp):
    """validate length specific to medicaid i.e no clarification for field not given"""
    df=df.fillna('')
    col, size, type, excepCol = funcInp
    if excepCol!=col:
        df[excepCol]=df[col]
    for idx, row in df.iterrows():
        if isErrorVal(row[col]):
            continue
        if type == "max":
            if row[col] and (len(str(row[col])) > int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size greater than " + size, row[col]])
        elif type == "eql":
            if row[col] and not (len(str(row[col])) == int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size not eql to " + size, row[col]])
    return df

def clarifyMedicaid(df, funInp):
    inp, output = funInp
    try:
        for idx, row in df.iterrows():
            if row[inp] != '' and not str(row[inp]).isspace() and row[inp] is not None and row[inp] != '00000':
                if (row[output] == '' or str(row[output]).isspace() or row[output] is None or row[output] == '00000') and not isErrorVal(str(row[output])):
                    df.loc[idx, output] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        return df
    except:
        return df

def clarifyFieldsLang(df, funInp):
    """clarify field specific to language, if language is blank or none no clarification is provided"""
    df = df.fillna('')
    clarifyCol = funInp[1]
    defaultVal = funInp[2] if len(funInp) > 2 else None
    try:
        for idx, row in df.iterrows():
            if isErrorVal(row[clarifyCol]):
                continue
            if not all(row[x] == "" or row[x].lower() == "none" or row[x] == None for x in funInp[0]): #   row[funInp[0]] != '' and row[funInp[0]].lower() != "none" and row[funInp[0]] != None:
                if row[clarifyCol] == '' or str(row[clarifyCol]).isspace() or row[clarifyCol] is None or row[clarifyCol] == '00000':
                    # df.loc[idx, clarifyCol] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
                    df.loc[idx, clarifyCol] = defaultVal if defaultVal else ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        return df
    except:
        return df

def updateConditionally(df,argDi):
    for di in argDi:
        updateValue=di['updateValue']
        errType=di.get('errType')
        condition=di['condition']
        inputCol=di['inputCol']
        outputCol=di['outputCol']
        if outputCol not in df.columns:
            df[outputCol]=''
        boolArr=eval(condition)
        if not boolArr.empty:
            lstTrue=[i for i,elm in enumerate(boolArr) if elm]
            if errType:
                i=-1
                for idx,row in df.iterrows():
                    i+=1
                    if i in lstTrue:
                        df.iloc[i,df.columns.get_loc(outputCol)]=ErrorMessage([], [errType, updateValue, row[inputCol]])
            else:
                df.loc[boolArr,outputCol]=updateValue
    return df

def var_removeSpecialCharacters(row, inp):
    if isErrorVal(row[inp]):
        return row[inp]
    replace_punctuation = str.maketrans(string.punctuation, "." * len(string.punctuation))
    text = str(row[inp]).translate(replace_punctuation).strip()
    text_new = text.replace(".", "")
    return text_new

def updateClarifyPhone(row, funInp, outCol):
    """ row[outCol] is a list of dictionary, funInp is the target column
        This function updates clarification in the list of dictionary
    """
    listOfDicts = eval(row[outCol])
    try:
        if all(((di[funInp] == '' or str(di[funInp]).isspace() or di[funInp] is None or di[funInp]=='00000') and not isErrorVal(str(di[funInp]))) for di in listOfDicts):
            listOfDicts[0][funInp] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        return str(listOfDicts)
    except:
        return row[outCol]

def updateClarifyListOfDictionary(row, funInp, outCol):
    """ row[outCol] is a list of dictionary, funInp is the target column
        This function updates clarification in the list of dictionary
    """
    # print row[outCol]
    listOfDicts = eval(row[outCol])
    try:
        for di in listOfDicts:
            if (di[funInp] == '' or str(di[funInp]).isspace() or di[funInp] is None or di[funInp]=='00000') and not isErrorVal(str(di[funInp])):
                di[funInp] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        return str(listOfDicts)
    except:
        return row[outCol]

def defaultAddressCorrespondence(df, funInp):
    df = df.fillna('')
    inputCol = funInp['inputCol']
    outputCol = funInp['outputCol']
    df[outputCol] = df[inputCol].apply(lambda val: "S" if val=='' else val)
    return df

def standardiseCommunicationType(df,argDi):
    inputCol=argDi['inputCol']
    outputCol=argDi['outputCol']
    stdKey = argDi['standardsKey']
    corpus=constants.standardsDict[stdKey]
    def func(inputStr,corpus):
        if not inputStr:
            return ""
        for k,v in list(corpus.items()):
            if inputStr.lower() in v:
                return k
        return ErrorMessage([], ["c", "Could not standardise", inputStr])

    df[outputCol]=df.apply(lambda row:func(row[inputCol],corpus),axis=1)
    return df


def standardiseContactType(df, argDi):
    inputCol = argDi['inputCol']
    outputColExc = argDi['outputColExc']
    outputColMaster = argDi['outputColMaster']
    excIdx = argDi['exceptionIdxName']
    masterIdx = argDi['masterIdxName']

    def lkpExc(inputStr, idxName):
        if not inputStr:
            return ""
        bestMatch = elastic.gridLookup(idxName, inputStr, 1, **{'match_fields': ["contact_type_input"]})
        if bestMatch:
            return bestMatch["contact_type_master"]
        else:
            return inputStr

    def lkpMaster(inputStr, idxName):
        if not inputStr:
            return ""
        bestMatch = elastic.gridLookup(idxName, inputStr, 1, **{'match_fields': ["comm_type_abb"]})
        if bestMatch:
            return bestMatch["comm_type_acrnm"]
        else:
            return ErrorMessage([], ["c", "Could not lookup in Grid", inputStr])
    df[outputColExc] = df.apply(lambda row: lkpExc(row[inputCol], excIdx), axis=1)
    df[outputColMaster] = df.apply(lambda row: lkpMaster(row[outputColExc], masterIdx), axis=1)
    return df

def checkZeros(row, inpCol, errMsg, msgType):
    if len(str(row[inpCol])) and all(i=="0" for i in str(row[inpCol])) and not isErrorVal(str(row[inpCol])):
        return ErrorMessage([], [msgType, errMsg, row[inpCol]])
    else:
        return row[inpCol]

def replace_age_delim(row, ageLimit):
    age_delim = constants.standardsDict['age_delim']
    ageLimitVal = str(row[ageLimit])
    for delim in age_delim:
        if delim in ageLimitVal:
            ageLimitVal = ageLimitVal.replace(delim, "-")

    return ageLimitVal

def var_validateLoc(row, StateLoc):
    if row[StateLoc]:
        bestMatchShort = elastic.gridLookup('us_states', row[StateLoc], 0,
                                       **{'match_fields': ["abbreviation"], 'operator': 'and', 'restrict': True})
        bestMatchFull = elastic.gridLookup('us_states', row[StateLoc], 0,
                                            **{'match_fields': ["us_state"], 'operator': 'and', 'restrict': True})

        if bestMatchShort:
            return row[StateLoc]
        elif bestMatchFull:
            return bestMatchFull['abbreviation']
        else:
            if isErrorVal(row[StateLoc]):
                return row[StateLoc]
            else:
                return ErrorMessage([], ["C", "Couldn't Validate State", row[StateLoc]])
    else:
        return ""

def normalizeCCC(df,argDi):
    phCol=argDi['phCol']
    faxCol=argDi['faxCol']
    outCol=argDi['outCol']
    commTypCol=argDi['commTypCol']
    drvCommTypCol=argDi['drvCommTypCol']
    lvls=argDi['lvls']
    lstRows=[]
    df[outCol]=''
    df[drvCommTypCol]=df[commTypCol]
    for idx,row in df.iterrows():
        i=0
        isCommType=row[drvCommTypCol]
        for elm in [phCol,faxCol]:
            if not row[elm]:
                continue
            i+=1
            row[outCol]=row[elm]
            if not isCommType:
                if elm == phCol:
                    row[drvCommTypCol]='P'
                else:
                    row[drvCommTypCol] = 'F'
            lstRows.append(copy.deepcopy(row))
        if not i:
            lstRows.append(copy.deepcopy(row))
    df1=pd.DataFrame(lstRows)
    df1=df1.fillna('')
    return df1.drop_duplicates(lvls).reset_index(drop=True)

def grpMissingFlag(df,argDi):
    grpCols = argDi['grpCols']
    flagCol = argDi['flagCol']
    def func(row, grpCols):
        for col in grpCols:
            if row[col] or isErrorVal(row[col]):
                return 1
        return 0

    df[flagCol] = df.apply(lambda row: func(row,grpCols),axis=1)
    return df

def clarifyGrpMissings(df,argDi):
    grpCols=argDi['grpCols']
    flagCol = argDi['flagCol']
    def func(row,col,flagCol):
        if row[flagCol] and not row[col]:
            return ErrorMessage([], ["C", "Mandatory field Missing Value", ""])
        else:
            return row[col]
    for col in grpCols:
        df[col]=df.apply(lambda row: func(row,col,flagCol),axis=1)
    return df


def finalExtNum(df,argDi):
    extCol = argDi['extCol']
    drvExtCol = argDi['drvExtCol']
    outCol = argDi['outputCol']
    def func(extColVal,drvExtCol):
        if extColVal:
            return extColVal
        else:
            return drvExtCol
    df[outCol]=df.apply(lambda row: func(row[extCol],row[drvExtCol]),axis=1)
    return df

def var_BothAgeLimitFlag(row, minAgeCol, maxAgeCol):
    if row[minAgeCol] and row[maxAgeCol]:
        return 1
    elif (row[maxAgeCol] and not row[minAgeCol]) or (row[minAgeCol] and not row[maxAgeCol]):
        return 0
    else:
        return ""

def var_ClarifyAgeLimitTaxIDAdd(row, ageCol, flagCol):
    ageVal = str(row[ageCol])
    flagVal = row[flagCol]

    if flagVal == 1:
        if not ageVal.isdigit():
            return ErrorMessage([], ["C", "Not in standard format", ageVal])
        else:
            return ageVal
    elif flagVal == 0:
        return ErrorMessage([], ["C", "Not in standard format", ageVal])
    else:
        return ""

def drvElecCommType(df,argDi):
    inputCol=argDi['inputCol']
    outputColEC=argDi['outputColEC']
    outputColECTyp=argDi['outputColECTyp']
    df[outputColECTyp]=''
    df[outputColEC]=df[inputCol]
    def func(inpStr,row,outputColEC,outputColECTyp):
        if not inpStr:
            row[outputColECTyp]=""
            return row
        typ=determineEmlUrl(inpStr)
        if typ:
            row[outputColECTyp] = typ
            return row
        row[outputColECTyp] = ErrorMessage([],["c","could not identify communication type",inpStr])
        row[outputColEC] = ErrorMessage([],["c","not in standard format",inpStr])
        return row

    df=df.apply(lambda row: func(row[inputCol],row,outputColEC,outputColECTyp),axis=1)
    return df

def fnlElecCommType(df, argDi):
    elecCommCol = argDi['inputCol'][0]
    drvElecCommCol = argDi['inputCol'][1]
    outputCol = argDi['outputCol']
    def func(elecCommVal,drvElecCommVal):
        return elecCommVal if elecCommVal else drvElecCommVal

    df[outputCol]=df.apply(lambda row:func(row[elecCommCol],row[drvElecCommCol]),axis=1)
    return df

def determineEmlUrl(inpStr):
    urlRegex = r'''(?:ftp:\/\/|www\.|http(?:s)?:\/\/){1}[a-zA-Z0-9]{2,}\.[a-zA-Z0-9]{2,}(?:\S*)'''
    emailRegex = r'''([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b)'''
    diMap = {'W': urlRegex, 'E': emailRegex}
    for k, v in list(diMap.items()):
        if re.search(v, inpStr):
            return k

def getLstClr(row, col,keyName):
    inp = eval(row[col])
    return (inp[keyName])

def reinstateStructForWH(row, col,keyName):
    inp = eval(row[col])
    return str(inp[keyName])

def determine24by7(inpStr):
    if re.search(constants.regex247, inpStr.strip()):
        return True

def updateExtracted(extracted, defaultWorkHours,defaultDays,inputWorkHours):
    if not inputWorkHours.strip() and defaultWorkHours:
        extracted = [{'line': inputWorkHours, 'daytime': {'days': defaultDays, 'time': defaultWorkHours}}]
        # print 'extracted1:', extracted
    elif inputWorkHours and len(extracted) == 0:
        extracted = [{'line': inputWorkHours, 'daytime': {'days': [], 'time': ''}}]
    if len(extracted) == 1 and extracted[0]['daytime']['time'].strip():
        if determine24by7(extracted[0]['daytime']['time']):
            extracted[0]['daytime']['time'] = constants.time247
            extracted[0]['daytime']['days'] = constants.allDays
    return extracted

def standardiseWh(inpStr):
    data=constants.standardsDict["workHour"]
    for time,lstWords in list(data.items()):
        regex = r'\b|\b'.join([re.escape(wrd) for wrd in lstWords])
        regex = r'\b' + regex + r'\b'
        inpStr = re.sub(regex, time, inpStr, flags=re.IGNORECASE)
    return inpStr

def segmentWh(inpStr):
    regex = constants.regexWdWhGroup
    groups=re.findall(regex, inpStr, re.IGNORECASE)
    return groups


def deriveInpWh(row,combinedWHclr,combinedWHString,whcol,wHclr):
    if row[combinedWHclr] and row[combinedWHString]:
        return row[combinedWHString]
    else:
        return row[whcol]


def standardizeWD(inp,regKeys):
    for regKey in regKeys:
        regDi=constants.regexDict[regKey]
        for k, v in list(regDi.items()):
            inp = re.sub('|'.join(v), k, inp, flags=re.IGNORECASE)
    return str(inp)

def cleanWorkDay(df,argDi):
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    regKeys=argDi['regKeys']
    def func(inpStr):
        inpStr=standardizeWD(inpStr,regKeys)
        return inpStr
    df[outCol]=df.apply(lambda row:func(row[inpCol]),axis=1)
    return df

def cleanWorkHours(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    regKeys = argDi['regKeys']
    def func(inpStr):
        inpStr = standardizeWD(inpStr,regKeys)
        return inpStr

    df[outCol] = df.apply(lambda row: func(row[inpCol]), axis=1)
    return df

def deriveWdWh(df,argDi,row=None):
    wdCol = argDi['inputCol'][0]
    whCol = argDi['inputCol'][1]
    defWdCol = argDi['inputCol'][2]
    defWhCol = argDi['inputCol'][3]
    drvdThruCol=argDi['outputCol'][0]
    outCol=argDi['outputCol'][1]
    df[drvdThruCol]='wh'
    df[outCol]=df[whCol]
    def isDayPresentInWh(result):
        return (any([item['daytime']['days'] for item in result]) or any(
            [bool(re.search(r'\b[a-z]*\b', item['line'].replace(item['daytime']['time'], ''))) for item in result]))

    def func(row,wd,wh,defWd,defWh,drvdThruCol,outCol):
        try:
            extracted,__=extractDayTime(wh)
            dayInWh=isDayPresentInWh(extracted)
            if wd.strip() and not dayInWh and wh.strip():
                row[drvdThruCol],row[outCol]='wd-wh',wd+' '+wh
            elif wh.strip() and wd.strip() and dayInWh:
                row[drvdThruCol], row[outCol] = 'wh', wh
            elif wh.strip() and not wd.strip() and not dayInWh and defWh :
                row[drvdThruCol], row[outCol] = 'wh-defWd', defWd + ' ' + wh
            elif wd.strip() and not wh.strip() and defWh:
                row[drvdThruCol], row[outCol] = 'wd-defWh', wd + ' ' + defWh
            elif not wd.strip() and not wh.strip() and defWh and defWd:
                row[drvdThruCol], row[outCol] = 'defWd-defWh', defWd + ' ' + defWh
        except:
            pass
        return row
    df=df.apply(lambda row: func(row,row[wdCol],row[whCol],row[defWdCol],row[defWhCol],drvdThruCol,outCol),axis=1)
    return df

def storeDefaultWhWd(df,argDi):
    whCol=argDi['inputCol'][0]
    diColMappingCol=argDi['inputCol'][1]
    defWdCol=argDi['outputCol'][0]
    defWhCol=argDi['outputCol'][1]
    df[defWdCol]='mon-fri'
    df[defWhCol] = ''
    diColMapping=eval(df.iloc[0][diColMappingCol])
    outWhCols=[key for key in list(diColMapping.keys()) if whCol in key]
    if outWhCols:
        inpWhCol = diColMapping.get(outWhCols[0]).replace('\n',' ')
        dtTup = extractDefaultWorkHours(inpWhCol)
        df[defWdCol]=dtTup[0]
        df[defWhCol]=dtTup[1]
    return df

def getDiColMappingAdr(df,diColMapping):
    df['diColMapping_Adr']=str(diColMapping)
    return df


import pandas as pd
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
import requests

def var_fnlState(row, parsedState, parsedAddLine1, addCity):
    bestMatchShort = elastic.gridLookup('us_states', row[parsedState], 0,
                                   **{'match_fields': ["abbreviation"], 'operator': 'and', 'restrict': True})
    bestMatchFull = elastic.gridLookup('us_states', row[parsedState], 0,
                                        **{'match_fields': ["us_state"], 'operator': 'and', 'restrict': True})

    #In case City has been parsed as a state
    pattern=r'\b{}\b'.format(row[parsedState])
    if re.search(pattern,row[addCity]):
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


def var_getAgeLimitTaxIDAdd(row, ageLimit, minAge, maxAge):
    if row[ageLimit]:
        return row[ageLimit]
    elif row[minAge] and row[maxAge]:
        return str(row[minAge]) + "-" + str(row[maxAge])
    else:
        return ""


def checkZeros(row, inpCol, errMsg, msgType):
    if len(row[inpCol]) and all(i=="0" for i in row[inpCol]) and not isErrorVal(row[inpCol]):
        return ErrorMessage([], [msgType, errMsg, row[inpCol]])
    else:
        return row[inpCol]


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


def var_fnlCity(row, parsedCity, parsedAddLine1, addCity):
    if row[addCity]:
        if row[parsedCity] == row[addCity]:
            return alphaNumericCheck(row[parsedCity],'alpha')
        else:
            return alphaNumericCheck(row[addCity],'alpha')
    else:
        return alphaNumericCheck(row[parsedCity],'alpha')


def standardisePSIndi(row,inputCol,applyList):
    for stdKey in applyList:
        if re.search('\\b' + '\\b|\\b'.join(constants.standardsDict[stdKey]) + '\\b', row[inputCol], re.IGNORECASE):
            if stdKey.lower() == "primary":
                return "P"
            else:
                return "S"
    return row[inputCol]


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


def var_removeSpecialCharacters(row, inp):
    if isErrorVal(row[inp]) or not row[inp].strip():
        return row[inp]
    return removePunctuation(row[inp], lstIgnore=[], lstPunct=[], compressionLvl=1)
    # replace_punctuation = string.maketrans(string.punctuation, "." * len(string.punctuation))
    # text = row[inp].translate(replace_punctuation).strip()
    # text_new = text.replace(".", "")
    # return text_new


def var_getZipCode(row, parsedAdd):
    zip = row[parsedAdd].split(";")[4]
    # if "-" in zip:
    #     return alphaNumericCheck(zip.split("-")[0],'numeric')
    zip=''.join(elm for elm in zip if elm.isalnum())#removes special character
    zip=zip[:5]
    return alphaNumericCheck(zip,'numeric')


def var_getAddress2(row,parsedAdd):
    add1 = row[parsedAdd].split(";")[0]
    if not add1 or add1.isspace():
        return ""
    else:
        return row[parsedAdd].split(";")[1]


def replace_age_delim(row, ageLimit):
    age_delim = constants.standardsDict['age_delim']
    ageLimitVal = str(row[ageLimit])
    for delim in age_delim:
        if delim in ageLimitVal:
            ageLimitVal = ageLimitVal.replace(delim, "-")

    return ageLimitVal


def var_getMinAgeTaxIDAdd(row, ageLimit, minAge):
    if row[minAge]:
        return row[minAge]
    elif row[ageLimit]:
        return row[ageLimit].split("-")[0].strip()
    else:
        return ""


def placeHolder(row):
    return ""


def var_getState(row,parsedAdd):
    return row[parsedAdd].split(";")[3]


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
            if isErrorVal(dicts[keys[0]]):
                allLang = [dicts[keys[0]]]
            else:
                replace_punctuation = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
                text = str(dicts[keys[0]]).translate(replace_punctuation).strip()
                if text.lower().strip() in [elm.strip().lower() for elm in ignoreEnglish]:
                    continue
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


def var_mergeDerivedWithInput(row, inputCol, derivedCol):
    row = row.fillna('')
    if row[inputCol] == '' or row[inputCol].isspace():
        return row[derivedCol]
    else:
        return row[inputCol]


def var_ConcatAddress(row, colMap):
    row = row.fillna('')
    AddDict = {}
    for key,val in colMap.items():
        AddDict[key] = row[val].strip()
    return str(AddDict)

def var_getZipPlus4(row, parsedAdd, zip4col):
    """get plus 4 of zip code"""
    if row[zip4col]:
        return alphaNumericCheck(row[zip4col], 'numeric')
    else:
        zip = row[parsedAdd].split(";")[4]
        zip=''.join(elm for elm in zip if elm.isalnum())#removes special character
        zip4=zip[5:]
        return alphaNumericCheck(zip4, 'numeric')


def var_getAddress1(row,parsedAdd):
    add1 = row[parsedAdd].split(";")[0]
    if not add1 or add1.isspace():
        return row[parsedAdd].split(";")[1]
    else:
        return row[parsedAdd].split(";")[0]


def combineProvLangAddrLang(row, provLangDf, provLangLOD, addrLangLOD):
    """ combines provider's language and language under address into one list of dictionary"""
    prov_lang_list_of_dict = eval(provLangDf[provLangLOD][0])
    addr_lang_list_of_dict = eval(row[addrLangLOD])

    for dicts in prov_lang_list_of_dict:
        if not all(v == "" for v in list(dicts.values())):
            addr_lang_list_of_dict = [dict_ for dict_ in addr_lang_list_of_dict if not all(v == "" for v in list(dict_.values()))]
            addr_lang_list_of_dict.append(dicts)

    return str(addr_lang_list_of_dict)


def var_parseDate(row, ColumnName, coltyp='C', deaNumCol=""):
    inpVal=row[ColumnName]
    if inpVal == '' or inpVal.isspace() or inpVal is None:
        if coltyp == 'DEA':
            if row[deaNumCol]:
                try:
                    parseVal = parser.parse(inpVal, default=datetime.datetime(9999, 12, 31))
                    cleansedDate = str(parseVal.month) + "/" + str(parseVal.day) + "/" + str(parseVal.year)
                    return cleansedDate
                except ValueError as e:
                    return ""  # ""Error: " + e.message + "| Input: " + row[ColumnName]
            else:
                return ""
        else:
            return inpVal
    elif inpVal == "00:00:00":
        return ErrorMessage([], ["R", "Unable to determine the Format", inpVal])
    else:
        if coltyp == 'C':
            val = inpVal
            if is_float(val):
                val = int(float(val))

            try:
                parseVal = parser.parse(str(val), default=datetime.datetime(1, 1, 1))
                cleansedDate = str(parseVal.month).rjust(2, "0") + "/" + str(parseVal.day).rjust(2, "0") + "/" + str(parseVal.year).rjust(4, "0")
                return cleansedDate
            except ValueError as e:
                # return "Error: " + e.message + "| Input: " + row[ColumnName]
                return ErrorMessage([], ["R", "Unable to determine the Format", inpVal])
        else:
            try:
                dt = parser.parse(inpVal)
            except ValueError as e:
                # if row[ColumnName].lower == ""
                return ErrorMessage([], ["E", str(e), inpVal])

            month = dt.month
            if len(inpVal) <= 4:
                month = 12

            year = dt.year
            lastday = calendar.monthrange(year, month)
            parseVal = parser.parse(inpVal, default=datetime.datetime(9999, 12, 31)).date()
            cleansedDate = str(parseVal.month).rjust(2, "0") + "/" + str(parseVal.day).rjust(2, "0") + "/" + str(parseVal.year).rjust(4, "0")
            return cleansedDate


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
        try:
            AddressString = re.sub(',', ' ', AddressString)
            OrderDict = ua.tag(AddressString, tag_mapping=tagMapping)
        except:
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


def var_getMaxAgeTaxIDAdd(row, ageLimit, maxAge):
    if row[maxAge]:
        return row[maxAge]
    elif row[ageLimit] and len(row[ageLimit].split("-")) > 1:
        return row[ageLimit].split("-")[1].strip()
    else:
        return ""


def var_getCity(row,parsedAdd):
    return row[parsedAdd].split(";")[2]


def var_BothAgeLimitFlag(row, minAgeCol, maxAgeCol):
    if row[minAgeCol] and row[maxAgeCol]:
        return 1
    elif (row[maxAgeCol] and not row[minAgeCol]) or (row[minAgeCol] and not row[maxAgeCol]):
        return 0
    else:
        return ""


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
'''
def var_deriveAddressIndicator(row, addInd, addType):
    if row[addType].lower() == "plsv" or row[addType].lower() == "combo":
        if row[addInd]:
            return row[addInd]
    return "S"

def var_standardiseString(row, inputCol, applyList):
    for st in applyList:
        if row[inputCol].lower() in [v.lower() for v in constants.standardsDict[st]]:
            return st
    return row[inputCol].lower()


def aboveAgeLimit(df,argDi):
    inpCol = argDi['inputCol']
    outputCol = argDi['outputCol']
    val = argDi['val']
    abv = argDi['standardsKeys'][0]
    abv = constants.standardsDict[abv]

    return cleanAgeLimit(df, inpCol, outputCol, abv, val, "above")


def concat_subframes(df, funInp):
    """form sub-dataframes from list of dictionary and
    concat it to the rest of the address frame"""
    inpList = []
    finalAddDf = pd.DataFrame()
    subframe1 = []
    groups=funInp['subFrames']
    exceptionDi=funInp.get("ExceptionColsForReplication",{})
    lot=[(group,exceptionDi.get(group,[]))for group in groups] #lot[('group1',[]),('group12',[])]
    for idx, row in df.iterrows():#looped this module to handle dynamic frames, was particularly done because of address change template in order to accomodate address old subframe
        inpList = [(pd.DataFrame(eval(row[tup[0]])),tup[1]) for tup in lot] # inpList = [(df1,expList),(df2,[])]
        subframe1=concatForwardFill(inpList)
        subframe2 = row
        addDf = concatForwardFill([pd.DataFrame([row]),subframe1])
        #addDfNew = pd.concat([addDf, subframe1], axis=1)
        finalAddDf = pd.concat([finalAddDf, addDf])
        finalAddDf = finalAddDf.reset_index(drop=True)
        finalAddDf = finalAddDf.fillna('')
    return finalAddDf


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
            ext = re.findall("[0-9]+", str1[0]) if str1 else []
            phn = re.findall(".+?(?=" + lst[0].lower() + ")", row[inpCol])
            if phn and ext:
                df.loc[idx, outCol] = ext[0]
                df.loc[idx, phnCol] = phn[0]
            else:
                df.loc[idx, outCol] = ""
        else:
            df.loc[idx, outCol] = ""

    return df


def getPhoneType(df, funInp):
    """determine the phone type if directly present else give address type as phone type"""
    df = df.fillna('')
    phn, phn_type, add_type = funInp
    df[phn_type] = df.apply(lambda row: var_getPhoneType(row, phn, phn_type, add_type), axis=1)
    return df


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

def blankCol(df,argDi):
    outputCol = argDi['outputCol']
    df[outputCol] = ""
    return df

def validateLength(df,funcInp):
    df=df.fillna('')
    col, size, type, excepCol = funcInp['params']
    mandatory = eval(funcInp.get('mandatory','True'))
    filter=0
    if funcInp.get('filter'):
        filter=1
    if excepCol!=col:
        df[excepCol]=df[col]
    for idx, row in df.iterrows():
        if isErrorVal(str(row[col])):
            continue
        if filter:
            if all([True if row[k] not in v else False for k,v in list(funcInp.get('filter').items())]):
                continue
        if type == "max":
            if row[col] == '' and mandatory:
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            elif row[col] and (len(str(row[col])) > int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size greater than " + size, row[col]])
        elif type == "eql":
            if row[col] == '' and mandatory:
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            if row[col] == '9999999999' and mandatory:
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "Field not given " + "", row[col]])
            elif row[col] and not (len(str(row[col])) == int(size)):
                df.loc[idx, excepCol] = ErrorMessage([], ["C", "size not eql to " + size, row[col]])
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


def applyRegex(df,argDi):
    df=df.fillna('')
    regexKey, inpColName=argDi['regexKey'],argDi['inputCol']
    for idx,row in df.iterrows():
        for regx in constants.regexDict[regexKey]:
            results=re.findall(regx, row[inpColName])
            if results:
                df.loc[idx, inpColName]= results[0]
                # return df
            else:
                continue
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


def nullAgeLimits(df,argDi):
    inpCol = argDi['inputCol']
    val = argDi['val']
    blankAgeLimits = argDi['standardsKeys'][0]
    blankAgeLimits = constants.standardsDict[blankAgeLimits]

    for key in blankAgeLimits:
        df[inpCol] = df[inpCol].replace({key: val})

    return df


def belowAgeLimit(df,argDi):
    inpCol = argDi['inputCol']
    outputCol = argDi['outputCol']
    val = argDi['val']
    below = argDi['standardsKeys'][0]
    below = constants.standardsDict[below]

    return cleanAgeLimit(df, inpCol, outputCol, below, val, "below")


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


def fnlElecCommType(df, argDi):
    elecCommCol = argDi['inputCol'][0]
    drvElecCommCol = argDi['inputCol'][1]
    outputCol = argDi['outputCol']
    def func(elecCommVal,drvElecCommVal):
        return elecCommVal if elecCommVal else drvElecCommVal

    df[outputCol]=df.apply(lambda row:func(row[elecCommCol],row[drvElecCommCol]),axis=1)
    return df


def defaultAddressCorrespondence(df, funInp):
    df = df.fillna('')
    inputCol = funInp['inputCol']
    outputCol = funInp['outputCol']
    df[outputCol] = df[inputCol].apply(lambda val: "S" if val=='' else val)
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


def getThreeLetterLanguageCode(df, funInp):
    """get 3 letter code for language"""
    lang = funInp["inputCol"]
    out_lang = funInp["outputCol"]
    df[out_lang] = df.apply(lambda row: var_getThreeLetterLanguageCode(row, lang), axis=1)
    return df


def abbreviateCol(df,argDi):
    df = df.fillna('')
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    maxLength = int(argDi['maxLength'])
    df[outCol]=df.apply(lambda row:abbreviate(row[inpCol], maxLength),axis=1)
    return df


def cleanSVDF(df, argDi):
    return cleanDF(df, argDi)


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


def getStdType(df, inp):
    """get standard value for the columns from standards dictionary using the key passed as argument"""
    df = df.fillna('')
    inputCol = inp["inputCol"]
    outputCol = inp["outputCol"]
    stdKey = inp["stdKey"]
    df[outputCol] = df.apply(lambda row: var_getStdType(row, inputCol, stdKey), axis=1)
    return df


def validatePhoneDf(df, funInp):
    """check if the phone number is of 10 digits or not; if not give clarification"""
    df = df.fillna('')
    phn = funInp["inputCol"]
    out_phn = funInp["outputCol"]
    df[out_phn] = df.apply(lambda row: var_validatePhoneTaxID(row, phn), axis=1)
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


def dropColumns(df,lstCols):
    commonCols=list(set(df.columns).intersection(set(lstCols)))
    df=df.drop(commonCols, axis=1)
    return df

def effectiveDatePsi(df,argDi):
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    def diffInYears(dateString):
        d0 = datetime.datetime.strptime((dateString.encode('ascii', 'ignore').strip()).decode('utf-8'), '%m/%d/%Y').date()
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

    df[outCol]=df.apply(lambda row:func(row[inpCol[0]].strip(), row[inpCol[1]].strip()), axis=1)
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
        bestMatch = elastic.gridLookup(idxName, inputStr, 1, **{'match_fields': ["comm_type_abb"], 'operator': 'and', 'restrict': True})
        if bestMatch:
            return bestMatch["comm_type_acrnm"]
        else:
            return ErrorMessage([], ["c", "Could not lookup in Grid", inputStr])
    df[outputColExc] = df.apply(lambda row: lkpExc(row[inputCol], excIdx), axis=1)
    df[outputColMaster] = df.apply(lambda row: lkpMaster(row[outputColExc], masterIdx), axis=1)
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

    df = df.fillna('')
    df=df.apply(lambda row: func(row[inputCol],row,outputColEC,outputColECTyp),axis=1)
    return df


def ErrorMessage(lst, Input):

    if not len(lst) >= 1:
        Type = Input[0]
        Message = Input[1]
        Inp = Input[2]
    else:
        Type = lst[0].populate(Input)
        Message = lst[1].populate(Input)
        Inp = lst[2].populate(Input)

    if Inp == '9999999999':
        intrimStr = str(Message) + "|" + "Input: "
    else:
        intrimStr = str(Message) + "|" + "Input: " + str(Inp)

    ##intrimStr = str(Message) + "|" + "Input: " + str(Inp)
    if Type == "R":
        return "Reject: " + intrimStr
    elif Type == "E":
        return "Error: " + intrimStr
    else:
        return "Clarify: " + intrimStr


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


def cleanDF(df, argDi):
    df=df.fillna('')
    col = argDi['inputCol']
    # outputCol = argDi['outputCol']
    blankVallst = argDi['standardsKeys']
    blankVallst = constants.standardsDict[blankVallst[0]]
    def replaceBlank(blankVallst, text):
        return re.sub('\\b' + '\\b|\\b'.join(blankVallst) + '\\b', '', text, flags=re.IGNORECASE)
    # deaSeries = df[deaCol]
    # for char in blankVallst:
        # df[col] = df[col].fillna('').str.lower().replace(char, "")
    df[col] = df.apply(lambda row: replaceBlank(blankVallst, row[col]), axis=1)
    return df

def determineEmlUrl(inpStr):
    urlRegex = r'''(?:ftp:\/\/|www\.|http(?:s)?:\/\/){1}[a-zA-Z0-9]{2,}\.[a-zA-Z0-9]{2,}(?:\S*)'''
    emailRegex = r'''([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b)'''
    diMap = {'W': urlRegex, 'E': emailRegex}
    for k, v in list(diMap.items()):
        if re.search(v, inpStr):
            return k


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


def isErrorVal(str):
    return bool(re.search("reject\:|error\:|clarify\:", str, re.IGNORECASE))


def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def var_getPhoneType(row, phn, phn_type, add_type):
    """utility for getPhoneType"""
    if row[phn] != "" and not str(row[phn]).isspace() and row[phn] is not None and row[phn] != '00000':
        if row[phn_type] == "" and row[add_type] != "":
            return row[add_type]
        elif row[phn_type] == "" and row[add_type] == "":
            return ErrorMessage([], ["C", "Mandatory field Missing Value" + "", ""])
        else:
            return row[phn_type]
    else:
        return ""


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


def var_getThreeLetterLanguageCode(row, lang):
    """utility for getting 3 letter language code row wise"""
    if isErrorVal(row[lang]):
        return row[lang]
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


def rowLvlFuncFromDfLayer(df,argDi):
    lstInputCol=argDi["inputCol"]
    outputCol=argDi["outputCol"]
    for idx,row in df.iterrows():
        df.loc[idx,outputCol]=eval(argDi['funcName'])(*[row]+lstInputCol)
    return df


def var_isInSot(row, degCol):
    return var_isInCol(row, degCol)


def var_getDegDetails(row, degdf, degprimCol, fltr, detailColName):
    return var_otherDFDetails(row, degdf, degprimCol, fltr, detailColName)


def var_ndbSpecIsMidLvl(row, colname, inSot):
    row = row.fillna('')
    if row[inSot] == "Y":
        if len([x for x in row[colname].split("|") if x]) > 0:
            return row[colname].split("|")[1]
        else:
            return ""
    else:
        return ""


def var_exceptionSpec(row, spec,degForLkp):
    spec = row[spec]
    degForLkp=row[degForLkp]
    if spec:
        bestMatch =lookupSpecialtyExceptions(spec,degForLkp)
        # bestMatch = elastic.gridLookup('specialty_exceptions', spec, 1, {}, **{'match_fields': ['specialty_name_in_sot'], 'operator': 'and', 'restrict': True})
        if bestMatch:
            return bestMatch["specialty_name_in_taxonomy_grid"]


def var_specialityFinalIndicator(row, Message, cleanColName):
    if row[cleanColName] == '' or str(row[cleanColName]).isspace() or str(row[cleanColName]).lower() == 'general':
        if row["ROW_NUM"] == 0:
            return "Primary"
        else:
            return "Secondary" #ErrorMessage([], ["C", Message, ""]) #ToDo: Check if this needs to be done here or in Field level logic
    else:
        return row[cleanColName]


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


def var_finalSpecPsi(row, inSot,sotExSpec, specCol):
    if row[inSot] == "Y":
        if row[sotExSpec]:
            return row[sotExSpec]
        else:
            return row[specCol]
    else:
        return ""


def var_ndbSpec(row, colname,spec, inSot):
    row = row.fillna('')
    if row[inSot] == "Y":
        if len([x for x in row[colname].split("|") if x]) > 0:
            return row[colname].split("|")[0]
        else:
            return ErrorMessage([], ["c", "could not match speciality", row[spec]])
    else:
        return ErrorMessage([], ["c", "speciality not found", row[spec]])


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


def updateRowsPsi(df,funcInp):
    diDf = funcInp.get('diDf')
    filterCondition=eval(funcInp.get('filterCondition','None'))
    colsToBeUpdated=funcInp['cols']
    updateValue=funcInp.get('replaceWithValue')
    updateCondition=funcInp.get('replaceWithCondition')
    updateWithCol=funcInp.get('replaceWithCol')
    clar=funcInp.get('clar')
    if isinstance(colsToBeUpdated,list):
        colsToBeUpdated={col:'' for col in colsToBeUpdated}
    for col,initCol in list(colsToBeUpdated.items()):
        if col not in list(df.columns):
            df[col]=df[initCol] if initCol else ''
        if filterCondition is not None:
            if clar:
                df.loc[filterCondition, col] = ErrorMessage([], [clar, updateValue, ""])

            else:
                if updateWithCol:
                    df.loc[filterCondition,col]=df.loc[filterCondition][updateWithCol]
                elif updateValue is not None:
                    df.loc[filterCondition, col]=updateValue
        else:
            if updateCondition is not None:
                resolvedDf=df.loc[eval(updateCondition)]
                if not resolvedDf.empty:
                    df.loc[:, col] = resolvedDf.iloc[0][updateWithCol]
            else:
                df.loc[:, col] = updateValue
    return df


def var_finalColIndicator(df, argDi):
    return var_degreeFinalIndicator(df, argDi)


def resetRowNumCount(df, prmlst):
    df["ROW_NUM"] = df.index
    df["ROW_COUNT"] = len(df.index)
    return df


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
            # if len(value) == 1:
            #     lstNormalizedRows.append(tdict)
            if [1 for key in list(splitColSets.keys()) if tdict[key] != None]:
                lstNormalizedRows.append(tdict)
    if lstNormalizedRows:
        dfNormalized = pd.DataFrame(lstNormalizedRows)
        return dfNormalized
    else:
        return splitDf


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


def sortDf(df,sortParams):
    df=df.sort_values(by=eval(sortParams[0]),ascending=eval(sortParams[1]))
    df=df.reset_index(drop=True)
    return df


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
                    if deg.lower() == dg.lower():
                        return di

    else:
        return res


def splitString(string, lstDelimsForSplit=constants.lstDelimsForHospSplit,exceptions=constants.hospitalDelimsExceptions):
    if any(ext in string for ext in exceptions):
        return [string]
    extSub = '|'.join(lstDelimsForSplit)
    regex = re.compile('(.+?)(?=' + extSub + ')(?:' + extSub + ')')
    return re.findall(regex, string)


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


def var_isInCol(row, col):
    if row[col] and not row[col].isspace():
        return "Y"
    else:
        return "N"


def var_otherDFDetails(row, df, specprimCol, fltr, detailColName):
    if not df.empty:
        result = df[df[specprimCol] == fltr]
        if not result.empty:
            return result[str(detailColName)].iloc[0]
        else:
            return ""


def mapStdValues(df, argDi):
    df = df.fillna('')
    col = argDi['inputCol']
    stdKeys = argDi['standardsKeys']
    outCol = argDi['outputCol']
    isMandatory = argDi.get('isMandatory',True)
    if outCol not in df.columns:
        df[outCol] = ''

    def func(inpStr):
        inpString=inpStr.strip().lower()
        if not inpString or isErrorVal(inpString):
            return inpStr
        for stdKey in stdKeys:
            for stdVal,inpVals in list(constants.standardsDict[stdKey].items()):
                inpVals=[elm.strip().lower() for elm in inpVals]
                if inpString in inpVals:
                    return stdVal
        if isMandatory:
            return ErrorMessage([], ["c", "Value is non blank but could not be standardized", inpString])
        else:
            return inpStr
    df[outCol]=df.apply(lambda row: func(row[col]),axis=1)

    return df

def npiLookup(df, argDi):
    if constants.preponeNPIReg:
        return df
    idx = df.index[0]
    npiParseFlagCol="npiParseFlag"
    npi = df.loc[idx, argDi["inpCol"]]
    npiregDicts = argDi["npiregDict"]
    npiRegDict = npiregDicts["NPI"]
    npiParseFlag = argDi[npiParseFlagCol]
    maxTries = argDi["maxTries"]
    outCol = argDi["output"]
    npiData=argDi["npiData"]
    npiRecord = ''
    while (maxTries > 0):
        maxTries -= 1
        # print("&&&&&&&&&&&&&&&&&&&&&&&&" + str(maxTries) + "&&&&&&&&&&&&&&&&&&&&")
        try:
            s = requests.session()
            r = s.get(constants.npiAPI + npi, proxies=constants.proxy, timeout=5,headers=constants.headers)
            # r = s.get(constants.npiAPI + npi, timeout=5, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"})
            npiRecord = json.loads(r.content)
        except: #requests.exceptions.RequestException as e:
            traceback.print_exc()
            continue
        else:
            npiRegDict, npiParseFlag = fillNpiColData(npiRecord, npiregDicts)
            break
        # npiParseFlag = "1"

    df["npiParseFlag"] = str(npiParseFlag)
    df[outCol] = json.dumps(npiRegDict)
    df[npiData]=str(npiRecord) if str(npiRecord) and str(npiParseFlag)=="1"  else ''
    return df


def npiFeildValidation(df, argDi):
    if argDi.get('ignore'):  # to ignore this step
        return df
    idx = df.index[0]

    col = argDi["col"]
    if isErrorVal(df.loc[idx, col]):
        return df
    colVal = removePunctuation(df.loc[idx, col].lower(),compressionLvl=1)

    npiCol = argDi["npiRegCol"]
    npiDict = json.loads( df.loc[idx, argDi["npiDict"]] )
    npiColVal = removePunctuation(npiDict[npiCol].lower(),compressionLvl=1)

    npiFlag = argDi["npiFlag"]

    if df.loc[idx, npiFlag] == "0":
        df.loc[idx, col] = ErrorMessage([], ["c", "Couldn't verify against NPI",colVal.upper()])
    elif colVal != npiColVal:
        df.loc[idx, col] = ErrorMessage([], ["c", "Verification with NPI failed"," -> " + colVal.upper() + " NPI_" + npiCol +" -> " + npiColVal.upper()])
    return df

def fillNpiColData(npiRecord, npiregDicts):
    if "results" in npiRecord and npiRecord["results"]:
        npiregDict = npiregDicts[npiRecord["results"][0]["enumeration_type"]]
        basicInfo = npiRecord["results"][0]["basic"]
        return {key: (basicInfo[npiregDict[key]] if npiregDict[key] in basicInfo else "") for key in list(npiregDict.keys())}, "1"
    else:
        return npiregDicts["NPI"], "0"

def cleanDot(val):
    if '.' in val:
        return val.replace('.', ' ').strip()
    else:
        return val


def removePunctuation(text, lstIgnore=[], lstPunct=[], compressionLvl=0):
    if isErrorVal(text) or not text.strip():
        return text
    if lstPunct:
        punct = "".join(lstPunct)
    elif lstIgnore:
        punct = re.sub('[{0}]+'.format(re.escape("".join(lstIgnore))), '', string.punctuation)
    else:
        punct = string.punctuation
    # replace_punctuation = string.maketrans(punct, " " * len(punct))
    replace_punctuation = dict((ord(char), " ") for char in punct)
    text = str(text).translate(replace_punctuation).strip()
    #text = text.translate(replace_punctuation).strip()    Pdf change
    if compressionLvl == 2:
        text = re.sub(r"[ ]+", "", text)
    elif compressionLvl == 1:
        text = re.sub(r"[ ]+", " ", text)
    return text


def removePunctDf(df,argDi):
    inpCol = argDi['inputCol']
    listToBeIgnored = argDi.get('lstIgnore',[])
    lstOfPuncs = argDi.get('lstOfPuncs', [])
    compress = argDi.get('compressionLvl', 0)
    outCol = argDi['outputCol']
    df[outCol] = df.apply(lambda row: removePunctuation(row[inpCol], listToBeIgnored, lstOfPuncs,compress), axis=1)
    return df

def removeRowDf(df, argDi):
    qry = argDi['qry']
    topRow= argDi.get('topRow', False)
    # df = df.query(qry[0])
    df = df.query(qry)
    if df.empty:
        df = createOneRowDf(df.columns)
    df = df.reset_index(drop=True)
    if topRow:
        return df.head(1)
    return df

def createOneRowDf(cols):
    df = pd.DataFrame([{col: None for col in cols}])
    df["ROW_COUNT"]=0
    df["ROW_NUM"]=0
    return df

def clarifyCorrelatedCols(df, funInp):
    """clarify the remaining cols when minEntry number of cols is having valid entry"""
    df = df.fillna('')
    correlatedCols = funInp.get("inputCol")
    minEntry = funInp.get("minEntry")
    ctr  = -1
    for idx, row in df.iterrows():
        ctr += 1
        rowBool = [row[x] == "" or row[x].lower() == "none" or row[x] == None for x in correlatedCols]
        if rowBool.count(False) >= int(minEntry):
            for x, trueV in enumerate(rowBool):
                if trueV == True:
                    if isErrorVal(row[correlatedCols[x]]):
                        continue
                    df.iloc[ctr, df.columns.get_loc(correlatedCols[x])] = ErrorMessage([], ["C", "Mandatory field Missing Value" + "",""])
    return df

def getCrossDfData(row, df,qry1,qry2,colSought,errMsg,msgType,ignoreErr=True):
    df1 = df.query(qry1)
    if df1.empty:
        df1 = df.query(qry2) if qry2 else df1
        if df1.empty:
            return ErrorMessage([], [msgType, errMsg, ""]) if errMsg else ""
        else:
            return ErrorMessage([], [msgType, errMsg, df1.iloc[0,df1.columns.get_loc(colSought)]])
    else:
        valSought=df1.iloc[0,df1.columns.get_loc(colSought)]
        if isErrorVal(valSought) and not ignoreErr:
            return ""

        return valSought

def accessDiColMapping(df,argDi):
    diDf=argDi['diDf']
    elmName=argDi['elmName']
    outCol=argDi['outputCol']
    df[outCol]=str(diDf[elmName])
    return df

def setDefaultValue(row, colName, defVal=""):
    if row[colName]:
        return row[colName]
    return defVal

######################## FUNCTIONS IN BELOW BLOCK ARE USED IN ADDR_ADD, HENCE MOVED TO COMMON  ########################

def getHandicapAccessType(df, argDi):
    df = df.fillna('')
    handicapAccess = argDi['inputCol'][0]
    handicapAccessType = argDi['inputCol'][1]
    outCol = argDi['outputCol']
    def func(handAccess, handAccessType):
        if handAccess == "Y":
            return handAccessType
        return ""
    df[outCol] = df.apply(lambda row: func(row[handicapAccess], row[handicapAccessType]), axis=1)
    return df

def getHandicapAccessTypeInd(df, argDi):
    df= df.fillna('')
    handicapAccess = argDi['inputCol'][0]
    handicapAccessType = argDi['inputCol'][1]
    handicapAccessInd = argDi['inputCol'][2]
    outCol = argDi['outputCol']
    def func(handAccess, handAccessType, handicapAccessInd):
        if handAccess == "Y":
            if handAccessType:
                if handicapAccessInd:
                    return handicapAccessInd
                return "Y"
        return ""
    df[outCol] = df.apply(lambda row: func(row[handicapAccess], row[handicapAccessType], row[handicapAccessInd]), axis=1)
    return df

def getDirInd(df,argDi):
    dirInd = argDi['inputCol'][0]
    addType = argDi['inputCol'][1]
    outCol = argDi['outputCol']
    def var_getDirInd(row, dirInd, addType):
        if row[addType].lower() == "d" or row[addType].lower() == "l":
            if row[dirInd] == "Y":
                return "YES"
            elif row[dirInd] == "N":
                return "NO"
            else:
                # return ErrorMessage([], ["C", "Directory Indicator not found" + "", ""])
                return "YES"
        else:
            return ""

    df[outCol] = df.apply(lambda row: var_getDirInd(row, dirInd, addType), axis=1)
    return df


def dbaNameDemoPrac(df, argDi):
    df= df.fillna('')
    diDf= argDi['diDf']
    dbaName = argDi['dba_name']
    grpName = argDi['grp_name']
    svDF= argDi['svDF']
    singleValDf = diDf[svDF].fillna('')
    groupName = singleValDf[grpName].iloc[0]
    outCol= argDi['outputCol']
    def func(dbaVal, grpVal):
        grpVal= grpVal.strip()
        dbaVal= dbaVal.strip()
        if grpVal and dbaVal and not isErrorVal(grpVal) and not isErrorVal(dbaVal):
            if grpVal == dbaVal:
                return grpVal
            else:
                return ErrorMessage([], ["c", "DBA Name and Group Name both are given", ""])
        elif grpVal and not isErrorVal(grpVal):
            return grpVal
        elif dbaVal and not isErrorVal(dbaVal):
            return dbaVal
        else:
            return dbaVal
    df[outCol] = df.apply(lambda row: func(row[dbaName], groupName), axis=1)
    return df

def combineAddressLine1n2(df,argDi):#moved from tax id to common
    """combines address line 1 and address line 2"""
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    df = df.fillna('')
    # df[outCol] = df[inpCol].apply(lambda x: ', '.join(x) if x!='' else ''.join(x), axis=1)
    df[outCol] = df[inpCol].apply(lambda x: ' '.join(x), axis=1)
    return df
############################################### ENDS HERE ##############################################

def negateDirInd(df,argDi):
    # returns 1 to suppress OUT_DIR_IND, 0 otherwise
    outputCol = argDi['outputCol']
    diColMappingCol = argDi['inputCol']
    orderCol = argDi['orderCol']
    searchKey = argDi['searchKey']
    standardsKey = argDi['standardsKeys']
    diColMapping = eval(df.iloc[0][diColMappingCol])
    for idx, row in df.iterrows():
        currOrder = str(row[orderCol]).strip()
        contTypeCol = searchKey + currOrder
        dirIndColName=[v for k,v in list(diColMapping.items()) if contTypeCol in k]
        if dirIndColName:
            dirIndColName = dirIndColName[0].lower()
            if any(substring in dirIndColName for substring in
                   constants.standardsDict[standardsKey[0]]) and \
                    any(substring not in dirIndColName for substring in
                        constants.standardsDict[standardsKey[1]]):
                df.loc[idx, outputCol] = 1
            else:
                df.loc[idx, outputCol] = 0
        else:
            df.loc[idx, outputCol] = 0
    return df


def getPhoneDirInd(df,argDi):
    dirInd = argDi['inputCol'][0]
    addType = argDi['inputCol'][1]
    phnType = argDi['inputCol'][2]
    outCol = argDi['outputCol']
    def var_getPhnDirInd(row, dirInd, addType, phnType):
        if row[addType].lower() == "d" or row[addType].lower() == "l":
            if row[phnType].lower() == "b" or row[phnType].lower() == "f":
                return ''
            elif row[dirInd]:
                return row[dirInd]
            else:
                return 'Y'
        else:
            return ""

    df[outCol] = df.apply(lambda row: var_getPhnDirInd(row, dirInd, addType,phnType), axis=1)
    return df

def setPhoneIndicator(df,funcInp):
    '''added this function to set value of of phonefaxindicator per address per address type per phoneType basis'''
    phnType=funcInp['PHONE_TYPE']
    adr1=funcInp['ADDRESS_LINE_1']
    adr2=funcInp['ADDRESS_LINE_2']
    city=funcInp['ADDRESS_CITY']
    state=funcInp['ADDRESS_STATE']
    zip=funcInp['ADDRESS_ZIP']
    adrType=funcInp['ADDRESS_TYPE']
    adrPhone = funcInp['ADDRESS_PHONE']
    addressList=[]
    addressListWithPhone=[]
    concatAddress=''
    outCol = funcInp['outputCol']
    for idx,row in df.iterrows():
        if row[phnType]=='F':
            df.loc[idx, outCol]='S'
        else:
            concatAddress=row[adr1]+row[adr2]+row[city]+row[state]+row[zip]+row[adrType]
            concatAddressWithPhone=concatAddress+row[adrPhone]
            if concatAddress not in addressList:
                addressList.append(concatAddress)
                df.loc[idx, outCol]='P'
                if row[adrPhone] and not isErrorVal(row[adrPhone]):
                    addressListWithPhone.append(concatAddressWithPhone)
            else:
                if concatAddressWithPhone in addressListWithPhone:
                    df.loc[idx, outCol] = 'P'
                else:
                    df.loc[idx, outCol]='S'
            concatAddress = ''
    return df


def removeSpecialCharacters(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    df[outCol] = ""
    df[outCol] = df.apply(lambda row: var_removeSpecialCharacters(row,inpCol),axis=1)
    return df


def addressLevelReplicationAtEnd(df, argDi):
    '''Redundant function, already handled in concat_subframes, so no longer required'''
    # replicates the first value at address level
    colsToBeReplicated = argDi['colsToBeReplicated']
    replicateByCol = argDi['replicateBy']
    df[colsToBeReplicated] = df[colsToBeReplicated].replace(r'^\s*$',np.nan,regex=True)
    for col in colsToBeReplicated:
        df[col] = df[col].fillna(df.groupby(replicateByCol)[col].transform('first'))
    df[colsToBeReplicated] = df[colsToBeReplicated].fillna('')
    return df

def replicationAtEnd(df,argDi):
    # replicates the first value at provider level; i.e. based upon max rows of masterdict dataframes
    colsToBeReplicated = argDi['colsToBeReplicated']
    diDF = argDi['diDf']
    df[colsToBeReplicated] = df[colsToBeReplicated].replace(r'^\s*$',np.nan,regex=True)
    #rowDi = df[colsToBeReplicated].dropna(how='all').to_dict('records')
    firstRowDi = df[colsToBeReplicated].iloc[0].to_dict()
    maxcount = max([len(dfi.index) for key, dfi in list(diDF.items())])
    lengthOfInpDf = len(df.index)
    tdict = {colNme: (firstRowDi[colNme] if colNme in colsToBeReplicated else np.nan) for colNme in df.columns}
    while lengthOfInpDf < maxcount :
        df = df._append(tdict, ignore_index=True)
        lengthOfInpDf +=1
    df = df.fillna('')
    return df

def var_specialityIdentifier(df, argDi):#added to move superspeciality from spec p/s to it's respective column
    specInd = argDi['specInd']
    superSpec = argDi['superSpec']
    specCol = argDi['spec']
    superSpecTmp = superSpec + "_TEMP"#added a temp column in order to avoid name ambiguity
    if df.empty:
        return df
    df1 = df.loc[(df[specInd] == 'supervising')]
    df2 = df.loc[(df[specInd] != 'supervising')]
    df1[superSpecTmp]=''
    if df2.empty:
        df1[specInd]=''
        df1[specCol]=ErrorMessage([], ["c", "speciality not found", ''])
        df=df1
    else:
        df1 = df1.reset_index(drop=True)
        df2 = df2.reset_index(drop=True)
        df1[superSpecTmp]=df1[superSpec]
        df3=pd.concat([df2,df1[superSpecTmp]], axis=1)
        df3[superSpec]=df3[superSpecTmp]
        df=df3
    return df.fillna('')

def alphaNumericCheck(colToCheck,chkType=''):
    if colToCheck.strip()=='':
        return colToCheck
    else:
        flag = False
        chkList=list(colToCheck)
        if chkType=='alpha':
            for i in chkList:
                if i.isdigit():
                    return ErrorMessage([], ["C", "Contains AlphaNumeric Values", colToCheck])
                else:
                    continue
        elif chkType=='numeric':
            for i in chkList:
                if i.isalpha():
                    return ErrorMessage([], ["C", "Contains AlphaNumeric Values", colToCheck])
                else:
                    continue
    return colToCheck

def createCol(df, argDi):
    df = df.fillna('')
    defVal = argDi['defVal']
    outCol = argDi['outputCol']
    df[outCol]=defVal
    return df

def addrCheck(df,argDi):
    addr = argDi['inputCol'][0]
    addType = argDi['inputCol'][1]
    outCol = argDi['outputCol']
    def var_getAddr(row, addr, addType):
        if ((row[addType].lower() in['plsv','combo','general']) & (not isErrorVal(row[addr]))):
            rgx = re.compile(r'\b[p][.]?\s?[o][.]?[-]?\s+(?:box)+', re.I)
            adrOut=''.join(row[addr])
            # df[outCol] = df[outCol].apply(lambda cellVal: addrCheck(cellVal))
            if rgx.search(adrOut):
                return ErrorMessage([], ["C", "PO Box found in Service Address", adrOut])
            else:
                return adrOut
        else:
            return row[addr]
    df[outCol] = df.apply(lambda row: var_getAddr(row, addr, addType), axis=1)
    return df

def concatForwardFill(inpList):
    '''concatenate and replicate the rows'''
    inpList=[elm if isinstance(elm,tuple) else (elm,[]) for elm in inpList] #[(df,[col1,col2]),(df1,[col1,col3])]
    rowCountAcrossSubframes = [len(tup[0]) for tup in inpList]
    inpList=[(tup[0],[tup[0].columns.get_loc(col) for col in tup[0].columns if col not in tup[1]]) for tup in inpList]
    #[(df,[location of the col index]),(df1,[location of the col index])]

    maxrowCountAcrossSubframes = max(rowCountAcrossSubframes)
    lot = list(zip(rowCountAcrossSubframes, inpList)) #lot [(2,(phoneDf,[1,2,3])),(3,(lagDf,[5,6,7]))]
    inpList = [pd.concat([tup[1][0]] + [tup[1][0].iloc[-1:, tup[1][1]]] * (maxrowCountAcrossSubframes - tup[0])) for tup in lot]
    concatResult = pd.concat([elm.reset_index(drop=True) for elm in inpList], axis=1)
    return concatResult

def sliceDf(df,argDi):
    return eval(argDi['qry'])


def negateCleanDirInd(df,argDi):
    dirInd = argDi['inputCol'][0]
    negate = argDi['inputCol'][1]
    outCol = argDi['outputCol']

    def negateDirInd(row,dirInd):
        if row[dirInd]:
            if int(row[negate]) == 1:
                return 'Y' if row[dirInd] == 'N' else 'N'
            else:
                return row[dirInd]
        else:
            return ""

    df[outCol] = df.apply(lambda row: negateDirInd(row, dirInd), axis=1)
    return df

def var_getMinAddrEffectiveDate(row, df, EffectiveDate, addrEffectiveDate):
    if not row[EffectiveDate]:
        df1=df[df[addrEffectiveDate]!='']
        df1['isError']=df.apply(lambda row:isErrorVal(row[addrEffectiveDate]),axis=1)
        df1=df1[df1['isError']!=True]
        if not df1.empty:
            try:
                df1['tempEffDate']=pd.to_datetime(df1[addrEffectiveDate])
                MinRowDf = df1[df1['tempEffDate']==min(df1['tempEffDate'])]
                if not MinRowDf.empty:
                   return list(MinRowDf[addrEffectiveDate])[0]
            except:
                return row[EffectiveDate]
    return row[EffectiveDate]

def getSpecValFromSvDF(df, argDi):
    diDf = argDi['diDf']
    svDF = diDf[argDi['svDF']].fillna('')
    apiSpeciality=argDi['apiSpeciality']
    useApiValCol=argDi['useApiVal']
    spec=argDi['spec']
    df.loc[(df[useApiValCol]=='Y'),spec]=svDF[apiSpeciality].iloc[0].split('|')[1]
    return df

def getTaxonomyFromAPIValues(df,argDi):
    diDf = argDi['diDf']
    specDF = diDf[argDi['specDF']].fillna('')
    taxonomy = argDi['taxonomy']
    apiTaxonomy=argDi['apiTaxonomy']
    useApiValCol=argDi['useApiVal']
    if any(specDF[useApiValCol]=='Y'):
        df[taxonomy]= df[apiTaxonomy].iloc[0].split('|')[0]
    return df

def isEmptySpec(df,argDi):
    df = df.fillna('')
    spec=argDi['spec']
    specInd = argDi['specInd']
    useApiValCol=argDi['outputCol']
    df[useApiValCol] = 'N'
    df.loc[((df[specInd]=='Primary')&(df[spec].str.strip()=='')),useApiValCol]='Y'
    return df

def checkAndCreateEmptyRow(df,argDi):
    df = df.fillna('')
    spltyInd = argDi['spltyInd']
    qry=argDi['qry']
    if not df.query(qry).empty :
        return df
    tdf=createOneRowDf(df.columns)
    tdf[spltyInd]='Primary'
    df = df._append(tdf, ignore_index = True)
    return df.reset_index(drop=True)

def wrapperForSpltyIndCalculation(df, argDi):
    df = df.fillna('')
    mergeSpltyIndInput=argDi['mergeSpltyIndInput']
    spltyInd=mergeSpltyIndInput[0]
    derivedSpltyInd=mergeSpltyIndInput[1]
    cleanSpltyIndInput=argDi['cleanSpltyIndInput']
    finalSpltyIndInput=argDi['finalSpltyIndInput']
    mrgSpltyInpOutput=argDi['mrgSpltyInpOutput']
    cleanSpltyIndOutput=argDi['cleanSpltyIndOutput']
    finalSpltyIndOutput=argDi['finalSpltyIndOutput']
    df[mrgSpltyInpOutput]=df.apply(lambda row:var_mergeDerivedWithInput(row, spltyInd,derivedSpltyInd),axis=1)
    df[cleanSpltyIndOutput]=df.apply(lambda row:var_standardiseString(row,mrgSpltyInpOutput,cleanSpltyIndInput),axis=1)
    df[finalSpltyIndOutput]=df.apply(lambda row:var_specialityFinalIndicator(row,finalSpltyIndInput,cleanSpltyIndOutput),axis=1)
    return df

def checkSpltyP_S(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    def func(row, inpCol):
        splty = row[inpCol]
        if splty == "Primary":
            return "P"
        elif splty == "Secondary":
            return "S"
        else:
            return splty
    df[outCol] = df.apply(lambda row: func(row, inpCol), axis=1)
    return df

def fetchSpecAndTaxonomyFromAPI(df,argDi):
    apiSpec = argDi['apiSpec']
    apiTaxonomyCode = argDi['apiTaxonomyCode']
    isPrimary=argDi['isPrimary']
    outCol = argDi['outputCol'][0]
    # outTaxonomyCol = argDi['outputCol'][1]
    df[outCol] = '' + '|' + ''
    npiData = df['npiData'].iloc[0]

    if not npiData.strip():
        return df
    npiRecord = eval(npiData)
    if npiRecord["results"]:
        taxonomies=npiRecord["results"][0].get("taxonomies",[])
        for taxonomy in taxonomies:
            inpSpecFromAPI=taxonomy.get(apiSpec,'')
            inpTaxonomyFromAPI=taxonomy.get(apiTaxonomyCode,'')
            primaryFlag=taxonomy.get(isPrimary,'')
            if primaryFlag and inpSpecFromAPI.strip():
                df[outCol]= inpTaxonomyFromAPI + '|' + inpSpecFromAPI
                # df[outTaxonomyCol]=inpTaxonomyFromAPI
                return df
    return df

def setDefaultValInRow(row,defVal=''):
    return defVal


def setDefaultClarification(row,clarType,msg):
    return ErrorMessage([],[clarType, msg, ''])

def setGenderFromAPI(df,funcInp):
    df=df.fillna('')
    inputCol=funcInp['inputCol']
    defVal = funcInp['defVal']
    condition=funcInp['condition']
    outputCol=funcInp['outputCol']
    df[outputCol] = df[inputCol]
    if df['npiParseFlag'].iloc[0]=='0':
        df.loc[eval(condition), outputCol] = defVal
    else:
        if eval(df['npiData'].iloc[0])['results'][0]["enumeration_type"] == "NPI-1":
            df.loc[eval(condition),outputCol]=eval(df['npiData'].iloc[0])['results'][0]['basic'].get('gender',defVal)
        else:
            df.loc[eval(condition), outputCol] = defVal
    return df

def createAndClarifyCol(df,argDi):
    df = df.fillna('')
    clrMsg=argDi['clrMsg']
    outCol = argDi['outputCol']
    df[outCol]=ErrorMessage([], ["C", clrMsg, ""])
    return df

# def updateColConditionally(df,argDi):
#     for di in argDi:
#         updateValue=di['updateValue']
#         condition=di['condition']
#         inputCol=di['inputCol']
#         outputCol=di['outputCol']
#         if outputCol not in df.columns:
#             df[outputCol]=''
#         boolVal=eval(condition)
#         if boolVal:
#             df.loc[boolVal,outputCol]=updateValue
#     return df

def lessThanXDigit(row, inpCol, errMsg, msgType, X):
    if len(row[inpCol]) < 9 and not isErrorVal(row[inpCol]):
        return ErrorMessage([], [msgType, errMsg, row[inpCol]])
    else:
        return row[inpCol]

def checkNonNumeric(row, inpCol, errMsg, msgType):
    if row[inpCol].isdigit() is False and not isErrorVal(row[inpCol]):
        return ErrorMessage([], [msgType, errMsg, row[inpCol]])
    else:
        return row[inpCol]

def getNpiDataFromAPI(df,funcInp):
    df = df.fillna('')
    inputCol = funcInp['inputCol']
    npiCol= funcInp['npiCol']
    df[inputCol] = eval(df['npiData'].iloc[0])['results'][0]['basic'][npiCol] if any(df['npiData']) else ''
    return df
#eval(df['npiData'].iloc[0])['results'][0]['basic']['gender'] if any(df['npiData']) else 'U'

def removeAndPrioritizeCategory(df,argDi):
    topRow = argDi.get('topRow', False)
    qry1 = argDi['qry1']
    qry2=argDi['qry2']
    df1 = df.query(qry1)
    if df1.empty:
        df1 = df.query(qry2) if qry2 else df1
        if df1.empty:
            df1 = createOneRowDf(df.columns)
    df1 = df1.reset_index(drop=True)
    if topRow:
        return df1.head(1)
    return df

def dateCheck(row,inpCol, dateToCheck, DOB_In_SOT, offSet=0):
    if row[inpCol]=='':
        return row[inpCol]
    inpDate = datetime.datetime.strptime(row[inpCol], '%m/%d/%Y')
    if str(inpDate.year)=='0001':
        return row[inpCol]
    DOB_In_SOT=row[DOB_In_SOT]
    dateNums = re.split('-+|/', DOB_In_SOT)
    Year_In_DOB=[True if len(num)==4 else False for num in dateNums]
    if len(DOB_In_SOT)<10 and not any(Year_In_DOB) and inpDate > dateToCheck:
        return (inpDate - relativedelta(years=offSet)).strftime('%d/%m/%Y')
    return row[inpCol]

def locateProviderInFile(df,argDi):
    levels = argDi['levels']
    fileNameCol=argDi["fileNameCol"]
    tabNameCol=argDi["tabNameCol"]
    outCol=argDi["outputCol"]
    template=argDi["template"]
    df=df.set_index(levels).join(pd.DataFrame({"tabs": df.groupby(levels)[tabNameCol].apply(lambda x: x.unique())})).reset_index()
    def fillTemplate(fname,tname):
        # if isinstance(tname,np.ndarray):
        return template.format(fname,','.join(tname))
        # else:
        #     return template.format(fname,tname)
    df[outCol]=df.apply(lambda row:fillTemplate(row[fileNameCol],row["tabs"]),axis=1)
    return df


def var_changetype(row,input):
    input = input['ACTION'][0]
    return input

def getBaidData(df, argDi):
    df = df.fillna('')
    out_inp_col = argDi['inputCol']
    npi_list = list(df['INDIVIDUAL_NPI'].unique())
    Addr_type = argDi['Addr_type']
    Address_value= argDi['Addr_value']

    for npi in npi_list:
        filter_df = df[(df["FINAL_NPI"] == npi) & (
                    (df[Addr_type].str.lower() == "h") | (df[Addr_type].str.lower() == "d"))]
        filter_df_Addr_unique_check =filter_df[Address_value].unique()
        for key, value in sorted(out_inp_col.items()):
            check_len_of_duplicate=len(filter_df_Addr_unique_check)
            index_of_filter_df = filter_df.index.values

            #------start-------
            if (not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower() == "h"), Addr_type].empty) and (
            not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower() == "d"), Addr_type].empty):

                df.loc[(df["FINAL_NPI"] == npi), value] = ""

            elif (not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower() == "h"), Addr_type].empty) or (
            not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower() == "d"), Addr_type].empty):
                if (key != Addr_type) and (check_len_of_duplicate == 1):
                    # df.loc[(df["FINAL_NPI"]==npi) & ((df[Addr_type].str.lower()=="h") | (df[Addr_type].str.lower() == "d")),value]=filter_df[key]
                    df.loc[(df["FINAL_NPI"] == npi), value] = filter_df[key].iloc[0]
                elif (key == Addr_type) and (check_len_of_duplicate == 1):
                    if not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower()=="h"),Addr_type].empty:
                        # df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower()=="h"),value]=filter_df[key].iloc[0]
                        df.loc[(df["FINAL_NPI"] == npi),value]=filter_df[key].iloc[0]
                    elif not df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower()=="d"),Addr_type].empty:
                        df.loc[(df["FINAL_NPI"] == npi),value] = filter_df[key].iloc[0]
                    else:
                        df.loc[(df["FINAL_NPI"] == npi) & (df[Addr_type].str.lower() != "d") & (df[Addr_type].str.lower() != "h"), value] = ""
                else:
                    df.loc[(df["FINAL_NPI"] == npi), value] = ""
            else:
                df.loc[(df["FINAL_NPI"] == npi), value] = ""
    return df

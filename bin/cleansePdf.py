import re
import numpy as np
import constants
import functionsForUI as fn
import pandas as pd
########################## cleanFinalDataframe ######################################################

def cleanString(df, argDi):
    # cleans column value as per regex specified
    # argDi= type:mv/sv, col:column to be cleaned, regex
    colStr = argDi['col']
    colType = argDi['type']
    if colType == 'mv':
        dfCols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==colStr]
    elif colType == 'sv':
        dfCols = [col for col in df.columns if colStr==col]
    if argDi['start'] and argDi['end']:
        regexPattern = r'{0}.*{1}'.format(argDi['start'], argDi['end'])
    else:
        regexPattern = argDi['regex']
    for ix, row in df.iterrows():
        for col in dfCols:
            df.loc[ix,col] = re.sub(regexPattern,' ', row[col], flags=re.IGNORECASE)
    return df

def createMvColumn(df, argDi):
    # creates output column with value taken from regex output on input string
    inpColStr = argDi['inpCol']   # inpCol: not a part of group
    dfInpCols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==inpColStr]
    outColStr = argDi['outCol']
    isInpColGrp = bool(constants.mvMappings[inpColStr].get('GROUP_DETAILS'))
    isOutColGrp = bool(constants.mvMappings[outColStr].get('GROUP_DETAILS'))
    dfOutCols = [col.replace(inpColStr,outColStr) for col in dfInpCols]
    if argDi['start'] and argDi['end']:
        regexPattern = r'(?<={0})(.*)(?={1})'.format(argDi['start'], argDi['end'])
    else:
        regexPattern = argDi['regex']
    for i, col in enumerate(dfInpCols):
        for ix, row in df.iterrows():
            val = re.findall(regexPattern, row[col], re.IGNORECASE)
            if val and isOutColGrp and not isInpColGrp:
                for j,elm in enumerate(val):
                    t = dfOutCols[i].rsplit('@',1)
                    outCol = t[0]+'#'+str(j)+'@'+t[1]
                    df.loc[ix,outCol] = elm
            elif val :
                df.loc[ix, dfOutCols[i]] = val[0]
    df.fillna('',inplace=True)
    return df

def cleanMvColVal(df, argDi):
    # no grouping
    inpColStr = argDi['inpCol'] # specifying tag and inpCol
    dfInpCols = [col for col in df.columns if col.rsplit('@',1)[0].split('#')[0] == inpColStr]
    outColStr = argDi['outCol'] # column to be cleaned
    dfOutCols = [col.replace(inpColStr,outColStr) for col in dfInpCols]
    for ix, row in df.iterrows():
        for i, col in enumerate(dfOutCols):
            ref = row[dfInpCols[i]].strip()
            if ref:
                if ref in row[col]: # checking if the entire string is present
                    row[col] = row[col].replace(ref, '')
                else:
                    inpParts = ref.split() # breaking string into parts and trying to remove parts
                    for part in inpParts:
                        row[col] = row[col].replace(part,'')
                df.loc[ix, col] = row[col]
    return df

def addClarifyColumn(df, argDi):
    # adds column(s) in dataframe and puts a clarification for all rows
    # default clarification message: "couldn't extract from PDF"
    colType = argDi['type']  # mv/sv
    outCol = argDi['col']    # e.g. DIR_IND, GROUP_NAME, etc
    refCol = argDi.get('mvColRef')   # helps to determine number of columns to create; corresponding to a reference for multivalue
    colTag = argDi.get('mvTag')  # e.g. GENERAL, otherwise None; if specified, columns corresponding only to that tag are created
    errMsg = argDi.get('errMsg','Not extracted from PDF')
    errType = argDi.get('errType','C')
    errInput = argDi.get('errInput','False')
    if colType == 'sv':
        dfOutCol = [outCol]
    elif colType == 'mv':
        if refCol != None:
            if colTag != None:
                colStr = colTag + '@' + refCol
                dfRefCol = [col for col in df.columns if col.rsplit('@',1)[0].split('#')[0] == colStr]
            else:
                dfRefCol = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0] == refCol]
            dfOutCol = [col.replace(refCol,outCol) for col in dfRefCol]
            isRefColGrp = bool(constants.mvMappings[refCol].get('GROUP_DETAILS'))
            isOutColGrp = bool(constants.mvMappings[outCol].get('GROUP_DETAILS'))
            if isRefColGrp and not isOutColGrp:
                dfOutCol = [re.sub(r'#\d','',col) for col in dfOutCol]
            if not isRefColGrp and isOutColGrp:
                dfOutCol = [col.rsplit('@',1)[0] + '#0@'+col.rsplit('@',1)[1] for col in dfOutCol]
        else:
            dfOutCol = [colTag + '@' + outCol + '@0']
    for ix, row in df.iterrows():
        for cx,col in enumerate(dfOutCol):
            inputInc = row[col] if errInput == 'True' else ""
            if not refCol:
                df.loc[ix, col] = ErrorMessage([], [errType,errMsg,inputInc])
            elif refCol and df.loc[ix,dfRefCol[cx]]:   # checking if there is a value in refCol of dataframe
                df.loc[ix, col] = ErrorMessage([], [errType,errMsg,inputInc])
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
    intrimStr = (Message) + "|" + "Input: " + (Inp)
    if Type == "R":
        return "Reject: " + intrimStr
    elif Type == "E":
        return "Error: " + intrimStr
    else:
        return "Clarify: " + intrimStr

def createMvDefaultCol(df, argDi):
    tag = argDi['tag']
    outCol = argDi['col']
    refCol = argDi['refCol']
    valDi = argDi['val']
    colStr = tag + '@' + refCol
    dfRefCol = sorted([col for col in df.columns if col.rsplit('@', 1)[0].split('#')[0] == colStr])
    for idx, row in df.iterrows():
        for ix, col in enumerate(dfRefCol):
            outColStr = col.replace(refCol, outCol) # forming string PLSV@ADDRESS_INDICATOR@2 corresponding to PLSV@ADDRESS_LINE_1@2
            if df[col][idx]:
                df.loc[idx,outColStr] = valDi[str(ix+1)] if str(ix+1) in valDi else valDi['default']
    df.fillna('', inplace=True)
    return df

###################################### cleanTableDataframeDi #######################################

def filterRows(df, argDi):
    refCol = argDi['refCol']
    df = df[df[refCol] != ''].reset_index(drop=True)
    return df

def mergeRows(df, argDi):
    refCol = argDi['refCol']
    df[refCol] = df[refCol].replace(r'^$', np.nan, regex=True).ffill()
    df = df.apply(lambda x:x.str.replace(r'(^.*$)',r' \1'))
    df = df.groupby(refCol, as_index=False).agg(sum).reset_index(drop=True)
    return df

def chooseRows(df,argDi):
    refCol = argDi['refCol']
    refVal = str(argDi['refVal'])
    notContainsFlag = argDi.get('notContains','')
    if notContainsFlag == 'True':
        return df[~df[refCol].str.contains(refVal, regex=False)]
    #outDf = df.loc[df[refCol] == refVal]
    return df[df[refCol].str.contains(refVal, regex=False)]

# def mergeByParenthesis(df,argDi):
#     refCol = argDi["refCol"]
#     df['order'] = ''
#     startIdx = 0
#     for ix, row in df.iterrows():
#         if '(' in row[refCol]:
#             startIdx = ix
#         df.loc[ix,'order'] = startIdx
#     outDf = df.groupby('order', as_index=False).agg(lambda x: ' '.join(x))
#     outDf.drop('order', axis=1, inplace=True)
#     return outDf

def splitAndSelectVal(df,argDi):
    inpCol=argDi['inpCol']
    outCol=argDi['outCol']
    splitBy=argDi['splitBy']
    startIdx=argDi['startIdx']
    EndIdx=argDi.get('EndIdx')
    colType = argDi['type']
    joinByVal=argDi.get('joinByVal',' ')
    if colType == 'mv':
        dfCols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==inpCol]
    elif colType == 'sv':
        dfCols = [col for col in df.columns if inpCol==col]
    for col in dfCols:
        df[col]=df.apply(lambda row:splitAndSelectIndex(row[col],splitBy,startIdx,EndIdx,joinByVal),axis=1)
    return df


def splitAndSelectIndex(col, splitBy, startIdx, EndIdx, joinByVal):
    col = col.strip().split(splitBy)
    return joinByVal.join(col[startIdx:EndIdx]) if len(col)>=startIdx+1 else ''

def removeDuplicateAddress(df,argDi):
    df=df.fillna('')
    newAddressCols=argDi['newAddressCols']
    oldAddressCols=argDi['oldAddressCols']
    for col1 in newAddressCols:
        df1Cols=[col for col in df.columns if len(col.split('@')) == 3 and col.split('@')[1].split('#')[0] == col1]
    for col2 in oldAddressCols:
        df2Cols = [col for col in df.columns if len(col.split('@')) == 3 and col.split('@')[1].split('#')[0] == col2]
    tempDF = pd.DataFrame()
    for col1 in df1Cols:
        tempDF[col1] = df[col1]
    for col2 in df2Cols:
        tempDF[col2]=df[col2]
    diDF={'temp': tempDF}
    mapping=fn.mimicUserInterimFile('',diDF)
    addressUserMapping = fn.transformUserMapping(mapping['temp']['mappings']['mv'], 'mv')['Address']

    def func(row, oldCols, newCols):
        newAddressData = ' '.join([row[col] for col in newCols if col in (df1Cols)])
        oldAddressData = ' '.join([row[col] for col in oldCols if col in (df2Cols)])
        if (newAddressData.strip() == oldAddressData.strip()) and oldAddressData.strip() != '':
            for col in newCols:
                row[col] = ''
        return row

    for ele2 in addressUserMapping[1]:
        for ele1 in addressUserMapping[1]:
            if (ele2[0].split('@')[-1] != ele1[0].split('@')[-1]):
                    df = df.apply(lambda row: func(row, ele2, ele1), axis=1)
    return df

def copyColValOnRefVal(df, argDi):
    inpCol = argDi['inpCol']
    refCol = argDi['refCol']
    outCol = argDi['outCol']
    refValLst = argDi['refVal']
    for ix, row in df.iterrows():
        if row[refCol].lower() in refValLst:
            df.loc[ix,outCol] = row[inpCol]
    return df

def removeColValOnRefColVal(df,argDi):
    inpCol = argDi['inpCol']
    refCol = argDi['refCol']
    refValLst = argDi['refVal']
    if inpCol in constants.mvMappings and refCol not in constants.mvMappings:
        inpDfCols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==inpCol]
        for ix, row in df.iterrows():
            if row[refCol].lower() in refValLst:
                for col in inpDfCols:
                    df.loc[ix, col] = ''
    else:
        for ix, row in df.iterrows():
            if row[refCol].lower() in refValLst:
                df.loc[ix, inpCol] = ''
    return df

def addClarificationOnRefColVal(df,argDi):
    inpCol = argDi['inpCol']
    refCol = argDi['refCol']
    refValLst = argDi['refVal']
    errMsg = argDi.get('errMsg', 'Not extracted from PDF')
    errType = argDi.get('errType', 'C')
    errInput = argDi.get('errInput', 'False')
    if inpCol in constants.mvMappings and refCol not in constants.mvMappings:
        inpDfCols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==inpCol]
        for ix, row in df.iterrows():
            if row[refCol].lower() in refValLst:
                for col in inpDfCols:
                    inputInc = row[col] if errInput == 'True' else ""
                    df.loc[ix, col] = ErrorMessage([], [errType,errMsg,inputInc])
    else:
        for ix, row in df.iterrows():
            if row[refCol].lower() in refValLst:
                inputInc = row[inpCol] if errInput == 'True' else ""
                df.loc[ix, inpCol] = ErrorMessage([], [errType,errMsg,inputInc])
    return df

def splitAndSelectTable(df,argDi):
    col = argDi['col']
    splitBy = argDi['splitBy']
    startIdx = argDi['startIdx']
    endIdx = argDi.get('endIdx')
    joinByVal = argDi.get('joinByVal', ' ')
    df[col] = df.apply(lambda row: splitAndSelectIndex(row[col], splitBy, startIdx, endIdx, joinByVal), axis=1)
    return df


def splitRows(df,argDi):
    refCol = argDi['refCol']
    refValChk = argDi['refVal']
    df[refCol] = df[refCol].str.split(refValChk)
    df = df.explode(refCol).reset_index(drop=True)
    return df


def decideColVal(df,argDi):
    regexValDi = argDi['regexDi']
    colStr = argDi['col']
    colType = argDi['type']
    if colType == 'mv':
        dfCols = [col for col in df.columns if len(col.split('@')) == 3 and col.split('@')[1].split('#')[0] == colStr]
    elif colType == 'sv':
        dfCols = [col for col in df.columns if colStr == col]
    for ix, row in df.iterrows():
        for col in dfCols:
            for val, regexPattern in regexValDi.items():
                if re.findall(regexPattern, row[col], re.IGNORECASE):
                    df.loc[ix,col] = val
                    break
    return df


def copyAndCreateCol(df,argDi):
    inpCol = argDi['inpCol']
    outCol = argDi['outCol']
    df[outCol] = df[inpCol]
    return df

def ffillCol(df,argDi):
    tag = argDi['tag']
    outCol = argDi['col']
    refCol = argDi['refCol']
    colStr = tag + '@' + refCol
    dfRefCol = sorted([col for col in df.columns if col.rsplit('@', 1)[0].split('#')[0] == colStr])
    colWithVal = dfRefCol[0].replace(refCol,outCol)
    for idx, row in df.iterrows():
        for ix, col in enumerate(dfRefCol):
            outColStr = col.replace(refCol,outCol)  # forming string PLSV@ADDRESS_INDICATOR@2 corresponding to PLSV@ADDRESS_LINE_1@2
            df.loc[idx, outColStr] = row[colWithVal]
    df.fillna('', inplace=True)
    return df


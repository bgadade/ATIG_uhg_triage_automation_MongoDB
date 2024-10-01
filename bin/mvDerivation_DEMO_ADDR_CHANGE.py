import pandas as pd
import copy

import mvDerivation_Common as mvCmn

def combineOldAddress(row, funInpPhone, funInpGeneric, createCol, colInp):
    """combines old phone and fax number's to one column"""
    inp = row[colInp]
    res=''
    newInp = []
    for dicts in eval(inp):
        dictColGeneric = {col:dicts[col] for col in funInpGeneric}
        for phoneRow in funInpPhone:
            tdictColGeneric=copy.deepcopy(dictColGeneric)
            phoneVal = dicts[phoneRow[0]] if dicts[phoneRow[0]] else str("9999999999")
            extVal = dicts[phoneRow[1]] if phoneRow[1] else ""
            phoneTypeVal = phoneRow[2] if phoneRow[2] and (phoneVal or extVal) else ""
            phoneCol,extCol,typeCol=createCol[0],createCol[1],createCol[2]
            tdictColGeneric[phoneCol] = phoneVal
            tdictColGeneric[extCol] = extVal
            tdictColGeneric[typeCol] = phoneTypeVal
            newInp.append(tdictColGeneric)
        df= pd.DataFrame(newInp).drop_duplicates()#.iterrows()
        phdf=df[(df[phoneCol]!='') | (df[extCol]!='')]
        edf=df[(df[phoneCol]=='') & (df[extCol]=='') ]
        if phdf.empty:
            res=edf.drop_duplicates()
        else:
            res=phdf
    return str([dict(row) for idx, row in res.iterrows()])


def copyOldNewAddress(df,argDi):
    oldAddrCols = argDi['oldAddressCols']
    newAddrCols = argDi['newAddressCols']
    lstColNmTup=list(zip(oldAddrCols, newAddrCols))
    for ix,row in df.reset_index().iterrows():
        if not any([row[col] for col in oldAddrCols]) and any([row[col] for col in newAddrCols]):
            for oldCol,newCol in lstColNmTup:
                df.iloc[ix, df.columns.get_loc(oldCol)] = row[newCol]
        elif any([row[col] for col in oldAddrCols]) and not any([row[col] for col in newAddrCols]):
            for oldCol, newCol in lstColNmTup:
                df.iloc[ix, df.columns.get_loc(newCol)] = row[oldCol]
    return df

def get_fnlState(df,argDi):
    outCol = argDi['outputCol']
    parsedState = argDi['inputCol'][0]
    parsedAddLine1 = argDi['inputCol'][1]
    addCity = argDi['inputCol'][2]
    df[outCol] = df.apply(lambda row: mvCmn.var_fnlState(row, parsedState, parsedAddLine1, addCity), axis=1)
    return df


# set phonefaxInd
def getPhoneFaxDirInd(df,argDi):
    oldAddrDirInd = argDi['inputCol'][0]
    oldAddrPhnType = argDi['inputCol'][1]
    outCol = argDi['outputCol']
    phoneTypeList = ['p','c']

    for idx,row in  df.iterrows():
        phnType = row[oldAddrPhnType]
        dirInd = row[oldAddrDirInd]
        if (phnType.lower() in phoneTypeList and (not mvCmn.isErrorVal(phnType))) and (dirInd and (not mvCmn.isErrorVal(dirInd))):
            df.loc[idx,outCol] =  dirInd
        else:
            df.loc[idx,outCol] = ''
    return df

def copyPhnType(df,funcInp):
    oldPhntype, oldAddrType, newPhnType = funcInp
    def checkPhnType(row,oldPhntype, newPhnType,oldAddrType):
        oldPhntype = row[oldPhntype]
        newPhnType = row[newPhnType]
        return newPhnType if ((not oldPhntype) and (not oldAddrType)) else oldPhntype

    df[oldPhntype] = df.apply(lambda row: checkPhnType(row,oldPhntype, newPhnType,oldAddrType), axis=1)
    return df


def clarifyPhoneDirInd(df,argDi):
    phnType = argDi['inputCol'][0]
    outPhnIndCol = argDi['outputCol'][0]
    outPhnDirIndCol = argDi['outputCol'][1]
    clarifyMsg = mvCmn.ErrorMessage([], ["c", "Phone type is invalid", ""])
    for idx,row in df.iterrows():
        if mvCmn.isErrorVal(row[phnType]):
            df.iloc[idx,df.columns.get_loc(outPhnIndCol)] = clarifyMsg
            df.iloc[idx,df.columns.get_loc(outPhnDirIndCol)] = clarifyMsg
    return df
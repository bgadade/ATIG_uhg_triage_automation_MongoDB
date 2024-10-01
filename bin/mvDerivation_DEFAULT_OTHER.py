import pandas as pd

import re

from commonFiles import diImport
import mvDerivation_Common as mv_cmn

def isErrorVal(str):
    return bool(re.search("reject\:|error\:|clarify\:", str, re.IGNORECASE))

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
            lambda x: ErrorMessage([], ["C", clrMsg, x]) if not isErrorVal(x) else x)
    df = df.set_index('index')
    return df

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
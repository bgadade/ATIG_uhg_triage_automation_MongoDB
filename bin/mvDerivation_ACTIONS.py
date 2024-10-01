import pandas as pd
def gatherColumns(df,argDi):
    lstCandidateCols = [col for col in df.columns if len(col.split('@')) == 3]
    lstKeys=[]
    template={}
    for col in lstCandidateCols:
        lstColElm = col.split('@')
        pcat, outCol, setIdx = lstColElm[0], lstColElm[1], lstColElm[-1]
        lstGrpElm = outCol.split('#')
        outCol = lstGrpElm[0]
        if len(lstGrpElm) == 2:
            lstKeys.append((outCol,(pcat + '@' + setIdx,lstGrpElm[1])))
            template.setdefault(outCol,{}).setdefault(pcat + '@' + setIdx,{}).update({lstGrpElm[1]:""})
        else:
            lstKeys.append((outCol, (pcat + '@' + setIdx,)))
            template.setdefault(outCol, {}).update({pcat + '@' + setIdx: ""})
    outCols=[tup[0] for tup in lstKeys]
    mvDf = df[lstCandidateCols]
    ser=mvDf.apply(lambda row: 'pi3.141'.join(row.values), axis=1)
    newSer=ser.apply(lambda val:gather(val,lstKeys,outCols))
    df=df.drop(lstCandidateCols,axis=1)
    df[outCols]=newSer.str.split('pi3.141',expand=True)
    return df

def gather(val,lstKeys,outCols):
    row=val.split('pi3.141')
    valDi={}
    for ix,tup in enumerate(lstKeys):
        outCol=tup[0]
        meta=tup[1]
        if len(meta)==3:
            valDi.setdefault(outCol, {}).setdefault(meta[0], {}).update({meta[1]: row[ix]})
        else:
            valDi.setdefault(outCol, {}).update({meta[0]: row[ix]})

    return 'pi3.141'.join([str(valDi[col]) for col in outCols])
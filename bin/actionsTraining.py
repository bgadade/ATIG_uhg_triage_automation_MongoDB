import constants
import datetime
import pandas as pd

def fetchDataAndConvertToDf():
    data = constants.handler.find_documents('ActionData')
    hashWiseData = {}
    for di in data:
        hashWiseData.setdefault(di[constants.hashInUse], {}).setdefault(tuple(di['validatedActions']), []).append(di)
    groupedData = {}
    for hash, di in list(hashWiseData.items()):
        for actionTup, lstData in list(di.items()):
            groupedData.setdefault(hash, []).extend(
                [(len(lstData), datetime.datetime.strptime(di['time'], "%Y-%m-%d %H:%M:%S.%f"), di['data']) for di in
                 lstData])
    flattenedData = []
    for hash, lst in list(groupedData.items()):
        flattenedData.append(
            sorted(lst, key=lambda tup: (tup[0], (tup[1] - datetime.datetime(1970, 1, 1)).total_seconds()),
                   reverse=True)[0][2])
    lstDf = []
    for lstDi in flattenedData:
        tdf=pd.DataFrame(lstDi)
        lstDf.append(tdf[constants.actionModelColsInclusion])
    df = pd.concat(lstDf)
    return df

def main():
    df=fetchDataAndConvertToDf()
    print(df)

if __name__=='__main__':
    main()
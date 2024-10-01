import re
import pandas as pd
import constants


def splitString(string, lstDelimsForSplit=constants.lstDelimsForSplit):
    extSub = '|'.join(lstDelimsForSplit)
    regex = re.compile('(.+?)(?=' + extSub + ')(?:' + extSub + ')')
    return re.findall(regex, string)


def splitColumn(df,parentCat):
    splitCols = constants.splitCols[parentCat]
    lstRows = []
    for index, row in df.iterrows():
        for col in splitCols:
            splitColValues = splitString(row[col])
            for idx, value in enumerate(splitColValues):
                row[col + '_'+str(idx)] = value
        lstRows.append(row)
    dfNew = pd.DataFrame(lstRows)
    splitColSets = {}
    for splitCol in splitCols:
        splitColSets.update({splitCol: [col for col in list(dfNew.columns) if re.search(splitCol + '_[0-9]+', col)]})
    excludedCols = list(splitColSets.keys()) + [item for sublist in list(splitColSets.values()) for item in sublist]

    lstNormalizedRows = []
    for index, row in dfNew.iterrows():

        for i in range(max([len(value) for key, value in list(splitColSets.items())])):
            tdict = {key: value for key, value in list(dict(row).items()) if key not in excludedCols}
            tdict.update({key: None if pd.isnull(row.get(value[i] if len(value) > i else None)) else row.get(
                value[i] if len(value) > i else None) for key, value in list(splitColSets.items())})
            if [1 for key in list(splitColSets.keys()) if tdict[key] != None]:
                lstNormalizedRows.append(tdict)
    dfNewFinal = pd.DataFrame(lstNormalizedRows)
    return dfNewFinal

if __name__=='__main__':
    df = pd.DataFrame({'DEGREE': pd.Series(['deg1,deg2  deg3', 'deg1,deg2', 'deg1,deg2;deg4', 'deg2,deg3,deg7']),
                       'SPECIALITY': pd.Series(['sp1,sp2,sp3,sp4', 'sp1,sp2', 'sp1,sp2;sp3', 'sp1,sp2,sp4']),
                       'id': pd.Series([100, 101, 102, 103])})
    print(splitColumn(df,'Degree'))
import json
shapes=[]
cols=[]
for masterDict in masterDictList:
    shapes.append({pc:df.columns.tolist() for pc,df in list(masterDict.items()) if pc !='diColMapping'})
print(shapes)
for val in list(shapes[0].values()):
    cols+=val
uniqCols=list(set(cols))
colDi={}
for col in uniqCols:
    colDi[col]=[]
    for k,v in list(shapes[0].items()):
       if col in v:
           colDi[col].append(k)
print(json.dumps(colDi))

newDi={}
for k,v in list(colDi.items()):
    if len(v)>1:
        newDi.update({k:(v,len(v))})
print(json.dumps(newDi))

pCats=['Credential', 'Speciality', 'Degree', 'Title', 'FinalDegree', 'Medicare', 'Board', 'Licence', 'Address', 'Dea', 'singleValue', 'Cdscsr']

diRemove={}
for pCat in pCats:
    diRemove.update({pCat:[]})
    for k,v in list(newDi.items()):
        if pCat in v[0]:
            diRemove[pCat].append(k)
print(json.dumps(diRemove))
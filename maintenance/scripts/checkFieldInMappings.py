from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.Triage

def func(collection):
    res= collection.find({})
    tmp=[]
    for i in res:
        tmp.append(i)
    lstSvMapInterim=[(usrMap['Provider'],doc["mappings"]["sv"]) if usrMap.get('Provider') else (usrMap['UserName'],doc["mappings"]["sv"]) for usrMap in tmp for tab,doc in usrMap["Doc"].items() if isinstance(doc["mappings"],dict) ]
    usermapInterim=[provNm for provNm,val in lstSvMapInterim for k,v in val.items() if v=='PRAC_DEL_CRED_DT']
        #usermap.append(lstSvMap[key])
           # print key
    for map in usermapInterim:
        print (map)

for typ,col in list({"usr":db.User,"prov":db.Provider}.items()):
    print('collection :',typ)
    func(col)
    print('*****')
import math
import json
import elastic
settings= {"analysis": {
      "filter" : {
                "my_stemmer" : {
                    "type" : "stemmer",
                    "name" : "english"
                },
                "my_snow":{
                    "type" : "snowball",
                    "language" : "english"
                },
                "english_stop":{
                    "type":"stop",
                    "language":"english"
                }
            },
      "analyzer": {
        "custom_stem": {
          "type": "custom",
          "tokenizer":"standard",
          "filter":["lowercase","my_snow","english_stop"]
        },
        "custom_nostem": {
          "type": "custom",
          "tokenizer":"standard",
          "filter":["lowercase","english_stop"]
        }
      }
    }
}






import constants
import elastic
import pickle as pickle

mappings = {"properties": {}}
if constants.elastic5pt6pt4:
    mappings = {"my_type":{"_all": {"enabled": False},"properties": {}}}

def indexing():
    docTyp = 'my_type'
    if not constants.elastic5pt6pt4:
        docTyp = constants.docTyp
    index_input_files = {constants.svNlpPickle:'sv_nlp', constants.mvNlpPickle:'mv_nlp'}
    for inpFile, idxName in list(index_input_files.items()):
        nlp_lookup = eval(constants.handler.find_one_document(inpFile)[inpFile])
        schema = elastic.generate_schema(idxName, settings, mappings)
        print('schema:',schema)
        # exit(0)
        elastic.create_index(idxName, 1, idxBody=schema)
        lst_body=[]
        for pcat,di in list(nlp_lookup.items()):
            lstSotColNameBufferOriginal,lstSotColNameNlpBufferWord,lstSotColNameNlpBufferSubWord=[],[],[]
            for sotCol,diTokenLst in list(di.items()):
                lstSotColNameBufferOriginal+=list(sotCol.strip().lower().replace('_',' ').split())
                # lstSotColNameNlpBufferWord.append(' '.join([item for item in diTokenLst['word_list'] if (item and len(item.strip())>1)]))
                lstSotColNameNlpBufferWord+=' '.join([item.strip().replace('_',' ') for item in diTokenLst['word_list'] if (item and len(item.strip())>1)]).split()
                # lstSotColNameNlpBufferSubWord.append(' '.join([item for sublist in diTokenLst['sub_word_list'] for item in sublist if (item and len(item.strip())>1)]))
                lstSotColNameNlpBufferSubWord+=' '.join([item.strip().replace('_',' ') for sublist in diTokenLst['sub_word_list'] for item in sublist if (item and len(item.strip())>1)]).split()
            if idxName=='sv_nlp':
                # body={'outputCol':pcat,'sotColNameBufferOriginal':' '.join([ token for token in lstSotColNameBufferOriginal if token]),'sotColNameNlpBufferWord':' '.join([token for token in lstSotColNameNlpBufferWord if token]),'sotColNameNlpBufferSubWord':' '.join([token for token in lstSotColNameNlpBufferSubWord if token])}
                body={'outputCol':pcat,'sotColNameBufferOriginal':' '.join(list(set(lstSotColNameBufferOriginal))),'sotColNameNlpBufferWord':' '.join(list(set(lstSotColNameNlpBufferWord))),'sotColNameNlpBufferSubWord':' '.join(list(set(lstSotColNameNlpBufferSubWord)))}
            else:
                # body={'parentCat':pcat,'sotColNameBufferOriginal':' '.join([token for token in lstSotColNameBufferOriginal if token]),'sotColNameNlpBufferWord':' '.join([token for token in lstSotColNameNlpBufferWord if token]),'sotColNameNlpBufferSubWord':' '.join([token for token in lstSotColNameNlpBufferSubWord if token])}
                body={'parentCat':pcat,'sotColNameBufferOriginal':' '.join(list(set(lstSotColNameBufferOriginal))),'sotColNameNlpBufferWord':' '.join(list(set(lstSotColNameNlpBufferWord))),'sotColNameNlpBufferSubWord':' '.join(list(set(lstSotColNameNlpBufferSubWord)))}
            elastic.es.index(index=elastic.updateIndexName(idxName), doc_type=docTyp, body=body)
            # lst_body.append(body)+

def indexingBags():# this function will index one mapping item as one entry
    docTyp='my_type'
    if not constants.elastic5pt6pt4:
        docTyp=constants.docTyp
    index_input_files={constants.svNlpPicklePath:'sv_nlp_test',constants.mvNlpPicklePath:'mv_nlp_test'}
    # index_input_files={constants.mvNlpPicklePath:'mv_nlp_test'}
    for inpFile,idxName in list(index_input_files.items()):
        with open(inpFile, 'rb') as fp:
            nlp_lookup = pickle.load(fp)#todo put a check to recreate index only when the picle file is not empty within keys
        schema = elastic.generate_schema(idxName, settings, mappings)
        print('schema:',schema)
        # exit(0)
        elastic.create_index(idxName, 1, idxBody=schema)
        lst_body=[]
        for pcat,di in list(nlp_lookup.items()):
            for sotCol,diTokenLst in list(di.items()):
                lstSotColNameBufferOriginal,lstSotColNameNlpBufferWord,lstSotColNameNlpBufferSubWord='','',''
                lstSotColNameBufferOriginal=sotCol.strip().lower().replace('_',' ')
                # lstSotColNameNlpBufferWord.append(' '.join([item for item in diTokenLst['word_list'] if (item and len(item.strip())>1)]))
                lstSotColNameNlpBufferWord=' '.join([item.strip().replace('_',' ') for item in diTokenLst['word_list'] if (item and len(item.strip())>1)])
                # lstSotColNameNlpBufferSubWord.append(' '.join([item for sublist in diTokenLst['sub_word_list'] for item in sublist if (item and len(item.strip())>1)]))
                lstSotColNameNlpBufferSubWord=' '.join([item.strip().replace('_',' ') for sublist in diTokenLst['sub_word_list'] for item in sublist if (item and len(item.strip())>1)])
                if idxName=='sv_nlp' or idxName=='sv_nlp_test':
                    # body={'outputCol':pcat,'sotColNameBufferOriginal':' '.join([ token for token in lstSotColNameBufferOriginal if token]),'sotColNameNlpBufferWord':' '.join([token for token in lstSotColNameNlpBufferWord if token]),'sotColNameNlpBufferSubWord':' '.join([token for token in lstSotColNameNlpBufferSubWord if token])}
                    body={'outputCol':pcat,'sotColNameBufferOriginal':lstSotColNameBufferOriginal,'sotColNameNlpBufferWord':lstSotColNameNlpBufferWord,'sotColNameNlpBufferSubWord':lstSotColNameNlpBufferSubWord}
                else:
                    # body={'parentCat':pcat,'sotColNameBufferOriginal':' '.join([token for token in lstSotColNameBufferOriginal if token]),'sotColNameNlpBufferWord':' '.join([token for token in lstSotColNameNlpBufferWord if token]),'sotColNameNlpBufferSubWord':' '.join([token for token in lstSotColNameNlpBufferSubWord if token])}
                    body={'parentCat':pcat,'sotColNameBufferOriginal':lstSotColNameBufferOriginal,'sotColNameNlpBufferWord':lstSotColNameNlpBufferWord,'sotColNameNlpBufferSubWord':lstSotColNameNlpBufferSubWord}
                elastic.es.index(index=idxName, doc_type=docTyp, body=body)


def getMinShouldMatchValue(searchString,pct):
    if int(elastic.getTokenLength(searchString))>2 and int(elastic.getTokenLength(searchString))<=5:
        return str(int(pct*100))+"%"
    elif int(elastic.getTokenLength(searchString))>5:
        return 5
    else:
        return int(math.ceil(int(elastic.getTokenLength(searchString))*pct))

def elasticMapping(idxName='sv_nlp',searchString='tx id',excluded=None):
    docTyp = 'my_type'
    if not constants.elastic5pt6pt4:
        docTyp = constants.docTyp
    idxName=elastic.updateIndexName(idxName)
    searchString=searchString.replace('_',' ')
    query = {"query": {"bool": {"must": [{"multi_match": {"query": searchString, "type": "best_fields",
                                                          "fields": [
                                                              "sotColNameNlpBufferWord"
                                                              ,
                                                              "sotColNameNlpBufferSubWord"
                                                                ,
                                                              # "sotColNameNlpBufferWord.english"
                                                              # ,"sotColNameNlpBufferSubWord.english"
                                                              # ,
                                                              "sotColNameBufferOriginal.english"
                                                          ]
        ,"fuzziness":1
        ,"minimum_should_match":"100%"
                                                          }}],
                                "should": [
                                    {"match": {"sotColNameBufferOriginal" : {"query": searchString, "fuzziness": "AUTO","minimum_should_match":"100%"}}}]}}}
    if excluded and idxName=='sv_nlp':
        if "must_not" not in query["query"]["bool"]:
            query["query"]["bool"].update({"must_not":[]})
        for item in excluded:
            query["query"]["bool"]["must_not"].append({"term":{"outputCol":item}})
    res = elastic.es.search(index=idxName,doc_type=docTyp,body=query)
    if res and res.get('hits'):
        for hit in res['hits']['hits']:
            # return hit,hit['_source'].get('parentCat',hit['_source'].get('outputCol'))
            print('***  query ****:',json.dumps(query),'*** 1st result ***',hit['_source'].get('parentCat',hit['_source'].get('outputCol')))
            return hit['_source'].get('parentCat',hit['_source'].get('outputCol'))
    else:
        return None


if __name__=='__main__':
    indexing()
    # print elastic.getTokenLength("group/site location name")
    # print elasticMapping(searchString="address 1",idxName="mv_nlp",excluded=[])
    # print getMinShouldMatchValue("phone number temp",0.75)
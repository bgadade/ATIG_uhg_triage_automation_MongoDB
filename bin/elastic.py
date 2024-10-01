import numpy as np
from elasticsearch import Elasticsearch,helpers
import requests
import tqdm
import numpy as np
import json
import pprint
# import json
import pandas as pd
# import elasticConfig as esconf
import re
from copy import deepcopy
import constants
import csv
import utils
debugValue=0
debugQuery=0
# logging
# to check if ElasticSearch is up
# res = requests.get('http://10.113.171.116:9200')
# print(res.content)

from elasticsearch import RequestsHttpConnection

class MyConnection(RequestsHttpConnection):
    def __init__(self, *args, **kwargs):
        proxies = kwargs.pop('proxies', {})
        super(MyConnection, self).__init__(*args, **kwargs)
        self.session.proxies = proxies

# es = Elasticsearch(["http://10.113.171.116:9200"], connection_class=MyConnection, proxies = {'http': 'http://10.113.50.55:8080'})
#es = Elasticsearch([{'host': '10.142.207.250', 'port': 9200}], connection_class=MyConnection, proxies = {'http': 'http://10.142.207.250:8080'}) #linux server
es = Elasticsearch([constants.elasticUrl])
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

index_input_files=constants.esConfig['index_input_files']
if constants.elastic5pt6pt4:
    index_input_files = constants.esConfig5pt6pt4['index_input_files']


class NestedDict(dict):
    def __getitem__(self, key):
        if key in self: return self.get(key)
        return self.setdefault(key, NestedDict())

def set_up_index(index_name,doc_type='my_type',recreate=1,reindex=0):
    create_index(index_name, recreate=recreate)
    if recreate:
        reindex=1
    index_data(index_name,doc_type=doc_type,reindex=reindex)

def create_index(index_name,recreate,idxBody=None):
    base_index_name=index_name
    index_name=updateIndexName(index_name)
    if recreate:
        if es.indices.exists(index=index_name):
            r=es.indices.delete(index=index_name)
            if r['acknowledged']:
                print('index deleted')

    if not es.indices.exists(index=index_name):
        # print 'esconf.generate_schema(index_name):',esconf.generate_schema(index_name)
        if idxBody:
            r = es.indices.create(index=index_name, body=idxBody, ignore=400)
        else:
            r=es.indices.create(index=index_name,body=generate_schema(base_index_name), ignore=400)
        print('r:',r)
        if r['acknowledged']:
            print("\nindex '{}' created".format(index_name))

def getTokenLength(string):
    regex = '[' + re.escape('''!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~ ''') + ']+'
    return str(len([item for item in re.split(regex, string) if item]))
    # return str(len(re.split(regex, string)))

def index_data(index_name,doc_type='my_type',reindex=0):
    if not reindex:
        return
    print("Adding data")
    index_data_bulk(index_name)

def match_all(es,index_name,doc_type):
    query=NestedDict()
    query['query']['match_all']={}
    if debugQuery:
        print('elastic_query: ',json.dumps(query))
    res = es.search(index=index_name, body=query,doc_type=doc_type,size=constants.match_all_size)
    return res

# def match_all(es,index_name,doc_type): # this function is not to be deleted. This might be used going forward
#     query=NestedDict()
#     query['query']['match_all']={}
#     scroll = '2m'
#     search_type = 'scan'
#     size = constants.match_all_size
#     if debugQuery:
#         print 'elastic_query: ',json.dumps(query)
#     res = es.search(index=index_name, body=query,doc_type=doc_type,scroll=scroll,search_type = search_type,size=size)
#     sid = res['_scroll_id']
#     scroll_size = res['hits']['total']
#     while (scroll_size > 0):
#         print "Scrolling..."
#         page = es.scroll(scroll_id=sid, scroll=scroll)
#         # Update the scroll ID
#         sid = page['_scroll_id']
#         # Get the number of results that we returned in the last scroll
#         scroll_size = len(page['hits']['hits'])
#         print "scroll size: " + str(scroll_size)
#         res['hits']['hits']+=page['hits']['hits']
#     return res

def get_matched_doc(es,index_name,search_string,fuzziness,customQuery=None,**kwargs):
    # es = Elasticsearch([{'host': '10.113.175.158', 'port': 9200}])
    # es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    es = Elasticsearch([constants.elasticUrl])
    # es = Elasticsearch([{'host': '10.142.207.250', 'port': 9200}], connection_class=MyConnection, proxies = {'http': 'http://10.142.207.250:8080'}) #linux server
    query=NestedDict()
    # query={}
    bool_param='must'
    doc_type=kwargs.get('doc_type','my_type')
    if not constants.elastic5pt6pt4:
        doc_type=constants.docTyp
    if kwargs.get('match_all'):
        return match_all(es,updateIndexName(index_name),doc_type)
    analyzed_fields=[key for key,value in list(constants.esConfig['fields'][index_name].items()) if value['index']=='analyzed']
    index_name = updateIndexName(index_name)
    if customQuery:
        for key, val in list(customQuery.items()):
            if val['bool_param'] not in query['query']['bool']:
                query['query']['bool'].update({val['bool_param']: []})
            lstBoost=[]
            if key in analyzed_fields and val.get('operator', 'or') == 'or':
                # lstMinMatch= ['100%','90%','80%']
                lstMinMatch= []
                tknCount=int(getTokenLength(val['search_string']))
                if tknCount>0:
                    for index in range(tknCount):
                        lstMinMatch.append(str(int(round((float((tknCount - index)) / tknCount) * 100))) + '%')
                else:
                    return None
                query['query']['bool'][bool_param].append({"match": {key: {"query": val['search_string'], "fuzziness": val['fuzziness'],"operator":"or"}}})
                for idx,minMatch in enumerate(lstMinMatch):
                    boost=len(lstMinMatch)-idx
                    lstBoost.append(boost)
                    matchItem = {"match": {key: {"query": val['search_string'], "fuzziness": val['fuzziness'],
                                                 "minimum_should_match":minMatch,"boost":boost}}}
                    if 'should' not in query['query']['bool']:
                        query['query']['bool'].update({'should':[]})
                    query['query']['bool']['should'].append(matchItem)
            else:
                matchItem = {"match": {key: {"query": val['search_string'], "fuzziness": val['fuzziness'],'operator':val.get('operator','or')}}}
                query['query']['bool'][val['bool_param']].append(matchItem)
            # if key in analyzed_fields and val.get('operator','or')=='and':
            if key in analyzed_fields:
                matchItem = {"match": {key+'_token_count': {"query": getTokenLength(str(val['search_string'])),"boost":max(lstBoost or [0])+1}}}
                if 'should' not in query['query']['bool']:
                    query['query']['bool'].update({'should':[]})
                query['query']['bool']['should'].append(matchItem)


        match_fields=list(customQuery.keys())
    else:
        match_fields=kwargs.get('match_fields',analyzed_fields)
        if kwargs.get('minimum_should_match'):
            query['query']['bool']['minimum_should_match']=kwargs.get('minimum_should_match')
        query['query']['bool'][bool_param]=[]
        for key in match_fields:
            # print {"match":{key:{"query":search_string,"fuzziness":fuzziness}}}
            lstBoost=[]
            if key in analyzed_fields and kwargs.get('operator', 'or') == 'or':
                lstMinMatch = []
                tknCount = int(getTokenLength(search_string))
                if tknCount > 0:
                    for index in range(tknCount):
                        lstMinMatch.append(str(int(round((float((tknCount - index)) / tknCount) * 100))) + '%')
                else:
                    return None
                if kwargs.get('stem'):
                    query['query']['bool'][bool_param].append({"multi_match": {
                        "type": "most_fields",
                        "fuzziness":"AUTO",
                        "query": search_string,
                        "fields": [key, key + '.english']}})
                    if "should" not in query['query']['bool']:
                        query['query']['bool'].update({"should":[]})
                    query['query']['bool']["should"].append({"multi_match": {
                        "type": "cross_fields",
                        "query": search_string,
                        "fields": [key],
                        "operator":"and","boost":2}})
                    query['query']['bool']["should"].append({"multi_match": {
                        "type": "cross_fields",
                        "query": search_string,
                        "fields": [key, key + '.english'],
                        "operator": "and","boost":1}})
                else:
                    query['query']['bool'][bool_param].append({"match": {
                        key: {"query": search_string, "fuzziness": fuzziness, "operator": "or"}}})
                for idx,minMatch in enumerate(lstMinMatch):
                    boost=len(lstMinMatch)-idx
                    lstBoost.append(boost)
                    # if kwargs.get('stem'):
                    #     matchItem = {"multi_match": {
                    #     "type": "cross_fields",
                    #     "query": search_string, "fuzziness": "AUTO",
                    #     "operator": "or",
                    #     "minimum_should_match": minMatch, "boost": boost,
                    #     "fields": [key, key + '.english']}}
                    # else:
                    if not kwargs.get('stem'):
                        matchItem = {"match": {key: {"query": search_string, "fuzziness": fuzziness,
                                                         "minimum_should_match":minMatch,"boost":boost}}}
                        if 'should' not in query['query']['bool']:
                            query['query']['bool'].update({'should':[]})
                        query['query']['bool']['should'].append(matchItem)
            else:
                # if kwargs.get('stem') and key in analyzed_fields and kwargs.get('operator','or')=='and':
                #     query['query']['bool'][bool_param].append({"multi_match": {
                #         "type":"cross_fields",
                #         "query": search_string, "fuzziness": "AUTO",
                #               "operator": "and",
                #     "fields":[key,key+'.english']}})
                # else:
                    query['query']['bool'][bool_param].append({"match":{key:{"query":search_string,"fuzziness":fuzziness,"operator":kwargs.get('operator','or')}}})
            # if key in analyzed_fields and kwargs.get('operator','or')=='and':
            if key in analyzed_fields:
                try:
                    matchItem = {"match": {key+'_token_count': {"query": getTokenLength(search_string),"boost":max(lstBoost or [0])+1}}}
                except:
                    print(('search_string:',search_string))
                if 'should' not in query['query']['bool']:
                    query['query']['bool'].update({'should':[]})
                query['query']['bool']['should'].append(matchItem)
    if debugQuery:
        print('elastic_query: ',json.dumps(query))
    res = es.search(index=index_name,doc_type=doc_type,body=query)
    if debugValue:
        print('elastic response: ',res)
        print("number of successful elastic hits for",match_fields,": ",res['hits']['total'])
    first=1
    match=None

    for hit in res['hits']['hits']:
        if debugValue:
            print(hit)
        if first:
            match={key:val for key,val in list(hit['_source'].items())}
            first=0
    if not match:
        return match
    def matchTokenCount(match=match):
        if customQuery:
            for key, val in list(customQuery.items()):
                if key not in analyzed_fields:
                    continue
                if val.get('operator','or')=='and' and val.get('restrict'):
                    if getTokenLength(val['search_string'])!=match[key+'_token_count']:
                        return 0
        else:
            match_fields = kwargs.get('match_fields', analyzed_fields)
            if kwargs.get('operator', 'or') == 'and' and kwargs.get('restrict'):
                for key in match_fields:
                    if key not in analyzed_fields:
                        continue
                    if getTokenLength(search_string)!=match[key+'_token_count']:
                        return 0
    if index_name==updateIndexName(constants.spec_excep):
        lstDups=determineDups(res['hits']['hits'],analyzed_fields)
        if len(lstDups)>1:
            if matchTokenCount(match=lstDups[0])==0:
                    return None
            return lstDups
    if matchTokenCount()==0:
        return None
    return match


def gridLookup(index_name,search_string,fuzziness, customQuery = {}, **otherparams):
    return get_matched_doc('', index_name, search_string, fuzziness, customQuery, **otherparams)

def generate_schema(index_name,settings=None,mappings=None):
    if constants.elastic5pt6pt4:
        properties=constants.esConfig5pt6pt4['fields'][index_name]
        settings=constants.esConfig5pt6pt4['settings'] if not settings else settings
        mappings=constants.esConfig5pt6pt4['mappings'] if not mappings else mappings
    else:
        properties = constants.esConfig['fields'][index_name]
        settings = constants.esConfig['settings'] if not settings else settings
        mappings = constants.esConfig['mappings'] if not mappings else mappings
    mappings_copy=deepcopy(mappings)
    if constants.elastic5pt6pt4:
        mappings_copy["my_type"]['properties'] = properties
    else:
        mappings_copy['properties']=properties
    return {"settings": settings,
     "mappings":mappings_copy}

def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else ' ' for i in text])

def index_data_bulk(index_name,doc_type='my_type',recreate=0,csv_file_path='../config/',csv_file_name=None):
    if not constants.elastic5pt6pt4:
        doc_type=constants.docTyp
    create_index(index_name,recreate)
    reader = constants.handler.find_one_document(index_name)[index_name]
    chunksize=500
    i=0
    lstRows=[]
    index_fields = constants.esConfig['fields'][index_name]
    for row in reader:
        i+=1
        for k,v in list(row.items()):
            row[k]=remove_non_ascii(re.sub('\r|\n',' ',v)).strip()
        if constants.elastic5pt6pt4:
            row.update({k + '_token_count': getTokenLength(str(row[k])) for k, v in list(index_fields.items()) if
                        v['index'] == 'analyzed'})
        else:
            row.update({k + '_token_count': getTokenLength(str(row[k])) for k, v in list(index_fields.items()) if
                        v['index'] == True and v['type']=='text'})
        lstRows.append(row)
        if i==chunksize :
            helpers.bulk(es,lstRows,index=updateIndexName(index_name),doc_type=doc_type)
            i=0
            lstRows=[]
    if lstRows:
        helpers.bulk(es, lstRows, index=updateIndexName(index_name), doc_type=doc_type)

def determineDups(hits,lstDupKeys):
    lstDups=[]
    first=1
    for hit in hits:
        if first:
            diUnique ={k: hit['_source'][k] for k in lstDupKeys}
            first=0
        if diUnique=={k:hit['_source'][k] for k in lstDupKeys}:
            lstDups.append(hit['_source'])
        else:
            return lstDups
    return lstDups
def updateIndexName(idxName):
    return constants.indexPrefixElastic + '_' +idxName

def main():
    doc_type = 'my_type'
    if not constants.elastic5pt6pt4:
        doc_type=constants.docTyp
    index_input_files=constants.esConfig['index_input_files']
    for input_file,index_name in list(index_input_files.items()):
        print(input_file,index_name)
        set_up_index(index_name,doc_type=doc_type,recreate=1,reindex=0)


def createMappingIndexes():
    settings = {"analysis": {
        "filter": {
            "my_stemmer": {
                "type": "stemmer",
                "name": "english"
            },
            "my_snow": {
                "type": "snowball",
                "language": "english"
            },
            "english_stop": {
                "type": "stop",
                "language": "english"
            }
        },
        "analyzer": {
            "custom_stem": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "my_snow", "english_stop"]
            },
            "custom_nostem": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "english_stop"]
            }
        }
    }
    }
    mappings = {"properties": {}}
    if constants.elastic5pt6pt4:
        mappings = {"my_type": {"_all": {"enabled": False}, "properties": {}}}

    svMap = constants.handler.find_one_document("mappings")["mappings"]
    mvMap = constants.handler.find_one_document("mvMappings")["mvMappings"]
    sv_nlp_lookup,mv_nlp_lookup=utils.getNlpLookup(svMap,mvMap)
    docTyp = 'my_type'
    if not constants.elastic5pt6pt4:
        docTyp = constants.docTyp
    index_input_files = {'sv_nlp':sv_nlp_lookup, 'mv_nlp':mv_nlp_lookup}
    for idxName,nlp_lookup in list(index_input_files.items()):
        # nlp_lookup = eval(constants.handler.find_one_document(inpFile)[inpFile])
        schema = generate_schema(idxName, settings, mappings)
        print('schema:',schema)
        # exit(0)
        create_index(idxName, 1, idxBody=schema)
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
            es.index(index=updateIndexName(idxName), doc_type=docTyp, body=body)


def elasticMapping(idxName='sv_nlp',searchString='tx id',excluded=None):
    docTyp = 'my_type'
    if not constants.elastic5pt6pt4:
        docTyp = constants.docTyp
    idxName=updateIndexName(idxName)
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
    res = es.search(index=idxName,doc_type=docTyp,body=query)
    if res and res.get('hits'):
        for hit in res['hits']['hits']:
            # return hit,hit['_source'].get('parentCat',hit['_source'].get('outputCol'))
            print('***  query ****:',json.dumps(query),'*** 1st result ***',hit['_source'].get('parentCat',hit['_source'].get('outputCol')))
            return hit['_source'].get('parentCat',hit['_source'].get('outputCol'))
    else:
        return None
if __name__=='__main__':
    doc_type = 'my_type'
    # csv_file_path='../input/NDB Taxonomy Grid_Elastic_search_poc.csv'
    for input_file,index_name in list(index_input_files.items()):
        print(input_file,index_name)
        set_up_index(index_name,doc_type=doc_type,recreate=1,reindex=0)
    createMappingIndexes()
    exit(0)

    #################### test case ################
    # code='AZ'
    # prov='Peds'
    # print get_matched_doc(es, 'c_and_s', code + " " + prov, 0,
    #                 customQuery={"code": {'search_string': code, 'bool_param': 'must', 'fuzziness': 0},
    #                              "provider_specialty": {'search_string': prov, 'bool_param': 'must', 'fuzziness': 1}})
    # exit(0)
    index_name = 'rfp_grid'
    # minimum_should_match=1
    search_string='Audiologgy department'
    match_fields=['specialty']
    fuzziness=1
    othParams={'match_fields':match_fields,'doc_type':doc_type}
    customQuery={'specialty': {'search_string': 'Behaviora Analys', 'bool_param': 'must','fuzziness':1},
                        'ndb_spec': {'search_string': '300.0', 'bool_param': 'must','fuzziness':0}}
    best_match=get_matched_doc(es,index_name,search_string,fuzziness,customQuery=customQuery,**othParams)
    print(best_match)
import sys
sys.path.append('../bin/')
import elastic
import elasticMapping as esmap
import traceback
import json


def setupESIndexes():
    sys.path.append("../bin")
    import time
    while True:
        time.sleep(5)
        try:
            if elastic.es.ping():
                print('elastic is up and running')
                print(elastic.es.info)
                indicesOnESServer = list(elastic.es.indices.get_alias().keys())
                print('\nindexes currently existing:',indicesOnESServer,'\n')
                indicesToBeCreated=list(elastic.index_input_files.values())+['sv_nlp','mv_nlp']
                if len([0 for idx in indicesToBeCreated if idx not in indicesOnESServer])>0:
                    elastic.main()
                    esmap.indexing()
                break
        except:
            traceback.print_exc()
            print('elastic connection failed')
            continue

if __name__=='__main__':
    masterConfigPath = 'masterConfig.json'
    with open(masterConfigPath, 'rb') as fp:
        mConfig = json.load(fp)
    setupESIndexes()
    sys.path.append('../UI/')
    import app
    app.app.run(host=mConfig['app']['host'], port=mConfig['app']['port'], threaded=True)
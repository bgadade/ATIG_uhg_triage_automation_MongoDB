import os
import json
import constants
from utils import fetchProviderCondition

def createPdfMapDir(delegateCode):
    path = constants.pdfMappingsPath+'/'+delegateCode
    conditionData = fetchProviderCondition(delegateCode, 'ProviderPdf')
    try:
        allJsons = constants.handler.find_one_document('ProviderPdf', conditionData)['Doc']
    except:
        allJsons = 'Pdf Mapping not found on mongoDB'
    if isinstance(allJsons,dict):
        if not os.path.isdir(path):
            os.mkdir(path)
        for fNm, jsonData in allJsons.items():
            filePath = path+'/'+fNm+'.json'
            with open(filePath,'w') as fp:
                json.dump(jsonData,fp,indent=2)
        print('Successfully created PDF mappings folder for',delegateCode)
    elif isinstance(allJsons, str):
        print(allJsons,'for',delegateCode)

def createMultiplePdfMapDir(lstDelegateCode):
    for delegateCode in lstDelegateCode:
        createPdfMapDir(delegateCode)

if __name__ == '__main__':
    lstDelegateCode = ['CA081','CA094']
    createMultiplePdfMapDir(lstDelegateCode)
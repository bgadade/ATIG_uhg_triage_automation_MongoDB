import csv
import json
import os
import re
import sys
sys.path.append('../bin/')
import constants

def csv_to_json(path, file_name):
    cFile = path + file_name + '.csv'
    csvData = list(csv.DictReader(open(cFile, 'r')))
    return json.loads(json.dumps(csvData), object_hook=constants.handler.encode_dot_key)

def get_json(path, file_name):
    jFile = path + file_name + '.json'
    return json.load(open(jFile, "r"), object_hook=constants.handler.encode_dot_key)

def updatePdfMappings(providerLst):
    for provider in providerLst:
        if provider in os.listdir('../pdfMappings/'):
            path = '../pdfMappings/'+provider+'/'
            allJsons = {}
            for jsonFile in os.listdir(path):
                fileKey = jsonFile.replace('.json','')
                jsonData = json.load(open(path+jsonFile, "r"), object_hook=constants.handler.encode_dot_key)
                allJsons[fileKey] = jsonData
            mongoDB_document = {}
            mongoDB_document["Provider"] = provider
            mongoDB_document["Type"] = "ProviderPdf"
            mongoDB_document["Doc"] = allJsons
            constants.handler.replace_one_document(mongoDB_document["Type"], mongoDB_document)
    print('PDF Mappings replaced')

# updating pdf mappings on mongoDB for a provider
providerLst = ['MO005','CA028','TX030','CA010','CA065','CA130','TX075','CA129','TX031','MA004', 'MI012']
updatePdfMappings(providerLst)

path="../config/"
json_File_Name=["credentials","templateDrivers","templates","mvMappings","mappings","Standards","variables_DEMO_ADDR_ADD","outputCols_DEMO_ADDR_ADD","variables_DEMO_PRAC_ADD","outputCols_DEMO_PRAC_ADD","outputCols_TAXID","variables_TAXID", "outputCols_DEMO_ADDR_INACTIVE","variables_DEMO_ADDR_INACTIVE", "variables_NonMass","outputCols_NonMass","outputCols_DEMO_TAXID_INACT","variables_DEMO_TAXID_INACT",
                "variables_DEMO_ADDR_CHANGE","outputCols_DEMO_ADDR_CHANGE","variables_AllTemplates","variables_DEFAULT_OTHER","variables_PRAC","outputCols_DEFAULT_OTHER","outputCols_PRAC","outputCols_DEMO_OTHER","variables_DEMO_OTHER","outputCols_PLMI","variables_PLMI","outputCols_ACTIONS","variables_ACTIONS","variables_CONTRACT_PRAC_ADD","outputCols_CONTRACT_PRAC_ADD",
                "variables_DEMO_NON_PRAC_ADD","outputCols_DEMO_NON_PRAC_ADD","variables_CONT_TIN_INACTIVATE","outputCols_CONT_TIN_INACTIVATE","variables_CONTRACT_AUTHOR_EXECUTE","outputCols_CONTRACT_AUTHOR_EXECUTE","variables_CONTRACT_EXECUTE","outputCols_CONTRACT_EXECUTE","variables_CONT_AMEND_EXECUTE","outputCols_CONT_AMEND_EXECUTE",
                "variables_CONTRACT_OTHER","outputCols_CONTRACT_OTHER","variables_CONT_MODF_INACT","outputCols_CONT_MODF_INACT","variables_CONT_MODF_ADD","outputCols_CONT_MODF_ADD","elasticConfig","elasticConfig5pt6pt4"]
csv_File_Name=["us_cities","us_states", "spec_psi", "specialty_exceptions",
               "degree_exceptions"]

if json_File_Name or csv_File_Name:
    if json_File_Name:
        for name in json_File_Name:
            file_name = name.replace(".", "_")
            jsonData = get_json(path, name)
            document={file_name:jsonData}
            constants.handler.replace_one_document(file_name, document)
    if csv_File_Name:
        for name in csv_File_Name:
            file_name = name.replace(".", "_")
            jsonData = csv_to_json(path, name)
            document = {file_name:jsonData}
            constants.handler.replace_one_document(file_name, document)

    print('files replaced successfully!!')
else:
    print('no file to replace')

# IMPORTANT : comment out mongodb parts in constants.py (after line 92)

import csv
import json
import os
import re
import sys
sys.path.append('../bin/')
import constants

def final_to_json(path, file_name):
    fFile = path + "/" + file_name + "/" + file_name + ".final"  #'../providerMappings/MD011/MD011.final'
    return json.load(open(fFile, "r"), object_hook=constants.handler.encode_dot_key)

def interim_to_json(path, root, file_name):
    iFile = path + "/" + root + "/" + file_name + ".interim"
    return json.load(open(iFile, "r"), object_hook=constants.handler.encode_dot_key)

def csv_to_json(path, file_name):
    cFile = path + file_name + '.csv'
    csvData = list(csv.DictReader(open(cFile, 'r')))
    return json.loads(json.dumps(csvData), object_hook=constants.handler.encode_dot_key)

def get_json(path, file_name):
    jFile = path + file_name + '.json'
    return json.load(open(jFile, "r"), object_hook=constants.handler.encode_dot_key)

def insert_file_to_mongodb(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            file_name = os.path.splitext(file)[0]

            if file.endswith(".csv"):
                jsonData = csv_to_json(path, file_name) #list
                esConfig_filename = constants.esConfigFile["index_input_files"]
                if file_name in esConfig_filename:
                    document = {esConfig_filename[file_name]: jsonData}
                    constants.handler.insert_one_document(esConfig_filename[file_name], document)

            elif re.match(r'.+(_mapping)$', file_name):
                jsonData = get_json(path, file_name)
                file_name = file_name.replace(".", "_")
                document = {}
                document["UserName"] = file_name.split("_")[-2]
                document["Type"] = "UserMapping"
                document["Doc"] = jsonData
                constants.handler.insert_one_document(document["Type"], document)

            elif re.match(r'.+(_specExcept)$', file_name):
                jsonData = get_json(path, file_name)
                file_name = file_name.replace(".", "_")
                document = {}
                document["UserName"] = file_name.split("_")[-2]
                document["Type"] = "SpecialtyException"
                document["Doc"] = jsonData
                constants.handler.insert_one_document(document["Type"], document)

            elif file.endswith(".final"):
                jsonData = final_to_json(path, file_name)
                document = {}
                document["Provider"] = file_name
                document["Type"] = "ProviderFinal"
                document["Doc"] = jsonData
                constants.handler.insert_one_document(document["Type"], document)

            elif file.endswith(".interim"):
                prefix = os.path.basename(root)
                jsonData = interim_to_json(path, prefix, file_name)
                document = {}
                document["Provider"] = prefix
                document["UserName"] = file_name
                document["Type"] = "ProviderInterim"
                document["Doc"] = jsonData
                constants.handler.insert_one_document(document["Type"], document)

            elif path == constants.configPath and file.endswith(".json"):
                jsonData = get_json(path, file_name)
                file_name = file_name.replace(".", "_")
                document = {file_name : jsonData}
                constants.handler.insert_one_document(file_name, document)

            else:
                print(file, " : File not inserted in mongoDB")

def main():

    insert_file_to_mongodb(constants.configPath)
    insert_file_to_mongodb(constants.userMappingsPath)
    insert_file_to_mongodb(constants.provMappingsPath)

if __name__ == '__main__':
    main()
    exit(0)
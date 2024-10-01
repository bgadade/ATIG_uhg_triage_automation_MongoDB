import csv
import json
import os
import re
import sys
sys.path.append('../bin/')
import constants

def remove_duplicate_foramt(inp_json):
    # inp_json is the value associated with "Doc" and return the same format
    # storing unique format
    unique_format, unique_format_idx  = [] , []
    for format_idx, format_det in list(inp_json.items()):
        format_ = format_det["format"]
        if format_ not in unique_format:
            unique_format.append(format_)
            unique_format_idx.append(format_idx)
    # creating back json
    new_final_json = {}
    format_idx = 1
    for idx in unique_format_idx:
        new_final_json[format_idx] = inp_json[idx]
        format_idx += 1
    return new_final_json

# constants.handler.replace_one_document(file_name, document)
for ix, prov in enumerate(constants.handler.find_documents('ProviderFinal',{"Provider":{"$exists":True}})):
    res=remove_duplicate_foramt(prov['Doc'])
    document={"Type": "ProviderFinal", "Provider": prov['Provider'], "Doc": res}
    constants.handler.replace_one_document('ProviderFinal', document)
    print('replaced:{0}'.format(prov['Provider']))

print("Done")
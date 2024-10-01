'''
gives a csv file with each row corresponding to an error message
Columns :    'LayerName', 'LayerType', 'DataFrame', 'FunctionName', 'ClarificationType', 'ClarificationMessage',
            'Clarification', 'FunctionInput', 'OutputCol'
'''

import inspect
import json
import csv
import re
import sys
sys.path.append('../bin')
import importlib
import constants

list_variables_json = ['variables_NonMass', 'variables_TAXID']
list_mvDerivation = ['mvDerivation_NonMass', 'mvDerivation_TAXID']
path_to_save_file = 'C:/Users/SH388982/Desktop/'


def gettingErrorMsg(list_variables_json, list_mvDerivation, path_to_save_file):
    for idx, each_variable_json in enumerate(list_variables_json):
        errorMsgs = []
        # getting each variable.json file into jsonfile variable
        jsonfile = json.load(open(constants.configPath + each_variable_json + '.json'))
        # importing corresponding mvDerivation file
        mvFile = importlib.import_module(list_mvDerivation[idx])
        # getting list for all the functions present in the mvDerivation file
        mvfile_functions = inspect.getmembers(mvFile, inspect.isfunction)
        # mvfile_functions = [(function_name1, function_object1) ,(function_name2, function_object2),...]

        for layer in jsonfile:
            for transformation in layer['Transformation']:
                for key, value in list(transformation.items()):
                    for func in value['derivations']:
                        if func['type'] == 'var':  # checking if type is var for the function
                            # retrieving function_object from mvfile_functions
                            to_inspect = [i[1] for i in mvfile_functions if func['name'] == i[0]]
                            # inspect.getsource gives function definition as string
                            func_def_lines = inspect.getsource(to_inspect[0])
                            # getting lines where ErrorMessage is called
                            r = [i.strip() for i in func_def_lines.splitlines() if 'ErrorMessage' in i]
                            if r:
                                for i in r:
                                    # constructing a dictionary for each occurence of ErrorMessage
                                    dic = {}
                                    dic['LayerName'] = layer['LayerName']
                                    dic['LayerType'] = layer['LayerType']
                                    dic['DataFrame'] = key
                                    dic['FunctionName'] = func['name']
                                    dic['FunctionInput'] = func['input']
                                    if dic['LayerType'] == 'ROW':
                                        dic['OutputCol'] = func['col']
                                    else:
                                        dic['OutputCol'] = ''
                                    dic['Clarification'] = r
                                    # getting Error type and message separated
                                    i = re.findall(r'ErrorMessage.*$', i)[0]
                                    temp = i.split(',')
                                    dic['ClarificationType'] = re.findall(r'[A-Za-z]+', temp[1])[0]
                                    dic['ClarificationMessage'] = temp[2].strip().replace('"', '')
                                    errorMsgs.append(dic)

        #  writing the dictionary into a csv file
        with open(path_to_save_file + 'error_messages_' + list_mvDerivation[idx] + '.csv', 'wb') as csvfile:
            fieldnames = (
            'LayerName', 'LayerType', 'DataFrame', 'FunctionName', 'ClarificationType', 'ClarificationMessage',
            'Clarification', 'FunctionInput', 'OutputCol')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for d in errorMsgs:
                writer.writerow(d)


gettingErrorMsg(list_variables_json, list_mvDerivation, path_to_save_file)
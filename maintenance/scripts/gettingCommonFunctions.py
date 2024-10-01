'''
compares different mvDerivation files as per variables.json and
gives text file containing common functions for each mvDerivation file
'''

import re
import inspect
import json
import sys
sys.path.append('../bin')
import constants
import importlib


list_variables_json = ['variables_NonMass','variables_TAXID', 'variables_DEMO_PRAC_ADD']
list_mvDerivation = ['mvDerivation_NonMass','mvDerivation_TAXID', 'mvDerivation_DEMO_PRAC_ADD']
path_to_save_file = 'C:/Users/SH388982/Desktop/'
# switch : to control the type of functions we get in output.
# 1: only variable.json function, 2:only inline functions, 3: both variable.json and inline functions
switch = 3


def gettingCommonFunctions(list_variables_json, list_mvDerivation, path_to_save_file,switch=3):
    '''
    Getting list of all functions from variables.json : variables_function_list
     [
        {variables_json : var1, functions : {'ROW': [f1,f2,...], 'DF':[f3,f4,...]}},
        {variables_json : var1, functions : {'ROW': [f1,f2,...], 'DF':[f3,f4,...]}},
        ...
     ]
    '''
    variables_function_list = []
    for each_variable_json in list_variables_json:
        each_variable_dic = {}
        each_variable_dic['variables_json'] = each_variable_json + '.json'
        each_variable_dic['functions'] = {'ROW': [], 'DF': []}
        jsonfile = json.load(open(constants.configPath + each_variable_dic['variables_json']))
        for layer in jsonfile:
            for transformation in layer['Transformation']:
                for key, value in list(transformation.items()):
                    for func in value['derivations']:
                        if func['type'] == 'var':
                            each_variable_dic['functions'][layer['LayerType']].append(func['name'])
        # taking unique functions
        each_variable_dic['functions']['ROW'] = list(set(each_variable_dic['functions']['ROW']))
        each_variable_dic['functions']['DF'] = list(set(each_variable_dic['functions']['DF']))
        variables_function_list.append(each_variable_dic)

    # Getting common functions across all variables.json
    common_functions = {}
    common_functions['ROW'] = list(set.intersection(
        *[set(each_variable_json['functions']['ROW']) for each_variable_json in variables_function_list]))
    common_functions['DF'] = list(set.intersection(
        *[set(each_variable_json['functions']['DF']) for each_variable_json in variables_function_list]))
    print(common_functions)
    # combining common functions into one list
    all_common_functions = common_functions['ROW'] + common_functions['DF']

    # Getting function definition for common functions
    for each_mvDerivation in list_mvDerivation:
        # importing mvDerivation file
        mvFile = importlib.import_module(each_mvDerivation)
        # getting list for all the functions present in the mvDerivation file
        mvfile_functions = inspect.getmembers(mvFile, inspect.isfunction)
        # mvfile_functions = [(function_name1, function_object1) ,(function_name2, function_object2),...]
        # retrieving function_object for all_common_functions from mvfile_functions
        all_common_functions_obj = [j[1] for i in all_common_functions for j in mvfile_functions if i == j[0]]
        # inside_func = list of functions that are called from any function in all_common_functions
        inside_func = []
        # function_definition_string = string having function definition for functions in all_common_functions
        function_definition_string = ''

        for each_function in all_common_functions_obj:
            # inspect.getsource gives function definition as string
            func_lines = inspect.getsource(each_function)
            # appending each function definition to function_definition_string
            function_definition_string = function_definition_string + '\n\n' + func_lines
            if switch == 2 or switch == 3 :
                # looking for function calls from the function definition and appending them to inside_func
                func_call = re.findall(r'([.a-zA-Z_0-9]+)\(', func_lines)
                inside_func.extend(func_call)
        if switch == 2 or switch == 3 :
            # taking unique functions inside inside_func
            # sorting the list alphabetically so that order is intact
            inside_func = sorted(list(set(inside_func)))
            # removing duplicates between inside_func and all_common_functions
            inside_func = [i for i in inside_func if i not in all_common_functions]
            # retrieving function_object for inside_func from mvfile_functions
            inside_func_obj = [j[1] for i in inside_func for j in mvfile_functions if i == j[0]]
            # getting function definition for all functions in inside_func in form of list
            inside_function_definition = [inspect.getsource(each_function) for each_function in inside_func_obj]
            # making inside_function_definition_string from the list
            inside_function_definition_string = '\n\n'.join(inside_function_definition)

        # getting output_text depending on switch
        if switch == 1:
            output_text = function_definition_string
        elif switch == 2:
            output_text = inside_function_definition_string
        else:
            output_text = function_definition_string + '\n\n' + inside_function_definition_string

        # writing output_text into a .txt file
        with open(path_to_save_file + 'common_functions_' + each_mvDerivation + '.txt', 'w') as f:
            f.write(output_text)
        f.close()


gettingCommonFunctions(list_variables_json, list_mvDerivation, path_to_save_file,switch)
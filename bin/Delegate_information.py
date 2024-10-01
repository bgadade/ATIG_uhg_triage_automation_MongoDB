# import xlrd
# import json
#
#
# delegateFilePath = "..\config\\"
# delegateFileName = "NDB_Delegate Groups v1.xlsx"
#
#
# def read_delegates_file():
#     workbook = xlrd.open_workbook(delegateFilePath + delegateFileName)
#     sheet = workbook.sheet_by_index(0)
#
#     delegates_dict = {}
#
#     num_rows = sheet.nrows
#
#     for row_num in range(1, num_rows):
#         row = sheet.row(row_num)
#
#         delegate_code = row[0].value.strip()
#         delegate_name = row[1].value.strip()
#         tax_id = int(row[3].value.strip())
#
#         if delegate_code not in delegates_dict:
#             delegates_dict[delegate_code] = {}
#             delegates_dict[delegate_code]["Delegate Name"] = delegate_name
#             delegates_dict[delegate_code]["TAX ID"] = []
#             delegates_dict[delegate_code]["TAX ID"].append(tax_id)
#
#         else:
#             delegates_dict[delegate_code]["TAX ID"].append(tax_id)
#
#     return delegates_dict
#
#
# def create_providers():
#
#     file = "..\config\providers.json"
#
#     workbook = xlrd.open_workbook(delegateFilePath + delegateFileName)
#     sheet = workbook.sheet_by_index(0)
#
#     delegates_dict = {}
#
#     num_rows = sheet.nrows
#
#     for row_num in range(1, num_rows):
#         row = sheet.row(row_num)
#
#         delegate_code = row[0].value.strip()
#         delegate_name = row[1].value.strip()
#
#         if delegate_code not in delegates_dict:
#             delegates_dict[delegate_code] = {}
#             delegates_dict[delegate_code]["name"] = delegate_name
#             delegates_dict[delegate_code]["status"] = "new"
#
#     fp = open(file, "w")
#     json.dump(delegates_dict, fp)
#
#
# create_providers()
#
# def delegate_extraction(tax_id):
#
#     delegates_dict = read_delegates_file()
#
#     delegate_info = {}
#
#     for delegate_code in delegates_dict.keys():
#
#         if tax_id in delegates_dict[delegate_code]["TAX ID"]:
#             delegate_info["Delegate Code"] = delegate_code
#             delegate_info["Delegate Name"] = delegates_dict[delegate_code]["Delegate Name"]
#
#             if 0 in delegates_dict[delegate_code]["TAX ID"]:
#                 delegate_info["Includes_All_Tax_ID"] = "False"
#             else:
#                 delegate_info["Includes_All_Tax_ID"] = "True"
#
#     print json.dumps(delegate_info)
#
#
# def tax_id_dlgt_code_check(tax_id_list, delegate_code):
#
#     delegates_dict = read_delegates_file()
#
#     tax_id_checklist = []
#
#     for del_code in delegates_dict.keys():
#         if delegate_code == del_code:
#
#             for tax_id in tax_id_list:
#                 if tax_id in delegates_dict[delegate_code]["TAX ID"]:
#                     tax_id_checklist.append(tuple((tax_id, "True")))
#                 else:
#                     tax_id_checklist.append(tuple((tax_id, "False")))
#
#     print tax_id_checklist
#
#
# # delegate_extraction(116450821)
# # tax_id_dlgt_code_check([360649108, 630469108, 630949108, 710445686], "AL002")

import os

basedir = os.path.dirname("../providerMappings/")
filename = "empty.txt"

for root, dirs, files in os.walk(basedir):
    if len(files) == 0:
        print("creating")
        open(root + "/" + filename, "a").close()



import xlrd
import json
import os

mappingsPath = "..\config\mappings.json"
excelPath = "..\Excel files\\"
delegateFilePath = "..\config\\"

with open(mappingsPath) as json_data:
    mappings = json.load(json_data)

tax_id_mappings = mappings["TAX_ID"]["Input_Column"]


def read_excel(path, file_name):

    tax_id = {}

    excel_file = path + file_name

    workbook = xlrd.open_workbook(excel_file)

    sheet_names = workbook.sheet_names()

    for sheet_name in sheet_names:
        tax_id[sheet_name] = {}
        tax_id[sheet_name]["TAX ID Column Names"] = []
        tax_id[sheet_name]["TAX ID per column"] = []

        sheet = workbook.sheet_by_name(sheet_name)

        num_columns = sheet.ncols
        num_rows = sheet.nrows

        header_row = 0
        for row in range(num_rows):
            headers = [str(cell.value) for cell in sheet.row(row) if str(cell.value) != '']
            count = len(headers)

            if count == num_columns:
                header_row = row
                break

        for column in range(num_columns):
            column_name = sheet.cell(header_row, column).value
            column_name = column_name.split("\n")[0].lower()

            if column_name in tax_id_mappings:
                tax_id[sheet_name]["TAX ID Column Names"].append(column_name)

                column_values = sheet.col_values(column, header_row + 1)
                column_values = list(set(column_values))
                column_values = list([_f for _f in column_values if _f])
                column_values = [value.replace("-", "") for value in column_values]
                column_values = list(map(int, column_values))

                tax_id[sheet_name]["TAX ID per column"].append(tuple((column_name, column_values)))

    # print file_name + ": " + json.dumps(tax_id, sort_keys=True, indent=4, default=str) + "\n"
    return json.dumps(tax_id)

# for root, dirs, files in os.walk(excelPath):
#         for file_name in files:
#
#             if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
#                 read_excel(excelPath, file_name)


def tax_id_dlgt_code_extraction(tax_id):

    delegate_info = {}

    delegate_file = delegateFilePath + "NDB_Delegate Groups v1.xlsx"

    delegate_workbook = xlrd.open_workbook(delegate_file)
    sheet = delegate_workbook.sheet_by_index(0)

    num_columns = sheet.ncols
    num_rows = sheet.nrows

    header_row = 0
    for row in range(num_rows):
        headers = [str(cell.value) for cell in sheet.row(row) if str(cell.value) != '']
        count = len(headers)

        if count == num_columns:
            header_row = row
            break

    for column in range(num_columns):
        column_name = sheet.cell(header_row, column).value
        column_name = column_name.split("\n")[0].lower()

        if column_name in tax_id_mappings:
            column_values = sheet.col_values(column, header_row + 1)
            column_values = list(map(int, column_values))
            row_num = column_values.index(tax_id)

            row = sheet.row(header_row + 1 + row_num)

            i = 0
            for cell in row:
                value = cell.value.strip()
                if value == "":
                    delegate_info[headers[i]] = None
                else:
                    delegate_info[headers[i]] = value
                i += 1

    return json.dumps(delegate_info)


# tax_id_dlgt_code_extraction(472300556)

# def checkColumns(inputDFcols, columns): #ToDo: append unsued config file columns, store list of unused columns in input file.
#     # print "inside checkColumns"
#     mappedOPColsDict = {} #renameColumns expects dictionary
#     mappedOPColsList = []
#     unmappedOPCols = [] #missing columns in the input file after checking with columns.json
#     newInpCols = inputDFcols.tolist()
#     for key, value in columns.items():
#         possibleColumns = columns[key]['Input_Column']
#         for i, column in enumerate(possibleColumns):
#             if column.lower() in inputDFcols and columns[key]['COLUMN_TYPE'] == "Output":
#                 mappedOPColsDict[column.lower()] = key
#                 mappedOPColsList.append(key)
#                 newInpCols.remove(column.lower())
#                 break
#             if i == len(possibleColumns) - 1:
#                 unmappedOPCols.append(key)
#     return mappedOPColsDict, mappedOPColsList, unmappedOPCols, newInpCols
#
# #
# # def updateInputDF(inputDFcols, columns):
# #     # print "inside updateInputDF"
# #     filteredColumns, identifiedColumns, missingColumns = checkColumns(inputDFcols, columns)
# #     interDF = renameColumns(inputDFcols, filteredColumns)
# #     return interDF, identifiedColumns, missingColumns
# #
# # def standardizeColumns(inputDFcols, columns):
# #     # print "inside standardizeColumns"
# #     interDF, identifiedColumns, missingColumns = updateInputDF(inputDFcols ,columns)
# #     return interDF, identifiedColumns, missingColumns
# #

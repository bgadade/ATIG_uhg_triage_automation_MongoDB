import utils as utils
import FetchNPIregistry as fnpi
import pandas as pd
import excelStructure as esx
import excelStructureXlrd as es
import mvDerivation_Common as mv
import functionsForUI as funcUI

InputFileLoc = "../input/cbug11.xlsx"
sheetIndex = 0
outputFileLoc = "../output/output-2.xlsx"
mappings = "../config/mappings.json"
#Multi value Columns json to be read
variables = "../config/variables.json"
outputCols = "../config/outputCols.json"
mvMappingsFile = "../config/mvMappings.json"
functionMappingsFile = "..\config\\functionMappings.json"
ndb_p = "../config/ndb_p.json"

#read files
headerIndx = 1
# columns = utils.getExcelFieldNames(InputFileLoc, sheetIndex, headerIndx)
inputDF = utils.readFile(InputFileLoc, type="xlsx", sheetIndex = sheetIndex, headerIndx = headerIndx)
# inputDF = funcUI.renameSOTCols(inputDF)
inputDF = funcUI.renameSOTCols(InputFileLoc, sheetIndex, headerIndx, inputDF)


mappings = utils.readFile(mappings, type="json")
mvMappings = utils.readFile(mvMappingsFile, type="json")
variables = utils.readFile(variables, type="json")
outputCols = utils.readFile(outputCols, type="json")
functionMappings = utils.readFile(functionMappingsFile, type="json")
multivaluedCols = variables["multivaluedCols"]
cleansingCols = variables["CleanseLayer1"]
cleansingDf=variables["CleanseDataFrame"]
ndb_p = utils.readFile(ndb_p, type="json")

midlevNdBSpec = [201, 331, 374, 375, 58, 121, 205, 300, 35, 23, 111, 69, 68, 268, 29, 376, 110, 377, 378, 40, 92, 379, 118, 41, 339, 320, 298, 243, 55, 17, 282, 380, 381, 382, 318, 514, 391, 106, 383, 384, 397, 244, 63, 387, 385, 53, 52, 386, 89, 49, 109, 62, 42, 108, 388, 389, 120, 119, 115, 43, 44, 104, 212, 113, 59, 368, 369, 367, 338, 112, 48, 45, 329, 9, 56, 395, 390, 51, 392, 246, 60, 507, 393, 99, 394, 100]

#-------------------------- Format detection -------------------------------------#
#ToDo - Tab detection Dictionary with tab names to be processed
#ToDo Integrate NLP logic at this point
#ToDo Hit UI screen after this
#open up a sample excel
workbook = es.loadWorkBook(InputFileLoc)

#get the sheetnames
data = es.exportExcelToJSON(workbook)
print(data)

#------------------------------------------Column Mapping --------------------------#
cols = inputDF.columns
def svMapColumns(cols, mappings):
    return utils.mapColumns(cols, mappings)

svMappedOPColsDict, mappedOPColsList, unmappedOPCols, newInpCols, ioFeildMappings = svMapColumns(cols, mappings)

svFeildMappings = funcUI.getSvFeildMappings(svMappedOPColsDict,inputDF,cols, mappedOPColsList, unmappedOPCols, newInpCols, ioFeildMappings, mappings)


#ToDo  Hit the UI screen for Single value at this stage
#ToDo  the Json receievd from the UI we need to recompute : mappedOPColsDict, mappedOPColsList, unmappedOPCols, newInpCols
#ToDo  get the ouput columns matched with NA on the sv mapping screen

#ToDo Call Multi value map columns
# mv_mappedOPColsDict, mv_mappedOPColsList, mv_unmappedOPCols, mv_unmappedInpCols = utils.mv_mapColumns(inputDF.columns, mvMappings)
dictMVMappings = mv.mvMapColumns(inputDF.columns, mvMappings,functionMappings)
mvMappedOPColsDict,mvMappedOPColsList,mvUnmappedOPCols,mvUnmappedInpCols=mv.combineMVMapping(dictMVMappings)

# mvFeildMappings=funcUI.getMvFeildMappings(dictMVMappings,mvUnmappedOPCols, mvMappings,mvUnmappedInpCols,'mv')
#{"sv":{"inpCol1":"outputCol1","inpCol2":"outputCol2"}
newMappings = {}
for obj in svFeildMappings["mappedOtFields"]:
    newMappings[obj["inputField"]] = obj["outputField"]

mvFeildMappings=funcUI.getMvFeildMappings(dictMVMappings,mvUnmappedOPCols, mvMappings,mvUnmappedInpCols,'mv',{"sv":newMappings},mvMappedOPColsDict,list(inputDF.columns))
#ToDo Integrate NLP logic at this point
#ToDo Hit the UI screen for Multi value at this stage (Exclude all input columns already mapped as part of Single value mapping)
#ToDo  the Json receievd from the UI we need to recompute : mvmappedOPColsDict, mvmappedOPColsList, mvunmappedOPCols, mvnewInpCols
#ToDo  get the ouput columns matched with NA on the sv mapping screen

#------------------------------------------Column Mapping completed --------------------------#

dataframe = utils.renameColumns(inputDF, svMappedOPColsDict)  #ToDo  rename it for single value
dataframe = utils.renameColumns(dataframe, mvMappedOPColsDict) #ToDo then rename it for multi value


# unmappedOPCols
missingCols = unmappedOPCols + mvUnmappedOPCols + list(variables["npiReg"].keys()) #Todo: if NPI reg fails #Todo: sv_unamapped and camel casing everywhere.
dataframe = utils.addMissingColumns(dataframe, missingCols) #Todo: Rohan to get back on This

#Then Do this for multi value

dataframe = utils.fillFinalNPIs(dataframe, "FINAL_NPI")
uniqueIds = utils.getUniqueIDs(dataframe) #returns list of tuples NPI + Tax_ID

frames = utils.getFrames(dataframe, uniqueIds) #returns list of dataframes for every unique id --> tuples of (NPI, TAX_ID)

sv_splitFrameCols = utils.merge_two_dicts( mappings, variables["mandatory"] ) #TODO: check with rohan, #ToDo This mappings should be the refreshed Single value Json #TODO: check with rohan
sv_splitFrameCols = utils.merge_two_dicts( sv_splitFrameCols,  variables["npiReg"])
mv_splitFrameCols = utils.merge_two_dicts( mvMappings, variables["mandatory"] ) #TODO: check with rohan, #ToDo do this same operation of merging for multivalue refreshed columns
splittedFrames, rejectedFrames = utils.splitFrames1(frames,sv_splitFrameCols,mv_splitFrameCols) #splittedFrames --> list of single valued frames, multi valued frames for every unique NPI + Tax_ID combination.
'''Above function should return the list of Tuples (Single value dataframe, multivalue dataframe)
pass two parameters to this function one is single value mapping another one is multivalue mapping'''


#todo for every tuple process Single value and multivalue data frame
npiCols = variables["npiReg"]
singleValuedFrames = []
multiValuedFrames = []
masterDictList = []
# VariableCols = outputCols

for tupple in splittedFrames:
    # Todo get NPI supplementary data.
    # enrichedDataFrame, emptyNpiDFs, failedNpis = fnpi.enrichDataFrame(tupple[0], npiCols) #ToDo this should be part of single value data frame operation
    enrichedDataFrame = tupple[0]
    # MultiValueOP = DeriveMultiValue(tupple[0], VariableCols, mv_mappings) # returns a dictionary {"Address": dataframe} (both notrmalized and with all associated derivations completed)
    # ndf=utils.mv_address_normalization(tupple[1], mv_mappedOPColsList)

    # Todo Normalization of Multi value dataframe to be done here.
    masterDict = mv.mvNormalizeColumns(tupple[1], functionMappings, dictMVMappings,mvMappings)  # TOdo: recheck

    # Todo Field wise derivation for Single value
    # singlevalueOP, failedSNpis = fnpi.deriveSingleValue(enrichedDataFrame,outputCols)  # returns a single output dataframe

    masterDict = utils.merge_two_dicts(masterDict, {"singleValue": enrichedDataFrame}) #Todo: check with rohan**

    # Todo Field wise derivation for Multi value
    masterDict = mv.cleanseDF(masterDict, cleansingDf)
    masterDict = mv.deriveMVCols(masterDict, cleansingCols, midlevNdBSpec)
    masterDict = mv.deriveMVCols(masterDict, multivaluedCols, midlevNdBSpec)

    masterDictList.append(masterDict)
    # multivalueOP, failedMNpis = fnpi.deriveMultiValue(enrichedDataFrame, normalizedMVFrame, outputCols)  # returns a single output dataframe
    # normalized_mv_frames.append(normalizedMVFrame)

    # singleValuedFrames.append(singlevalueOP)
    # multiValuedFrames.append(multivalueOP)
fdf=funcUI.getFinalDf(masterDictList)
# print fdf.columns.tolist()
outputJson=funcUI.getOutputJson(fdf)
# print outputJson
funcUI.exportReviewedData(outputJson)





# singlevalueOP = pd.concat(singleValuedFrames, ignore_index=True)
#
# # finalDF.reindex_axis(outputCols.keys(), axis=1)
#
# singlevalueOP.fillna(value="",inplace=True)
# writer = pd.ExcelWriter(outputFileLoc)
# singlevalueOP.to_excel(writer, 'Sheet1', index_label=False)
# writer.save()
# print "failedNpis --> ", failedNpis


#Todo: revisit: ssn_4 column
#ToDo: [(<TAX_ID>,<NPI>)] --> ******
#Todo:PRIMARY GROUP TAX ID, PRIMARY GROUP NPI, Board_Expiration_Date, group_np,State_License_Expires

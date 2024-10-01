import utils
import re
import excelStructureXlrd as es

import mongoDBHandler
import sys
sheetIndex = 1



# paths required
loc = "../input/"
configPath='../config/'
userMappingsPath='../userMappings/'
provMappingsPath='../providerMappings'
esConfigPath = "../config/elasticConfig.json"
esConfigFile = utils.readFile(esConfigPath, type="json")
masterConfigPath = '../setup/masterConfig.json'
mConfig = utils.readFile(masterConfigPath, type="json")
elasticConHost=mConfig['elastic']['host']
elasticConPort=mConfig['elastic']['port']
elasticUrl='{}:{}'.format(elasticConHost,elasticConPort)

mongoConHost = mConfig['mongo']['host']
mongoConPort = mConfig['mongo']['port']
mongoUrl = '{}:{}'.format(mongoConHost, mongoConPort)
mongoUri = "mongodb://"+'{}:{}'.format(mongoConHost, mongoConPort)+"/"+'Triage'

# creating an object for the class MongoDBHandler
handler = mongoDBHandler.MongoDBHandler()



collection_file_mapping = {
    "c_and_s": "Elastic",
    "comm_types": "Elastic",
    "contact_type_exceptions": "Elastic",
    "degree_exceptions": "Elastic",
    "degree_psi": "Elastic",
    "hbp_speciality": "Elastic",
    "hosp_aff_status": "Elastic",
    "language_code": "Elastic",
    "master_degree": "Elastic",
    "medicaid_numbers": "Elastic",
    "mid_level_ndbpSpec": "Elastic",
    "mid_specialty": "Elastic",
    "hospital_list": "Elastic",
    "ndb_taxonomy": "Elastic",
    "nucc": "Elastic",
    "pcp_vs_specialist": "Elastic",
    "reason_desc_code": "Elastic",
    "rfp_grid": "Elastic",
    "school_code": "Elastic",
    "spec_psi": "Elastic",
    "specialty_exceptions": "Elastic",
    "us_cities": "Elastic",
    "us_states": "Elastic",
    "ver_source": "Elastic",

    "pickledSvNlpLookup": "Elastic",
    "pickledMvNlpLookup": "Elastic",

    "columns": "Config",
    "columns-json_bkp": "Config",
    "credentials": "Config",
    "elasticConfig": "Config",
    "elasticConfig5pt6pt4": "Config",
    "functionMappings": "Config",
    "mappings": "Config",
    "mvMappings": "Config",
    "ndb_p": "Config",
    "outputCols": "Config",
    "outputCols_DEMO_PRAC_ADD": "Config",
    "outputCols_CONTRACT_PRAC_ADD": "Config",
    "outputCols_DEMO_ADDR_ADD": "Config",
    "outputCols_NonMass": "Config",
    "outputCols_TAXID": "Config",
    "outputCols_DEMO_TAXID_INACT": "Config",
    "outputCols_DEMO_ADDR_INACTIVE":"Config",
    "outputCols_DEMO_ADDR_CHANGE":"Config",
    "outputCols_DEFAULT_OTHER":"Config",
    "outputCols_PRAC":"Config",
    "outputCols_PLMI":"Config",
    "outputCols_ACTIONS":"Config",
    "providers": "Config",
    "regex": "Config",
    "Standards": "Config",
    "sv_mappings": "Config",
    "templateDrivers": "Config",
    "templates": "Config",
    "variables_DEMO_PRAC_ADD": "Config",
    "variables_CONTRACT_PRAC_ADD": "Config",
    "variables_DEMO_ADDR_ADD": "Config",
    "variables_NonMass": "Config",
    "variables_TAXID": "Config",
    "variablesCommon": "Config",
    "variables_DEMO_TAXID_INACT": "Config",
    "variables_DEMO_ADDR_INACTIVE":"Config",
    "variables_DEMO_ADDR_CHANGE":"Config",
    "variables_DEFAULT_OTHER":"Config",
    "variables_PRAC":"Config",
    "variables_PLMI":"Config",
    "variables_ACTIONS":"Config",
    "outputCols_DEMO_OTHER":"Config",
    "variables_DEMO_OTHER":"Config",
    "variables_DEMO_NON_PRAC_ADD":"Config",
    "outputCols_DEMO_NON_PRAC_ADD":"Config",
    "variables_CONT_TIN_INACTIVATE":"Config",
    "outputCols_CONT_TIN_INACTIVATE":"Config",
    "variables_CONTRACT_AUTHOR_EXECUTE":"Config",
    "outputCols_CONTRACT_AUTHOR_EXECUTE":"Config",
    "variables_CONTRACT_EXECUTE":"Config",
    "outputCols_CONTRACT_EXECUTE":"Config",
    "variables_CONT_AMEND_EXECUTE":"Config",
    "outputCols_CONT_AMEND_EXECUTE":"Config",
    "variables_CONTRACT_OTHER":"Config",
    "outputCols_CONTRACT_OTHER":"Config",
    "variables_CONT_MODF_INACT":"Config",
    "outputCols_CONT_MODF_INACT":"Config",
    "variables_CONT_MODF_ADD":"Config",
    "outputCols_CONT_MODF_ADD":"Config",
    "ProviderFinal": "Provider",
    "ProviderPdf": "Provider",

    "UserMapping": "User",
    "ProviderInterim": "User",
    "SpecialtyException": "User",

    "InpDF": "PickleData",
    "sotHeader": "PickleData",

    "Bkp": "UserBackup",
    "variables_AllTemplates":"Config",
    "ActionData":"Actions",
    "trainData":"NER",
    "logging":"Logs",
    "reconciliation":"reconciliation",
    "input":"inputFiles",

}

devMode=False
if devMode:
    sys.path.append('../setup')
    import replaceMongoFile_DevelopmentAid

#try:
standardsDict = handler.find_one_document("Standards")["Standards"]
regexDict = handler.find_one_document("regex")["regex"]
mappings = handler.find_one_document("mappings")["mappings"]
mvMappings = handler.find_one_document("mvMappings")["mvMappings"]
variablesCommon = handler.find_one_document("variablesCommon")["variablesCommon"]
credentialsData = handler.find_one_document("credentials")["credentials"]
functionMappings = handler.find_one_document("functionMappings")["functionMappings"]
esConfig = handler.find_one_document("elasticConfig")["elasticConfig"]
esConfig5pt6pt4 = handler.find_one_document("elasticConfig5pt6pt4")["elasticConfig5pt6pt4"]
provAdmScrJson = handler.find_one_document("providers")["providers"]
templates = handler.find_one_document("templates")["templates"]
templateDrivers = handler.find_one_document("templateDrivers")["templateDrivers"]
#hiddenTemplates = handler.find_one_document("hiddenTemplates")["hiddenTemplates"]
#except TypeError:
#    pass
#mappingsPath = "../config/mappings.json"
#Multi value Columns json to be read
#variablesCommonPath = "../config/variablesCommon.json"
#outputColsSotPath = "../config/outputCols.json"
#outputColsPsiPath = "../config/outputCols_NonMass.json"
#mvMappingsPath = "../config/mvMappings.json"
#functionMappingsPath = "../config/functionMappings.json"
#ndbPPath = "../config/ndb_p.json"
#credentialsPath="../config/credentials.json"
#specialty_exceptions = "../config/specialty_exceptions.csv"
#master_degree = "../config/Master_Degree.csv"
#standards = "../config/Standards.json"
#regex = "../config/regex.json"

#delegateFilePath = "../config/NDB_Delegate Groups v1.xlsx"
# delegates_dict = es.read_delegates_file(delegateFilePath)

#standardsDict = utils.readFile(standards, type="json")
#regexDict = utils.readFile(regex, type="json")
#mappings = utils.readFile(mappingsPath, type="json")
#mvMappings = utils.readFile(mvMappingsPath, type="json")
#outputColsPsi = utils.readFile(outputColsPsiPath, type="json")
#credentialsData = utils.readFile(credentialsPath, type="json")
#functionMappings = utils.readFile(functionMappingsPath, type="json")
# multivaluedCols = variables["multivaluedCols"]
# ndb_p = utils.readFile(ndbPPath, type="json")
# multivaluedCols = variables["multivaluedCols"]
# cleansingCols = variables["CleanseLayer1"]
# cleansingDf=variables["CleanseDataFrame"]
# finalCleansingDf=variables["FinalCleansingLayer"]
# psiLayerDf=variables["PSILayerDf"]
# masterDictListLayer=variables["MasterDictListLayer"]
# fdfLayerDf=variables["fdfLayer"]


##--------------------- PSI Sheet Names -----------------------##
psi_sheet_names = {
    "tax-id" : "DEMO TAX ID ADD",
    "demo-taxid-inact" : "DEMO TAX ID INACTIVATE",
    "cont-tin-inactivate" : "DEMO CONTRACT TAX ID INACTIVATE",
    "demo-addr-add" : "DEMO ADDRESS ADD",
    "demo-addr-inact" : "DEMO ADDRESS INACTIVATE",
    "demo-addr-change" : "DEMO ADDRESS CHANGE",
    "demo-non-prac-add" : "DEMO NP PRAC ADD",
    "taxid_cont_grp" : "DEMO CONTRACT PRAC ADD TO GROUP",
    "np_prac_add_cont_grp": "DEMO NP CONTRACT ADD TO GROUP"
}



diWeekDays ={int(key):value for key,value in list(standardsDict["diWeekDays"].items())}
wdReg0="(?:^|\s|\\b)"
wdReg1="(?:\s|,|$|\\b)"
expanders = ['thru', 'to', 'through']
expanders1 = ['-']
time247='12:01am-11:59pm'
regex247='(24)(?:.*?)(7|seven)|(7|seven)(?:.*?)(24)|(24)'
wd= '|'.join([wdReg0+item+wdReg1 for sublist in [v["words"] for k,v in list(diWeekDays.items())] for item in sublist])
splitCols = {'Degree':['DEGREE'], 'Address':['SPECIALITY']}
lstDelimsForSplit=["[,;&]+", '$']
lstDelimsForHospSplit=["[,;]+", '$']
hospitalDelimsExceptions=['dba', 'aka']
#svNlpPicklePath='../config/pickledSvNlpLookup.p'
#mvNlpPicklePath='../config/pickledMvNlpLookup.p'
svNlpPickle = 'pickledSvNlpLookup' # file_mapping
mvNlpPickle = 'pickledMvNlpLookup'
#userMappingsBkpPath='../userMappingsBkp/'

#esConfigPath = "../config/elasticConfig.json"
#esConfig = utils.readFile(esConfigPath, type="json")
indexPrefixElastic=esConfig['project_name']
wd1= '|'.join([item for sublist in [v["words"] for k,v in list(diWeekDays.items())] for item in sublist])

#provAdmScrJsonPath='../config/providers.json'
#provAdmScrJson = utils.readFile(provAdmScrJsonPath, type="json")
match_all_size=10000
debug=False # changes to this variable should not be checked in
spec_excep='specialty_exceptions'
#templatesPath='../config/templates.json'
#templates=utils.readFile(templatesPath, type="json")
#templateDriversPath='../config/templateDrivers.json'
#templateDrivers=utils.readFile(templateDriversPath, type="json")
nmTName='prac'
uidLevels=["FINAL_NPI", "TAX_ID", "NAME_FIRST", "NAME_LAST","VALIDATED_ACTION"]
recIdLevels=['sheetIdx','recordIdx']
mappingMatchThreshold = 70
globalDelegateName = "GLOBAL"

whColsExtHrInd = ['WORKING_HOURS','MONDAY_START_TIME', 'MONDAY_END_TIME', 'TUESDAY_START_TIME', 'TUESDAY_END_TIME', 'WEDNESDAY_START_TIME', 'WEDNESDAY_END_TIME', 'THURSDAY_START_TIME', 'THURSDAY_END_TIME', 'FRIDAY_START_TIME', 'FRIDAY_END_TIME', 'SATURDAY_START_TIME', 'SATURDAY_END_TIME', 'SUNDAY_START_TIME', 'SUNDAY_END_TIME']
lstStartTime=['OUT_MON_START_TIME', 'OUT_TUE_START_TIME', 'OUT_WED_START_TIME', 'OUT_THU_START_TIME', 'OUT_FRI_START_TIME']

timeDelim=['-','to',' ']
ampm=["am","pm","a","p"]
ampmRegex='|'.join([re.escape(elm) for elm in ampm])
timeDelimRegex='|'.join([re.escape(elm) for elm in timeDelim])
regexTimeGroup=regexDict['TIME_GROUP'].format(ampmRegex,timeDelimRegex)
regexTimePart=regexDict['TIME_PART'].format(ampmRegex)
regex12hr=regexDict['12HR'].format(ampmRegex)
regexWdWhGroup=regexDict['WDWH_GROUP'].format(timeDelimRegex)
regexWhClrJoin = '(?:\\b|\\s)|(?:\\b|\\s|\\$)'
regexWhClrString = regexDict['WH_CLR']
regexExtHrInd = regexDict['TIME_EXTHRIND'].format(ampmRegex)

npiAPI = 'https://npiregistry.cms.hhs.gov/api/?version=2.1&address_purpose=&number='
usr = ""
pwd = ""
proxy = {}
#proxy = {'http': 'http://{}:{}@proxy1.wipro.com:8080'.format(usr,pwd), 'https': 'https://{}:{}@proxy1.wipro.com:8080'.format(usr,pwd)}
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"}
commonUidLevels=["Prac_NPI", "Prac_TIN", "Prac_FirstNm", "Prac_LastNm"]
cmnTTyp=["demo-addr-add","demo-addr-inact"]
allTmpNm='allTemplates'
runInterTmpltLayer=True
validatedActionColNm="VALIDATED_ACTION"
defaultTTyp='DEMO OTHER'
# defaultTTyp='DEFAULT OTHER'
reGroupId=['TAX_ID', 'FINAL_NPI']
#reGroupTTyp={defaultTTyp:["DEMO ADDR ADD","DEMO ADDR INACTIVATE","DEMO ADDR CHANGE"]}
reGroupTTyp={defaultTTyp:[]}

# path to security key and cert file
keyFile = mConfig['security']['keyFilePath']


crtFile = mConfig['security']['crtFilePath']
plmiGlobalData={"delegates":"https://plmi.uhc.com/api/GetAll/GetDelegates","system":"https://plmi.uhc.com/api/Delegates/System","market":"https://plmi.uhc.com/api/Delegates/Markets"}
plmiDelData={"platform":  {"url":"https://plmi.uhc.com/api/PlatformOverview/Get","params":{"DelegateID":0}},"taxId":  {"url":"https://plmi.uhc.com/api/TaxIdInfo/Get","params":{"DelegateID":0}}}
fetchPlmi=True
storeActions=True
predictActions=False
actionModelDataFilePath=configPath+'/actionModelData.txt'
predictedActionColNm='predictedAction'
useNlpModel=True
fileNmCol='FILENAME'
tabNmCol='TABNAME'
actionColForHash='ACTION_FOR_HASH'
combinedInpActionColNm='COMBINED_INP_ACTION'
commonHashCols=["FINAL_NPI", "TAX_ID", "NAME_FIRST", "NAME_LAST"]
weakHashColsForActionsData=commonHashCols
strongHashColsForActionsData=commonHashCols+[actionColForHash]
hashInUse='weakHash'
actionModelColsInclusion=[fileNmCol,tabNmCol,'ACTION']
# hashInUse='strongHash'
exceptionTransaction=["contract-prac-add-to-grp"]
# hiddenTempDI={"CONTRACT PRAC ADD TO GRP":["DEMO TAXID ADD","DEMO NON PRAC ADD"]}
# hiddenExcpTempDI={"CONT TIN INACTIVATE":["DEMO TAXID INACTIVATE"]}
# pdfExtraction
lstParentCategory = ['singleValue', 'Address', 'Speciality', 'Degree', 'Licence', 'Dea', 'SchoolDetails', 'Medicaid', 'ProviderEmail_WebAddr', 'ProviderLanguage', 'Board', 'Hospital', 'ChangeType', 'Medicare', 'Cdscsr', 'CommCredCont', 'Credential', 'Title', 'ProviderEmail/WebAddr']
skipConv=True
diSel={'tagsWithNoChildren':".//page[@id='{0}']//*[@bbox and  not(*)]",'imgTxtLn2':".//page[@id='{0}']//*[local-name()='textline' or local-name()='figure']",'pgChildrenTags':".//page[@id='{0}']/*",'rect':".//page[@id='{0}']//rect",'fig':".//page[@id='{0}']//figure",'textline':".//page[@id='{0}']//textline",'curve':".//page[@id='{0}']//curve",'line':".//page[@id='{0}']//line","fig-curve-rect":".//page[@id='{0}']//*[self::curve or self::figure or self::rect]","textbox":".//page[@id='{0}']//textbox","textPg":".//page[@id='{0}']//text"}
pdfMappingsPath = '../pdfMappings/'
tableExtractionUrl = 'http://localhost:8098/extractTable'
# assuming QC_Automation and uhg_triage_automation_MongoDB project are present in the same directory
pklDir = '../../uhg_triage_automation_MongoDB/tmp/'
pdfDir = '../../uhg_triage_automation_MongoDB/input/'
xMargin = 1
yMargin = 0

# pdf to Html conversion
inFileLoc = '../input/'
pdf2HtmlApiHost='localhost'
pdf2HtmlApiPort='8099'
pdf2HtmlApiStaticLoc='http://{}:{}'.format(pdf2HtmlApiHost,pdf2HtmlApiPort)
pdf2HtmlApi="{}/convertPdfToHtml".format(pdf2HtmlApiStaticLoc)
docTyp='_doc'
elastic5pt6pt4=False
userMapRecord = 1000
chunkLimit=15000000
nrowsForSnapshot=100
nrowsSnapshotMapping=10
snapshotInplace=False
nRowsMp=20000
nColsMp=20
perfLogging=False
dbLogging=False
initBatSize=10
preponeNPIReg=True
finalNpiCol="FINAL_NPI"
threshMPW=5
windows_max_cpu= 60
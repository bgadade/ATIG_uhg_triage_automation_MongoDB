import pandas as pd
import numpy as np
import constants
import mvDerivation_Common as mvCmn

def findIntersection(fdfDict,diIsectId):
    '''used to find intersection between the contributing templates on the basis
    of keys (diIsectId) provided in the json'''
    diIx = {template:fdf.index.unique() for template,fdf in list(fdfDict.items()) if template in diIsectId}
    #finding the respective unique indexes for each contributing template
    lolIx=list(diIx.values())#list of list indexes from the dictionary index values
    lstIsect=set(lolIx[0]).intersection(*lolIx[1:])#find common (intersection) keys between contributing templates
    return lstIsect#we are only returning the NPI, Tax-Id, effective date and address type as this will be our key to identify rows from contributing templates

def fillTemplate(fdfDict,lstIsect,diRecordId,diOutputMap,diOthCols):
    '''The purpose of the function is to fill the new template with the contributing template data
    This function maps the contributing templates columns with the new template columns'''
    markIsect=[]#markIsect will be used to mark the intersecting rows in contributing termplates(npi,tax-id, effective date and address type)
    lstRecords=[]
    for isect in lstIsect:#common (intersection) keys between contributing templates
        tdi={}#it's a template dictionary in order to store the template data
        for template,recordId in list(diRecordId.items()):
            #we are checking for contributing templates against the  address line 1&2, city, state and zip
            tdi[template]=fdfDict[template].loc[[isect]].set_index(recordId,drop=False)
            #setting index(address line 1&2, city, state and zip) for contributing template for the intersecting row only
        diIx={template:fdf.index.unique()for template,fdf in list(tdi.items())}
        #getting a dictionary for the unique combination of the address line 1&2, city, state and zip against each contributing template
        isOneToOne=[True if len(ix.unique())==1 else False for ix in list(diIx.values())]
        '''check for one to one because we move the data to new template only if there's unique address(address line 1&2, city, state and zip) for each index,
        if there are more than 1 unique address for a corresponding record we will not move the data to new template'''
        if not all(isOneToOne):
            continue
        markIsect.append(isect)
        diReqCols={template:{v:k for k,v in list(outputMap.items()) if v} for template,outputMap in list(diOutputMap.items())}
        for template,othCols in list(diOthCols.items()):
            diReqCols[template].update(othCols)
            #Other cols are basically static values such as npi, tax-id,fname,lname,record idx,sheet idx and validated action
        lstTemplateData=[tdi[template].loc[[diIx[template][0]],list(diReqCols[template].keys())].rename(columns=diReqCols[template]).reset_index(drop=True) for template, recordId in list(diRecordId.items())]
        #list of dataframes created from the contributing templates columns which were mapped with the new template cols
        record = mvCmn.concatForwardFill(lstTemplateData)#concatenated the contributing templates to form final new template dataframe
        lstRecords.append(record)
    return markIsect,lstRecords


def setFlag(fdfDict,diDelFlag,markIsect):
    '''used to set flag in the contributing templates for the rows which needs to be deleted based on the markIsect received from the fill template '''
    for template,delFlagCol in list(diDelFlag.items()):
        fdfDict[template][delFlagCol]=False
        fdfDict[template].loc[markIsect,delFlagCol]=True
    return fdfDict

def updateActionInNM(idForNmUpdate,fdfDict,outFdf,actionCol,mrgdAction,diDelFlag, nonMassRecordRef):
    '''Manipulate and cleanse Non Mass for further join downstream'''
    nmTName=constants.nmTName
    # outFdf is the join result of individual templates where match happens
    ixForNMUpdate = outFdf.set_index(idForNmUpdate, drop=False).index.unique()#idForNmUpdate contains npi,taxid,fname & lname
    fdfDict[nmTName]["deleteRow"] = False
    fdfDict[nmTName].set_index(idForNmUpdate, inplace=True,
                                  drop=False)
    fdfDict[nmTName]['ixCol'] = fdfDict[nmTName].apply(lambda row: ''.join([row[elm] for elm in idForNmUpdate]), axis=1)
    for ix in ixForNMUpdate:
        tdi = {}
        for template, delFlagCol in list(diDelFlag.items()):
            '''diDelFlag holds the Columns for delete flag for each contributing template. This is used to determine whether all rows 
            from contributing templates are marked for delete or there are few left over (for multiindex ix). In case of no left over rows 
            (for multiindex ix) in a contributing template the corresponding rows in Non-Mass are marked for delete else left as it is for 
            further join downstream joins'''
            fdfDict[template].set_index(idForNmUpdate, inplace=True, drop=False)
            action = list(set(fdfDict[template][actionCol]))
            distinctDeleteFlag=fdfDict[template].loc[ix, delFlagCol].tolist() if isinstance(fdfDict[template].loc[ix, delFlagCol],pd.Series) else [fdfDict[template].loc[ix, delFlagCol]]
            # distinctDeleteFlag = list(set([fdfDict[template].loc[ix, delFlagCol]])) if len(fdfDict[template])>1 else [fdfDict[template].loc[ix, delFlagCol]]
            '''This flag is used to determiner whether to delete records from Non-Mass based on delete flag in contributing templates data'''
            if len(distinctDeleteFlag) == 1 and distinctDeleteFlag[0]: #truth value of this condition say that all the recoreds of particular template belonging to one key(fname,lname,npi,taxid) have contributed to the combined template and therefore marked for deletion
                deleteFlg = True
            else:
                deleteFlg = False

            tdi[template] = {"allDeleted":deleteFlg, "action":action[0] if len(action)>0 else "",
                             "nonMassRef": True if template == nonMassRecordRef else False}

        for k,v in list(tdi.items()):
            if v["nonMassRef"]: #duplicates records of any one (parameterized) of the contributing templates
                #df = fdfDict[nmTName].loc[ix].loc[fdfDict[nmTName][actionCol] == v["action"]]# duplicating the candidate record
                df = fdfDict[nmTName].loc[((fdfDict[nmTName]['ixCol']==''.join(ix))&(fdfDict[nmTName][actionCol] == v["action"]))]# duplicating the candidate record
                df[actionCol] = mrgdAction #Setting the action in the new duplicate row same as that for new template (mrgd). This is to enable join downstream (PrependNonMass) between the new template and Non-mass
                fdfDict[nmTName] = fdfDict[nmTName]._append(df)
            if v["allDeleted"]: # deletes orphan rows only in case when all the rows of a particular contributing template belonging to one key(fname,lname,npi,taxid) have been deleted

                fdfDict[nmTName].loc[((fdfDict[nmTName]['ixCol']==''.join(ix))&(fdfDict[nmTName][actionCol] == v["action"])), "deleteRow"] = True

    outFdf[actionCol]=mrgdAction
    #fdfDict[nmTName].reset_index(drop=True,inplace=True)
    return fdfDict,outFdf


def identifyRelationsAcrossTemplates(templateNm,templateOutCols,fdfDict,argDi):
    '''This function is to identify the changed address when we merge the addresses from contributing templates and
    the data will only fall under the new template if the contributing templates have the same key value (this includes blank value).
    '''
    diIsectId={template:di["intersection"] for template,di in list(argDi["cols"].items())} #{template:di["record"] for template,di in argDi["cols"].items() if di.get("intersection", None)}
    diRecordId={template:di["record"] for template,di in list(argDi["cols"].items())}
    diOutputMap={template:di["output"] for template,di in list(argDi["cols"].items())}
    diDelFlag={template:di["deleteFlag"] for template,di in list(argDi["cols"].items())}
    diOthCols=argDi["othCols"]
    idForNMUpdate=argDi["idForNMUpdate"]
    actionCol=argDi["actionCol"]
    mrgdAction=argDi["mrgdAction"]
    NonMassRecordRef = argDi["NonMassRecordRef"]
    sourceColNm = argDi["sourceColNm"]
    if templateNm in fdfDict:
        fdfDict[templateNm][sourceColNm] = False
    #NonMassRecordRef is used to keep track of the duplicated row which has to be appended in Nonmass(with transaction type as 'mrgd'

    if not all([not(fdfDict[template].empty) if template in fdfDict else False for template,isectId in list(diIsectId.items())]):
        #if any of the contributing template is empty, we return the fdf as it is
        return fdfDict
    for template,isectId in list(diIsectId.items()):
        fdfDict[template].set_index(isectId,inplace=True,drop=False)#setting the index of the contributing templates
    lstIsect=findIntersection(fdfDict,diIsectId)#finding intersection between the contributing templates
    if not lstIsect:#incase there's no common data in the contributing templates, we return the fdfdict as it is
        return fdfDict
    markIsect,lstRecords=fillTemplate(fdfDict,lstIsect,diRecordId,diOutputMap,diOthCols)
    if not markIsect:#if no intersection between the contributing templates we return the fdfdict
        return fdfDict
    outFdf=pd.concat(lstRecords).rename(columns=templateOutCols).fillna('').reset_index(drop=True)
    #concatenating all the intersecting rows from contributing templates into a single dataframe
    fdfDict=setFlag(fdfDict,diDelFlag,markIsect)
    #used to set flag for the rows which needs to be deleted based on the markIsect received from the fill template
    fdfDict,outFdf = updateActionInNM(idForNMUpdate, fdfDict, outFdf, actionCol, mrgdAction, diDelFlag, NonMassRecordRef)
    if templateNm not in fdfDict:
        templateOutCols_list = list(templateOutCols.values())
        fdfDict[templateNm] = pd.DataFrame([], columns=np.unique(
            templateOutCols_list))
    outFdf[sourceColNm]=True
    fdfDict[templateNm]=fdfDict[templateNm]._append(outFdf).fillna('')
    fdfDict= {tmpltNm:tmpltDf.reset_index(drop=True) for tmpltNm, tmpltDf in fdfDict.items()}
    return fdfDict

def deleteRows(templateNm,templateOutCols,fdfDict,argDi):
    '''used to delete rows'''
    if templateNm not in fdfDict or fdfDict[templateNm].empty:
        return fdfDict
    delFlagCol=argDi["deleteFlagCols"]
    if delFlagCol in fdfDict[templateNm].columns:
        fdfDict[templateNm]=fdfDict[templateNm].loc[fdfDict[templateNm][delFlagCol]==False]
        fdfDict[templateNm].drop([delFlagCol], inplace=True, axis=1)
    return fdfDict


def swapValues(templateNm,templateOutCols,fdfDict,argDi):
    if templateNm not in fdfDict:
        return fdfDict
    sourceCol = argDi['sourceColNm']
    colMapping=argDi['colMapping']

    for idx,row in fdfDict[templateNm].reset_index().iterrows():
        if not row[sourceCol]:
            continue
        for outCol,inCol in list(colMapping.items()):
            fdfDict[templateNm].loc[idx, outCol] = row[inCol]
    return fdfDict

def setDefault(templateNm,templateOutCols,fdfDict,argDi):
    if templateNm not in fdfDict:
        return fdfDict
    defVal=argDi['defVal']
    outCol=argDi['outputCol']
    sourceCol = argDi['sourceColNm']
    for idx,row in fdfDict[templateNm].reset_index().iterrows():
        if not row[sourceCol]:
            continue
        fdfDict[templateNm][outCol]=defVal
    return fdfDict

def setBAID(templateNm,templateOutCols,fdfDict,argDi):
    if templateNm not in fdfDict:
        return fdfDict
    out_inp_col = argDi['inputCol']
    npi_list = list(fdfDict[templateNm]["FINAL_NPI"].unique())
    Addr_type = argDi['Addr_type']
    Address_value = argDi['Addr_value']


    for npi in npi_list:
        filter_df = fdfDict[templateNm][(fdfDict[templateNm]["FINAL_NPI"] == npi) & ((fdfDict[templateNm][Addr_type].str.lower() == "h") | (fdfDict[templateNm][Addr_type].str.lower() == "d"))]
        filter_df_Addr_unique_check = filter_df[Address_value].unique()
        for key, value in sorted(out_inp_col.items()):
            check_len_of_duplicate = len(filter_df_Addr_unique_check)
            index_of_filter_df = filter_df.index.values
            if (not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "h"), Addr_type].empty) and (not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "d"), Addr_type].empty):
                fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = ""

            elif (not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "h"), Addr_type].empty) or (not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "d"), Addr_type].empty):
                if (key != Addr_type) and (check_len_of_duplicate == 1):
                    fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = filter_df[key].iloc[0]

                elif (key == Addr_type) and (check_len_of_duplicate == 1):
                    if not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "h"), Addr_type].empty:
                        fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = filter_df[key].iloc[0]
                    elif not fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() == "d"), Addr_type].empty:
                        fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = filter_df[key].iloc[0]
                    else:
                        fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi) & (fdfDict[templateNm][Addr_type].str.lower() != "d") & (
                                    fdfDict[templateNm][Addr_type].str.lower() != "h"), value] = ""
                else:
                    fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = ""
            else:
                fdfDict[templateNm].loc[(fdfDict[templateNm]["FINAL_NPI"] == npi), value] = ""
    return fdfDict

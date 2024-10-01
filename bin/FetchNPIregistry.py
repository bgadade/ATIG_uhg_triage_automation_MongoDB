import json
import pandas as pd
import requests
import columnTrees as ct
import numpy as np
import sys
import elastic

def checkMaxRetries(npi):
    if npi == checkMaxRetries.npi:
        checkMaxRetries.retries = checkMaxRetries.retries + 1
        if checkMaxRetries.retries == checkMaxRetries.limit:
            checkMaxRetries.retries = 0
            return True
        else:
            return False
    else:
        checkMaxRetries.npi = npi
        checkMaxRetries.retries = checkMaxRetries.retries + 1
        return False


checkMaxRetries.retries = 0
checkMaxRetries.limit = 10
checkMaxRetries.npi = ""

#################### Hit the NPI registry url and get the json for that particular NPI ####
def getNpiRecord(npi):
    try:
        npi = str(int(float(npi)))
        s=requests.session()
        # s.get('https://npiregistry.cms.hhs.gov',headers={'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'})
        r = s.get('https://npiregistry.cms.hhs.gov/api/?number=' + npi)
        npiRecord = json.loads(r.content)
    except requests.exceptions.RequestException as e:
        print(e)
        maxRetries = checkMaxRetries(npi)
        if maxRetries:
            raise AssertionError("<Error: NPI registery Failed>") #Return empty dataframe for failed record
        else:
            return getNpiRecord(npi)
    
    # extract the individual rows from the json
    #################### Extracting the Level -1 Attributes #####################

    basic = [item['basic'] for item in npiRecord['results']]
    taxonomies = [ item['taxonomies'] for item in npiRecord['results']]
    
    # Extracting First name
    firstName = [i['first_name'] for i in basic]
    
    # Extracting lastname
    lastName = [i['last_name'] for i in basic]
    
    # Extracting gender
    gender = [i['gender'] for i in basic]

    credential = [i['credential'] for i in basic]

    # Extracting the primary Taxonomy
    primaryTaxonomy = [(i["code"], i["desc"]) for i in taxonomies[0] if i["primary"] == True]
    specialityString = None
    taxonomyString = None
    count = 0
    for spec in primaryTaxonomy:
        bestMatch = elastic.gridLookup('ndb_taxonomy', spec[1], 1,**{'match_fields': ["prov_type_name"]})
        if bestMatch:
            specialityString = bestMatch["prov_type_name"]
            if count == 0:
                taxonomyString = spec[0] + ":" + bestMatch["ndb_spec"]
            else:
                taxonomyString = taxonomyString + "|" + spec[0] + ":" + bestMatch["ndb_spec"]
            count += 1

    
    #Extracting the enumeration type
    enumeration_type = [item['enumeration_type'] for item in npiRecord['results']]

    # Extracting the enumeration date
    enumeration_date = [item['basic']['enumeration_date'] for item in npiRecord['results']]

    try:
        df1 = pd.DataFrame(
                           {'Gender': gender,
                            'Last name': lastName,
                            'First name': firstName,
                            # 'Taxonomy Primary': primaryTaxonomy,
                            'Taxonomy Primary': [taxonomyString],
                            'Speciality Primary': specialityString,
                            'Enumeration Type': enumeration_type,
                            'Enumeration Date': enumeration_date,
                            'Credential': credential
                           })
    except Exception as e:
        print("<Error : " + npi + " has multiple values.>")
        return pd.DataFrame(columns=["Gender","Last name","First name","Taxonomy Primary","Enumeration Type","Enumeration Date"])
    else:
        return df1

def enrichDataFrame(frames, npiCols):
    """takes the input as dataframe and enriches it"""
    emptyNpiDFs = None
    multiValNPIs = None
    enrichedDataFrames = None

    intermediateDF = frames
    try:
        npiDF = getNpiRecord(str(int(intermediateDF["FINAL_NPI"][0])))
    except Exception as e:
        print(e)
        return frames, None, str(int(intermediateDF["FINAL_NPI"][0]))
    else:
        if npiDF.empty:
            try:
                raise AssertionError("Empty intermediateDF")
            except Exception as e:
                print("<Error : " + str(int(intermediateDF["FINAL_NPI"][0])) + " has zero or multiple values.>")
                emptyNpiDFs = str(int(intermediateDF["FINAL_NPI"][0]))
        else:
            #fill the records
            try:
                for key, value in npiCols.items():
                    val = npiDF[value["Input_Column"]]
                    if value["DATAFRAME_TYPE"] == "Single value" and len(val) > 1:
                        raise AssertionError("multivalued intermediateDF")
                        break
                    else:
                        intermediateDF[key] = val[0]
            except Exception as e:
                print("<Error : " + str(int(intermediateDF["FINAL_NPI"][0])) + " " + key + " has multiple values.>")
                multiValNPIs = str(int(intermediateDF["FINAL_NPI"][0]))
            else:
                enrichedDataFrames = intermediateDF
        return enrichedDataFrames, emptyNpiDFs, multiValNPIs

def deriveSingleValue(enrichedDataFrame, outputCols):
    singlevalueOP = pd.DataFrame(columns= outputCols)
    row = -1 #for appending enrichedrecord to df.
    failedNpis = []

    # for df in enrichedDataFrames:
    df = enrichedDataFrame
    print(str(int(df["FINAL_NPI"].unique()[0])), str(df["TAX_ID"].unique()[0]))

    newRow = {}
    for key, value in outputCols.items():
        if value["DATAFRAME_TYPE"] == "Single value" and value["VARIABLE_NAME"] is not None:
            #Todo:
            newRow[key] = str(getattr(ct, value["VARIABLE_NAME"])().populate([df] + value["Input_Column"]))
    row = row + 1
    for key in list(newRow.keys()):
        singlevalueOP.loc[row, key] = newRow[key]
    return singlevalueOP, failedNpis

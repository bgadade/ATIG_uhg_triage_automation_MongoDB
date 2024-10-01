import mvDerivation_Common as mvCmn

def checkSystem(df,argDi):
    outCol=argDi['outCol']
    def func(row,outCol):
        delId=row['delId']
        sysData=eval(row['globalData'])[argDi['plmiGlobalDataKey']]
        sysNm=argDi['systemNm']
        if delId is None:
            return ""
        lstSysForDelId=[di["SystemName"] for di in sysData["DelegateSystemList"] if di["DelegateId"]==delId]
        
        if outCol =="DelPracAdd_OxPar?" or outCol == "DelPracAdd_UHCPar?":
            return "Y" if sysNm in lstSysForDelId else "N"
        return "TRUE" if sysNm in lstSysForDelId else "FALSE"
    df[outCol]=df.apply(lambda row:func(row,outCol),axis=1)      
    return df

def checkPlatform(df,argDi):
    
    outCol = argDi['outCol']
    condition=argDi['condition']
    transformDi={v:k for k,v in list(argDi.get('transform').items())}
    def func(row,condition,transformDi):
        platformData=eval(row['delData'])[argDi['plmiDelDataKey']]
        apiVal=platformData
        for k in condition["keys"]:
            apiVal=apiVal.get(k)
            if not apiVal:
                break
        if condition.get("existence"):
            truthVal=bool(apiVal)
        else:
            truthVal=apiVal==condition["val"]
        return transformDi[truthVal]
    if outCol == "Import_Del_Product_IFP" or outCol == "Import_Del_Product_MEDICAID" or outCol == "Import_Del_Product_DSNP":
        df[outCol] = df.apply(lambda row: "", axis=1)    
    else:
        df[outCol] = df.apply(lambda row: func(row,condition,transformDi), axis=1)    
    return df 

def checkMarket(df, argDi):
    outCol = argDi['outCol']

    def func(row, outCol):
        delId = row['delId']
        sysData = eval(row['globalData'])[argDi['plmiGlobalDataKey']]
        marketDES = argDi.get('marketDes')
        #VAccnMarketDesc = argDi.get('va_ccn_marketDesc')
        VAMarket = argDi.get('VAMarket')
        VARegion = argDi.get('VARegion')
        
        if delId is None:
            return ""

        '''if VAccnMarketDesc:
            VAccnMarketDesc = [item.lower() for item in VAccnMarketDesc]
            for di in sysData["DelegateMarketList"]:
                if VAccnMarketDesc and di.get("DelegateId") == delId and di.get("MarketDescription").lower() in VAccnMarketDesc:
                    return "Yes" if outCol == "Prac_ParStatus_VA CCN" else "TRUE"
                continue
            return "No" if outCol == "Prac_ParStatus_VA CCN" else "FALSE" '''
        
        if VAMarket and VARegion:
            for di in sysData["DelegateMarketList"]:
                if di.get("DelegateId") == delId and di.get("VAMarket") is not "null" and di.get("VARegion") is not "N/A":
                    return "Yes" if outCol == "Prac_ParStatus_VA CCN" else "TRUE"
                continue
            return "No" if outCol == "Prac_ParStatus_VA CCN" else "FALSE"                    
            
        if marketDES:
            for di in sysData["DelegateMarketList"]:   
                if marketDES and di.get("DelegateId") == delId and di.get("MarketDescription").lower()==marketDES.lower():
                    return "TRUE"
                continue
            return "FALSE"
        
    df[outCol] = df.apply(lambda row: func(row, outCol), axis=1)
    return df

def setDefault(df,argDi):
    outCol = argDi['outCol']
    defaultVal=argDi['defaultVal']
    if df['delId'][0] is None:
        df[outCol] = df.apply(lambda row: "", axis=1)      
    else:
        df[outCol] = df.apply(lambda row: defaultVal, axis=1)
    return df

def setDefaultClarification(df,argDi):
    outCol = argDi['outCol']
    if df['delId'][0] is None:
        df[outcol]=df.apply(lambda row: "",axis=1)
    else:
        defaultVal = argDi['defaultVal']
        df[outCol] = df.apply(lambda row: mvCmn.ErrorMessage([],["c",defaultVal,'']), axis=1)
    return df
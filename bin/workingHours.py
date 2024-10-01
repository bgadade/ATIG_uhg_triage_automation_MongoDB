import re
import constants
import datetime
import traceback
import mvDerivation_Common as mv

def extractDays(DaysMatchedString):
    reComp = '|'.join(['\s+' + item + '\s+' for item in constants.expanders])
    reComp += '|' + '|'.join(['\s*' + item + '\s*(?!$)' for item in constants.expanders1])
    DaysMatchedString = DaysMatchedString.strip()
    if re.search(reComp, DaysMatchedString):
        lstDays = re.split(',', DaysMatchedString)
    else:
        lstDays = re.split(',|[ ]+', DaysMatchedString)
    lstDays = [item for item in lstDays if item]
    lstDaysIndex = []
    captured = []
    for day in lstDays:
        try:
            if not day:
                continue
            brk = 0
            for exp in constants.expanders + constants.expanders1:
                # print 'day:',day
                if exp in day:
                    dayIdx = [getKey(re.search(constants.wd, d, re.IGNORECASE).group(0).strip()) for d in
                              day.replace(',', '').split(exp) if d]
                    dayIdx.sort()
                    dayIdx = tuple(dayIdx)
                    # lstDaysIndex = [constants.diWeekDays[idx]['day'] for idx in range(dayIdx[0], dayIdx[1] + 1)]
                    lstDaysIndex.extend([constants.diWeekDays[idx]['day'] for idx in range(dayIdx[0], dayIdx[1] + 1)])
                    # lstDaysIndex += constants.diWeekDays[dayIdx[0]:dayIdx[1] + 1]
                    brk = 1
                    if dayIdx:
                        captured.append(DaysMatchedString)
                    break
            if brk:
                continue
            # idx = getKey(re.search(constants.wd, day, re.IGNORECASE).group(0).strip())
            # d=constants.diWeekDays[idx]['day']
            # lstDaysIndex.append(d)
            lstD = re.findall(constants.wd, day, re.IGNORECASE)
            for d in lstD:
                idx = getKey(d.strip())
                dkey = constants.diWeekDays[idx]['day']
                lstDaysIndex.append(dkey)
                captured.append(day)
        except:
            traceback.print_exc()
            continue
    return lstDaysIndex,captured

def extractDayTime(line_string):
    # print line
    def func(line):
        capturedString = []
        groups = segmentWh(line)
        lst_groups = []
        for index, item in enumerate(groups):
            if not re.search('[0-9]+', item, re.IGNORECASE):
                lst_groups.append([item, '', ''])
            else:
                combined = item
                days = re.search('(.*?)([0-9].*$)', combined, re.IGNORECASE).group(1)
                time = re.search('(.*?)([0-9].*$)', combined, re.IGNORECASE).group(2)
                lst_groups.append([combined, days, time])

        for group in lst_groups:
            try:
                # print group[1]
                grpString=group[1].replace('&amp;', ',').strip()
                lstDaysIndex,captured = extractDays(grpString)
                capturedString.extend(captured)
            except:

                traceback.print_exc()
                continue
            # print lstDays
            group.append({"days": lstDaysIndex, "time": group[2]})

        lstG = []
        # print 'lst_groups:',lst_groups
        # exit(0)
        for group in lst_groups:
            try:
                # print 'group:',group
                diCleaned = {}
                diCleaned["line"] = group[0]
                diCleaned["daytime"] = group[3]
                lstG.append(diCleaned)
            except:
                continue
        return lstG,capturedString

    # lstG = {line:'MON:9am-10pm', daytime: {'days': [MON], time:'9am-10pm'}}
    lines = line_string.splitlines()

    finalLst=[]
    capturedString = []

    for line in lines:
        lst, captured = func(line)
        finalLst.extend(lst)
        capturedString.extend(captured)
    if finalLst==[]:
        finalLst=[{'line': line_string, 'daytime': {'days': [], 'time': ''}}]
    return finalLst, capturedString

def captureTime(timeString):
    timeString=timeString.strip()
    diTime = {'startTime': "", 'endTime': ""}
    if not timeString:
        return diTime
    regex=re.compile(constants.regexTimeGroup,re.IGNORECASE)
    regex12hr=constants.regex12hr
    try:
        diTime['startTime'], diTime['endTime'] = re.search(regex, timeString).groups()
        if re.search(regex12hr,timeString):
            diTime['startTime'], diTime['endTime']='12:01am','11:59pm'
    except:
        print('\n*****************************error in capturing time from the string "{}"  *****************'.format(
            timeString))
        diTime['startTime'], diTime['endTime'] = 'error in capturing time','error in capturing time'
        pass
    return diTime

def getWorkHours(row, workHoursCol,diColMapping,workDayCol=None):
    inputWorkHours=row[workHoursCol].strip()
    inputWorkHours = standardiseWh(inputWorkHours)
    extracted,capturedString = extractDayTime(inputWorkHours)

    finalDi = {'workHours': {}}
    for dayTimeSet in extracted:
        dayTimeSet['daytime'].update(captureTime(dayTimeSet['daytime']['time'].strip()))

    regex=constants.regexTimePart
    empty_values = ['', '-']
    for idx, dayTimeSet in enumerate(extracted):
        for day in dayTimeSet['daytime']['days']:
            st=dayTimeSet['daytime']['startTime']
            et=dayTimeSet['daytime']['endTime']
            if (st and et):
                if len(re.findall(regex,st))==2 or len(re.findall(regex,et))==2:
                    finalDi['workHours'].update({day: (convertTimeFmt(str(st),'am-pm') , convertTimeFmt(str(et),'pm-pm'))})
                else:
                    finalDi['workHours'].update({day: (convertTimeFmt(str(st),'am') + '-' + convertTimeFmt(str(et),'pm'), '')})
            else:
                finalDi['workHours'].update({day: ('', '')})
            if not(finalDi['workHours'][day][0] in empty_values and finalDi['workHours'][day][1] in empty_values and not (st=='00' and et=='00')):
                capturedString.append(dayTimeSet['daytime']['time'])

    finalDi['line'] = row[workHoursCol]
    lstCapturedStrings = list(set(capturedString))
    flag = WHcapturedStringClarification(lstCapturedStrings,inputWorkHours)
    return str({"parsed": finalDi['workHours'], "clarification":flag})

def WHcapturedStringClarification(lstCapturedStrings, inputWorkHours):
    lstCapturedStrings = sorted(lstCapturedStrings, reverse=True, key=len)
    WhClrRegex = constants.regexWhClrString.format(constants.regexWhClrJoin.join([re.escape(i) for i in lstCapturedStrings]))
    inputWorkHours=re.sub(WhClrRegex,'',inputWorkHours,flags=re.IGNORECASE)

    return bool(re.search(r'[0-9a-z]+',inputWorkHours))


def getWorkHoursDayWise(row, day, index, drvWorkingHoursCol, drvSotWhCol):
    # print '**********came in getWorkHoursDayWise'
    # print '++++++++++++row,day,index,drvWorkingHoursCol,inpDayWH',row,day,index,drvWorkingHoursCol,inpDayWH
    try:
        if row[drvSotWhCol] and eval(row[drvSotWhCol])[day][index]:
            diWH = eval(row[drvSotWhCol])
            # print '*************row[inpDayWH] for {}'.format(inpDayWH),row[inpDayWH]
            # exit(0)
            return diWH[day][index]


        elif row[drvWorkingHoursCol]:
            diWH = eval(row[drvWorkingHoursCol])
            # print '*************diWH[day][index] for {}'.format(inpDayWH),diWH[day][index]
            # exit(0)
            return diWH[day][index]
    except:
        return ''

def getKey(wrd):
    for k,v in list(constants.diWeekDays.items()):
        if wrd.lower() in v['words']:
            return k

def getExtHrIndicator(row, diColmapping,_8to5Col):
    if mv.isErrorVal(row[_8to5Col]):
        return row[_8to5Col]
    lenBlnk=0
    for col in constants.whColsExtHrInd:
        if not row[col].strip() or row[col] == '0':
            lenBlnk+=1
    wkEvening=0
    wkndEvening=0
    weekend=0
    extHrInd=''

    regexTime = re.compile(constants.regexExtHrInd)
    for startTime in constants.lstStartTime:
        try:
            if re.search(regexTime, str(row[startTime])):
                lstMatches = re.findall(constants.regexExtHrInd, str(row[startTime]))
                if len(lstMatches) == 2:
                    # tString,fmt=determineTimeFmt(lstMatches[1])
                    # _24hrFmtTime=formatTime(tString,fmt)
                    # if _24hrFmtTime:
                    #     if float(_24hrFmtTime.replace(':', '.')) > 17:
                    #         wkEvening+=1
                    floatTime=float('.'.join(lstMatches[1].split(':')[0:2]))
                    if floatTime > 5 and floatTime<12:
                        wkEvening += 1
        except:
            continue
    for startTime in ['OUT_SAT_START_TIME', 'OUT_SUN_START_TIME']:
        try:
            if re.search(regexTime, str(row[startTime])):
                weekend+=1
                lstMatches = re.findall(constants.regexExtHrInd, str(row[startTime]))
                if len(lstMatches) == 2:
                    # tString, fmt = determineTimeFmt(lstMatches[1])
                    # _24hrFmtTime = formatTime(tString, fmt)
                    # if _24hrFmtTime:
                    #     if float(_24hrFmtTime.replace(':', '.')) > 17:
                    #         wkndEvening+=1
                    floatTime=float('.'.join(lstMatches[1].split(':')[0:2]))
                    if floatTime > 5 and floatTime<12:
                        wkndEvening += 1
        except:
            continue
    if lenBlnk==len(constants.whColsExtHrInd):
        return ''
    if weekend and wkEvening:
        extHrInd='B'
    elif not weekend and wkEvening:
        extHrInd='E'
    elif weekend and not wkEvening:
        extHrInd='W'
    elif not weekend and not wkEvening:
        extHrInd='N'
    # print '****extHrInd:', extHrInd
    return extHrInd

def extractDefaultWorkHours(colName):
    dtTup=('','')
    lst=re.findall('(?:default\s*to\s*)(.*?$)', colName, re.IGNORECASE)
    if lst:
        extracted,captured=extractDayTime(lst[0])
        if captured:
            dtTup=[(','.join(item['daytime']['days']), item['daytime']['time']) for item in extracted if captured[0] in item['line']][0]
        else:
            dtTup=[(','.join(item['daytime']['days']), item['daytime']['time']) for item in extracted if (item['daytime']['time'])][0]
    return dtTup

def combineSotDayWiseWorkHours(row,diSotWH):
    diCombined = {}
    empty_values = ['', '-']
    flagclr = False
    combinedString = ''
    regex = constants.regexTimePart
    for day,tup in list(diSotWH.items()):
        st = row[tup[0]].strip()
        et = row[tup[1]].strip()

        if st or et:
            combinedString = combinedString + day + ': ' + st + ' ' + et + ' '

        st = standardiseWh(st)
        et = standardiseWh(et)

        if re.search(constants.regex247, st, re.IGNORECASE):
            st = constants.time247

        if re.search(constants.regex247, et, re.IGNORECASE):
            et = constants.time247

        if st and not et:
                diCombined.update({day: (convertTimeFmt(str(st), 'am-pm'), "")})

        elif (st and et):
            if len(re.findall(regex,st))==2 or len(re.findall(regex,et))==2:
                diCombined.update({day: (convertTimeFmt(str(st),'am-pm') , convertTimeFmt(str(et),'pm-pm'))})
            else:
                diCombined.update({day: (convertTimeFmt(str(st),'am') + '-' + convertTimeFmt(str(et),'pm'), '')})
        else:
            diCombined.update({day: ('', '')})
        if flagclr == False:
            if diCombined[day][0] in empty_values and diCombined[day][1] in empty_values and not (st == '00' and et == '00') and not (st == '' and et == '') and not (st == '00-00' and et == '00-00'):
                flagclr = True  # True indicates clarification is required

    diCombined = {day:(tup[0],'') if tup[0]==tup[1] else tup for day, tup in diCombined.items()}
    return str({"parsed": diCombined, "clarification": flagclr, "combinedString":combinedString})

def getBreakHours(row, breakHoursCol,diColMapping):
    lst=[]
    for strr in row[breakHoursCol].split():
        if not re.search('[0-9]\s*am|[0-9]\s*pm|\bto\b|\bthru\b',strr,re.IGNORECASE) and re.search('^[a-zA-Z\-]+$',strr) and not re.search(constants.wd,strr,re.IGNORECASE):
            pass
        else:
            lst.append(strr)
    strr=' '.join(lst)
    row[breakHoursCol] = strr
    breakHours=getWorkHours(row, breakHoursCol,diColMapping)
    return str(breakHours)


def subtractBreakHours(row, timeRangeDiColName, breakHoursColName):
    officeHours = eval(row[timeRangeDiColName])
    breakHours = eval(row[breakHoursColName])
    for k, v in list(officeHours.items()):
        if breakHours.get(k) and v[0]:
            lst1 = officeHours[k][0].split('-')
            lst2 = breakHours[k][0].split('-')
            if len(lst1) == 2 and len(lst2) == 2:
                officeHours[k] = (lst1[0] + '-' + lst2[0], lst2[1] + '-' + lst1[1])
    return str(officeHours)

def convertTimeFmt(string,ampm=''):
    # if len(ampm.split('-'))==2:
        regex=constants.regexTimePart
        lst=re.findall(regex,string,re.IGNORECASE)
        # print "zip(lst,ampm.split('-')):",zip(lst,ampm.split('-'))
        lstConverted=[]
        for tString,ampm in zip(lst,ampm.split('-')):
            string,fmt=determineTimeFmt(tString,ampm)
            _24hrTString=formatTime(string,fmt)
            if _24hrTString:
                ts=datetime.datetime.strptime(_24hrTString,"%H:%M")
                lstConverted.append(ts.strftime("%I:%M%p"))
            else:
                lstConverted.append('')
        return '-'.join(lstConverted)

def determineTimeFmt(string,ampm='pm'):
    ampmRegex=constants.ampmRegex
    string=string.replace('.',':')
    lstTimeParts=re.split(r'[\:]',string)
    if len(lstTimeParts)==3:
        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I:%M:%S%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I:%M:%S%p'
                string = string + ampm
            else:
                fmt = '%H:%M:%S'
    elif len(lstTimeParts)==2:
        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I:%M%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I:%M%p'
                string=string+ampm
            else:
                fmt = '%H:%M'
    elif len(lstTimeParts) == 1:
        string_temp = lstTimeParts[0]

        if len(string_temp) > 2 and not re.search(ampmRegex,string_temp,re.IGNORECASE):
            if len(string_temp) % 2 != 0:
                string_temp = '0' + string_temp

            # string_temp = str(datetime.datetime.now().date()) + " " + string_temp
            # string_temp = str(parser.parse(string_temp).time())

            for i in range(2, len(string_temp), 2):
                string_temp = string_temp[:i] + ":" + string_temp[i:]

            string, fmt = determineTimeFmt(string_temp, ampm)
            return string, fmt

        if re.search(ampmRegex, string, re.IGNORECASE):
            fmt = '%I%p'
        else:
            if int(lstTimeParts[0]) <= 12:
                fmt = '%I%p'
                string=string+ampm
            else:
                fmt = '%H'
    return string,fmt

def formatTime(string,fmt):
    ampmRegex=constants.ampmRegex
    if not string or not re.search('[0-9]+',str(string)):
        return ''
    try:
        if re.search(ampmRegex,string,re.IGNORECASE):
            # print '**',re.sub('\s+','',string),fmt
            t=datetime.datetime.strptime(re.sub('\s+','',string), fmt)
            return t.strftime('%H:%M')
        elif not re.search(ampmRegex,string,re.IGNORECASE):
            # print '**', re.sub('\s+', '', string), fmt
            t = datetime.datetime.strptime(re.sub('\s+','',string), fmt)
            return t.strftime('%H:%M')
    except:
        return ''

def getLstClr(row, col,keyName):
    inp = eval(row[col])
    return (inp[keyName])

def reinstateStructForWH(row, col,keyName):
    inp = eval(row[col])
    return str(inp[keyName])

def determine24by7(inpStr):
    if re.search(constants.regex247, inpStr.strip()):
        return True

def updateExtracted(extracted, defaultWorkHours,defaultDays,inputWorkHours):
    if not inputWorkHours.strip() and defaultWorkHours:
        extracted = [{'line': inputWorkHours, 'daytime': {'days': defaultDays, 'time': defaultWorkHours}}]
        # print 'extracted1:', extracted
    elif inputWorkHours and len(extracted) == 0:
        extracted = [{'line': inputWorkHours, 'daytime': {'days': [], 'time': ''}}]
    if len(extracted) == 1 and extracted[0]['daytime']['time'].strip():
        if determine24by7(extracted[0]['daytime']['time']):
            extracted[0]['daytime']['time'] = constants.time247
            extracted[0]['daytime']['days'] = constants.allDays
    return extracted

def standardiseWh(inpStr):
    data=constants.standardsDict["workHour"]
    for time,lstWords in list(data.items()):
        regex = r'\b|\b'.join([re.escape(wrd) for wrd in lstWords])
        regex = r'\b' + regex + r'\b'
        inpStr = re.sub(regex, time, inpStr, flags=re.IGNORECASE)
    return inpStr

def segmentWh(inpStr):
    regex = constants.regexWdWhGroup
    groups=re.findall(regex, inpStr, re.IGNORECASE)
    return groups


def deriveInpWh(row,combinedWHclr,combinedWHString,whcol,wHclr):
    if row[combinedWHclr] and row[combinedWHString]:
        return row[combinedWHString]
    else:
        return row[whcol]


def standardizeWD(inp,regKeys):
    for regKey in regKeys:
        regDi=constants.regexDict[regKey]
        for k, v in list(regDi.items()):
            inp = re.sub('|'.join(v), k, inp, flags=re.IGNORECASE)
    return str(inp)

def cleanWorkDay(df,argDi):
    inpCol=argDi['inputCol']
    outCol=argDi['outputCol']
    regKeys=argDi['regKeys']
    def func(inpStr):
        inpStr=standardizeWD(inpStr,regKeys)
        return inpStr
    df[outCol]=df.apply(lambda row:func(row[inpCol]),axis=1)
    return df

def cleanWorkHours(df,argDi):
    inpCol = argDi['inputCol']
    outCol = argDi['outputCol']
    regKeys = argDi['regKeys']
    def func(inpStr):
        inpStr = standardizeWD(inpStr,regKeys)
        return inpStr

    df[outCol] = df.apply(lambda row: func(row[inpCol]), axis=1)
    return df

def deriveWdWh(df,argDi,row=None):
    wdCol = argDi['inputCol'][0]
    whCol = argDi['inputCol'][1]
    defWdCol = argDi['inputCol'][2]
    defWhCol = argDi['inputCol'][3]
    drvdThruCol=argDi['outputCol'][0]
    outCol=argDi['outputCol'][1]
    df[drvdThruCol]='wh'
    df[outCol]=df[whCol]
    def isDayPresentInWh(result):
        return (any([item['daytime']['days'] for item in result]) or any(
            [bool(re.search(r'\b[a-z]*\b', item['line'].replace(item['daytime']['time'], ''))) for item in result]))

    def func(row,wd,wh,defWd,defWh,drvdThruCol,outCol):
        try:
            extracted,__=extractDayTime(wh)
            dayInWh=isDayPresentInWh(extracted)
            if wd.strip() and not dayInWh and wh.strip():
                row[drvdThruCol],row[outCol]='wd-wh',wd+' '+wh
            elif wh.strip() and wd.strip() and dayInWh:
                row[drvdThruCol], row[outCol] = 'wh', wh
            elif wh.strip() and not wd.strip() and not dayInWh and defWh :
                row[drvdThruCol], row[outCol] = 'wh-defWd', defWd + ' ' + wh
            elif wd.strip() and not wh.strip() and defWh:
                row[drvdThruCol], row[outCol] = 'wd-defWh', wd + ' ' + defWh
            elif not wd.strip() and not wh.strip() and defWh and defWd:
                row[drvdThruCol], row[outCol] = 'defWd-defWh', defWd + ' ' + defWh
        except:
            pass
        return row
    df=df.apply(lambda row: func(row,row[wdCol],row[whCol],row[defWdCol],row[defWhCol],drvdThruCol,outCol),axis=1)
    return df

def storeDefaultWhWd(df,argDi):
    whCol=argDi['inputCol'][0]
    diColMappingCol=argDi['inputCol'][1]
    defWdCol=argDi['outputCol'][0]
    defWhCol=argDi['outputCol'][1]
    df[defWdCol]='mon-fri'
    df[defWhCol] = ''
    diColMapping=eval(df.iloc[0][diColMappingCol])
    outWhCols=[key for key in list(diColMapping.keys()) if whCol in key]
    if outWhCols:
        inpWhCol = diColMapping.get(outWhCols[0]).replace('\n',' ')
        dtTup = extractDefaultWorkHours(inpWhCol)
        df[defWdCol]=dtTup[0]
        df[defWhCol]=dtTup[1]
    return df

def getDiColMappingAdr(df,diColMapping):
    df['diColMapping_Adr']=str(diColMapping)
    return df

# def apply_8_to_5(row, inpCols,whclrFlagCol,whCol, combinedclrFlagCol, combinedString):
#     """determine if working hours are 8-5 for weekdays"""
#     if row[combinedString] and row[combinedclrFlagCol]:
#         return mv.ErrorMessage([], ["C", "could not parse working hours", row[combinedString]])
#     elif row[whCol] and row[whclrFlagCol]:
#         return mv.ErrorMessage([], ["C", "could not parse working hours", row[whCol]])
#     count = 0
#     inpCols = inpCols
#     for col in inpCols[0:]:
#         if row[col] == "08:00AM-05:00PM":
#             count += 1
#     if count == 5:
#         return "True"
#     else:
#         return "False"


# def apply_24_hours(row, inpCols,_8to5Col):
#     """determine if working hours are 24 hours for all days"""
#     if mv.isErrorVal(row[_8to5Col]):
#         return row[_8to5Col]
#     count = 0
#     inpCols = inpCols
#     for col in inpCols[0:]:
#         if row[col] == '12:01AM-11:59PM':
#             count += 1
#     if count == 7:
#         return "True"
#     else:
#         return "False"


def apply_24_hours(row, inpCols,whclrFlagCol,whCol, combinedclrFlagCol, combinedString):
    """determine if working hours are 24 hours for all days"""
    if row[combinedString] and row[combinedclrFlagCol]:
        return mv.ErrorMessage([], ["C", "could not parse working hours", row[combinedString]])
    elif row[whCol] and row[whclrFlagCol]:
        return mv.ErrorMessage([], ["C", "could not parse working hours", row[whCol]])
    count = 0
    for col in inpCols[0:]:
        if row[col] == '12:01AM-11:59PM':
            count += 1

    return "True" if count > 5 else "False"


def apply_8_to_5(row,_24hours):
    """determine if working hours are 8 to 5 for all days"""
    if mv.isErrorVal(row[_24hours]):
        return row[_24hours]
    return "True" if row[_24hours] == "False" else "False"


"""
{"name": "apply_24_hours", "type": "wh", "input": ["['OUT_MON_START_TIME', 'OUT_TUE_START_TIME', 'OUT_WED_START_TIME', 'OUT_THU_START_TIME','OUT_FRI_START_TIME', 'OUT_SAT_START_TIME', 'OUT_SUN_START_TIME']","wHclr","WORKING_HOURS","combinedWHclr","combinedWHString"], "col":"APPLY_24HOURS"},
{"name": "apply_8_to_5", "type": "wh", "input": ["APPLY_24HOURS"], 
"col":"APPLY_8TO5"},
"""



from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter, XMLConverter, HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import BytesIO,StringIO
import lxml.html
import re
from functools import cmp_to_key
import pandas as pd
import json
import os
import traceback
import pickle
import requests
import copy
import math

import constants
from utils import fetchProviderCondition
import altPdfTextExtraction as textExt
import importlib
cleanseFile = importlib.import_module('cleansePdf')

################ GETTING XML FROM PDF ################################################
converters={"xml":XMLConverter,"html":HTMLConverter,"text":TextConverter}
def convert_pdf_doc(pdf_path,output_type="xml"):
    # returns xml/html/text content of the PDF
    resource_manager = PDFResourceManager()
    fake_file_handle = BytesIO()
    converter = converters[output_type](resource_manager, fake_file_handle,laparams=LAParams())
    page_interpreter = PDFPageInterpreter(resource_manager, converter)
    with open(pdf_path, 'rb') as fh:
        for page in PDFPage.get_pages(fh,caching=constants.skipConv,check_extractable=True):
            page_interpreter.process_page(page)
        text = fake_file_handle.getvalue()
    # close open handles
    converter.close()
    fake_file_handle.close()
    if text:
        return text

###################### PARSING AND SORTING ###############################################
def parseForText(xml,pages=[],selKey='imgTxtLn2', sort=True, textFontSize=False):
    # parses xml to return page-wise content (pageDi), and coordinates of the entire page (pagebbox)
    tree = lxml.html.fromstring(xml)
    pageTg = findPgTag(tree)
    numOfPages=len(pageTg)
    pagebboxStr = pageTg[0].xpath("normalize-space(.//@bbox)")
    pagebbox = tuple([float(num) for num in pagebboxStr.split(',')])
    pageDi={}
    if pages:
        for pgNo in pages:
            parsed = parseTreeForText(tree,selKey=selKey,pageNo=pgNo,sort=sort, textFontSize=textFontSize)
            pageDi.update({pgNo:parsed})
    else:
        for pgNo in range(1,numOfPages+1):
            parsed = parseTreeForText(tree,selKey=selKey,pageNo=pgNo, sort=sort, textFontSize=textFontSize)
            pageDi.update({pgNo:parsed})
    return pagebbox, pageDi

def findPgTag(tree):
    return tree.xpath("//page")

def parseTreeForText(tree,selKey='imgTxtLn2',pageNo=1,sort=True, textFontSize=False):
    # calls queryXmlForText for getting text and its corresponding coordinates for a single page
    # sorts the contents as per bbox if sort=True
    diTags = queryXmlForText(tree, constants.diSel[selKey].format(int(pageNo)), textFontSize=textFontSize)
    if sort:
        # previous
        #srt = sorted(zip(diTags.keys(), diTags.values()), key=lambda tup: (-tup[0][1],tup[0][0]))
        # modified
        # sorting left to right
        srt = sorted(zip(list(diTags.keys()), list(diTags.values())), key=lambda tup: tup[0][0])
        # sort top bottom while considering a margin
        srt = sorted(srt, key=cmp_to_key(custom_compare))
        return srt
    return diTags

def custom_compare(item1, item2):
    # takes care of slight mismatch in y-coordinate using yMargin
    return -int(item1[0][1] - item2[0][1]-yMargin)

def queryXmlForText(tree,sel,textFontSize):
    # getting text and its corresponding coordinates for a single page
    wordXDiff = 4.5
    lstElm = tree.xpath(sel)
    diElm = {}
    for elm in lstElm:
        # getting coordinates of each word in textline
        lstTextElm = elm.xpath('.//text')
        textline = elm.xpath('.//text()')
        textline = [ele for ele in textline if ele != '\n']
        i = 0
        while i < len(textline):
            if textline[i]!= ' ':
                txt = ''
                initialTextbbox = lstTextElm[i].xpath("normalize-space(.//@bbox)").split(',')
                initialTextbbox = tuple([float(num) for num in initialTextbbox])
                prevX = initialTextbbox[2]
                while i < len(textline) and textline[i] != ' ' :    # extracts the whole word
                    bb = lstTextElm[i].xpath("normalize-space(.//@bbox)").split(',')
                    bb = tuple([float(num) for num in bb ])
                    # change in y
                    if abs(bb[1] - initialTextbbox[1]) > yMargin and abs(bb[3] - initialTextbbox[3]) > yMargin:
                        diElm.update(storebboxText(initialTextbbox, txt, textFontSize, lstTextElm[i-1]))
                        initialTextbbox = bb
                        txt = ''
                    # different words
                    if bb[0]-prevX>wordXDiff:
                        diElm.update(storebboxText(initialTextbbox, txt, textFontSize, lstTextElm[i-1]))
                        initialTextbbox = bb
                        txt = ''
                    prevX  = bb[2]
                    txt = txt+textline[i]
                    i += 1
                diElm.update(storebboxText(initialTextbbox, txt, textFontSize, lstTextElm[i-1]))
            else:
                i=i+1
    return diElm

def storebboxText(initialTextbbox,txt, textFontSize,lstTextElmi):
    # stores details for every word found
    finalTextbbox = lstTextElmi.xpath("normalize-space(.//@bbox)").split(',')
    finalTextbbox = tuple([float(num) for num in finalTextbbox])
    bbox = initialTextbbox[:2] + finalTextbbox[2:]
    if textFontSize:
        wordFont = lstTextElmi.xpath("normalize-space(.//@font)")
        wordSize = lstTextElmi.xpath("normalize-space(.//@size)")
        return {bbox: (txt, wordFont, wordSize)}
    else:
        return {bbox: (txt)}


#####################RECORD DELIMITER ######################################################

def getTextline(pageDi):
    # gives page-wise dict of all textlines contained in each page
    diPg = {}
    for pgNm, pageInfo in pageDi.items():
        diPg[pgNm] = []
        initBB, text = pageInfo[0] # taking first word of the textline
        for i in range(1,len(pageInfo)):
            # if y-coordinates fall under yMargin, then update text
            if abs(pageInfo[i][0][1] - initBB[1]) <= constants.yMargin and abs(pageInfo[i][0][3] - initBB[3]) <= constants.yMargin:
                text = text + ' ' + pageInfo[i][1]
            # else end of textline is attained, then re-initialize initBB, text
            else:
                finalBB = pageInfo[i-1][0]
                textlineBB = initBB[:2]+finalBB[2:]
                diPg[pgNm].append((textlineBB,text))
                initBB, text = pageInfo[i]
            # appending last element
            if i==len(pageInfo)-1:
                finalBB = pageInfo[i][0]
                textlineBB = initBB[:2] + finalBB[2:]
                diPg[pgNm].append((textlineBB, text))
    return diPg

def getHeaderBB(pagebbox, diPg, headerDetails):
    # getting header bounding box coordinates from page 1 using lineCount from pdfInfo json
    if headerDetails != "None":
        lineCount = headerDetails['lineCount']
        bottomLeft = diPg[1][lineCount-1][0]
        return (pagebbox[0], bottomLeft[1], pagebbox[2], pagebbox[3])
    else:
        return ()

def getFooterBB(pagebbox, diPg, footerDetails):
    # getting footer bounding box coordinates from page 1 using lineCount from pdfInfo json
    if footerDetails != "None":
        lineCount = footerDetails['lineCount']
        topRight = diPg[1][-lineCount][0]
        return (pagebbox[0], pagebbox[1], pagebbox[2], topRight[3])
    else:
        return()


# def getPdfSections(pagebbox, pageTextlineDi, json):
#     # returns header and footer details from PDF
#     headerBB = getHeaderBB(pagebbox, pageTextlineDi, json['header'])
#     footerBB = getFooterBB(pagebbox, pageTextlineDi, json['footer'])
#     return headerBB, footerBB

def getPdfSections(pagebbox, pageTextlineLst, json):
    if json['header'] != 'None':
        lineCount = json['header']['lineCount']
        bottomLeft = pageTextlineLst[lineCount-1][0]
        headerBB = (pagebbox[0], bottomLeft[1], pagebbox[2], pagebbox[3])
    else:
        headerBB = (0,pagebbox[3],pagebbox[2],pagebbox[3])
    if json['footer'] != 'None':
        lineCount = json['footer']['lineCount']
        topRight = pageTextlineLst[-lineCount][0]
        footerBB = (pagebbox[0], pagebbox[1], pagebbox[2], topRight[3])
    else:
        footerBB = (0,pagebbox[1],pagebbox[2],pagebbox[1])
    return headerBB, footerBB

def getRecords(pageDi, pageTextlineDi, json):
    # returns dictionary containing record-wise information
    recordDi = {}
    if json['recordType'] == 'single':
        recordDi, recordToPdfPg = getSingleRecord(pageDi)
    elif json['recordType'] == 'multiple':
        recordDi, recordToPdfPg = getMultipleRecords(pageDi,pageTextlineDi,json)
    return recordDi, recordToPdfPg

def getSingleRecord(pageDi):
    return {1:[{pg:pgInfo} for pg,pgInfo in pageDi.items()]}, {1:[pg for pg,pgInfo in pageDi.items()]}

def getMultipleRecords(pageDi,pageTextlineDi,json):
    recordDi = {}
    diExtractDetails = json['extractDetails']
    if json['ruleType'] == 'pageRule':
        recordDi, recordToPdfPg = recordByPage(pageDi, diExtractDetails['pageCount'])
    elif json['ruleType'] == 'textlineRule':
        if diExtractDetails['identifierTextCheck'] != 'None':
            recordDi, recordToPdfPg = recordByTextCheck(pageDi, pageTextlineDi, diExtractDetails)
        elif diExtractDetails['identifierLine'] != 0:
            recordDi, recordToPdfPg = recordByUniqueLine(pageDi, pageTextlineDi, diExtractDetails)
    return recordDi, recordToPdfPg

def recordByPage(pageDi, pgDelim):
    # getting recordDi using number of pages as delimiter
    totalRecords = len(pageDi)//pgDelim  #python3 - integer division to avoid TypeError
    page = 1
    recordDi = {}
    recordToPdfPg = {}
    for nm in range(1,totalRecords+1):
        recordDi[nm], recordToPdfPg[nm] = [], []
        for i in range(1,pgDelim+1):
            recordDi[nm].append({i:pageDi[page]})
            recordToPdfPg[nm].append(i)
            page += 1
    return recordDi, recordToPdfPg

# def recordByTextCheck(pageDi, pageTextlineDi, diExtractDetails):
#     # getting recordDi by checking textline in PDF against identifierTextCheck
#     if diExtractDetails['location'] == 'top':
#         identifierLineIdx = diExtractDetails['identifierLine'] - 1
#     else:
#         identifierLineIdx = -diExtractDetails['identifierLine']
#     identifierText = [textlines[identifierLineIdx][1] for pgNm, textlines in pageTextlineDi.iteritems()]
#     recordCount = 0
#     recordDi = {}
#     recordToPdfPg = {}
#     for ix, foundText in enumerate(identifierText):
#         if foundText == diExtractDetails['identifierTextCheck']:
#             recordCount += 1
#             recordGrp = recordToPdfPg.setdefault(recordCount, [])
#             recordDiGrp = recordDi.setdefault(recordCount,[])
#             pageCount = 1
#         else:
#             pageCount += 1
#         recordGrp.append(ix+1)
#         recordDiGrp.append({pageCount:pageDi[ix+1]})
#     return recordDi, recordToPdfPg

def recordByTextCheck(pageDi, pageTextlineDi, diExtractDetails):
    # getting recordDi by checking textline in PDF against identifierTextCheck
    recordCount = 0
    recordDi = {}
    recordToPdfPg = {}
    for pgNm, pageInfo in pageTextlineDi.items():
        textlines = [elm[1] for elm in pageInfo]
        if diExtractDetails['identifierTextCheck'] in textlines:
            recordCount += 1
            recordGrp = recordToPdfPg.setdefault(recordCount, [])
            recordDiGrp = recordDi.setdefault(recordCount,[])
            pageCount = 1
        else:
            pageCount += 1
        recordGrp.append(pgNm)
        recordDiGrp.append({pageCount:pageDi[pgNm]})
    return recordDi, recordToPdfPg


def recordByUniqueLine(pageDi, pageTextlineDi, diExtractDetails):
    # getting recordDi using index information (pdfInfo) of textline in PDF
    # identifierLineIdx : index of textline to be taken as a unique delimiter
    if diExtractDetails['location'] == 'top':
        identifierLineIdx = diExtractDetails['identifierLine'] - 1
    else:
        identifierLineIdx = -diExtractDetails['identifierLine']
    identifierText = [textlines[identifierLineIdx][1] for pgNm, textlines in pageTextlineDi.items()]
    recordCount = 0
    recordDi = {}
    recordToPdfPg = {}
    prevText = ''
    for ix, foundText in enumerate(identifierText):
        if foundText != prevText:
            recordCount += 1
            recordGrp = recordToPdfPg.setdefault(recordCount, [])
            recordDiGrp = recordDi.setdefault(recordCount, [])
            pageCount = 1
            prevText = foundText
        else:
            pageCount += 1
        recordGrp.append(ix + 1)
        recordDiGrp.append({pageCount: pageDi[ix + 1]})
    return recordDi, recordToPdfPg

def extractPdfInfo(pageDi, pagebbox, pdfInfoJson):
    # return pdfSections and dictionary containing record-wise information
    pageTextlineDi = getTextline(pageDi)
    # pdfSections['headerBB'], pdfSections['footerBB'] = getPdfSections(pagebbox, pageTextlineDi, pdfInfoJson['pdfSections'])
    # pagebbox = list(pagebbox)
    # if pdfSections['headerBB']:
    #     pagebbox[3] = pdfSections['headerBB'][1]
    # else:
    #     pdfSections['headerBB'] = (0,pagebbox[3],pagebbox[2],pagebbox[3])
    # if pdfSections['footerBB']:
    #     pagebbox[1] = pdfSections['footerBB'][3]
    # else:
    #     pdfSections['footerBB'] = (0,pagebbox[1],pagebbox[2],pagebbox[1])
    pdfSectionsDi = {}
    recordDi, recordToPdfPg = getRecords(pageDi, pageTextlineDi, pdfInfoJson['recordDelimitation'])
    pdfSectionJson = pdfInfoJson['pdfSections']
    for recNm, recLst in recordDi.items():
        pdfSectionsDi[recNm] = {}
        for ix, pgDi in enumerate(recLst):
            di = {}
            textline = pageTextlineDi[ recordToPdfPg[recNm][ix] ]
            di['headerBB'], di['footerBB'] = getPdfSections(pagebbox, textline, pdfSectionJson)
            pdfSectionsDi[recNm].update({ix+1:di})
    return pdfSectionsDi, recordDi, recordToPdfPg, pagebbox


############################### EXTRACTING ENTITIES ################################################

def parseBboxDetails(bboxDetails, bboxTuple, wordsTup, pagebbox):
    # parses parentbbox details to give page number and bbox coordinates of the record for found entity
    nonePosition = []
    positionDi = {0:'min_X', 1:'min_Y', 2:'max_X', 3:'max_Y'}
    di = {}
    if 'start' in bboxDetails and bboxDetails['start']['text']:
        occurences = getParseTextOccurences(wordsTup, bboxDetails['start']['text'][0], 'BOTTOM_LEFT')
        modifiedWordsTup = {}
        modifiedBboxTuple = {}
        if occurences:
            for pgNm, wlst in wordsTup.items():
                if pgNm==occurences[0][0]:
                    idx = occurences[0][1]
                    modifiedWordsTup[pgNm] = wlst[idx:]
                    modifiedBboxTuple[pgNm] = bboxTuple[pgNm][idx:]
                if pgNm>occurences[0][0]:
                    modifiedWordsTup[pgNm] = wlst
                    modifiedBboxTuple[pgNm] = bboxTuple[pgNm]
        wordsTup = modifiedWordsTup
        bboxTuple = modifiedBboxTuple
    bboxDetails = {k:v for k,v in bboxDetails.items() if k in ['min_X','max_X','min_Y','max_Y']}
    for position, element in bboxDetails.items():
        #occurences = []
        if element['text'] != 'None':
            for item in element['text']:
                occurences = getParseTextOccurences(wordsTup, item, element['point'])
                # wordsToSearch = item.split()
                # ln = len(wordsToSearch)
                # for pgNm, pageInfo in wordsTup.iteritems():
                #     for i in range(len(pageInfo) - ln + 1):
                #         if all(wordsToSearch[j] in pageInfo[i + j] for j in range(ln)):
                #             if element['point'] == 'TOP_RIGHT':
                #                 occurences.append((pgNm,i+ln-1))
                #             else:
                #                 occurences.append((pgNm,i))
                if occurences:
                    break
            if occurences:
                if element['occurence'] == 'last':
                    foundIdx = occurences[-1]
                else:
                    foundIdx = occurences[ element['occurence'] - 1 ]
                bbox = bboxTuple[foundIdx[0]][foundIdx[1]]
                coordinate = bbox[ getCoord(element['point'],position) ]
                di[position] = (foundIdx[0], coordinate)
            else:
                di[position] = ()
        else:
            di[position] = (-1, pagebbox[getCoord(element['point'], position)])
    foundFlag = all(True if v else False for k,v in di.items())
    resultLst = [di[positionDi[i]] if di.get(positionDi[i]) else () for i in range(0,4)]
    return foundFlag, resultLst

def getParseTextOccurences(wordsTup, item, point):
    occurences = []
    wordsToSearch = item.split()
    ln = len(wordsToSearch)
    for pgNm, pageInfo in wordsTup.items():
        for i in range(len(pageInfo) - ln + 1):
            if all(wordsToSearch[j] in pageInfo[i + j] for j in range(ln)):
                if point == 'TOP_RIGHT':
                    occurences.append((pgNm, i + ln - 1))
                else:
                    occurences.append((pgNm, i))
    return occurences


def getCoord(point, position):
    # gives apt index for bbox coordinates depending on point and position
    # point: TOP_RIGHT/BOTTOM_LEFT; position:X/Y (from min_X, max_X, min_Y, max_Y)
    tmp = [0,1] if point=='BOTTOM_LEFT' else [2,3]
    pos = tmp[0] if position.split('_')[1]=='X' else tmp[1]
    return pos


def getSearchRegion(foundFlag, searchRegionTup, pdfSections):
    # handles multiple page searchRegion
    # gives page-wise searchRegion
    if foundFlag:
        pageLst, searchRegion = list(zip(*searchRegionTup))
        pageLst = [pg for pg in pageLst if pg!=-1]
        firstPage = min(pageLst)
        if all(x == firstPage for x in pageLst) and searchRegion[1]<searchRegion[3]: # single page
            return {firstPage:searchRegion}
        else : # multiple pages
            if searchRegionTup[1][0] != searchRegionTup[3][0] and searchRegionTup[1][0]!=-1 and searchRegionTup[3][0]!=-1:
                # max_Y on one page, min_Y on another page
                outSearchRegions = {}
                betweenDi = {}
                firstPgSearchRegion = (searchRegion[0], pdfSections[firstPage]['footerBB'][3], searchRegion[2], searchRegion[3])
                firstPageDi = {firstPage:firstPgSearchRegion}
                lastPage = max(pageLst)  # last page of searchRegion
                lastPgSearchRegion = (searchRegion[0], searchRegion[1], searchRegion[2], pdfSections[lastPage]['headerBB'][1])
                lastPageDi = {lastPage:lastPgSearchRegion}
                if lastPage - firstPage != 1:
                    #region = (searchRegion[0], pdfSections['footerBB'][3], searchRegion[2], pdfSections['headerBB'][1])
                    betweenDi = {pg: (searchRegion[0], pdfSections[pg]['footerBB'][3], searchRegion[2], pdfSections[pg]['headerBB'][1]) for pg in range(firstPage+1, lastPage)}
                for di in (firstPageDi, betweenDi, lastPageDi):
                    outSearchRegions.update(di)
                return outSearchRegions
            else:
                if searchRegionTup[1][1] > searchRegionTup[3][1]:  # max_Y:None and max_Y lies on previous page
                    prevPg = searchRegionTup[1][0] - 1
                    prevPgSearchRegion = (searchRegion[0], pdfSections[prevPg]['footerBB'][3], searchRegion[2], searchRegion[3])
                    currPg = searchRegionTup[1][0]
                    currPgSearchRegion = (searchRegion[0], searchRegion[1], searchRegion[2], pdfSections[currPg]['headerBB'][1])
                    return {prevPg:prevPgSearchRegion, currPg: currPgSearchRegion}
                elif searchRegionTup[1][0]!=-1:
                    return {searchRegionTup[1][0]: searchRegion}
                elif searchRegionTup[3][0]!=-1:
                    return {searchRegionTup[3][0]: searchRegion}
    else:
        return {}


def getTextInSearchRegion(searchRegionDi, bboxTuple, wordTuple):
    # gives text found in the searchRegion
    foundText = ''
    for pgNm, searchRegion in searchRegionDi.items():
        bboxIdxLst = getbboxLst(bboxTuple, searchRegion, pgNm)
        foundText = foundText + ' ' + ' '.join(wordTuple[pgNm][idx] for idx in bboxIdxLst)
    return foundText.strip()

def getTextlineInSearchRegion(searchRegionDi, bboxTuple, wordTuple):
    # gets list of textline found in the searchRegion
    textline = []
    for pgNm, searchRegion in searchRegionDi.items():
        bboxIdxLst = getbboxLst(bboxTuple, searchRegion, pgNm)
        bboxLst = [bbox for ix,bbox in enumerate(bboxTuple[pgNm]) if ix in bboxIdxLst]
        wordLst = [word for ix,word in enumerate(wordTuple[pgNm]) if ix in bboxIdxLst]
        textline += getTextlineFromBbox(bboxLst, wordLst)
    return textline

def getElementInSearchRegion(searchRegionDi, bboxTuple, wordTuple):
    # gives new bboxTuple and wordTuple found in the searchRegion
    newbbox, newWord = {},{}
    for pgNm, searchRegion in searchRegionDi.items():
        bboxIdxLst = getbboxLst(bboxTuple, searchRegion, pgNm)
        newbbox[pgNm] = [bbox for ix, bbox in enumerate(bboxTuple[pgNm]) if ix in bboxIdxLst]
        newWord[pgNm] = [word for ix, word in enumerate(wordTuple[pgNm]) if ix in bboxIdxLst]
    newbbox = {pgNm: lst for pgNm, lst in newbbox.items() if newbbox[pgNm]}
    newWord = {pgNm: lst for pgNm, lst in newWord.items() if newWord[pgNm]}
    return newbbox, newWord

def getTextlineFromBbox(bboxLst, wordLst):
    # gives list of textlines depending upon bboxLst and wordLst found in searchRegion
    textlineLst = []
    if len(bboxLst)>1 :
        initBB, text = bboxLst[0], wordLst[0]
        for i in range(1, len(bboxLst)):
            # if y-coordinates fall under yMargin, then update text
            if abs(bboxLst[i][1] - initBB[1]) <= constants.yMargin and abs(bboxLst[i][3] - initBB[3]) <= constants.yMargin:
                text = text + ' ' + wordLst[i]
            # else end of textline is attained, then store textline and re-initialize initBB, text
            else:
                textlineLst.append(text)
                initBB, text = bboxLst[i], wordLst[i]
            # appending last element
            if i == len(bboxLst) - 1:
                textlineLst.append(text)
    else:
        textlineLst = wordLst
    return textlineLst

def getbboxLst(bboxTuple, searchRegion, page):
    # give list of bboxTuple indexes for the bboxes found in searchRegion
    # xMargin, yMargin included to take care of slight overlapping with the searchRegion

    # bboxDi[entity] = [{'idx':bboxIdx, 'bbox':bbox} for bboxIdx, bbox in enumerate(bboxTuple) if searchRegion[0]<=bbox[0] and searchRegion[2]>=bbox[0]  and searchRegion[1]<=bbox[1] and searchRegion[3]>=bbox[1]  and searchRegion[0]<=bbox[2] and searchRegion[2]>=bbox[2]  and searchRegion[1]<=bbox[3] and searchRegion[3]>=bbox[3]]
    # bboxIdxLst = [bboxIdx for bboxIdx, bbox in enumerate(bboxTuple[page]) if searchRegion[0]<=bbox[0] and searchRegion[2]>=bbox[0]  and searchRegion[1]<=bbox[1] and searchRegion[3]>=bbox[1]  and searchRegion[0]<=bbox[2] and searchRegion[2]>=bbox[2]  and searchRegion[1]<=bbox[3] and searchRegion[3]>=bbox[3]]
    bboxIdxLst = [bboxIdx for bboxIdx, bbox in enumerate(bboxTuple[page]) if
                  searchRegion[0] - bbox[0] <= constants.xMargin and searchRegion[2] >= bbox[0]
                  and searchRegion[1] - bbox[1] <= constants.yMargin and searchRegion[3] >= bbox[1]
                  and searchRegion[0] <= bbox[2] and bbox[2] - searchRegion[2] <= constants.xMargin
                  and searchRegion[1] <= bbox[3] and bbox[3] - searchRegion[3] <= constants.yMargin]
    return bboxIdxLst

######################## TABLE EXTRACTION #######################################################

def tableExtractionWrapper(token, pagebbox, recordDi, pdfSectionsDi, origPagebbox, origRecordDi, origPdfSectionsDi, recordToPdfPg, allJsons):
    outTableDi = {}
    if allJsons.get('tableInfo'):
        print('extracting tables ...')
        tableInfoJson = allJsons['tableInfo']
        type1TableInfoJson, type2TableInfoJson = {}, {}
        for cat, catLst in tableInfoJson.items():
            table1 = []
            table2 = []
            for diCat in catLst:
                if diCat['type'] == 'table1':
                    table1.append(diCat)
                else:
                    table2.append(diCat)
            if table1:
                type1TableInfoJson.update({cat:table1})
            if table2:
                type2TableInfoJson.update({cat:table2})
        if allJsons.get('cleanse'):
            cleanseJson = allJsons['cleanse']
        else:
            cleanseJson = {}
        outTableDi.update(processTableType1(token, origPagebbox, origRecordDi, origPdfSectionsDi, recordToPdfPg, type1TableInfoJson))
        type2Res = processTableType2(pagebbox, recordDi, pdfSectionsDi, type2TableInfoJson)
        for recordNm, recordDiDf in type2Res.items():
            if not outTableDi or recordNm not in outTableDi:
                outTableDi.update({recordNm:{}})
            for cat, catDi in recordDiDf.items():
                if cat in outTableDi[recordNm]:
                    outTableDi[recordNm][cat].update(catDi)
                else:
                    outTableDi[recordNm].update(recordDiDf)
        outTableDi = applyCleanTableDfVariables(outTableDi, cleanseJson)
    return outTableDi

############################## Processing Tables Type 2 #############################################

def processTableType2(pagebbox, recordDi, pdfSectionsDi, tableInfoJson):
    outTableDi = {}
    if tableInfoJson:
        for recordNm, record in recordDi.items():
            wordsTup, bboxTuple = {}, {}
            for di in record:
                for pgNm, diPg in di.items():
                    bboxTuple[pgNm], wordsTup[pgNm] = list(zip(*diPg))
            res = {}
            pdfSections = pdfSectionsDi[recordNm]
            for cat, tableDetailsLst in tableInfoJson.items():
                res[cat] = {}
                for tableDetails in tableDetailsLst:
                    foundFlag, searchRegionTup = parseBboxDetails(tableDetails['parentBB'], bboxTuple, wordsTup, pagebbox)
                    searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                    df = getTableType2(searchRegionDi,bboxTuple,wordsTup, tableDetails['xMargin'])
                    res[cat][tableDetails['tableId']] =  df
            outTableDi[recordNm] = res
        outTableDi = cleanHeaders(outTableDi, tableInfoJson)
    return outTableDi


def getTableType2(searchRegionDi, bboxTuple, wordTuple, tableXMargin):
    # gets list of textline found in the searchRegion and use it to get table as a dataframe
    textline = []
    for pgNm, searchRegion in searchRegionDi.items():
        bboxIdxLst = getbboxLst(bboxTuple, searchRegion, pgNm)
        bboxLst = [bbox for ix,bbox in enumerate(bboxTuple[pgNm]) if ix in bboxIdxLst]
        wordLst = [word for ix,word in enumerate(wordTuple[pgNm]) if ix in bboxIdxLst]
        textline += getTableTextlineFromBbox(bboxLst, wordLst, tableXMargin)
    tableDf = formatTableDfType2(textline)
    tableDf = tableDf.reset_index(drop=True)
    return tableDf

def getTableTextlineFromBbox(bboxLst, wordLst, tableXMargin):
    # gives list of textlines from bboxLst and wordLst found in searchRegion depending upon change in y and x
    textlineLst = []
    if bboxLst and len(bboxLst) > 1 :
        t = []
        inittextlineBB, initBB, text = bboxLst[0], bboxLst[0], wordLst[0]
        prevTR = initBB[2:]
        for i in range(1, len(bboxLst)):
            # if y-coordinates fall under yMargin, then update text
            if abs(bboxLst[i][1] - initBB[1]) <= constants.yMargin and abs(bboxLst[i][3] - initBB[3]) <= constants.yMargin:
                if bboxLst[i][0]- prevTR[0] > tableXMargin :  # change in x-coordinate between words
                    t.append((text, initBB[:2]+prevTR))
                    initBB, text = bboxLst[i], wordLst[i]
                else:
                    text = text + ' ' + wordLst[i]
                prevTR = bboxLst[i][2:]
            # else end of textline is attained, then re-initialize initBB, text
            else:
                t.append((text, initBB[:2]+prevTR))
                textlineLst.append(t)
                t = []
                inittextlineBB, initBB, text = bboxLst[i], bboxLst[i], wordLst[i]
                prevTR = bboxLst[i][2:]
            # appending last element
            if i == len(bboxLst) - 1:
                t.append((text, initBB[:2]+prevTR))
                textlineLst.append(t)
    elif wordLst and bboxLst:
        textlineLst = [[(wordLst[0],bboxLst[0])]]
    return textlineLst

def formatTableDfType2(textlineLst):
    # determines columns of the table
    table = []
    tableDf = pd.DataFrame()
    if textlineLst:
        maxCols = max([len(row) for row in textlineLst])
        refRow = [row for row in textlineLst if len(row)==maxCols][0]
        for row in textlineLst:
            if len(row) != maxCols:
                newVal = ['']*maxCols
                for ix, col in enumerate(refRow):
                    for val in row:
                        if (val[1][0]<=col[1][0] and val[1][2]>=col[1][0]) or (col[1][0]<=val[1][0] and col[1][2]>=val[1][0]) :
                            newVal[ix] = val[0]
                            break
                table.append(newVal)
            else:
                table.append([val[0] for val in row])
        columnIndex = [i+1 for i in range(maxCols)]
        tableDf = pd.DataFrame(columns=columnIndex, data=table)
    return tableDf

# def cleanTable(tableDf, cleanDi):
#     # merges or filters the extra rows identified
#     col = cleanDi['columnIndex']
#     if cleanDi['cleanType'] == 'filter':
#         tableDf = tableDf[tableDf[col]!=''].reset_index(drop=True)
#     elif cleanDi['cleanType'] == 'merge':
#         tableDf[col] = tableDf[col].replace(r'^$', np.nan, regex=True).ffill()
#         tableDf = tableDf.groupby(cleanDi['columnIndex'], as_index=False).agg(lambda val:sum(str(val))).reset_index(drop=True)
#     return tableDf

############################ Processing tables type 1 #####################################################

def processTableType1(token, pagebbox, recordDi, pdfSectionsDi, recordToPdfPg, tableInfoJson):
    outTableDi = {}
    if tableInfoJson:
        filePath = '../tmp/'+token+'.pkl'
        diPages = {}
        diTblIn = getTableInput(pagebbox, recordDi, pdfSectionsDi, tableInfoJson, recordToPdfPg)
        for pg, inputLst in diTblIn.items():
            diPages[pg] = [elm[2] for elm in inputLst]
        # updating pkl file with diPages input for table extraction
        with open(filePath,'rb') as f:
            data = pickle.load(f)
        data['diPagesPdfBbx'] = diPages
        with open(filePath, 'wb') as f:
            pickle.dump(data,f)
        # API call
        param = {'token':token, 'pklDir':constants.pklDir, 'pdfDir':constants.pdfDir}
        response = requests.get(constants.tableExtractionUrl, params=param)
        # reading table extraction output dict
        with open('../tmp/'+token+'.pkl','rb') as f:
            data = pickle.load(f)
        diTbl = data['diTbl']
        outTableDi = getRecordTableOutput(diTbl,diTblIn, recordToPdfPg, tableInfoJson)
    return outTableDi

def getTableInput(pagebbox, recordDi, pdfSectionsDi, tableJson, recordToPdfPg):
    # gets table search region for each record with original pdf pgnm
    outDi = {}
    for recordNm, record in recordDi.items():
        wordsTup, bboxTuple = {}, {}
        for ix, diPg in enumerate(record):
            bboxTuple[ix + 1], wordsTup[ix+1] = list(zip(*diPg[ix+1]))
        pdfSections = pdfSectionsDi[recordNm]
        for cat, diLst in tableJson.items():
            for bboxDetails in diLst:
                foundFlag, searchRegionTup = parseBboxDetails(bboxDetails['parentBB'], bboxTuple, wordsTup, pagebbox)
                searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                tablebbox, tableWords = getElementInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
                modifiedSearchRegion = {}
                tableId = bboxDetails['tableId']
                # taking table searchRegion coordinates as per words inside the found region
                for pgNm, bboxLst in tablebbox.items():
                    modifiedSearchRegion[pgNm] = getSRPagebbox({pgNm:bboxLst})
                for key, value in modifiedSearchRegion.items():
                    res= (cat, tableId, value)
                    # replacing record pgNm by pdf pgNm and
                    grp = outDi.setdefault(recordToPdfPg[recordNm][key - 1], [])
                    grp.append(res)
    return outDi

def getRecordTableOutput(diTbl,diTblIn, recordToPdfPg, tableInfoJson):
    # formats table output from table extraction output
    diTblOut = {}
    for pg, input in diTblIn.items():
        recordNm = [k for k, v in recordToPdfPg.items() if pg in v][0]
        grp = diTblOut.setdefault(recordNm, {})
        for ix,elm in enumerate(input):
            catGrp = grp.setdefault(elm[0],{})
            df = tableXmltoText(diTbl[pg][ix][1])   # converting xml to text
            dfCols = list(df.columns)
            dfDataLst = df.values.tolist()
            dfDataLst.insert(0, list(df.columns.values))
            columnIndex = [i+1 for i in range(len(dfCols))]     # table columnName: 1,2,...
            finalDf = pd.DataFrame(columns=columnIndex, data=dfDataLst)
            if elm[0] in list(grp.keys()):     # handling tables that spill from one page to another
                if elm[1] in grp[elm[0]]:
                    oldDf = grp[elm[0]][elm[1]]
                    finalDf = pd.concat([oldDf, finalDf], ignore_index=True)
            catGrp.update({elm[1]:finalDf})
    diTblOut = cleanHeaders(diTblOut, tableInfoJson)
    return diTblOut

def xmlToText(x):
    # converts the xml content to plain text
    text = ''
    cellVal=eval(x)
    xmlData = cellVal['data']
    if xmlData:
        tr = lxml.html.fromstring(xmlData)
        t = [] #sorting xml content
        for elm in tr.xpath('.//text'):
            bbox = list(map(float,elm.xpath('string(.//@bbox)').split(',')))
            t.append((bbox[0], bbox[1], elm))
        t = sorted(t, key=lambda tup:(tup[0], tup[1]))
        text = ''.join([ele[2].text for ele in t])
    return text

def tableXmltoText(pdfTableDF):
    # converts xml content of dataframe to plain text
    textTableDF = pdfTableDF.applymap(xmlToText)
    return textTableDF

################################################################################################
def cleanHeaders(diTblOut, tableInfoJson):
    # removes headers from extracted tables using tableHeader information from tableInfo.json
    #tableHeaderDi = {cat: di['tableHeader'] for cat, di in tableInfoJson.iteritems()}
    tableHeaderDi = {}
    for cat, tableDetailsLst in tableInfoJson.items():
        tableHeaderDi[cat] = {}
        for tableDetails in tableDetailsLst:
            tableHeaderDi[cat][tableDetails['tableId']] = tableDetails['tableHeader']
    for recordNm, diRecTbl in diTblOut.items():
        for cat, diDf in diRecTbl.items():
            for tableId, df in diDf.items():
                if len(df)!=0 and tableHeaderDi[cat][tableId] != "None":
                    diTblOut[recordNm][cat][tableId].drop_duplicates(inplace=True)  # removes duplicate rows of header, if present
                    diTblOut[recordNm][cat][tableId].drop(diTblOut[recordNm][cat][tableId].index[:tableHeaderDi[cat][tableId]], inplace=True)
                diTblOut[recordNm][cat][tableId] = diTblOut[recordNm][cat][tableId].reset_index(drop=True)
    return diTblOut

def applyCleanTableDfVariables(tableDiDf, cleanseJson):
    if cleanseJson:
        layerDi = [di for di in cleanseJson if di['LayerName'] == 'CleanTableDataframe']
        if layerDi:
            for catDi in layerDi[0]['Transformation']:
                for cat, derivationLst in catDi.items():
                    for derivation in derivationLst:
                        funcNm = derivation['name']
                        argDi = derivation['input']
                        for recordNm, recordDiDf in tableDiDf.items():
                            for tableId, df in tableDiDf[recordNm][cat].items():
                                if len(df) != 0:   # checking if dataframe is not empty
                                    try:
                                        tableDiDf[recordNm][cat][tableId] = getattr(cleanseFile, funcNm)(tableDiDf[recordNm][cat][tableId], argDi)
                                    except:
                                        traceback.print_exc()
                                        continue
    return tableDiDf

################################# LOOP EXTRACTION #############################################

def loopExtractionWrapper(pagebbox, recordDi, pdfSectionsDi, allJsons):
    outLoopDi = {}
    if allJsons.get('loopInfo'):
        print('extracting loop details ...')
        loopInfoJson = allJsons['loopInfo']
        for recordNm, record in recordDi.items():
            wordsTup, bboxTuple = {}, {}
            for di in record:
                for pg, diPg in di.items():
                    bboxTuple[pg], wordsTup[pg] = list(zip(*diPg))
            pdfSections = pdfSectionsDi[recordNm]
            recordResult = {}
            for cat, catLst in loopInfoJson.items():
                recordResult[cat] = {}
                for loopDetails in catLst:
                    recordResult[cat][loopDetails['loopId']] = getLoopBlockDetails(loopDetails, bboxTuple, wordsTup, pagebbox, pdfSections)
            outLoopDi[recordNm] = recordResult
    return outLoopDi


def getLoopBlockDetails(loopDetails, bboxTuple, wordsTup, pagebbox, pdfSections):
    # getting parent search region
    # result format : (blockBboxTuple, blockWordTuple, modifiedPagebbox)
    res = []
    foundFlag, parentSearchRegionTup = parseBboxDetails(loopDetails['parentBB'], bboxTuple, wordsTup, pagebbox)
    parentSearchRegionDi = getSearchRegion(foundFlag, parentSearchRegionTup, pdfSections)
    delimiter = loopDetails['delimiter']
    direction = loopDetails['direction']
    parentBboxTuple, parentWordTuple = getElementInSearchRegion(parentSearchRegionDi, bboxTuple, wordsTup)
    parentBBWords = getTextInSearchRegion(parentSearchRegionDi, bboxTuple, wordsTup)
    delimiterLst = getDelimLst(delimiter, parentBBWords)
    blockSearchRegionTupLst = getBlockSearchRegionTup(direction, delimiter, delimiterLst, parentBboxTuple, parentWordTuple, parentSearchRegionTup, pagebbox)
    for blockSearchRegionTup in blockSearchRegionTupLst:
        blocksearchRegionDi = getSearchRegion(foundFlag, blockSearchRegionTup, pdfSections)
        # removing page:region from blocksearchRegionDi which doesn't contain any words
        blocksearchRegionDi = {pg: region for pg, region in blocksearchRegionDi.items() if pg in list(parentBboxTuple.keys())}
        blockBboxTuple, blockWordTup = getElementInSearchRegion(blocksearchRegionDi, parentBboxTuple, parentWordTuple)
        # modifying pagebbox
        modifiedPagebbox = getSRPagebbox(blockBboxTuple)
        res.append( (blockBboxTuple, blockWordTup, modifiedPagebbox) )
    return res

def getDelimLst(delimiter, parentBBWords):
    delimiterWords = '|'.join([re.escape(v) for k, lst in delimiter.items() if k != 'type' for v in lst])  # forming regex
    # getting list of all delimiters found in completeParentBB
    delimiterLst = re.findall(delimiterWords, parentBBWords, re.IGNORECASE)
    modifiedDelimLst = []
    if delimiterLst and delimiter.get('2'):
        if delimiter['type'] == 'blockEnd':
            for i in range(0, len(delimiterLst)-1):  # blockend
                if not (delimiterLst[i] in delimiter["1"] and delimiterLst[i + 1] in delimiter["2"]):
                    modifiedDelimLst.append(delimiterLst[i])
            modifiedDelimLst.append(delimiterLst[-1]) # appending the last element in any case
        elif delimiter['type'] == 'blockStart':
            modifiedDelimLst.append(delimiterLst[0])
            for i in range(1, len(delimiterLst)):
                if not (delimiterLst[i] in delimiter["1"] and delimiterLst[i - 1] in delimiter["2"]):
                    modifiedDelimLst.append(delimiterLst[i])
    else:
        modifiedDelimLst = delimiterLst
    return modifiedDelimLst

def computeDelimOccurence(delimiter, delimiterLst, ix):
    count = delimiterLst[:ix].count(delimiterLst[ix]) + 1
    if delimiter.get('2'):
        if delimiterLst[ix] in delimiter['1']:
            count = ix + 1
    return count

def getBlockSearchRegionTup(direction, delimiter, delimiterLst, parentBboxTuple, parentWordTuple, parentSearchRegionTup, pagebbox):
    # extracting each block inside loop (from the entire looping region)
    blocksearchRegionTupLst = []
    for i in range(0, len(delimiterLst)):
        blockDetails = {}
        if direction == 'X' and delimiter['type'] == 'blockEnd':
            if i + 1 < len(delimiterLst):
                blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
                blockDetails['max_X'] = {'text': [delimiterLst[i + 1]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i + 1)}
            else:
                blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
            if i + 1 == len(delimiterLst):
                blocksearchRegionTup[2] = parentSearchRegionTup[2]
            blocksearchRegionTup[1] = parentSearchRegionTup[1]
            blocksearchRegionTup[3] = parentSearchRegionTup[3]
        elif direction == 'X' and delimiter['type'] == 'blockStart':
            if i==0:
                blockDetails['max_X'] = {'text': [delimiterLst[i]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            else:
                blockDetails['min_X'] = {'text': [delimiterLst[i-1]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i-1)}
                blockDetails['max_X'] = {'text': [delimiterLst[i]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
            if i == 0:
                blocksearchRegionTup[0] = parentSearchRegionTup[0]
            blocksearchRegionTup[1] = parentSearchRegionTup[1]
            blocksearchRegionTup[3] = parentSearchRegionTup[3]

        elif direction == 'Y' and delimiter['type'] == 'blockEnd':
            if i == 0:
                blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            else:
                blockDetails['max_Y'] = {'text': [delimiterLst[i - 1]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i-1)}
                blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
            if i == 0:
                blocksearchRegionTup[3] = parentSearchRegionTup[3]
            blocksearchRegionTup[0] = parentSearchRegionTup[0]
            blocksearchRegionTup[2] = parentSearchRegionTup[2]
        elif direction == 'Y' and delimiter['type'] == 'blockStart':
            if i + 1 < len(delimiterLst):
                blockDetails['max_Y'] = {'text': [delimiterLst[i]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
                blockDetails['min_Y'] = {'text': [delimiterLst[i+1]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i+1)}
            else:
                blockDetails['max_Y'] = {'text': [delimiterLst[i]], 'point': 'TOP_RIGHT', 'occurence': computeDelimOccurence(delimiter, delimiterLst, i)}
            foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
            if i + 1 == len(delimiterLst):
                blocksearchRegionTup[1] = parentSearchRegionTup[1]
            blocksearchRegionTup[0] = parentSearchRegionTup[0]
            blocksearchRegionTup[2] = parentSearchRegionTup[2]

        blocksearchRegionTupLst.append(blocksearchRegionTup)
    return blocksearchRegionTupLst


# def getBlockSearchRegionTup(direction, delimiterLst, parentBboxTuple, parentWordTuple, parentSearchRegionTup, pagebbox):
#     # extracting each block inside loop (from the entire looping region)
#     blocksearchRegionTupLst = []
#     for i in range(0, len(delimiterLst)):
#         blockDetails = {}
#         if direction == 'X':
#             if i + 1 < len(delimiterLst):
#                 blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
#                 blockDetails['max_X'] = {'text': [delimiterLst[i + 1]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i + 1].count(delimiterLst[i + 1]) + 1}
#             else:
#                 blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
#             foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
#             if i + 1 == len(delimiterLst):
#                 blocksearchRegionTup[2] = parentSearchRegionTup[2]
#             blocksearchRegionTup[1] = parentSearchRegionTup[1]
#             blocksearchRegionTup[3] = parentSearchRegionTup[3]
#
#         elif direction == 'Y':
#             if i == 0:
#                 blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
#             else:
#                 blockDetails['max_Y'] = {'text': [delimiterLst[i - 1]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i - 1].count(delimiterLst[i - 1]) + 1}
#                 blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
#             foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, parentBboxTuple, parentWordTuple, pagebbox)
#             if i == 0:
#                 blocksearchRegionTup[3] = parentSearchRegionTup[3]
#             blocksearchRegionTup[0] = parentSearchRegionTup[0]
#             blocksearchRegionTup[2] = parentSearchRegionTup[2]
#
#         blocksearchRegionTupLst.append(blocksearchRegionTup)
#     return blocksearchRegionTupLst





###############################################################################################

############################## EXTRACTION FROM PDF ############################################
def singleValueExtraction(pagebbox, bboxTuple, wordsTup, svJson, pdfSections, setMarginFlag = False):
    # wrapper function to extract singleValue entities from the record using singleValue json
    out = {}
    for entity, detailDi in svJson.items():
        if detailDi.get('multipleMap',False) == 'True':  # multiple mappings possiblities
            for eachDetailDi in detailDi['map']:
                foundFlag, searchRegionTup = parseBboxDetails(eachDetailDi, bboxTuple, wordsTup, pagebbox)
                searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                if setMarginFlag:
                    out[entity] = searchRegionDi
                else:
                    out[entity] = getTextInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
                if out[entity]!='':
                    break
        else:
            foundFlag, searchRegionTup = parseBboxDetails(detailDi, bboxTuple, wordsTup, pagebbox)
            searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
            if setMarginFlag:
                out[entity] = searchRegionDi
            else:
                out[entity] = getTextInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
    return out


def mvLoopX(colStr, di, bboxTuple, wordsTup, pagebbox, pdfSections):
    # loops in X direction to get all the values of entity
    out = {}
    bboxDetails = {}
    # extracting the entire looping region
    foundFlag, completeParentSearchRegionTup = parseBboxDetails(di['Location']['parentBB'], bboxTuple, wordsTup, pagebbox)
    completeParentSearchRegionDi = getSearchRegion(foundFlag, completeParentSearchRegionTup, pdfSections)
    newBboxTuple, newWordTuple = getElementInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
    completeParentBBwords = getTextInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
    # getting list of exact delimiter words found in the entire searchRegion
    #countDelimiter = [word for word in parentBBwords if di['Location']['delimiter'] in word]
    delimiter = di['Location']['loopDetails']['delimiter']
    start = di['Location']['loopDetails']['start']
    skip = di['Location']['loopDetails']['skip']
    # getting list of all delimiters found in completeParentBB
    if isinstance(delimiter, list):
        delimiter = '|'.join([elm for elm in delimiter])  # forming regex
    delimiterLst = re.findall(delimiter, completeParentBBwords, re.IGNORECASE)
    for i in range(0, len(delimiterLst)):
        col = colStr + str(i)
        if start != 'None':
            bboxDetails['min_X'] = {'text': [start], 'point': 'BOTTOM_LEFT', 'occurence': 1}
        else:
            bboxDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
        if skip != 'None':
            bboxDetails['max_X'] = {'text': [skip], 'point': 'BOTTOM_LEFT', 'occurence': 1}
        elif i + 1 < len(delimiterLst):
            bboxDetails['max_X'] = {'text': [delimiterLst[i + 1]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i + 1].count(delimiterLst[i + 1]) + 1}
        else:
            orig_max_X = di['Location']['parentBB']['max_X']
            bboxDetails['max_X'] = {'text': orig_max_X['text'], 'point': orig_max_X['point'], 'occurence': 1}
        foundFlag, searchRegionTup = parseBboxDetails(bboxDetails, newBboxTuple, newWordTuple, pagebbox)
        searchRegionTup[1] = completeParentSearchRegionTup[1]
        searchRegionTup[3] = completeParentSearchRegionTup[3]
        searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
        out[col] = getTextInSearchRegion(searchRegionDi, newBboxTuple, newWordTuple)
    return out

def mvLoopXNew(colStr, di, bboxTuple, wordsTup, pagebbox, pdfSections):
    # loops in X direction to get all the values of entity
    out = {}
    bboxDetails = {}
    # extracting the entire looping region
    foundFlag, completeParentSearchRegionTup = parseBboxDetails(di['Location']['parentBB'], bboxTuple, wordsTup, pagebbox)
    completeParentSearchRegionDi = getSearchRegion(foundFlag, completeParentSearchRegionTup, pdfSections)
    newBboxTuple, newWordTuple = getElementInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
    completeParentBBwords = getTextInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
    # getting list of exact delimiter words found in the entire searchRegion
    # countDelimiter = [word for word in parentBBwords if di['Location']['delimiter'] in word]
    delimiter = di['Location']['loopDetails']['delimiter']
    # getting list of all delimiters found in completeParentBB
    if isinstance(delimiter, list):
        delimiter = '|'.join([elm for elm in delimiter])  # forming regex
    delimiterLst = re.findall(delimiter, completeParentBBwords, re.IGNORECASE)
    for i in range(0,len(delimiterLst)):
        blockDetails = {}
        col = colStr + str(i)
        if di['Location']['loopDetails']['locationX'] != "None" and di['Location']['loopDetails']['locationY'] != "None":
            # extracting one block inside loop (from the entire looping region)
            if i+1 < len(delimiterLst):
                blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
                blockDetails['max_X'] = {'text': [delimiterLst[i + 1]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i + 1].count(delimiterLst[i + 1]) + 1}
            else:
                blockDetails['min_X'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
            foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, newBboxTuple, newWordTuple, pagebbox)
            if i+1 == len(delimiterLst):
                blocksearchRegionTup[2] = completeParentSearchRegionTup[2]
            blocksearchRegionTup[1] = completeParentSearchRegionTup[1]
            blocksearchRegionTup[3] = completeParentSearchRegionTup[3]
            blocksearchRegionDi = getSearchRegion(foundFlag, blocksearchRegionTup, pdfSections)
            blockBboxTuple, blockWordTup = getElementInSearchRegion(blocksearchRegionDi, newBboxTuple, newWordTuple)
            # modifying pagebbox
            modifiedPagebbox = getSRPagebbox(blockBboxTuple)
            # groupDetails - loop within loop
            if di['Location'].get('groupDetails'):
                res = getGroupDetails(blockBboxTuple, blockWordTup, modifiedPagebbox, di['Location']['groupDetails'], col)
                for col, output in res.items():
                    out[col] = output
            else:
                # extracting each entity from the block
                # taking min/max X/Y dictionaries from locationX/locationY
                bboxDetails = {}
                bboxDetails['min_X'] = di['Location']['loopDetails']['locationX']['min_X']
                bboxDetails['max_X'] = di['Location']['loopDetails']['locationX']['max_X']
                bboxDetails['min_Y'] = di['Location']['loopDetails']['locationY']['min_Y']
                bboxDetails['max_Y'] = di['Location']['loopDetails']['locationY']['max_Y']
                foundFlag, searchRegionTup = parseBboxDetails(bboxDetails, blockBboxTuple, blockWordTup, modifiedPagebbox)
                searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                out[col] = getTextInSearchRegion(searchRegionDi, blockBboxTuple, blockWordTup)
        else:
            out[col] = ''
    return out



def mvLoopY(colStr, di, bboxTuple, wordsTup, pagebbox, pdfSections):
    # loops in Y direction to get all the values of entity
    out = {}
    bboxDetails = {}
    delimiter = di['Location']['loopDetails']['delimiter']
    if delimiter == 'None':     # values on different lines without any delimiter
        foundFlag, searchRegionTup = parseBboxDetails(di['Location']['parentBB'], bboxTuple, wordsTup, pagebbox)
        searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
        outTextlinesLst = getTextlineInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
        for ix, textline in enumerate(outTextlinesLst):
            col = colStr + str(ix)  # e.g. GENERAL@ADDRESS_LINE_1@0
            out[col] = textline
    else:
        # extracting entire looping region
        regionDetails = {"min_X": {"text": "None", "point": "BOTTOM_LEFT", "occurence": "None"},
                         "max_X": {"text": "None", "point": "TOP_RIGHT", "occurence": "None"}}
        regionDetails['min_Y'] = di['Location']['parentBB']['min_Y']
        regionDetails['max_Y'] = di['Location']['parentBB']['max_Y']
        foundFlag, completeParentSearchRegionTup = parseBboxDetails(regionDetails, bboxTuple, wordsTup, pagebbox)
        completeParentSearchRegionDi = getSearchRegion(foundFlag, completeParentSearchRegionTup, pdfSections)
        newBboxTuple, newWordTuple = getElementInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
        completeParentBBwords = getTextInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
        start = di['Location']['loopDetails']['start']
        skip = di['Location']['loopDetails']['skip']
        # getting list of all delimiters found in completeParentBB
        if isinstance(delimiter, list):
            delimiter = '|'.join([elm for elm in delimiter])  # forming regex
        delimiterLst = re.findall(delimiter, completeParentBBwords, re.IGNORECASE)
        if not isinstance(start, list):
            start = [start]
        if not isinstance(skip, list):
            skip = [skip]
        for i in range(0,len(delimiterLst)):
            col = colStr + str(i)
            # extracting one block inside loop (from the entire looping region)
            if i == 0:
                orig_max_Y = di['Location']['parentBB']['max_Y']
                regionDetails['max_Y'] = {'text': orig_max_Y['text'], 'point': 'BOTTOM_LEFT', 'occurence': 1}
                regionDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
            else:
                regionDetails['max_Y'] = {'text': [delimiterLst[i-1]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i-1].count(delimiterLst[i-1]) + 1}
                regionDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
            foundFlag, blocksearchRegionTup = parseBboxDetails(regionDetails, newBboxTuple, newWordTuple, pagebbox)
            blocksearchRegionDi = getSearchRegion(foundFlag, blocksearchRegionTup, pdfSections)
            blockBboxTuple, blockWordTup = getElementInSearchRegion(blocksearchRegionDi, newBboxTuple, newWordTuple)

            # extracting each entity from the block
            # fetching x-coordinate values
            bboxDetailsX = {}
            bboxDetailsX['min_X'] = di['Location']['parentBB']['min_X']
            bboxDetailsX['max_X'] = di['Location']['parentBB']['max_X']
            foundFlag, xSearchRegionTup = parseBboxDetails(bboxDetailsX,blockBboxTuple,blockWordTup,pagebbox)
            if i==0:    # storing x-ccordinates for 0th iteration as a reference
                xSearchRegionTupRef = xSearchRegionTup
            if not foundFlag and i>0:
                # in case min_X/max_X text values are not present in block, take from 0th iteration reference
                xSearchRegionTup = [xSearchRegionTupRef[k] if xSearchRegionTup[k]==() else xSearchRegionTup[k] for k in range(0,4)]
            bboxDetailsY = {}
            if start != 'None' :
                bboxDetailsY['max_Y'] = {'text': start, 'point': 'BOTTOM_LEFT', 'occurence': 1}
            if skip != 'None' :
                bboxDetailsY['min_Y'] = {'text': skip, 'point': 'TOP_RIGHT', 'occurence': 1}
            else:
                bboxDetailsY['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT', 'occurence': 1}
            foundFlag, ySearchRegionTup = parseBboxDetails(bboxDetailsY, blockBboxTuple, blockWordTup, pagebbox)
            if foundFlag and start == 'None':
                ySearchRegionTup[3] = blocksearchRegionTup[3]
            # merging xSearchRegionTup and ySearchRegionTup
            derivedSearchRegionTup = [xSearchRegionTup[0], ySearchRegionTup[1], xSearchRegionTup[2], ySearchRegionTup[3]]
            searchRegionDi = getSearchRegion(foundFlag, derivedSearchRegionTup, pdfSections)
            out[col] = getTextInSearchRegion(searchRegionDi, blockBboxTuple, blockWordTup)
    return out

def mvLoopYNew(colStr, di, bboxTuple, wordsTup, pagebbox, pdfSections):
    # loops in Y direction to get all the values of entity
    out = {}
    delimiter = di['Location']['loopDetails']['delimiter']
    if delimiter == 'None':     # values on different lines without any delimiter
        foundFlag, searchRegionTup = parseBboxDetails(di['Location']['parentBB'], bboxTuple, wordsTup, pagebbox)
        searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
        outTextlinesLst = getTextlineInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
        for ix, textline in enumerate(outTextlinesLst):
            col = colStr + str(ix)  # e.g. GENERAL@ADDRESS_LINE_1@0, GENERAL@ADDRESS_LINE_1@1
            out[col] = textline
    else:
        # extracting entire looping region
        regionDetails = di['Location']['parentBB']
        foundFlag, completeParentSearchRegionTup = parseBboxDetails(regionDetails, bboxTuple, wordsTup, pagebbox)
        completeParentSearchRegionDi = getSearchRegion(foundFlag, completeParentSearchRegionTup, pdfSections)
        newBboxTuple, newWordTuple = getElementInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)
        completeParentBBwords = getTextInSearchRegion(completeParentSearchRegionDi, bboxTuple, wordsTup)

        # getting list of all delimiters found in completeParentBB
        if isinstance(delimiter, list):
            delimiter = '|'.join([elm for elm in delimiter])  # forming regex
        delimiterLst = re.findall(delimiter, completeParentBBwords, re.IGNORECASE)

        for i in range(0,len(delimiterLst)):
            blockDetails = {}
            col = colStr + str(i)
            if di['Location']['loopDetails']['locationX'] != "None" and di['Location']['loopDetails']['locationY'] != "None":
                # extracting one block inside loop (from the entire looping region)
                if i == 0:
                    blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
                else:
                    blockDetails['max_Y'] = {'text': [delimiterLst[i-1]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i-1].count(delimiterLst[i-1]) + 1}
                    blockDetails['min_Y'] = {'text': [delimiterLst[i]], 'point': 'BOTTOM_LEFT','occurence': delimiterLst[:i].count(delimiterLst[i]) + 1}
                foundFlag, blocksearchRegionTup = parseBboxDetails(blockDetails, newBboxTuple, newWordTuple, pagebbox)
                if i==0:
                    blocksearchRegionTup[3] = completeParentSearchRegionTup[3]
                blocksearchRegionTup[0] = completeParentSearchRegionTup[0]
                blocksearchRegionTup[2] = completeParentSearchRegionTup[2]
                blocksearchRegionDi = getSearchRegion(foundFlag, blocksearchRegionTup, pdfSections)
                # removing page:region from blocksearchRegionDi which doesn't contain any words
                blocksearchRegionDi = {pg:region for pg,region in blocksearchRegionDi.items() if pg in list(newBboxTuple.keys())}
                blockBboxTuple, blockWordTup = getElementInSearchRegion(blocksearchRegionDi, newBboxTuple, newWordTuple)
                # modifying pagebbox
                modifiedPagebbox = getSRPagebbox(blockBboxTuple)
                # groupDetails - loop within loop
                if di['Location'].get('groupDetails'):
                    res = getGroupDetails(blockBboxTuple, blockWordTup, modifiedPagebbox, di['Location']['groupDetails'], col)
                    for col, output in res.items():
                        out[col] = output
                else:
                    # extracting each entity from the block
                    # taking min/max X/Y dictionaries from locationX/locationY
                    bboxDetails = {}
                    bboxDetails['min_X'] = di['Location']['loopDetails']['locationX']['min_X']
                    bboxDetails['max_X'] = di['Location']['loopDetails']['locationX']['max_X']
                    bboxDetails['min_Y'] = di['Location']['loopDetails']['locationY']['min_Y']
                    bboxDetails['max_Y'] = di['Location']['loopDetails']['locationY']['max_Y']
                    foundFlag, searchRegionTup = parseBboxDetails(bboxDetails, blockBboxTuple, blockWordTup, modifiedPagebbox)
                    searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                    out[col] = getTextInSearchRegion(searchRegionDi, blockBboxTuple, blockWordTup)
            else:   # creating empty columns
                out[col] = ''
    return out

def getSRPagebbox(srBboxTuple):
    pageLst = list(srBboxTuple.keys())
    # getting min_X and max_X
    allBbox = [bbox for eachPg in pageLst for bbox in srBboxTuple[eachPg]]
    min_X = min([bbox[0] for bbox in allBbox])
    max_X = max([bbox[2] for bbox in allBbox])
    # getting max_Y using first page of searchRegion
    firstPg = min(pageLst)
    max_Y = max([bbox[3] for bbox in srBboxTuple[firstPg]])
    # getting min_Y using last page in searchRegion
    lastPg = max(pageLst)
    min_Y = min([bbox[1] for bbox in srBboxTuple[lastPg]])
    return (min_X, min_Y, max_X, max_Y)

# def findAllInstances(searchString, bboxTuple, wordTup):
#     results = []
#     wordsToSearch = searchString.split()
#     ln = len(wordsToSearch)
#     occurence = 0
#     for pgNm, pageInfo in wordTup.iteritems():
#         for i in range(len(pageInfo) - ln + 1):
#             if all(wordsToSearch[j] in pageInfo[i + j] for j in range(ln)):
#                 foundWord = ' '.join(pageInfo[i: i+ln])
#                 bboxCoordinate = bboxTuple[pgNm][i][:2]+ bboxTuple[pgNm][i+ln-1][2:]
#                 results.append((foundWord, bboxCoordinate, pgNm, range(i,i+ln)))
#                 occurence += 1
#     return results

def findAllInstances(searchStringLst, bboxTuple, wordTup):
    # find all instances of each string in searchStringLst
    results = []
    for searchString in searchStringLst:
        wordsToSearch = searchString.split()
        ln = len(wordsToSearch)
        occurence = 0
        for pgNm, pageInfo in wordTup.items():
            for i in range(len(pageInfo) - ln + 1):
                if all(wordsToSearch[j] in pageInfo[i + j] for j in range(ln)):
                    foundWord = ' '.join(pageInfo[i: i+ln])
                    bboxCoordinate = bboxTuple[pgNm][i][:2]+ bboxTuple[pgNm][i+ln-1][2:]
                    results.append((foundWord, bboxCoordinate, pgNm, list(range(i,i+ln))))
                    occurence += 1
    return results


def getGroupTextInstances(txtLst, blockBbox, blockWords):
    # gets coordinates corresponding to each instance of groupText
    # forming text string using all the words in blockWords
    blockTextStr = ' '.join([word for pgNm, wordLst in blockWords.items() for word in wordLst])
    # forming regex pattern
    regex = '|'.join([text for text in txtLst])
    groupText = list(set(re.findall(regex, blockTextStr)))
    # finding all the instances of groupText
    foundInstances = findAllInstances(groupText, blockBbox, blockWords)
    # identifying duplicate instances to delete
    toDelete = []
    for i, x in enumerate(foundInstances):
        for j, y in enumerate(foundInstances):
            if i != j and all(elm in x[3] for elm in y[3]):
                toDelete.append(y)
    groupTextInstances = [elm for elm in foundInstances if elm not in toDelete]
    return groupTextInstances


def getGroupDetails(blockBbox, blockWords, modifiedPageBbox, groupDetails):
    grpOut = []
    bboxDetails = groupDetails['bbox']
    # collecting all texts from groupDetails
    allText = []
    for position, di in bboxDetails.items():
        if di.get('text'):
            for eachText in di['text']:
                allText.append(eachText)
    allText = list(set(allText))
    # getting coordinates, pgNm for all texts from groupDetails and groupText
    allTextInstances = findAllInstances(allText, blockBbox, blockWords)
    groupTextInstances = getGroupTextInstances(groupDetails['groupText'], blockBbox, blockWords)
    # setting textline flag and column count
    textlineFlag = True if groupDetails['textline'] == "True" else False
    colCount = 0
    positionDi = {'min_X':0, 'min_Y':1, 'max_X':2, 'max_Y':3}
    for ix, instance in enumerate(groupTextInstances):
        out = {}
        for position, di in bboxDetails.items():
            ref = instance[1][getCoord(di['ref'], position)]
            if di.get('text') and di.get('point'): # get distance and take minimum
                textCoordinates = []
                point = di['point']
                textLst = di['text'] + groupDetails['groupText']   # taking into account text mentioned in groupText
                # adding all groupTextInstances except the one for which loop is running
                textInstances = allTextInstances + [elm for elm in groupTextInstances if elm[0]!=instance[0]]
                # forming list of coordinates to compare with reference
                for text in textLst:
                    for txt in textInstances:
                        if txt[0] == text:
                            textCoordinates.append(txt[1][getCoord(point, position)])
                textCoordinates.append(modifiedPageBbox[positionDi[position]]) # taking into account block boundaries
                # collecting valid coordinates
                if 'min' in position:
                    validCoordinates = [coord for coord in textCoordinates if coord<=ref]
                else:
                    validCoordinates = [coord for coord in textCoordinates if coord>=ref]
                # calculating distance
                distance = [abs(coord - ref) for coord in validCoordinates]
                # selecting the coordinate with minimum distance
                index = distance.index(min(distance))
                out[position] = validCoordinates[index]
            else:
                out[position] = ref
        coords = [0]*4
        for k, v in positionDi.items():  # arranging coordinates in order: [min_X, min_Y, max_X, max_Y]
            coords[v] = out[k]
        searchRegionDi = {instance[2]:coords}
        if textlineFlag:
            textlines = getTextlineInSearchRegion(searchRegionDi, blockBbox, blockWords)
            for textline in textlines:
                grpOut.append(textline)
        else:
            grpOut.append(getTextInSearchRegion(searchRegionDi, blockBbox, blockWords))
    return grpOut

def getValFromTableDetails(tableDetails, tableDi, tableCat, parentCategory):
    out = []
    if parentCategory in tableCat:
        tableId = tableDetails['tableId']
        allTableCols = tableDi[parentCategory][tableId].columns
        jsonCol = tableDetails['columnIndex']
        out = [row[jsonCol] for i, row in tableDi[parentCategory][tableId].iterrows() if jsonCol in allTableCols]
    return out

def getValFromLoopDetails(parentCategory, locationInfo, loopDi, pdfSections):
    out = []
    loopId = locationInfo['loopDetails']['loopId']
    derivedLoopInfoLst = loopDi[parentCategory][loopId]
    for ix, loopTuple in enumerate(derivedLoopInfoLst): #(blockBboxTuple, blockWordTup, modifiedPagebbox)
        blockBboxTuple = loopTuple[0]
        blockWordTup = loopTuple[1]
        modifiedPagebbox = loopTuple[2]
        if 'groupDetails' in locationInfo and locationInfo['groupDetails'] != 'None':
            res = getGroupDetails(blockBboxTuple, blockWordTup, modifiedPagebbox, locationInfo['groupDetails'])
            out.append(res)   # nested list appended in case of groupDetails

        else:
            # extracting each entity from the block
            # taking min/max X/Y dictionaries from locationX/locationY
            locationX = locationInfo['loopDetails']['locationX']
            locationY = locationInfo['loopDetails']['locationY']
            bboxDetails = {}
            bboxDetails['min_X'] = locationX['min_X']
            bboxDetails['max_X'] = locationX['max_X']
            bboxDetails['min_Y'] = locationY['min_Y']
            bboxDetails['max_Y'] = locationY['max_Y']
            foundFlag, searchRegionTup = parseBboxDetails(bboxDetails, blockBboxTuple, blockWordTup,modifiedPagebbox)
            searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
            out.append( getTextInSearchRegion(searchRegionDi, blockBboxTuple, blockWordTup) )
    return out

def multivalueExtraction(pagebbox, bboxTuple, wordsTup, multiValueJson, pdfSections, tableDi, loopDi):
    # wrapper function to extract multiValue entities from the record
    out = {}
    tableCat = []
    if tableDi:
        tableCat = list(tableDi.keys())
    for parentCategory,json in multiValueJson.items():
        for entity, details in json.items():
            isGrp = bool(constants.mvMappings[entity].get('GROUP_DETAILS'))
            for tagDi in details['Column_Type']:
                tagOutLst = []
                # extracting entities
                for di in tagDi['Location']:
                    if 'tableDetails' in di and di['tableDetails'] != 'None':# di['Location']['tableDetails'] != 'None':
                        res = getValFromTableDetails(di['tableDetails'], tableDi, tableCat, parentCategory)
                        for val in res:
                            tagOutLst.append(val)
                    elif 'loopDetails' in di and di['loopDetails'] != 'None':
                        #print entity+'@'+tagDi['Tag']
                        res = getValFromLoopDetails(parentCategory, di, loopDi, pdfSections)
                        for val in res:
                            tagOutLst.append(val)
                    elif 'groupDetails' in di and di['groupDetails'] != 'None':
                        foundFlag, searchRegionTup = parseBboxDetails(di['parentBB'], bboxTuple, wordsTup, pagebbox)
                        searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                        blockBbox, blockWords = getElementInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
                        modifiedPagebbox = getSRPagebbox(blockBbox)
                        tagOutLst.append(getGroupDetails(blockBbox, blockWords, modifiedPagebbox, di['groupDetails']))
                    else:
                        #print entity+'@'+tagDi['Tag']
                        foundFlag, searchRegionTup = parseBboxDetails(di['parentBB'], bboxTuple, wordsTup, pagebbox)
                        searchRegionDi = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
                        if 'textline' in di and di['textline'] == 'True':
                            res = getTextlineInSearchRegion(searchRegionDi, bboxTuple, wordsTup)
                            for val in res:
                                tagOutLst.append(val)
                        else:
                            tagOutLst.append(getTextInSearchRegion(searchRegionDi, bboxTuple, wordsTup))
                for ix, output in enumerate(tagOutLst):
                    if isinstance(output,list):     # groupDetails output
                        for grpIx, grpOut in enumerate(output):
                            col = tagDi['Tag'] + '@' + entity + '#' + str(grpIx) + '@' + str(ix)
                            out[col] = grpOut
                    else:
                        if isGrp:
                            colStr = tagDi['Tag'] + '@' + entity + '#' + '0' + '@' + str(ix)
                        else:
                            colStr = tagDi['Tag'] + '@' + entity + '@' + str(ix)
                        out[colStr] = output
    return out

# def getEntityJsonsLocal(fNm, lstParentCategory):
#     # loads all the jsons for entity extraction
#     entityJson = {}
#     path = constants.pdfMappingsPath + fNm +'/'
#     folderContent = [os.path.splitext(file)[0] for file in os.listdir(path)]
#     for cat in lstParentCategory:
#         if cat in folderContent:
#             entityJson[cat] = json.load(open(path+cat+'.json'))
#     return entityJson

def getAllJsonsMongoDB(provider):
    conditionData = fetchProviderCondition(provider, 'ProviderPdf')
    try:
        allJsons = constants.handler.find_one_document('ProviderPdf', conditionData)['Doc']
    except:
        allJsons = 'Pdf Mapping not found on mongoDB'
    existingPdfStatus = True if isinstance(allJsons,dict) else False
    return existingPdfStatus,allJsons

def getEntityJsons(allJsons, lstParentCategory):
    entityJson = {cat: json for cat, json in allJsons.items() if cat in lstParentCategory}
    return entityJson

def extractEntities(pagebbox, recordDi, entityJson, pdfSectionsDi, recordTableDi, recordLoopDi):
    # iterates over each record to extract singleValue and multiValue entities
    result = []
    singleValueJson = entityJson['singleValue']
    entityJson.pop('singleValue')
    for recordNm, record in recordDi.items():
        wordsTup, bboxTuple = {}, {}
        for di in record:
            for pgNm, diPg in di.items():
                bboxTuple[pgNm], wordsTup[pgNm] = list(zip(*diPg))
        #Fprint recordNm
        pdfSections = pdfSectionsDi[recordNm]
        resultDi = singleValueExtraction(pagebbox, bboxTuple, wordsTup, singleValueJson, pdfSections, False)
        tableDi = recordTableDi.get(recordNm)
        loopDi = recordLoopDi.get(recordNm)
        multiValueResult = multivalueExtraction(pagebbox,bboxTuple,wordsTup,entityJson, pdfSections, tableDi, loopDi)
        resultDi.update(multiValueResult)
        result.append(resultDi)
    df = pd.DataFrame(result)
    df.fillna('', inplace=True)
    return df

# def modifyColCount(df, entityJson):
#     newCols = {}
#     for pCat, json in entityJson.iteritems():
#         entities = json.keys()
#         maxColCount = 0
#         pCatColDi = {}
#         for eachEntity in entities:
#             cols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==eachEntity]
#             pCatColDi.update({eachEntity: cols})
#             if len(cols) > maxColCount:
#                 maxColCount = len(cols)
#                 maxColEntity = eachEntity
#         colInfo = [(col.split('@',1)[0], col.rsplit('@',1)[1], str(newIx)) for newIx, col in enumerate(pCatColDi[maxColEntity])]
#         for entity, colLst in pCatColDi.iteritems():
#             for col in colLst:
#                 newIndex = [info[2] for info in colInfo if info[0] == col.split('@',1)[0] and info[1] == col.rsplit('@',1)[1]]
#                 newCols.update( {col: col.rsplit('@',1)[0]+'@'+str(newIndex[0])} )
#     df.rename(newCols, axis='columns', inplace=True)
#     return df

def modifyColCount(df):
    entityJson = {}
    dfCols = df.columns
    for col in dfCols:
        if '@' in col:  # picking only multivalue cols out of df
            colNm = col.split('@')[1].split('#')[0]
            # getting parent category from mappings.json
            pCat = constants.mvMappings[colNm]['PARENT_CATEGORY']
            grp = entityJson.setdefault(pCat, [])
            grp.append(colNm)
    entityJson = {pCat: list(set(colLst)) for pCat, colLst  in entityJson.items()}
    newCols = {}
    for pCat, entities in entityJson.items():
        tagMaxColCount = {}
        pCatColDi = {}
        for eachEntity in entities:
            cols = [col for col in df.columns if len(col.split('@'))==3 and col.split('@')[1].split('#')[0]==eachEntity]
            pCatColDi.update({eachEntity: cols})
            pCatTags = set([col.split('@',1)[0] for col in cols])
            for tag in pCatTags:
                if bool(constants.mvMappings[eachEntity].get('GROUP_DETAILS')):
                    tagCols = set([re.sub('#\d+', '', col) for col in cols if col.split('@',1)[0] == tag])
                else:
                    tagCols = [col for col in cols if col.split('@',1)[0] == tag]
                if tag not in tagMaxColCount:
                    tagMaxColCount[tag] = 0
                if tagMaxColCount[tag] < len(tagCols):
                    tagMaxColCount[tag] = len(tagCols)
        newIx = 0
        colInfo = []
        for tag, tagColCount in tagMaxColCount.items():
            for tagIx in range(tagColCount):
                colInfo.append( (tag, str(tagIx), str(newIx)) )
                newIx += 1
        for entity, colLst in pCatColDi.items():
            for col in colLst:
                newIndex = [info[2] for info in colInfo if info[0] == col.split('@',1)[0] and info[1] == col.rsplit('@',1)[1]]
                newCols.update( {col: col.rsplit('@',1)[0]+'@'+str(newIndex[0])} )
    #df.rename(newCols, axis='columns', inplace=True)
    df.rename(columns=newCols, inplace=True)
    return df

def applyCleanFinalDfVariables(allJsons,df):
    if allJsons.get('cleanse'):
        cleanseJson = allJsons['cleanse']
        layerDi = [di for di in cleanseJson if di['LayerName'] == 'CleanFinalDataframe']
        if layerDi:
            for derivation in layerDi[0]['Transformation']:
                funcNm = derivation['name']
                argDi = derivation['input']
                try:
                    df = getattr(cleanseFile, funcNm)(df, argDi)
                except:
                    traceback.print_exc()
                    continue
    return df

def extractXmlPageDi(fNm, token, existingPdfFlag, allJsons):
    # converting to xml
    print("converting to XML ...")
    xml = convert_pdf_doc(constants.loc + fNm + '.pdf')
    pklData = {"fNm": fNm, "xml": xml}
    with open('../tmp/' + token + '.pkl', 'wb') as fp:
        pickle.dump(pklData, fp)
    global xMargin, yMargin  # declared global for custom_compare fn
    if existingPdfFlag:
        pdfInfoJson = allJsons['pdfInfo']
        yMargin = pdfInfoJson['yMargin']
        xMargin = pdfInfoJson['xMargin']
    else:
        xMargin = 0
        yMargin = 0
    print('getting pageDi ...')
    pagebbox, pageDi = parseForText(xml, selKey='imgTxtLn2', sort=True, textFontSize=False)
    return pagebbox, pageDi

def extractPdfInfoWrapper(pageDi, pagebbox, pdfInfoJson):
    # getting pdfSections and recordDi using pdfInfo.json
    pdfSectionsDi, recordDi, recordToPdfPg, pagebbox = extractPdfInfo(pageDi, pagebbox, pdfInfoJson)
    return pdfSectionsDi, recordDi, recordToPdfPg, pagebbox

def processPdfWrapper(token, allJsons, pageDi, originalPageDi, pagebbox, origPagebbox):
    pdfSectionsDi, recordDi, recordToPdfPg, pagebbox = extractPdfInfoWrapper(pageDi, pagebbox, allJsons['pdfInfo'])
    origPdfSectionsDi, origRecordDi, recordToPdfPg, origPagebbox = extractPdfInfoWrapper(originalPageDi, origPagebbox, allJsons['pdfInfo'])
    # extracting all the tables from PDF using tableInfo.json
    recordTableDi = tableExtractionWrapper(token, pagebbox, recordDi, pdfSectionsDi, origPagebbox, origRecordDi, origPdfSectionsDi, recordToPdfPg, allJsons)
    # getting loop information from PDF using loopInfo.json
    recordLoopDi = loopExtractionWrapper(pagebbox, recordDi, pdfSectionsDi, allJsons)
    # extracting entities
    dataframe = entityExtractionWrapper(pagebbox, pdfSectionsDi, recordDi, recordTableDi, recordLoopDi, allJsons)
    diDFs = {"pdfTab": dataframe}
    dfname = token + "InpDF" + '.pkl'
    with open("../tmp/" + dfname, 'wb') as handle:
        pickle.dump(diDFs, handle)
    return diDFs


def entityExtractionWrapper(pagebbox, pdfSectionsDi, recordDi, recordTableDi, recordLoopDi, allJsons):
    print('extracting entities...')
    entityJson = getEntityJsons(allJsons, constants.lstParentCategory)
    resultDf = extractEntities(pagebbox, recordDi, entityJson, pdfSectionsDi, recordTableDi, recordLoopDi)
    resultDf = modifyColCount(resultDf)
    cleanedDf = applyCleanFinalDfVariables(allJsons, resultDf)
    return cleanedDf

######################## FUNCTIONS FOR UI #####################################################

def sampleRunSV(singleValueJson, recordDi, pagebbox, pdfSections, setMarginFlag):
    sampleRecord = recordDi[1]
    bboxTuple = {}
    wordsTup = {}
    for ix, diPg in enumerate(sampleRecord):
        bboxTuple[ix + 1], wordsTup[ix + 1] = list(zip(*diPg[ix + 1]))
    out = singleValueExtraction(pagebbox, bboxTuple, wordsTup, singleValueJson, pdfSections, setMarginFlag)
    return out

def setMarginSV(expectedVal, singleValueJson, recordDi, pagebbox, pdfSections):
    # expectedVal = {entity1:val1, entity2:val2, ...}
    global xMargin
    global yMargin
    # filtering json
    filteredJSon = {entity: valDi for entity, valDi in singleValueJson.items() if entity in expectedVal}
    # extracting again to get searchRegion
    outSR = sampleRunSV(filteredJSon, recordDi, pagebbox, pdfSections, True)
    # expanding min/max from json
    expandedJson = copy.deepcopy(filteredJSon)
    expand = {'min_X':'BOTTOM_LEFT', 'max_X':'TOP_RIGHT', 'min_Y':'BOTTOM_LEFT', 'max_Y':'TOP_RIGHT'}
    for entity, valDi in filteredJSon.items():
        for position, di in valDi.items():
            expandedJson[entity][position]['point'] = expand[position]
    # getting expanded searchRegion
    sampleRecord = recordDi[1]
    bboxTuple = {}
    wordsTup = {}
    for ix, diPg in enumerate(sampleRecord):
        bboxTuple[ix + 1], wordsTup[ix + 1] = list(zip(*diPg[ix + 1]))
    expandedSR = {}
    for entity, bboxDetails in expandedJson.items():
        foundFlag, searchRegionTup = parseBboxDetails(bboxDetails, bboxTuple, wordsTup, pagebbox)
        expandedSR[entity] = getSearchRegion(foundFlag, searchRegionTup, pdfSections)
    # trying to find the expected val in searchRegion
    tol = 2.5
    for entity, expSr in expandedSR.items():
        pgNm = list(expSr.keys())[0]  # as of now, assuming one page
        expSrBbox = list(expSr.values())[0] # as of now, assuming one page
        # adding a tolerance to expSr, just to find the expected val in the searchRegion
        newExpSr = [val+tol for val in expSrBbox]
        foundBboxTup, foundWordsTup = getElementInSearchRegion({pgNm:newExpSr}, bboxTuple, wordsTup)
        res = findAllInstances([expectedVal[entity]], foundBboxTup, foundWordsTup)
        actualBbox = res[0][1]
        srBbox = list(outSR[entity].values())[0]
        if len(res) == 1:
            # modifying the margin accordingly
            if actualBbox[0] < srBbox[0]:
                xMargin = math.ceil(max(xMargin, srBbox[0] - actualBbox[0]))
            if actualBbox[1] < srBbox[1]:
                yMargin = math.ceil(max(yMargin, srBbox[1] - actualBbox[1]))
            if actualBbox[2] > srBbox[2]:
                xMargin = math.ceil(max(xMargin, actualBbox[2] - srBbox[2]))
            if actualBbox[3] > srBbox[3]:
                yMargin = math.ceil(max(yMargin, actualBbox[3] - srBbox[3]))
    return xMargin, yMargin


################################ FUNCTIONS FOR FORMING JSON FOR UI #################################

def jsonBackendToUiWrapper(provider):
    allJsons = getAllJsonsMongoDB(provider)[1]
    uiJson = {}
    uiJson['pdfInfo'] = jsonBackendToUi(allJsons, 'pdfInfo')
    uiJson['singleValue'] = jsonBackendToUi(allJsons, 'singleValue')
    uiJson['loopInfo'] = jsonBackendToUi(allJsons, 'loopInfo')
    uiJson['tableInfo'] = jsonBackendToUi(allJsons, 'tableInfo')
    uiJson['parentCategory'] = [cat for cat in constants.lstParentCategory if cat!='singleValue']
    uiJson['multivalue'] = jsonBackendToUi(allJsons, 'multivalue')
    return uiJson

def jsonBackendToUi(allJsons, type):
    if type == 'pdfInfo':
        pdfInfoJson = allJsons['pdfInfo'] if isinstance(allJsons,dict) and 'pdfInfo' in allJsons else {}
        modifiedPdfInfoJson = {}
        if pdfInfoJson:
            modifiedPdfInfoJson['pdfInfoStatus'] = 'existing'
            modifiedPdfInfoJson['pdfInfoData'] = [pdfInfoJson]
        else:
            modifiedPdfInfoJson['pdfInfoStatus'] = 'new'
            modifiedPdfInfoJson['pdfInfoData'] = []
        return modifiedPdfInfoJson

    elif type == 'singleValue':
        singleValueJson = allJsons['singleValue'] if isinstance(allJsons,dict) and 'singleValue' in allJsons else {}
        modifiedSVJson = {'mapped': []}
        mappedColsLst = []
        for entity, mapping in singleValueJson.items():
            modifiedSVJson['mapped'].append({'outField': entity, 'mapping': mapping})
            mappedColsLst.append(entity)
        modifiedSVJson['unmappedCols'] = [col for col, val in constants.mappings.items() if col not in mappedColsLst]
        modifiedSVJson['referenceJson'] = {'start': {'text': []},
                                           'min_X': {'text': [], 'point': 'None', 'occurence': 'None'},
                                           'min_Y': {'text': [], 'point': 'None', 'occurence': 'None'},
                                           'max_X': {'text': [], 'point': 'None', 'occurence': 'None'},
                                           'max_Y': {'text': [], 'point': 'None', 'occurence': 'None'}}
        return modifiedSVJson

    elif type == 'tableInfo':
        tableInfoJson = allJsons['tableInfo'] if isinstance(allJsons,dict) and 'tableInfo' in allJsons else {}
        modifiedTableInfoJson = {'tableInfo': []}
        for pCat, lst in tableInfoJson.items():
            mapping = [infoDi for infoDi in lst]
            modifiedTableInfoJson['tableInfo'].append({'pCat': pCat, 'mapping': mapping})
        modifiedTableInfoJson['referenceJson'] = {
            "tableId": 0,
            "type": "None",
            "xMargin": 4.5,
            "tableHeader": 0,
            "parentBB": {
                'start': {'text': []},
                "min_Y": {"text": [], "point": "None", "occurence": "None"},
                "max_Y": {"text": [], "point": "None", "occurence": "None"},
                "min_X": {"text": [], "point": "None", "occurence": "None"},
                "max_X": {"text": [], "point": "None", "occurence": "None"}}
        }
        return modifiedTableInfoJson

    elif type == 'loopInfo':
        loopInfoJson = allJsons['loopInfo'] if isinstance(allJsons,dict) and 'loopInfo' in allJsons else {}
        modifiedLoopInfoJson = {'loopInfo': []}
        for pCat, lst in loopInfoJson.items():
            mapping = [infoDi for infoDi in lst]
            modifiedLoopInfoJson['loopInfo'].append({'pCat': pCat, 'mapping': mapping})
        modifiedLoopInfoJson['referenceJson'] = {
            "loopId": 0,
            "direction": "None",
            "delimiter": {"1": [], "type":"None"},
            "parentBB": {
                'start': {'text': []},
                "min_Y": {"text": [], "point": "None", "occurence": "None"},
                "max_Y": {"text": [], "point": "None", "occurence": "None"},
                "min_X": {"text": [], "point": "None", "occurence": "None"},
                "max_X": {"text": [], "point": "None", "occurence": "None"}}
        }
        return modifiedLoopInfoJson

    elif type == 'multivalue':
        modifiedMvJson = {'multivalue':[]}
        pCatLst = [val for val in constants.lstParentCategory if val != 'singleValue']
        for pCat in pCatLst:
            pCatDetails = {'pCat': pCat, 'allTags':[], 'allOutFields':[], 'allGrps':[], 'mapped':[]}
            for field, di in constants.mvMappings.items():
                if di['PARENT_CATEGORY'] == pCat:
                    pCatDetails['allOutFields'].append(field)
                    if di.get('GROUP_DETAILS'):
                        pCatDetails['allGrps'].append(field)
                    if di['DATAFRAME_TYPE'] != 'AsIs':
                        for tagDi in di['Column_Type']:
                            if tagDi['Tag'] not in pCatDetails['allTags']:
                                pCatDetails['allTags'].append(tagDi['Tag'])
            pCatJson = allJsons[pCat] if isinstance(allJsons,dict) and pCat in allJsons else {}
            replaceTag = {}
            for field, di in pCatJson.items():
                for tagDetailsDi in di['Column_Type']:
                    for ix, locationDi in enumerate(tagDetailsDi['Location']):
                        if ix != 0:
                            tagNm = tagDetailsDi['Tag'] + str(ix)    # creating GENERAL1, GENERAL2, ..
                            replaceTag = {tagNm:tagDetailsDi['Tag']}
                        else:
                            tagNm = tagDetailsDi['Tag']
                        tagMappingData = {'outField': field}
                        if 'tableDetails' in locationDi and locationDi['tableDetails'] != 'None' and locationDi['tableDetails']:
                            detailType = 'tableDetails'
                            tagMappingData['mapping'] = {'tableDetails':locationDi['tableDetails']}
                        elif 'loopDetails' in locationDi and locationDi['loopDetails'] != 'None' and locationDi['loopDetails']:
                            detailType = 'loopDetails'
                            tagMappingData['mapping'] = {'loopDetails': locationDi['loopDetails']}
                        else:
                            detailType = 'parentBB'
                            tagMappingData['mapping'] = {'parentBB': locationDi['parentBB']}
                            if 'textline' in locationDi and locationDi['textline'] == 'True':
                                tagMappingData['mapping']['textline'] = 'True'
                            else:
                                tagMappingData['mapping']['textline'] = 'False'
                        if field in pCatDetails['allGrps'] and 'groupDetails' in locationDi and locationDi['groupDetails'] != 'None':
                            tagMappingData['groupDetails'] = locationDi['groupDetails']
                        else:
                            tagMappingData['groupDetails'] = {}
                        tagFoundFlag = 0
                        for dic in pCatDetails['mapped']:
                            if dic['tag'] == tagNm:
                                dic['tagMapping'].append(tagMappingData)
                                tagFoundFlag = 1
                        if tagFoundFlag == 0:
                            pCatDetails['mapped'].append({'tag':tagNm, 'detailType':detailType, 'tagMapping':[tagMappingData]})
            for idx, tagInfoDi in enumerate(pCatDetails['mapped']):
                if tagInfoDi['tag'] in replaceTag:
                    pCatDetails['mapped'][idx]['tag'] = replaceTag[tagInfoDi['tag']]
            modifiedMvJson['multivalue'].append(pCatDetails)
            modifiedMvJson['referenceJson'] = {
                "parentBB": {
                    'start': {'text': []},
                    "min_Y": {"text": [], "point": "None", "occurence": "None"},
                    "max_Y": {"text": [], "point": "None", "occurence": "None"},
                    "min_X": {"text": [], "point": "None", "occurence": "None"},
                    "max_X": {"text": [], "point": "None", "occurence": "None"}},
                "loopDetails": {"loopId": 0,
                                "locationX": {"min_X": {"text": [], "point": "None", "occurence": "None"},
                                              "max_X": {"text": [], "point": "None", "occurence": "None"}},
                                "locationY": {"min_Y": {"text": [], "point": "None", "occurence": "None"},
                                              "max_Y": {"text": [], "point": "None", "occurence": "None"}}},
                "tableDetails": {"tableId":0, "columnIndex": 0},
                "textline": "None",
                "groupDetails": {"groupText": [], "textline": "None",
                                 "bbox": {"min_X": {"ref": "None"},
                                          "max_X": {"text": [], "point": "None", "ref": "None"},
                                          "min_Y": {"text": [], "point": "None", "ref": "None"},
                                          "max_Y": {"text": [], "point": "None", "ref": "None"}}}

            }
        return modifiedMvJson


def jsonUiToBackendWrapper(uiJsons):
    allJsons = {}
    allJsons['pdfInfo'] = jsonUiToBackend(uiJsons['pdfInfo'], 'pdfInfo')
    allJsons['singleValue'] = jsonUiToBackend(uiJsons['singleValue'], 'singleValue')
    allJsons['tableInfo'] = jsonUiToBackend(uiJsons['tableInfo'], 'tableInfo')
    allJsons['loopInfo'] = jsonUiToBackend(uiJsons['loopInfo'], 'loopInfo')
    multiValueJson = jsonUiToBackend(uiJsons['multivalue'], 'multivalue')
    for pCat, pCatMapping in multiValueJson.items():
        if pCatMapping:
            allJsons[pCat] = pCatMapping
    return allJsons


def jsonUiToBackend(json, type):
    if type == 'pdfInfo':
        return json['pdfInfoData'][0]   # list of dictionary
    elif type == 'singleValue':
        return {di['outField']:di['mapping'] for di in json['mapped']}
    elif type == 'tableInfo':
        return {di['pCat']:di['mapping'] for di in json['tableInfo']}
    elif type == 'loopInfo':
        return {di['pCat']:di['mapping'] for di in json['loopInfo']}
    elif type == 'multivalue':
        mvMap = json['multivalue']
        mvJson = {}
        emptyJson = {'loopDetails': 'None', 'tableDetails': 'None', 'parentBB': 'None'}
        for pCatDi in mvMap:
            pCat = pCatDi['pCat']
            if pCat not in mvJson:
                mvJson[pCat] = {}
            tagCountDi = {tag:0 for tag in pCatDi['allTags']}
            for tagMap in pCatDi['mapped']:
                tagNm = tagMap['tag']
                for fieldMap in tagMap['tagMapping']:
                    outField = fieldMap['outField']
                    if outField in pCatDi['allGrps'] and fieldMap['groupDetails']:
                        fieldMap['mapping']['groupDetails'] = fieldMap['groupDetails']
                    if outField not in mvJson[pCat]:
                        mvJson[pCat][outField] = {'PARENT_CATEGORY':pCat, 'Column_Type':[]}
                    tagFoundFlag = 0
                    for ix, ctypeDi in enumerate(mvJson[pCat][outField]['Column_Type']):
                        if tagNm == mvJson[pCat][outField]['Column_Type'][ix]['Tag']:
                            tagFoundFlag = 1
                            tagCount = tagCountDi[tagNm]
                            if tagCount != len(mvJson[pCat][outField]['Column_Type'][ix]['Location']):
                                for k in range(0,tagCount):
                                    mvJson[pCat][outField]['Column_Type'][ix]['Location'].append(emptyJson)
                            else:
                                mvJson[pCat][outField]['Column_Type'][ix]['Location'].append(fieldMap['mapping'])
                            break
                    if tagFoundFlag == 0:
                        mvJson[pCat][outField]['Column_Type'].append({'Tag': tagNm, 'Location':[fieldMap['mapping']]})
                tagCountDi[tagNm] += 1
        return mvJson

############################## UPDATED PAGEDI EXTRACTION ######################################
def newExtractXmlPageDi(fNm, token):
    print("converting to XML ...")
    xml = convert_pdf_doc(constants.loc + fNm + '.pdf')
    pklData = {"fNm": fNm, "xml": xml}
    with open('../tmp/' + token + '.pkl', 'wb') as fp:
        pickle.dump(pklData, fp)
    print('getting pageDi ...')
    originalPageDi, modifiedPageDi, modifiedPageTextlineDi, origPagebbox, pagebbox = textExt.getPdfWordsWrapper(xml)
    return originalPageDi, modifiedPageDi, modifiedPageTextlineDi, origPagebbox, pagebbox


############################ MAIN ########################################################

if __name__ == '__main__':
    # testing PDF extraction
    # fNm = 'elkinmd_Other_Other_20180827T110636.783'
    # provider = 'CA028'
    # diDF = processPdfWrapper(fNm,provider,token='0')

    #pickling the dataframe
    # with open('samplePdfDataframe.pkl','wb') as fp:
    #     pickle.dump(dataframe)

    # saving dataframe in excel format
    #diDF['pdfTab'].to_excel("E:/output.xlsx")


    # testing setMargin
    # fNm = '07-12-18 initial board profiles final_1-2'
    # provider = 'CA065'
    # pdfInfoJson = json.load(open('../pdfMappings/CA065/pdfInfo.json'))
    # singleValueJson = json.load(open('../pdfMappings/CA065/singleValue.json'))
    # pdfSections, recordDi, recordToPdfPg, pagebbox = extractPdfInfoWrapper(fNm, '0', pdfInfoJson)
    # out = sampleRunSV(singleValueJson, recordDi, pagebbox, pdfSections, False)
    # print('NAME_LAST = ' + out['NAME_LAST'])
    # expectedVal = {'NAME_LAST':'Bradford'}
    # xMargin, yMargin = setMarginSV(expectedVal, singleValueJson, recordDi, pagebbox, pdfSections)
    # out = sampleRunSV(singleValueJson, recordDi, pagebbox, pdfSections, False)
    # print('NAME_LAST = ' + out['NAME_LAST'])

    # testing ui json structures and testing extraction using these jsons
    #fNm = '07-12-18_initial_board_profiles_final_1-6'
    fNm = 'TIN_ADD__TIN_TERM'
    #fNm = 'Lindsay_Sarah_Profile'
    #fNm = 'EwaidaNader_Other_Other_20180913T135209.593'
    #fNm = 'cocalismd_Other_Other_20180827T110241.049'
    provider = 'CA010'
    token = '0'
    originalPageDi, modifiedPageDi, modifiedPageTextlineDi, origPagebbox, pagebbox = newExtractXmlPageDi(fNm, token)
    #json = jsonBackendToUiWrapper(provider)
    #allJsons = jsonUiToBackendWrapper(json)
    allJsons = getAllJsonsMongoDB(provider)[1]
    diDf = processPdfWrapper(token, allJsons, modifiedPageDi, originalPageDi, pagebbox, origPagebbox)
    print(diDf)
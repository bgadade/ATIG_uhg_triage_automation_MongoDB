# creating training dataset for triage ner

import re
import lxml.html
import constants

def getTag(triageColumn, tagType=1):
    '''
    :param triageColumn: str: mapped column (NAME_LAST, PRIM_PLSV@ADDRESS_LINE_1@0)
    :param tagType: int: 1/2/3
    :return: tag
    tagetype:1- <PRIM_PLSV__ADDRESS_LINE_1>
             2- <ADDRESS_LINE_1>
             3- <PRIM_PLSV>
    '''
    if '@' not in triageColumn: # singleValue
        return triageColumn
    elif tagType == 1:
        return triageColumn.rsplit('@',1)[0].split('#')[0].replace('@','__')
    elif tagType == 2:
        return triageColumn.split('@')[1].split('#')[0]
    elif tagType == 3:
        return triageColumn.split('@')[0]

def getContinuousId(valLst, tag, idLst):
    if not valLst[1]:    # handling valLst = ['',None]
        return {}
    else:
        key = valLst[1][0][0]
        idStIdx = idLst.index(key)
        di = {key:[[valLst[1][0]],tag]}
        count = 1
        for lst in valLst[1][1:]:
            if lst[0] == idLst[idStIdx+count]:
                di[key][0].append(lst)
                count += 1
            else:
                key = lst[0]
                idStIdx = idLst.index(key)
                count = 1
                di = {key:[[lst],tag]}
        return di


def tagOneDocument(tagType, htmlData, userMapping):
    # parsing html
    doc = lxml.html.fromstring(htmlData)
    divElms = doc.xpath('//div[contains(@style, "border-style: ridge;overflow: hidden; position: relative; background-color: white;")]/div')
    idLst = [elm.attrib['id'] for elm in divElms]

    # modifying structure of userMapping- collecting consecutive words with same tag
    modifiedUserMapping = {}
    for recDi in userMapping:
        if tagType != 3:
            for tag,val in recDi['sv'].items():
                modifiedUserMapping.update(getContinuousId(val, tag, idLst))
        for pCat, pCatDi in recDi['mv'].items():
            for tag, val in pCatDi.items():
                modifiedUserMapping.update(getContinuousId(val, tag, idLst))

    # initialization
    ixPg = 0
    ixLn = 0
    i = 0
    text = '<document>'

    # tag logic
    while i < len(divElms):
        id = divElms[i].attrib.get('id')
        pgId, lnId, subLnId, clustId, wrdId = [int(x) for x in id.split('|')]
        if ixPg < pgId: # keeping track of page; initializing line count accordingly
            ixPg = pgId
            ixLn = 0
        if ixLn < lnId:  # keeping track of line id; adding newline character accordingly
            ixLn = lnId
            text += '<newLine>'  # adjusting space and newline characters
        if id in modifiedUserMapping:
            triageColumn = modifiedUserMapping[id][1]
            tag = getTag(triageColumn,tagType)
            lst = modifiedUserMapping[id][0]
            length = len(lst)
            if length == 1:
                wrd = divElms[i].xpath('.//text()')[0]
                text += wrd[:lst[0][1]]+'<'+tag +'>'+wrd[lst[0][1]:lst[0][2]]+'</'+tag+'>'+wrd[lst[0][2]:]+' '
                i += 1
            elif length > 1:
                wrd = divElms[i].xpath('.//text()')[0] # first word
                text += wrd[:lst[0][1]] + '<' + tag + '>' + wrd[lst[0][1]:lst[0][2]]+' '
                for j in range(1,length-1):  # middle words
                    wrd = divElms[i+j].xpath('.//text()')[0]
                    text += wrd+' '
                wrd = divElms[i+length-1].xpath('.//text()')[0] # last word
                text += wrd[:lst[-1][2]]+'</'+tag+'>'+wrd[lst[-1][2]:]+' '
                i += length
        else:
            text += divElms[i].xpath('.//text()')[0]+' '
            i += 1
    text = re.sub(r'\s+', ' ', text).replace('<newLine>','\n')   # adjusting space and newline charaters
    text += '</document>\n'
    return text

def tagMain(tagType=1):
    lstFiles = constants.handler.find_documents('trainData')
    finalText = ''
    for doc in lstFiles:
        htmlData = doc['htmlData']
        userMapping = doc['userMapping']
        finalText += tagOneDocument(tagType, htmlData, userMapping)
    with open('../tmp/trainData.xml','w') as f:
        f.write(finalText.encode('utf-8'))

if __name__ == '__main__':
    # # read trainData json from disk
    # with open('C:/Users/SH388982/PycharmProjects/uhg_triage_automation_MongoDB/tmp/A.PerryDemo.pdf_trainData.json') as f:
    #     trainData = json.load(f)
    # htmlData = trainData['htmlData'].replace('\n', '')
    # userMapping = trainData['userMapping']
    tagMain()


from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter, XMLConverter, HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from io import BytesIO
import lxml.html
import re
import math

import constants

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

def addBboxToBlankTextTag(tree):
    t = list(zip(tree.xpath('//text[text()=" " and not(@bbox)]/preceding-sibling::text[1]'),
                 tree.xpath('//text[text()=" " and not(@bbox)]/following-sibling::text[1]')))
    t1 = [(tuple(map(float, tup[0].xpath('.//@bbox')[0].split(','))),
           tuple(map(float, tup[1].xpath('.//@bbox')[0].split(',')))) for tup in t]
    t2 = [(round(tup[0][2] + 0.1, 2), tup[0][1], round(tup[1][0] - 0.1, 2), tup[0][3]) for tup in t1]
    for blankTextTag,newBbox in zip(tree.xpath('//text[text()=" "]'),t2):
        blankTextTag.attrib['bbox']=','.join([str(elm) for elm in newBbox])
    return tree

def parseMultipleOrig(xml,pages=None,selKey='textPg',text=True):
    tree = lxml.html.fromstring(xml)
    tree=addBboxToBlankTextTag(tree)
    pageTg = findPgTag(tree)
    numOfPages = len(pageTg)
    pagebboxStr = pageTg[0].xpath("normalize-space(.//@bbox)")
    pagebbox = [float(num) for num in pagebboxStr.split(',')]
    pageDi={}
    if pages:
        for pgNo in pages:
            parsed = parseTree(pageTg[pgNo-1], selKey='textPg', tagAttribAndText=True, qry=".//text")
            pageDi.update({pgNo:parsed})
    else:
        for pgNo in range(1,numOfPages+1):
            parsed = parseTree(pageTg[pgNo-1], selKey='textPg', tagAttribAndText=True, qry=".//text")
            pageDi.update({pgNo:parsed})
    return pageDi, pagebbox


def parseMultiple(xml,pages=None,selKey='textPg',text=True):
    tree = lxml.html.fromstring(xml)
    tree=addBboxToBlankTextTag(tree)
    pageTg = findPgTag(tree)
    numOfPages = len(pageTg)
    pagebboxStr = pageTg[0].xpath("normalize-space(.//@bbox)")
    pagebbox = [float(num) for num in pagebboxStr.split(',')]
    if pages:
        for pgNo in pages:
            parsed = parseTree(pageTg[pgNo-1], selKey='textPg', tagAttribAndText=True, qry=".//text")
            yield (parsed,pagebbox)
    else:
        for pgNo in range(1,numOfPages+1):
            parsed = parseTree(pageTg[pgNo-1], selKey='textPg', tagAttribAndText=True, qry=".//text")
            yield (parsed,pagebbox)


def queryXml(tree,sel,text=False,tagAttrib=False,tagAttribAndText=False):
    lstElm=tree.xpath(sel)
    diElm={}
    for elm in lstElm:
        if elm.attrib:
            bboxStr=elm.xpath("normalize-space(.//@bbox)")#"69.000,708.401,562.163,723.401"
            bbox=tuple([float(x) for x in bboxStr.split(',')])
            if text:
                text = "".join(elm.xpath('.//text()'))
                text = re.sub('\n+', '', text)
                diElm[bbox]=text.strip()
            else:
                if tagAttrib:
                    diElm[bbox] = (elm.tag,elm,dict(elm.attrib))
                elif tagAttribAndText:
                    diElm[bbox] = (elm.tag, elm, dict(elm.attrib),elm.text)
                else:
                    diElm[bbox] = (elm.text)
    return diElm

def parseTree(tree,selKey='textline',sort=True,text=False,qry=None,tagAttribAndText=False):
    # if not qry:
    #     qry=constants.diSel[selKey].format(int(pageNo))
    diTags = queryXml(tree, qry,text=text,tagAttribAndText=tagAttribAndText)
    if sort:
        srt = sorted(zip(list(diTags.keys()), list(diTags.values())), key=lambda tup: (-tup[0][1],tup[0][0]))
        return srt
    return diTags

def findPgTag(tree):
    return tree.xpath("//page")

def determineOverlap(bbx1,bbx2):
    if (bbx1[1] <= bbx2[1] < bbx2[3] <= bbx1[3] or bbx2[1] <= bbx1[1] < bbx1[3] <=bbx2[3]) \
            or (bbx1[1] <= bbx2[3] <= bbx1[3] and not (bbx1[1] <= bbx2[1] <= bbx1[3]) and (((bbx2[3] - bbx1[1]) / (bbx1[3] - bbx1[1])) > 0.5 or ((bbx2[3] - bbx1[1]) / (bbx2[3] - bbx2[1])) > 0.5)) \
            or (bbx1[1] <= bbx2[1] <= bbx1[3] and not (bbx1[1] <= bbx2[3] <= bbx1[3]) and (((bbx1[3] - bbx2[1]) / (bbx1[3] - bbx1[1])) > 0.5 or ((bbx1[3] - bbx2[1]) / (bbx2[3] - bbx2[1])) > 0.5)):
        return True
    return False
def wordAndTextline(diTags):

    def getModeVal(lstValues):
        if not lstValues:
            return 0
        di={}
        for val in lstValues:
            if val not in di:
                di.setdefault(val,[]).append(val)
            else:
                di[val].append(val)
        di={k:len(v) for k,v in list(di.items())}
        return sorted(list(di.items()),key=lambda tup:tup[1],reverse=True)[0][0]


    def getWords(lstTup):
        tmp=[tup for tup in lstTup if tup[1][3] and tup[1][3].strip()]
        if not tmp:
            return []
        # modeSpace=statistics.mode([tup2[0][0]-tup1[0][2] for tup1,tup2 in zip(tmp[0:-1],tmp[1:])])
        modeSpace=getModeVal([max(0,tup2[0][0]-tup1[0][2]) for tup1,tup2 in zip(tmp[0:-1],tmp[1:])])
        lstWrdBrk=[]
        for ix,tups in enumerate(zip(tmp[0:-1],tmp[1:])):
            tup1,tup2=tups[0],tups[1]
            tup1FontInfo=(tup1[1][2]['font'],tup1[1][2]['size'])
            tup2FontInfo=(tup2[1][2]['font'],tup2[1][2]['size'])
            if math.floor(tup2[0][0]-tup1[0][2])>modeSpace or tup1FontInfo!=tup2FontInfo:
                lstWrdBrk.append(ix+1)
        extLstWrdBrk=[0]+lstWrdBrk+[len(tmp)]

        lstWrdTuples=[]
        for st,end in zip(extLstWrdBrk[0:-1],extLstWrdBrk[1:]):
            lstWrdTuples.append(tmp[st:end])

        return lstWrdTuples

    def getNewBbx(lstTup):
        minY,maxY=min([tup[0][1] for tup in lstTup]),max([tup[0][3] for tup in lstTup])
        minX,maxX=min([tup[0][0] for tup in lstTup]),max([tup[0][2] for tup in lstTup])
        newBbx=(minX,minY,maxX,maxY)
        return newBbx

    diTextLine={}
    #diTags = sorted(diTags, key=lambda tup:tup[0][1])
    for bbx,tagTup in diTags:
        if tagTup[3]!=' ':
            if bbx not in diTextLine:
                brk=False
                for bbx1,lstTags in list(diTextLine.items()):
                    if determineOverlap(bbx,bbx1):
                        diTextLine.setdefault(bbx1,[]).append((bbx,tagTup))
                        brk=True
                        break
                if brk:
                    continue
                diTextLine.setdefault(bbx,[]).append((bbx,tagTup))

    diTextLineSrt={}
    for bbx,lstTup in list(diTextLine.items()):
        newBbx=getNewBbx(lstTup)
        diTextLineSrt[newBbx]=sorted(lstTup,key=lambda tup:tup[0])
    diWords={}
    for bbx,lstTup in list(diTextLineSrt.items()):
        wrds=getWords(lstTup)
        lstWrds=[]
        for wrd in wrds:
            newBbx = getNewBbx(wrd)
            text=''.join([tup[1][3] for tup in wrd])
            fontInfo=(wrd[0][1][2]['font'],wrd[0][1][2]['size'])
            lstWrds.append((newBbx,text,fontInfo))
        if lstWrds:
            correctBbx = getNewBbx(lstWrds)
            diWords[correctBbx]=lstWrds
    return sorted(list(diWords.items()), key=lambda tup:tup[0][1], reverse=True)

def modifyPageDiCoordinates(wordTextlineDi):
    modifiedPageDi = {}
    modifiedPageTextlineDi = {}
    for pgNm, textlineLst in wordTextlineDi.items():
        start = 20
        pgTextlineLst = []
        pgWordLst = []
        for line in textlineLst[::-1]:     # starting to modify coordinates from bottom
            newMinY = start
            newMaxY = round(start + (line[0][3] - line[0][1]), 3)  #start + lineWidth
            start = newMaxY+1
            pgTextlineLst.append( (line[0][0], newMinY, line[0][2], newMaxY) )
        pgTextlineLst = pgTextlineLst[::-1]
        for ix, line in enumerate(textlineLst):
            wordsFound = []
            for wordTup in line[1]:
                newBbox = (wordTup[0][0], pgTextlineLst[ix][1], wordTup[0][2], pgTextlineLst[ix][3])
                pgWordLst.append( (newBbox, wordTup[1]) )
                wordsFound.append(wordTup[1])
            pgTextlineLst[ix] = (pgTextlineLst[ix], ' '.join(wordsFound))
        modifiedPageDi[pgNm] = pgWordLst
        modifiedPageTextlineDi[pgNm] = pgTextlineLst
    return modifiedPageDi, modifiedPageTextlineDi

def getPdfWordsWrapper(xml):
    # diTags, origPagebbox = parseMultipleOrig(xml, selKey='textPg')
    # pagebbox = origPagebbox
    # wordTextlineOutDi = {}
    # for pgNm, diTag in diTags.iteritems():
    #     wordTextlineOutDi[pgNm] = wordAndTextline(diTag)
    diTags = parseMultiple(xml, selKey='textPg')
    wordTextlineOutDi = {}
    for idx, diTag in enumerate(diTags):
        wordTextlineOutDi[idx+1] = wordAndTextline(diTag[0])
    origPagebbox = diTag[1]
    pagebbox = origPagebbox
    # removing the empty textlines
    wordTextlineDi = {pgNm: [line for line in textlineLst if line[1]] for pgNm, textlineLst in wordTextlineOutDi.items() if textlineLst}
    # storing original pageDi
    originalPageDi = {pgNm: [(word[0], word[1]) for wordLst in textlineLst for word in wordLst[1]] for pgNm, textlineLst in wordTextlineDi.items()}
    # modifying coordinates, according to textline width
    modifiedPageDi, modifiedPageTextlineDi = modifyPageDiCoordinates(wordTextlineDi)
    maximumMaxYcoordinates = max([pgInfo[0][0][3] for pgNm, pgInfo in modifiedPageDi.items()])
    pagebbox[3] = max(origPagebbox[3], maximumMaxYcoordinates)
    return originalPageDi, modifiedPageDi, modifiedPageTextlineDi, origPagebbox, tuple(pagebbox)

if __name__ == '__main__':
    #fNm = 'ChintalaVijaya_Other_Other_20180913T135121.075'
    #fNm = '07-12-18 initial board profiles final_1-2'
    fNm = 'TIN_ADD__TIN_TERM'
    xml = convert_pdf_doc(constants.loc + fNm + '.pdf')
    originalPageDi, modifiedPageDi, modifiedPageTextlineDi, pagebbox = getPdfWordsWrapper(xml)
    print('done')
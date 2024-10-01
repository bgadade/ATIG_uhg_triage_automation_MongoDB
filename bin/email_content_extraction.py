import re
from bs4 import BeautifulSoup
import json
import lxml.html
import traceback
import json

def bs_preprocess(html):
    """remove distracting whitespaces and newline characters"""
    pat = re.compile('(^[\s]+)|([\s]+$)', re.MULTILINE)
    html = re.sub(pat, '', html)  # remove leading and trailing whitespaces
    html = re.sub('\n', ' ', html)  # convert newlines to spaces
    # this preserves newline delimiters
    html = re.sub('[\s]+<', '<', html)  # remove whitespaces before opening tags
    html = re.sub('>[\s]+', '>', html)  # remove whitespaces after closing tags
    return html

def table_data(soup, contents):
    for tbl in soup.find_all("table"):
        if tbl.table == None:
            find_table_elemens(tbl, contents)
    return contents

def find_table_elemens(tbl, contents):
    # print "calling"
    for row in tbl.findAll("tr"):
        lst = []
        elements = row.findAll("td")
        for ele in elements:
            text = ele.get_text().strip()
            soup = BeautifulSoup(text, "html.parser")
            lst.append(soup.get_text().strip())
        if len(lst) > 1:
            if lst[0] != "":
                lst[0] = lst[0].replace(":", "")
                contents[lst[0]] = lst[-1]
        else:
            contents["Body"] = lst[0]
        # print lst

#----------------------------------MAIN----------------------------------#
# def main(filename):
#     dataPath = "../input/"+filename
#     html = open(dataPath).read()
#     # html = bs_preprocess(html)
#     soup = BeautifulSoup(html, "html.parser")
#     # print soup.prettify()
#     contents = table_data(soup, {})
#
#     with open('../data/email_dictionary.json', 'wb') as outfile:
#         json.dump(contents, outfile)
#     return contents
#     # print contents
#     # for k,v in contents.items():
#     #     print k, " = ", v

def main(filename):
    dataPath = "../input/"+filename
    diSelectors = {
        "Sent" : "string(//tr[./td[contains(.,'Sent:')]]/td[2])",
        "From" : "string(//tr[./td[contains(.,'From:')]]/td[2])",
        "To":"string(//tr[./td[contains(.,'To:')]]/td[2])",
        "Subject" : "string(//tr[./td[contains(.,'Subject:')]]/td[2])",
        "Body" : "string(//textarea[@id='corrBody'])"
    }
    try:
        with open(dataPath) as fp:
            string = fp.read()
            dom = lxml.html.fromstring(string)
        diData = {}
        for tag, sel in list(diSelectors.items()):
            if tag == 'Body':
                interim = dom.xpath(sel)
                interimClean = re.sub('^\\<.*?\\>', "", interim)
                dom2 = lxml.html.fromstring(interimClean)
                tag1 = re.search('^\\<(.*?)\\>', lxml.html.tostring(dom2)).group(1).split()[0]
                if tag1 == "html":
                    tag1 = "body"
                diData[tag] = dom2.xpath("string(//{})".format(tag1))
            else:
                diData[tag] = dom.xpath(sel)
        with open('../data/email_dictionary.json', 'wb') as outfile:
            json.dump(diData, outfile)
        return diData
    except Exception as e:
        print("******************************", e)
        traceback.print_exc()
# main("../emails/sample_3.htm")

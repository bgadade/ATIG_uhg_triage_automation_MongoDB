import classes as fwork
import numpy as np
from datetime import datetime
import pandas as pd
import mvDerivation_Common
import re




def distinct(lst, inp):
    """gets the unique value from the columns"""
    # print lst[1]
    # print "Inside distinct"
    df = lst[0].populate(inp)
    cn = lst[1].populate(inp)
    if cn in df.columns:
        if type(df[cn].unique()) is np.ndarray:
            return df[cn].unique()[0]
        else:
            return df[cn].unique()
    else:
        return None


def getValue(lst, inp):
    df = lst[0].populate(inp)
    cn = lst[1].populate(inp)

    return df[cn]


def isFound(lst, inp):
    """checks if the particular column is found in the dataframe"""
    df = lst[0].populate(inp)
    cn = lst[1].populate(inp)
    input_col = [col for col in df.columns if cn in col]
    # print "Inside isfound"
    print(len(input_col))
    return len(input_col)


def ifFunc(lst, inp):
    """Conditionally checks which branch of the tree gets executed"""
    # print "Inside iffunc"
    res = lst[0].populate(inp)
    if isinstance(res, str):
       res = int(res)

    if res and res > 0:  # check if the condition is true
        return lst[1].populate(inp)  # execute the true branch
    else:
        return lst[2].populate(inp)  # execute the false branch of the tree


def populateTarget(lst):
    tgt_df = lst[0]
    tgt_df[lst[1]] = lst[2]
    return tgt_df


def reject(lst, inp):
    """this method rejects the string"""
    return lst[0].populate(inp)


# Last Name functions
def andFunc(lst, inp):
    """checks for the attribute is present inside the dataframe"""
    # print "inside andfunc"
    if lst[0].populate(inp) and lst[1].populate(inp):
        return 1  # starndizing boolean as [1, 0]
    else:
        return None


def compareString(lst, inp):
    """This method compares and checks if we are refering to the same strings or diffrent """
    # print "inside compareString"
    # if len(lst[0].populate(inp)) > 1:
    #     print "more than 1 unique string", len(lst[0].populate(inp))
    if (lst[0].populate(inp).strip().lower() == lst[1].populate(inp).strip().lower()):
        return lst[0].populate(inp).strip().lower()
    else:
        return None


def isNotEmpty(lst, inp):
    # print "inside isNotEmpty"
    df = lst[0].populate(inp)
    cn = lst[1].populate(inp)

    #TOdo: changed by hari,
    if df[cn] and df[cn] is not np.nan and df[cn] is not None:
        return 1
    else:
        return None

def isEqualInt(lst, inp):
    # print "inside is equal"
    int1 = int(lst[0].populate(inp))
    int2 = int(lst[1].populate(inp))
    if int1 == int2:
        return 1
    else:
        return None

def isEqual(lst, inp):
    # print "inside is equal"
    str1 = lst[0].populate(inp)
    str2 = lst[1].populate(inp)
    if str1.strip().lower() == str2.strip().lower():
        return 1
    else:
        return None

def getDictValue(lst, inp):
    inDict = lst[0].populate(inp)
    paramdict = lst[1].populate(inp)
    key = paramdict["key"]
    lstidx = paramdict["lstidx"]
    name = paramdict["name"]
    print(key, " ", lstidx, " ", name)
    out = {}
    if lstidx == None:
        if name == None:
            return inDict[key]
        else:
            out[name] = inDict[key]
            return out
    else:
        if name == None:
            return inDict[key][lstidx]
        else:
            out[name] = inDict[key][lstidx]
            return out

def getDiffReal(lst, inp):
    d1 = lst[0].populate(inp)
    d2 = lst[1].populate(inp)

    try:
        d1 = datetime.strptime(d1, "%m/%d/%Y")
        d2 = datetime.strptime(d2, "%m/%d/%Y")
    except ValueError:
        return None
    return (d1 - d2).days

def getDiff(lst, inp):
    d1 = lst[0].populate(inp)
    d2 = lst[1].populate(inp)

    try:
        d1 = datetime.strptime(d1, "%m/%d/%Y")
        d2 = datetime.strptime(d2, "%m/%d/%Y")
    except ValueError:
        return None
    return abs((d2 - d1).days)

def isGreaterThen(lst, inp):
    try:
        return int(lst[0].populate(inp)) > int(lst[1].populate(inp))
    except:
        return 0

def isGreaterDate(lst, inp):
    date1 = lst[0].populate(inp)
    date1 = datetime.datetime.strptime(date1, '%m/%d/%Y').date()
    date2 = lst[1].populate(inp)
    date2 = datetime.datetime.strptime(date2, '%m/%d/%Y').date()
    days = date1 - date2
    if days > 0:
        return 1
    else:
        return 0



def listContains(lst, inp):
    parentlist = lst[0].populate(inp)
    val = lst[1].populate(inp)

    return val in parentlist

def orFunc(lst, inp):
    """checks for the attribute is present inside the dataframe"""
    # print "inside andfunc"
    if lst[0].populate(inp) or lst[1].populate(inp):
        return 1  # starndizing boolean as [1, 0]
    else:
        return None
def checkFormat(lst,inp):
    str1 = lst[0].populate(inp)
    str2 = lst[1].populate(inp)
    r = re.compile('.*/.*/.*:.*')
    if r.match('xx/xx/xxxx') :
        return 1
    else:
        return 0
ifW = fwork.fwrapper(ifFunc, 3, 'if')
isFoundW = fwork.fwrapper(isFound, 2, 'isFound')
getDistinctW = fwork.fwrapper(distinct, 2, 'distinct')
populateDFW = fwork.fwrapper(populateTarget, 3, 'populateTarget')
rejectW = fwork.fwrapper(reject, 1, 'reject')
andW = fwork.fwrapper(andFunc, 2, 'and')
compareStringW = fwork.fwrapper(compareString, 2, 'compareString')
isNotEmptyW = fwork.fwrapper(isNotEmpty, 2, 'isNotEmpty')
isEqualW = fwork.fwrapper(isEqual, 2, 'isEqual')
isEqualIntW = fwork.fwrapper(isEqualInt, 2, 'isEqualInt')
getValueW = fwork.fwrapper(getValue, 2, 'getValue')
getDiffW = fwork.fwrapper(getDiff, 2, 'getDiff')
getDiffRealW = fwork.fwrapper(getDiffReal, 2, 'getDiffReal')
isGreaterThenW = fwork.fwrapper(isGreaterThen, 2, 'isGreaterThen')
handleErrorW = fwork.fwrapper(mvDerivation_Common.ErrorMessage, 3, 'reject')
isGreaterDateW = fwork.fwrapper(isGreaterDate, 2, 'isGreaterDate')
listContainsW = fwork.fwrapper(listContains, 2, 'listContains')
orW = fwork.fwrapper(orFunc, 2, 'or')
checkFormatW =fwork.fwrapper(checkFormat, 2, 'checkFormatW')
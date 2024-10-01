import re
from nltk.metrics import edit_distance
import numpy as np
import traceback


# str = "What1 would you23 like to acronymize? "  # input("What would you like to acronymize? ")
def get_sub_words_from_camel_case(str):
    word_list = []
    str = (re.sub('\W+', '', str))
    # if not str in word_list:
    #     word_list.append(str.lower())

    # get acronym and sub words
    startIdx = 0
    idx = 0
    for c in str:
        if c.isupper():  # append cap letter

            if startIdx < idx:
                word = str[startIdx:idx]
                word_list.append([word.lower()])
                startIdx = idx

        idx += 1

    word = str[startIdx:idx]
    word_list.append([word])

    return  word_list

def make_word_combination(str):
    '''

    :param str:
    :return:
    '''

    dict_word_combination = {
        'word_list':[str],
        'sub_word_list':[]
    }
    # get acronym and subwords
    acronym = ""
    str = re.sub('_', ' ', str) # treat underscore as space

    if  len(str.split())>1: # if input is space separated e.g Date of birth
        # remove non alphanumeric characters
        str = (re.sub('\W+', ' ', str))
        if not str in dict_word_combination['word_list']:
            dict_word_combination['word_list'].append(str.lower())

        # get acronym and sub words
        for word in str.split():
            word_list = get_sub_words_from_camel_case(word)
            if len(word_list) == 0:
                acronym += word[0].upper() + re.sub('[^0-9]', '', word) # get 1st char as upper and numbers if available
                #  add subwords
                dict_word_combination['sub_word_list'].append([word.lower()])
            else:
                for word_splited in word_list:
                    acronym += word_splited[0][0].upper() + re.sub('[^0-9]', '',
                                                                word_splited[0])  # get 1st char as upper and numbers if available
                    #  add subwords
                    dict_word_combination['sub_word_list'].append([word_splited[0].lower()])

        if not acronym in dict_word_combination['word_list']:
            dict_word_combination['word_list'].append(acronym)

    else: #if single word with upper camel format "DateOfBirth"
        # remove non alphanumeric characters
        str = (re.sub('\W+', '', str))
        if not str in dict_word_combination['word_list']:
            dict_word_combination['word_list'].append(str.lower())

        # get acronym and sub words
        startIdx = 0
        idx = 0
        for c in str:
            if c.isupper(): # append cap letter
                acronym += c
                if startIdx < idx:
                    word = str[startIdx:idx]
                    dict_word_combination['sub_word_list'].append([word.lower()])
                    startIdx = idx
            if c.isdigit():# append number
                acronym += c
            idx += 1

        word = str[startIdx:idx]
        dict_word_combination['sub_word_list'].append([word])

    # generate possible words for extracted sub words

    for sub_word in dict_word_combination['sub_word_list']:
        sub_str = sub_word[0]
        # remove vowels
        if not sub_str:
            continue
        if len(sub_word)>0:
            try:
                sub_str = sub_str[0] + re.sub(r'[aeiou]', '', sub_str[1:])
            except:
                print(' *****************sub_str:', sub_str)
                traceback.print_exc()
                continue
        if not sub_str.lower() in sub_word:
            sub_word.append(sub_str.lower())
        # get acronym
        acronym_sub_word =  sub_str[0].upper() + re.sub('[^0-9]', '', sub_str)
        if not acronym_sub_word.lower() in sub_word:
            sub_word.append(acronym_sub_word.lower())

    return dict_word_combination

def get_sub_word_list(str):
    '''

    :param str:
    :return:
    '''
    str1 = re.sub('_', ' ', str)  # treat underscore as space
    sub_words = []
    if len(str1.split()) > 1:
        sub_words_splited = str1.split()
        for sub_word in sub_words_splited:
            word_list = get_sub_words_from_camel_case(sub_word)
            if len(word_list)>0:
                for word in word_list:
                    sub_words.append([word[0].lower()])
            else:
                sub_words.append([sub_word.lower()])

    else:  # check for camel case
        startIdx = 0
        idx = 0
        for c in str1:
            if c.isupper():  # check cap letter
                if startIdx < idx:
                    word = str[startIdx:idx]
                    sub_words.append([word.lower()])
                    startIdx = idx
            idx += 1
        sub_words.append([(str[startIdx:idx]).lower()]) # append last word

    return sub_words

def match_percent(str1, str2):
    '''

    :param str1:
    :param str2:
    :return:
    '''
    dist = edit_distance(str1.lower(), str2.lower())
    percent_match = 1 - (dist * 1.0 / np.max([len(str1), len(str2)]))
    return (percent_match, dist)

def match_sub_word(sub_words, dict_word_combination):
    thresh = 0.75

    matched_min_dict_sub_word = np.zeros(len(dict_word_combination['sub_word_list'])) # min distance matched
    len_sub_word = np.zeros(len(dict_word_combination['sub_word_list'])) # len of string which is matched

    is_mathched_dict_sub_word = np.zeros(len(dict_word_combination['sub_word_list'])) # track which dict sub word matched
    is_mathched_ip_sub_word = np.zeros(len(sub_words)) # track which i/p subword matched


    for idx,sub_word in enumerate(dict_word_combination['sub_word_list']):
        matched_min_dict_sub_word[idx]=len(sub_word[0])
        len_sub_word[idx]=len(sub_word[0])

    for idx1, ip_sub_word in enumerate(sub_words):  # compare input subwords with all dictionary sub words
        is_matched = False
        for idx2, sub_word_list in enumerate(dict_word_combination['sub_word_list']):
            if is_matched == True:
                break

            for sub_word in sub_word_list:
                percent_match = 0
                try:
                    if ip_sub_word[0] and sub_word:
                        (percent_match, dist) = match_percent(ip_sub_word[0], sub_word)
                except:
                    print("subwords-> ", ip_sub_word[0], sub_word)

                if percent_match>= 0.66 and dist < matched_min_dict_sub_word[idx2]:
                    matched_min_dict_sub_word[idx2] = dist
                    len_sub_word[idx2] = len(sub_word)

                if percent_match >= thresh:
                    is_mathched_dict_sub_word[idx2] = 1
                    is_mathched_ip_sub_word[idx1] = 1
                if percent_match == 1.0:
                    is_matched = True
                    break

    total_percent_match = 1-(sum(matched_min_dict_sub_word)/sum(len_sub_word))
    if np.sum(is_mathched_dict_sub_word) / len(is_mathched_dict_sub_word) >= 2 / 3\
            and np.sum(is_mathched_ip_sub_word) / len(is_mathched_ip_sub_word) >= 2 / 3:
        if total_percent_match>thresh:
            return (True,total_percent_match)
        else:
            return (False,total_percent_match)
    else:
        return (False,total_percent_match)

def match_word(str, dict_word_combination):
    '''

    :param str:
    :param dict_word_combination:
    :return:
    '''

    percent_match = 0.0
    thresh = 0.7
    max_percent_match = 0
    is_matched = False
    for word in dict_word_combination['word_list']:
        (percent_match, dist) = match_percent(str,word)
        if percent_match > max_percent_match:
            max_percent_match = percent_match
        if dist == 0: # if exact match , no need to check further
            is_matched = True
            break

    # get sub words from input string
    if is_matched == False:
        sub_words = get_sub_word_list(str)
        is_matched,percent_match = match_sub_word(sub_words,dict_word_combination)
        if percent_match > max_percent_match:
            max_percent_match = percent_match

    return (is_matched,max_percent_match)

def get_nearest_matching_word (word_list, ip_str):
    if isinstance(word_list, dict):
        diTokens = word_list
    else:
        diTokens = {}
        for word in word_list:
            dict_word_combination = make_word_combination(word)
            diTokens[word] = dict_word_combination
    score_dict = {}
    for word in word_list:
        is_match, score = match_word(ip_str, diTokens[word])
        score_dict.update({word: score})
    max_score_lst=[]
    max_score=0
    for key,val in list(score_dict.items()):
        if val>max_score:
            max_score=val
            max_score_lst=[]
            max_score_lst.insert(0,key)
            max_score_lst.insert(1,val)
    return score_dict,max_score_lst


if __name__=='__main__':
    # str = "DateOfBirth"
    # dict_word_combination = make_word_combination(str)
    #
    str1 = "Brth"#"Acc_Ben1Post97_PreA"
    # is_match, score=  match_word(str1, dict_word_combination)
    # print( "Match:" , is_match , " Score:",score)
    word_list = ['DateOfBirth', 'Date_of','of birth','Brth']
    # word_list = ['AccBen1Post97_PreA','Accured Ben1 Post97 PreA']
    score_dict, max_score_lst= get_nearest_matching_word(word_list,str1)
    print('score_dict:',score_dict)
    print('max_score_lst:',max_score_lst)
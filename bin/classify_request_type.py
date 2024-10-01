import re
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS as stopwords
import string
import en_core_web_sm
import constants

punctuations = string.punctuation

nlp = en_core_web_sm.load()

stopwords_new = list(stopwords)
stopwords_new.remove('move')
stopwords_new.remove('name')
stopwords_new.remove('a')

# baseClass_dict = {"TERM": ["deactivate", "inactive", "term", "delete", "remove", "close"],
#                   "ADD": ["add", "load", "new"],
#                   "CHANGE": ["update", "match", "change", "switch", "move"]}
#
# subClass_dict = {"TIN": ["tin", "tax", "provider", "physician", "practioner"],
#                  "ADDRESS": ["location", "address"],
#                  "DEMOGRAPHIC": ["demographic", "license", "billing_address", "phone", "fax",
# "credential", "category",
#                                  "directory", "affiliation", "school"],
#                  "SPECIALTY": ["specialty"],
#                  "CONTRACTUAL": ["contract", "fee", "panel", "grole", "commercial", "product", "fs"],
#                  "TIN TERM": ["resign"],
#                  "PRACTITIONER PROFILE IDS": ["medicare", "name"]}


def find_class(lst, class_dict):
    fetch_class = [key for word in lst for key in list(class_dict.keys()) if word in class_dict[key]]
    return set(fetch_class)

# def find_base_class(lst):
#     baseClass = [key for word in lst for key in baseClass_dict.keys() if word in baseClass_dict[key]]
#     return set(baseClass)
#
#
# def find_sub_class(lst):
#     subClass = [key for word in lst for key in subClass_dict.keys() if word in subClass_dict[key]]
#     return set(subClass)


# specialClass = ['DEMOGRAPHIC', 'CONTRACTUAL', 'TIN TERM', 'PRACTITIONER PROFILE']


def classify(content):
    content = re.sub(r'[^a-zA-Z0-9\s]', ' ', content)
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', content)  # split camel Case words
    content_with_split_camelCase_list = [m.group(0) for m in matches]
    content_with_split_camelCase = ' '.join(content_with_split_camelCase_list)
    content = content_with_split_camelCase.lower()
    content = str(content)

    doc = nlp(content)
    std_dict = constants.standardsDict
    tokens = [tok.lemma_.lower().strip() for tok in doc if tok.is_digit == False and tok.lemma_ != "-PRON-" and tok.text not in stopwords_new]
    tokens = [tok for tok in tokens if (tok not in stopwords_new and tok not in punctuations)]

    # print(tokens)

    baseClass = find_class(tokens, std_dict["baseClass_dict"])
    subClass = find_class(tokens, std_dict["subClass_dict"])

    finalClass = []
    if subClass and baseClass:
        finalClass = [s + " " + b if s not in std_dict["specialClass"] else s for b in baseClass for s in subClass]
    elif subClass:
        finalClass = [s+" ADD" if s not in std_dict["specialClass"] else s for s in subClass]
        # finalClass = list(subClass)
    elif baseClass:
        finalClass = ["TIN "+i for i in baseClass]
        # if "ADD" in baseClass:
        #     finalClass = ['TIN ADD']
        # else:
        #     finalClass = list(baseClass)

    # print(finalClass)

    # if len(finalClass) == 1:
    #     finalClass = ','.join([str(i) for i in finalClass])

    return finalClass


# def custom_Accuracy(true, predicted): #true and pedict are list
#     total = len(true)
#     correct = 0
#
#     for rec_true, rec_pred in zip(true, predicted):
#         correct_pred = False
#         for pred in rec_pred:
#             #             print(pred)
#             if pred.strip() == rec_true.strip():
#                 correct_pred = True
#                 correct += 1
#                 continue
#         if not correct_pred:
#             print("Incorrect: {} , {}".format(rec_true, rec_pred))
#     accuracy = correct / total * 100
#     print(accuracy)

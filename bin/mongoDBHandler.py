from pymongo import MongoClient
import constants
import json
import pickle as pickle
from bson.objectid import ObjectId

class MongoDBHandler:

    # initialize MongoDb connection
    def __init__(self):
        self.client = MongoClient(constants.mongoUrl)  ## setup mongodb on windows via cmd
        self.db = self.client.Triage

    def create_collection(self, collection_name):
        # function creates collection
        if collection_name not in self.db.list_collection_names():
            self.db.collection_name

    def get_collection(self, file_mapping):
        # function returns collection using dictionary constants.collection_file_mapping
        return constants.collection_file_mapping[file_mapping]

    def get_condition(self, collection, file_mapping, data = None, operation = None):
        # function returns a dictionary of condition required to perform mongoDB operation
        if collection == 'Elastic' or collection == 'Config' :
            condition = {file_mapping: {"$exists": True}}
        elif collection == 'Provider' :
            condition = {"Provider": data["Provider"], "Type": data["Type"]}
        elif collection == 'User' :
            if file_mapping == 'ProviderInterim' :
                if operation == 'delete_many' or operation == 'find_many' :
                    condition = {"Provider": data["Provider"]}
                else :
                    condition = {"Provider": data["Provider"], "UserName": data["UserName"]}
            else :      # file_mapping = 'UserMapping'/'SpecialtyException'
                if operation == 'find_many' or operation == 'delete_many' :
                    condition = {"Type": file_mapping}
                else :
                    condition = None
        elif collection == 'PickleData' :
            if operation == 'find' or operation == 'find_many' :
                condition = {"Token":data["Token"],"Type":data["Type"]}
            elif operation == 'delete_many' :
                condition = {"Type":file_mapping}
            else :
                condition = None
        elif collection == 'Logs' :
            if operation == 'delete_many' or operation == 'find_many':
                condition = None
            else:
                condition = {"token":data["token"]}
        elif collection == 'reconciliation':
            if operation == 'find':
                condition = {"token": data["token"]}
            else:
                condition = None
        else :
            condition = None
        return condition

    # removing '.' from keys
    # function used with json.loads as json.loads(data, object_hook=remove_dot_key)
    def encode_dot_key(self,obj):
        for key in list(obj.keys()):
            new_key = key.replace(".", "_dot_")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    def decode_dot_key(self,obj):
        for key in list(obj.keys()):
            new_key = key.replace("_dot_",".")
            if new_key != key:
                obj[new_key] = obj[key]
                del obj[key]
        return obj

    def insert_one_document(self, file_mapping, data):
        collection = self.get_collection(file_mapping)
        self.create_collection(collection)
        pickled = ['pickledSvNlpLookup','pickledMvNlpLookup','InpDF','sotHeader','input']
        if file_mapping not in pickled:
            try:
                data = json.loads(json.dumps(data), object_hook=self.encode_dot_key)
            except:
                print('temp')
        condition = self.get_condition(collection, file_mapping, data)
        if condition == None :
            try:
                self.db[collection].insert_one(data)
            except Exception as e:
                print(file_mapping + ' ------ mongodb exception in inserting one document' + str(e))
        else :
            if not self.db[collection].count_documents(condition):
                try:
                    self.db[collection].insert_one(data)
                except Exception as e:
                    print(file_mapping + ' ------ exception in inserting mongoDBHandler' + str(e))
            else :
                print('file already exists, not inserting')

    def insert_many_documents(self, file_mapping, data):
        collection = self.get_collection(file_mapping)
        self.create_collection(collection)
        data = json.loads(json.dumps(data), object_hook=self.encode_dot_key)
        try :
            self.db[collection].insert_many(data)
        except Exception as e :
            print(file_mapping + ' ------ mongodb exception in inserting one document')

    def find_one_document(self, file_mapping, conditional_data = None):
        collection = self.get_collection(file_mapping)
        condition = self.get_condition(collection, file_mapping, conditional_data, 'find')
        document_file = self.db[collection].find_one(condition)
        document_file = {k: v for k, v in document_file.items() if k != '_id'}
        if not collection == 'PickleData':
            document_file = json.loads(json.dumps(document_file), object_hook=self.decode_dot_key)
        return document_file

    def find_documents(self, file_mapping, conditional_data = None):
        collection = self.get_collection(file_mapping)
        condition = self.get_condition(collection, file_mapping, conditional_data, 'find_many')
        document_file = self.db[collection].find(condition)
        document_file = [{k: v for k, v in each_document.items() if k != '_id'} for each_document in document_file]
        document_file = json.loads(json.dumps(document_file), object_hook=self.decode_dot_key)
        return document_file

    def replace_one_document(self, file_mapping, data):
        collection = self.get_collection(file_mapping)
        data = json.loads(json.dumps(data), object_hook=self.encode_dot_key)
        condition = self.get_condition(collection, file_mapping, data)
        try:
            # upsert = True : performs insert operation if no document matches replace condition
            self.db[collection].replace_one(condition, data, True)
        except Exception as e:
            print(e, ' mongodb : exception in replacing a document ')

    def delete_one_document(self, file_mapping, data=None):
        collection = self.get_collection(file_mapping)
        condition = self.get_condition(collection, file_mapping)
        try:
            self.db[collection].delete_one(condition)
        except Exception as e:
            print(file_mapping + ' ------ exception in deleting document' + str(e))

    def delete_many_documents(self, file_mapping, data=None):
        collection = self.get_collection(file_mapping)
        condition = self.get_condition(collection, file_mapping, data, 'delete_many')
        try:
            self.db[collection].delete_many(condition)
        except Exception as e:
            print(file_mapping + ' ------ exception in deleting document' + str(e))

    def find_n_documents(self, file_mapping, conditional_data=None):
        collection = self.get_collection(file_mapping)
        condition = self.get_condition(collection, file_mapping, conditional_data, 'find_many')
        document_file = self.db[collection].find(condition).limit(constants.userMapRecord)
        user_mapping_ids = self.db[collection].find(condition)
        totalCount = self.db[collection].count_documents(condition)
        user_mapping_ids = [str(ids['_id']) for ids in user_mapping_ids][:constants.userMapRecord]
        document_file_data = [{k: v for k, v in each_document.items() if k != '_id'} for each_document in document_file]
        document_file = json.loads(json.dumps(document_file_data), object_hook=self.decode_dot_key)
        return document_file, user_mapping_ids,totalCount

    def delete_by_id(self,file_mapping,_id):
        collection = self.get_collection(file_mapping)
        try:
            self.db[collection].delete_one({'_id': ObjectId(_id)})
        except Exception as e:
            print(file_mapping + ' ------ exception in deleting document by id' + str(e))
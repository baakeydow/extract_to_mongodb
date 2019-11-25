''' # ------- MongoCore ------- # '''
import json
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, UpdateOne, errors, DESCENDING, ASCENDING
from bson import json_util, objectid


class MongoCore:

    def __init__(self, uri, debug):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.debug = debug
        self.check_connect()

    def check_connect(self):
        try:
            self.client.server_info()
            print('[JupyterUser from JupyterHub in MongoCore] => Successfully Connected to DB')
            self.client.close()
        except errors.ServerSelectionTimeoutError as err:
            print(
                "%s%s" % ('[JupyterUser from JupyterHub in MongoCore] => Failed to connect to database:', err))
            exit()

    def get_client(self):
        return self.client

    def get_jup_db(self):
        return self.client['User_JupyterHub']

    def get_euronex_db(self):
        return self.client['cs_euronext_quandl']

    def get_forex_db(self):
        return self.client['forex_data']

    def find_in_coll_by_index(self, coll, cs_index, cs_sort):
        if cs_sort is None:
            cs_sort = 'CS_INDEX'
        if cs_index is None:
            results = coll.find({}).sort(cs_sort, DESCENDING)
        else:
            results = coll.find({'CS_INDEX': cs_index})
        to_send = []
        for data in results:
            to_send.append(json_util.dumps(data))
        return to_send

    def get_valid_data(self, data):
        cleaned_data = {}
        for key, value in dict(data).items():
            if key != '_id':
                cleaned_data[key.upper()] = value
        return cleaned_data

    def return_result_as_df(self, coll, cs_index, cs_sort):
        if cs_sort is None:
            cs_sort = 'CS_INDEX'
        if cs_index is None:
            results = coll.find({}).sort(cs_sort, DESCENDING)
        else:
            results = coll.find({'CS_INDEX': cs_index})
        if results is None:
            return None
        raw_data = []
        for frame in results:
            data = self.get_valid_data(frame)
            raw_data.append(data)
        return pd.DataFrame.from_dict(raw_data)

    def get_dict_from_df(self, df):
        to_save = []
        for i in df.index:
            to_save.append(json.loads(df.loc[i].to_json()))
        return to_save

    def save_many(self, db, collection, data_as_list):
        try:
            coll = self.client[db][str(collection).upper()]
            coll.create_index(
                [('CS_INDEX', ASCENDING)], name='CS_INDEX', unique=True)
            if data_as_list is None:
                return
            if isinstance(data_as_list, pd.DataFrame):
                data_as_list = self.get_dict_from_df(data_as_list)
            try:
                bulk_write_list = []
                for raw_data in data_as_list:
                    data = dict(raw_data)
                    cs_index = data.get('CS_INDEX', None)
                    if cs_index is None:
                        data['CS_INDEX'] = datetime.timestamp(datetime.now())
                        bulk_write_list.append(
                            UpdateOne(data, {'$set': data}, upsert=True))
                    else:
                        _id = data.get('_id', None)
                        if _id is not None:
                            del data['_id']
                        del data['CS_INDEX']
                        bulk_write_list.append(UpdateOne({'CS_INDEX': int(cs_index)}, {
                            '$set': data}, upsert=True))
                    if len(bulk_write_list) == 1000:
                        result = coll.bulk_write(bulk_write_list, ordered=True)
                        bulk_write_list = []
                if len(bulk_write_list) > 0:
                    result = coll.bulk_write(bulk_write_list, ordered=True)
                if self.debug:
                    print(
                        "[JupyterUser from JupyterHub in MongoCore]:(save) => -> {0}".format(result.bulk_api_result))
            except errors.BulkWriteError as bwe:
                print(bwe.details)
                raise bwe
        except Exception as e:
            print("[JupyterUser Error]:(save) => -> {0}".format(e))

    def save_one(self, db, collection, raw_data):
        try:
            coll = self.client[db][str(collection).upper()]
            if raw_data is None:
                return
            if isinstance(raw_data, pd.DataFrame):
                raw_data = self.get_dict_from_df(raw_data)
            data = dict(raw_data)
            try:
                cs_index = dict(data).get('CS_INDEX', None)
                if cs_index is None:
                    data['CS_INDEX'] = datetime.timestamp(datetime.now())
                _id = dict(data).get('_id', None)
                if _id is not None:
                    del dict(data)['_id']
                data['_id'] = objectid.ObjectId()
            except Exception as e:
                print(e)
            coll.create_index(
                [('CS_INDEX', ASCENDING)], name='CS_INDEX', unique=True)
            result = coll.insert_one(data)
            if (result is None):
                return None
            if self.debug:
                print('[JupyterUser from JupyterHub in MongoCore]:(save_one) => inserted_id: {0}'.format(
                    result.inserted_id))
        except errors.PyMongoError as e:
            print("[JupyterUser Error]:(save_one) => -> {0}".format(e))

    def update_one(self, db, collection, query, raw_data):
        try:
            coll = self.client[db][str(collection).upper()]
            if raw_data is None:
                return
            if isinstance(raw_data, pd.DataFrame):
                raw_data = self.get_dict_from_df(raw_data)
            data = dict(raw_data)
            _id = dict(data).get('_id', None)
            if _id is not None:
                del dict(data)['_id']
            result = coll.update_one(query, {'$set': dict(data)}, upsert=True)
            if (result is None):
                return None
            if self.debug:
                print('[JupyterUser from JupyterHub in MongoCore]:(update_one) => matched_count: {0}'.format(
                    result.matched_count))
        except errors.PyMongoError as e:
            print("[JupyterUser Error]:(update_one) => -> {0}".format(e))

    def get(self, db, collection, query):
        try:
            coll = self.client[db][str(collection).upper()]
            result = []
            if query is None:
                for doc in coll.find():
                    result.append(doc)
                if self.debug:
                    print(
                        '[JupyterUser from JupyterHub in MongoCore]:(get_all) => {0}'.format(len(result)))
            else:
                for doc in coll.find(query):
                    result.append(doc)
            if result is None or len(result) == 0:
                return None
            return result
        except errors.PyMongoError as e:
            print("[JupyterUser Error]:(get) => -> {0}".format(e))

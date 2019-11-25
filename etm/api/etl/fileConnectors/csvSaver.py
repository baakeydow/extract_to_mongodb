''' everything csv related'''
import json
import pandas as pd
from pymongo import UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from pathlib import Path
from termcolor import colored
from etm.api import db_manager
from etm.api.etl.fileConnectors.index import extractor
from etm.api import LOG


class CsvSaver():

    def __init__(self, file_name):
        self.file_path = file_name
        self.db_name = extractor.get_db_name_from_fs(file_name)

    def update_docs(self, df, mongo, collection_name):
        bulk_write_list = list()
        for i in df.index:
            row_record = json.loads(df.loc[i].to_json())
            index_str = dict(row_record).get('CS_INDEX', None)
            if index_str is None:
                # append documents
                bulk_write_list.append(
                    UpdateOne(dict(row_record), {'$set': dict(row_record)}, upsert=True))
            elif str(index_str).isdigit() and int(index_str) == i:
                # update existing documents or append otherwise
                del dict(row_record)['CS_INDEX']
                bulk_write_list.append(
                    UpdateOne({'CS_INDEX': int(index_str)}, {'$set': dict(row_record)}, upsert=True))
                index_str = None
            if len(bulk_write_list) == 1000:
                result = mongo[collection_name].bulk_write(
                    bulk_write_list, ordered=True)
                LOG.info(
                    "big csv update -> {0}".format(result.bulk_api_result))
                bulk_write_list = []
        if len(bulk_write_list) > 0:
            result = mongo[collection_name].bulk_write(
                bulk_write_list, ordered=True)
            LOG.info(
                "end of collection update from csv -> {0}".format(result.bulk_api_result))

    def handle_csv(self):
        collection_name = None
        records = None
        error = 0
        try:
            df = self.get_clean_csv_df()
            if df is None:
                error = 1
                raise Exception
            collection_name = extractor.get_valid_string(
                Path(self.file_path).stem)
            records = json.loads(df.T.to_json())
            mongo = db_manager.get_client_instance_with_db_name(self.db_name)
            if db_manager.coll_exists(self.db_name, collection_name) == True:
                LOG.info(
                    'attempting to update collection -> {0}'.format(collection_name))
                try:
                    self.update_docs(df, mongo, collection_name)
                except BulkWriteError as bwe:
                    print(bwe.details)
            else:
                LOG.info(
                    'attempting to create collection -> {0}'.format(collection_name))
                if ('CS_INDEX' in df.columns.values):
                    mongo[collection_name].create_index(
                        [('CS_INDEX', ASCENDING)], name='CS_INDEX', unique=True)
                mongo[collection_name].insert_many(list(records.values()))
        except Exception as e:
            error = e
            if collection_name is not None:
                LOG.critical(
                    'failed to save to collection: {0}'.format(collection_name))
            if records is not None:
                LOG.critical('-> data not saved !!!! => {0}'.format(
                    json.dumps(records, indent=4, sort_keys=True)))
            LOG.critical('ETM_CSV_ERROR -> {0}'.format(e))
        finally:
            if error == 0 and collection_name is not None:
                LOG.info(
                    colored('--------------- CSV successfully saved in ' + self.db_name + ' database to collection: ' + collection_name + ' --- \\o/ supa dupa dope !!! :) ', 'green'))

    def get_clean_csv_df(self):
        missing_values = ["n/a", "na", "--", 'Null', None, ""]
        try:
            df_with_unnamed = pd.read_csv(
                self.file_path, encoding='utf-8', na_values=missing_values)
            df_with_unnamed.columns = extractor.keys_replacement(
                df_with_unnamed)
            df_with_nan_values = df_with_unnamed.loc[:, ~df_with_unnamed.columns.str.contains('^Unnamed')]  # noqa
            valid_df = df_with_nan_values.dropna(axis=0, how='any')
            valid_df['CS_INDEX'] = list(range(len(valid_df.index)))
            LOG.info('final csv keys: {0}'.format(valid_df.columns.values))
            return valid_df
        except Exception as e:
            LOG.critical('Failed getting dataFrame from csv: {0}'.format(e))
            return None

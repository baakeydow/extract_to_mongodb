''' everything excel related'''
import json
import pandas as pd
from pymongo import UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from termcolor import colored
from etm.api import db_manager
from etm.api.etl.fileConnectors.index import extractor
from etm.api import LOG


class XlSaver():

    def __init__(self, file_name):
        self.file_path = file_name
        self.db_name = extractor.get_db_name_from_fs(file_name)

    def sheet_is_not_parsable(self, sheet):
        if sheet.isdigit() != True and (sheet not in ["ENTITIES", "SCENARIOS"]) and sheet.find('REF_') == -1 and sheet.find('MAP_') == -1:
            return True
        return False

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
                    "big excel update -> {0}".format(result.bulk_api_result))
                bulk_write_list = []
        if len(bulk_write_list) > 0:
            result = mongo[collection_name].bulk_write(
                bulk_write_list, ordered=True)
            LOG.info(
                "end of collection update from excel-> {0}".format(result.bulk_api_result))

    def save_sheet_to_db(self, sheet):
        collection_name = None
        records = None
        error = 0
        if self.sheet_is_not_parsable(sheet):
            return
        LOG.info('parsing sheet: {0}'.format(sheet))
        try:
            df = self.get_clean_xl_df(sheet)
            if df is None:
                error = 1
                raise Exception
            collection_name = extractor.get_valid_string(sheet)
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
                mongo[collection_name].insert(list(records.values()))
        except Exception as e:
            error = e
            if collection_name is not None:
                LOG.critical(
                    'failed to save to collection: {0}'.format(collection_name))
            if records is not None:
                LOG.critical('-> data not saved !!!! => {0}'.format(
                    json.dumps(records, indent=4, sort_keys=True)))
            LOG.critical('ETM_SAVE_SHEET_ERROR -> {0}'.format(e))
        if error == 0 and collection_name is not None:
            LOG.info(
                colored('--------------- Sheet successfully saved in ' + self.db_name + ' database to collection: ' + collection_name + ' --- \\o/ supa dupa dope !!! :) ', 'green'))

    def handle_excel(self):
        xl_file = pd.ExcelFile(self.file_path)
        sheets_array = xl_file.sheet_names
        LOG.info('sheets_array => {0}'.format(sheets_array))
        for sheet in sheets_array:
            self.save_sheet_to_db(sheet)

    def get_clean_xl_df(self, sheet):
        try:
            skip_rows = 0
            if sheet in ["ENTITIES", "SCENARIOS"]:
                skip_rows = 1
            try:
                df_with_unnamed = pd.read_excel(
                    self.file_path, sheet_name=sheet, skiprows=skip_rows,  encoding='utf-8')
            except Exception as e:
                LOG.critical('Failed getting dataFrame: {0}'.format(e))
                return None
            df_with_unnamed.columns = extractor.keys_replacement(
                df_with_unnamed)
            df_with_nan_values = df_with_unnamed.loc[:, ~df_with_unnamed.columns.str.contains('^Unnamed')]  # noqa
            valid_df = df_with_nan_values.dropna(axis=1, how='any').copy()
            valid_df['CS_INDEX'] = list(range(len(valid_df.index)))
            LOG.info('final keys: {0}'.format(valid_df.columns.values))
            return valid_df
        except Exception as e:
            LOG.critical('Failed cleaning dataFrame from excel: {0}'.format(e))
            return None

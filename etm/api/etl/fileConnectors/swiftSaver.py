''' everything mt-940 related'''
import json
import mt940
import pprint
from pathlib import Path
from termcolor import colored
from etm.api import db_manager
from etm.api.etl.fileConnectors.index import extractor
from etm.api import LOG


class SwiftSaver():

    def __init__(self, file_name):
        self.file_path = file_name
        self.db_name = 'cs_swift_data'

    def handle_mt940(self):
        try:
            error = 0
            collection_name = extractor.get_valid_string(
                Path(self.file_path).stem)
            transactions = mt940.parse(self.file_path)
            if transactions.data is not None and len(transactions.data) != 0:
                # LOG.info('-> transactions => {0}'.format(
                #     json.dumps(transactions.data, indent=4, sort_keys=True, cls=mt940.JSONEncoder)))
                records = json.loads(json.dumps(
                    transactions.data, cls=mt940.JSONEncoder))
                mongo = db_manager.get_client_instance_with_db_name(
                    self.db_name)
                LOG.info(
                    'attempting to create collection -> {0}'.format(collection_name))
                LOG.info(
                    'saving mt940 format -> {0}'.format(records))
                mongo[collection_name].insert_one(records)
            else:
                error = 1
                LOG.critical(
                    'mt-940 library failed to parse file need custom parsing RTFM => file not saved !!!!! :=> {0}'.format(self.file_path))
        except Exception as e:
            error = e
            LOG.critical(
                'failed to save to collection: {0}'.format(collection_name))
            if extractor.is_valid_json(transactions.data):
                LOG.critical('-> data not saved !!!! => {0}'.format(
                    json.dumps(transactions.data, indent=4, sort_keys=True, cls=mt940.JSONEncoder)))
            LOG.critical('ETM_MT940_ERROR -> {0}'.format(error))
            raise SwiftSaverFileError(e)
        finally:
            if error == 0:
                LOG.info(
                    colored('--------------- mt940 successfully saved in ' + self.db_name + ' database to collection: ' + collection_name + ' --- \\o/ supa dupa dope !!! :) ', 'green'))


class SwiftSaverFileError(Exception):
    '''raise when failed to save the entire file'''

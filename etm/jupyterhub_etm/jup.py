import os
from dotenv import load_dotenv
from .core import MongoCore

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(ROOT_PATH, '.env')
load_dotenv(dotenv_path=env_path)

URI = os.getenv("LOCALE_MONGO_URI")


class Jup:

    def welcome(self):
        print('[JupyterUser from JupyterHub] => Welcome to your financial data pipeline, read the documentation')

    def __init__(self, debug=None):
        print('[RTFM]')
        self.uri = URI
        self.debug = True
        if debug is None or debug == False:
            self.debug = debug
        if self.uri is None:
            print('[JupyterUser from JupyterHub] => Failed to retrieve database instance uri')
            exit()
        self.mongo_core = MongoCore(self.uri, self.debug)

# ------- EURONEXT ------- #

    def get_euronext_tickers(self):
        return self.mongo_core.get_euronex_db().list_collection_names()

    def get_euronext_data(self, ticker, cs_index=None, return_value=None):
        euronex_db = self.mongo_core.get_euronex_db()
        coll_names = euronex_db.list_collection_names()
        if ticker in coll_names:
            coll = euronex_db[str(ticker).upper()]
            if return_value is None:
                to_send = self.mongo_core.find_in_coll_by_index(
                    coll, cs_index, 'CS_INDEX')
                if self.debug:
                    print('[JupyterUser from JupyterHub]:(get_euronext_data) => {0} found'.format(
                        len(to_send)))
                return to_send
            elif return_value == 'df':
                return self.mongo_core.return_result_as_df(coll, cs_index, 'CS_INDEX')
        return None

# ------- FOREX ------- #

    def get_forex_curr(self):
        return self.mongo_core.get_forex_db().list_collection_names()

    def get_forex_data(self, from_curr, to_curr, cs_index=None, return_value=None):
        forex_db = self.mongo_core.get_forex_db()
        coll_names = forex_db.list_collection_names()
        coll = from_curr + '_' + to_curr
        if coll in coll_names:
            coll = forex_db[str(coll).upper()]
            if return_value is None:
                to_send = self.mongo_core.find_in_coll_by_index(
                    coll, cs_index, 'CS_INDEX')
                if self.debug:
                    print('[JupyterUser from JupyterHub]:(get_forex_data) => {0} found'.format(
                        len(to_send)))
                return to_send
            elif return_value == 'df':
                return self.mongo_core.return_result_as_df(coll, cs_index, 'CS_INDEX')
        return None

# ------- CORE ------- #

    def cs_update_many(self, db, coll, raw_data):
        self.mongo_core.save_many('JupyterUser_{0}'.format(db), coll, raw_data)

    def cs_save_many(self, db, coll, raw_data):
        self.mongo_core.save_many('JupyterUser_{0}'.format(db), coll, raw_data)

    def cs_save_one(self, db, coll, raw_data):
        self.mongo_core.save_one('JupyterUser_{0}'.format(db), coll, raw_data)

    def cs_update_one(self, db, coll, query, raw_data):
        self.mongo_core.update_one('JupyterUser_{0}'.format(db), coll, query, raw_data)

    def cs_get(self, db, coll, query):
        return self.mongo_core.get('JupyterUser_{0}'.format(db), coll, query)

# ------------------------------------------------------------------------------------
# ----------------------------- 'Euronex Example' (dict) -----------------------------
# ------------------------------------------------------------------------------------
# import json
# from datetime import datetime
# from cashstory import Jup

# jup = Jup()

# def createDomain(project='finmarkets', data={}, domain=42_1337_42, history=None):
#     try:
#         if data is None:
#             return
#         if history is not None:
#             del data[:len(data) - history]
#         if data is not None:
#             print('HISTORY: {0}'.format(len(data)))
#             to_save = list()
#             for frame in data:
#                 data_view = json.loads(frame)
#                 data_view['SCENARIO'] = 'TODAY'
#                 data_view['ENTITY'] = 'Global'
#                 to_save.append(data_view)
#             jup.cs_save_many(project, domain, to_save)
#             print("------------> {0} data saved to ({1})".format(ticker, str(domain)))
#     except Exception as e:
#             print(e)

# tickers = jup.get_euronext_tickers()
# for ticker in tickers:
#     result = jup.get_euronext_data(ticker, None)
#     createDomain('finmarkets', result, 201, 10)
# print('----------------------------> Domain 201 created !')

# ---------------------------------------------------------------------------
# ----------------------------- Get & Save (df) -----------------------------
# ---------------------------------------------------------------------------

# import json
# import pandas as pd
# from cashstory import Jup

# _jup = Jup(True) # get jup instance

# df = _jup.get_euronext_data('EURONEXT_AC', None, 'df') # get pandas dataframe

# if df is not None:
#     _jup.cs_save_many('get_and_save_as_dataframe', 301, df) # save as pandas dataframe
#     df.head(10)

# -------------------------------------------------------------------------------
# ----------------------------- Get all Date (dict) -----------------------------
# -------------------------------------------------------------------------------

# import json
# from cashstory import Jup

# jup = Jup()

# tickers = jup.get_euronext_tickers()

# for ticker in tickers:
#     all_data = jup.get_euronext_data(ticker, None)
#     for data in all_data:
#         data_view = json.loads(data)
#         print('{0} date: {1}'.format(ticker, data_view['DATE']))

# -------------------------------------------------------------------------------
# ----------------------------- Get all Date (df) -------------------------------
# -------------------------------------------------------------------------------

# import json
# from cashstory import Jup

# jup = Jup()

# tickers = jup.get_euronext_tickers()

# for ticker in tickers:
#     df = jup.get_euronext_data(ticker, None, 'df')
#     for i in df.index:
#         print('{0} date: {1}'.format(ticker, df.loc[i].get('DATE')))

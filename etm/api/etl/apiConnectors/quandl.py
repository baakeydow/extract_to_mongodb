import time
import json
import schedule
import quandl
import pandas as pd
from pymongo import InsertOne, ASCENDING
from pymongo.errors import BulkWriteError
from datetime import datetime
from termcolor import colored
from etm.api import db_manager
from etm.api import LOG


class Quandl_task:
    QUANDL_API_KEY = 'TOTALLY_FAKE_API_KEY'

    TICKERS = [
        'EURONEXT/AC',
        'EURONEXT/AF',
        'EURONEXT/AI',
        'EURONEXT/AIR',
        'EURONEXT/ALD',
        'EURONEXT/ALO',
        'EURONEXT/ATE',
        'EURONEXT/ALT',
        'EURONEXT/AMUN',
        'EURONEXT/APAM',
        'EURONEXT/MT',
        'EURONEXT/AKE',
        'EURONEXT/ATO',
        'EURONEXT/CS',
        'EURONEXT/BIM',
        'EURONEXT/BNP',
        'EURONEXT/BOL',
        'EURONEXT/EN',
        'EURONEXT/BVI',
        'EURONEXT/CAP',
        'EURONEXT/CA',
        'EURONEXT/CO',
        'EURONEXT/CGG',
        'EURONEXT/CNP',
        'EURONEXT/COV',
        'EURONEXT/ACA',
        'EURONEXT/BN',
        'EURONEXT/DSY',
        'EURONEXT/DBV',
        'EURONEXT/EDEN',
        'EURONEXT/FGR',
        'EURONEXT/EDF',
        'EURONEXT/ELIOR',
        'EURONEXT/ELIS',
        'EURONEXT/ENGI',
        'EURONEXT/ERA',
        'EURONEXT/EL',
        'EURONEXT/RF',
        'EURONEXT/ERF',
        'EURONEXT/ENX',
        'EURONEXT/EUCAR',
        'EURONEXT/ETL',
        'EURONEXT/EO',
        'EURONEXT/GFC',
        'EURONEXT/GNFT',
        'EURONEXT/GET',
        'EURONEXT/ADP',
        'EURONEXT/FNAC',
        'EURONEXT/SK',
        'EURONEXT/GTT',
        'EURONEXT/RMS',
        'EURONEXT/ICAD',
        'EURONEXT/ILD',
        'EURONEXT/NK',
        'EURONEXT/ING',
        'EURONEXT/IPN',
        'EURONEXT/IPS',
        'EURONEXT/DEC',
        'EURONEXT/KER',
        'EURONEXT/LI',
        'EURONEXT/KORI',
        'EURONEXT/OR',
        'EURONEXT/MMB',
        'EURONEXT/LR',
        'EURONEXT/MC',
        'EURONEXT/MMT',
        'EURONEXT/MDM',
        'EURONEXT/MERY',
        'EURONEXT/ML',
        'EURONEXT/KN',
        'EURONEXT/NEO',
        'EURONEXT/NEX',
        'EURONEXT/NXI',
        'EURONEXT/ORA',
        'EURONEXT/ORP',
        'EURONEXT/RI',
        'EURONEXT/UG',
        'EURONEXT/POM',
        'EURONEXT/PUB',
        'EURONEXT/RCO',
        'EURONEXT/RNO',
        'EURONEXT/RXL',
        'EURONEXT/ROTH',
        'EURONEXT/RUI',
        'EURONEXT/SAF',
        'EURONEXT/SGO',
        'EURONEXT/SAN',
        'EURONEXT/DIM',
        'EURONEXT/SU',
        'EURONEXT/SCR',
        'EURONEXT/SESG',
        'EURONEXT/BB',
        'EURONEXT/GLE',
        'EURONEXT/SW',
        'EURONEXT/SOI',
        'EURONEXT/SOLB',
        'EURONEXT/SOP',
        'EURONEXT/SPIE',
        'EURONEXT/STM',
        'EURONEXT/SEV',
        'EURONEXT/TKTT',
        'EURONEXT/TCH',
        'EURONEXT/FTI',
        'EURONEXT/TEP',
        'EURONEXT/TFI',
        'EURONEXT/HO',
        'EURONEXT/FP',
        'EURONEXT/TRI',
        'EURONEXT/UBI',
        'EURONEXT/FR',
        'EURONEXT/VK',
        'EURONEXT/VIE',
        'EURONEXT/VCT',
        'EURONEXT/DG',
        'EURONEXT/VIV',
        'EURONEXT/MF',
        'EURONEXT/UL',
        'EURONEXT/WLN',
    ]

    def __init__(self):
        quandl.ApiConfig.api_key = self.QUANDL_API_KEY
        self.mongo = db_manager.get_client_instance_with_db_name(
            'cs_euronext_quandl')

    def get_data_to_save(self, date, value, ticker, raw_data):
        to_save = {}
        to_save['TICKER'] = ticker
        to_save['CS_INDEX'] = datetime.timestamp(datetime.now())
        to_save['DATE'] = datetime.fromtimestamp(
            int(date)/1000).strftime('%Y-%m-%d %H:%M:%S')
        to_save['OPEN'] = value
        to_save['HIGH'] = raw_data['HIGH'][date]
        to_save['LOW'] = raw_data['LOW'][date]
        to_save['LAST'] = raw_data['LAST'][date]
        to_save['TURNOVER'] = raw_data['TURNOVER'][date]
        to_save['VOLUME'] = raw_data['VOLUME'][date]
        return to_save

    def write_bucket(self, condition, coll, bulk_write_list):
        try:
            if condition:
                coll.bulk_write(bulk_write_list, ordered=True)
                bulk_write_list = []
            return bulk_write_list
        except BulkWriteError as bwe:
            print(bwe.details)

    def save_quandl_euronext(self, ticker, valid_data):
        try:
            error = 0
            df = pd.DataFrame(valid_data.items())
            coll_name = ticker.replace("/", "_")
            self.mongo[coll_name].create_index(
                      [('CS_INDEX', ASCENDING)], name='CS_INDEX', unique=True)
            valid_json = {}
            for i in df.index:
                row_record = json.loads(df.loc[i].to_json())
                valid_json[dict(row_record)['0']] = dict(row_record)['1']
            open_list = [x for x in valid_json['OPEN'].items()]
            open_list.sort(key=lambda x: x[0])
            open_list.reverse()
            bulk_write_list = list()
            for date, value in open_list:
                to_save = self.get_data_to_save(
                    date, value, ticker, valid_json)
                bulk_write_list.append(InsertOne(to_save))
                bulk_write_list = self.write_bucket(len(bulk_write_list) ==
                                                    1000, self.mongo[coll_name], bulk_write_list)
            bulk_write_list = self.write_bucket(
                len(bulk_write_list) > 0, self.mongo[coll_name], bulk_write_list)
        except Exception as e:
            error = e
            LOG.critical('-> Failed to save Quandl data  => {0}'.format(e))
        finally:
            if error == 0:
                LOG.info(
                    colored('-> Quandl data successfully extracted: {0}'.format(ticker), 'green'))

    def get_valid_data(self, data):
        cleaned_data = {}
        for key, value in dict(data).items():
            cleaned_data[key.upper()] = value
        return cleaned_data

    def save_quandle_data(self):
        for ticker in self.TICKERS:
            valid_data = self.get_valid_data(quandl.get(ticker))
            self.save_quandl_euronext(ticker, valid_data)

    def run(self):
        schedule.every().day.at("21:42").do(self.save_quandle_data)
        while 1:
            schedule.run_pending()
            '''Wait to tomorrow 00:00 am.'''
            t = time.localtime()
            t = time.mktime(t[:3] + (0, 0, 0) + t[6:])
            time.sleep(t + 24*3600 - time.time())


qdl_task = Quandl_task()


# https://www.quandl.com/data/EURONEXT-Euronext-Stock-Exchange => Euronext Stock Exchange


# SCENARIO = "TODAY" (force value) # added via jupyterhub
# ENTITY = "Global" (force value) # added via jupyterhub
# DATE = date
# OPEN = open
# HIGH = high
# LAST = last
# TURNOVER = turnover
# VOLUME = volume

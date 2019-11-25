import json
import schedule
import time
import re
from datetime import datetime
from pymongo import ASCENDING
from dateutil.parser import parse
from alpha_vantage.foreignexchange import ForeignExchange
from etm.api import LOG
from etm.api import db_manager


class AlphaVantage_task:
    ALPHA_V_API_KEY = 'TOTALLY_FAKE_API_KEY'

    FX_LIST = [
        'USD', 'GBP', 'AED', 'AED', 'BRL', 'GMD', 'CNY', 'JPY', 'INR', 'XOF', 'XAG', 'XAU'
    ]

    def __init__(self):
        self.cc = ForeignExchange(key=self.ALPHA_V_API_KEY)
        self.mongo = db_manager.get_client_instance_with_db_name(
            'forex_data')

    def get_valid__key_string(self, text):
        regex = re.compile(r"^[0-9]['.'][\s]")
        match = re.match(regex, text)
        if match is not None:
            cleaned = text.replace(match.group(), '')
            return cleaned.replace(' ', '_').upper()
        return text.replace(' ', '_').upper()

    def save_alpha_vantage_data(self):
        for to_curr in self.FX_LIST:
            try:
                clean_data = {}
                data, _ = self.cc.get_currency_exchange_rate(from_currency='EUR', to_currency=to_curr)  # noqa
                for key, value in dict(data).items():
                    valid_key = self.get_valid__key_string(key)
                    if valid_key == 'LAST_REFRESHED':
                        dt = parse(value)
                        clean_data[valid_key] = dt.timestamp()
                    else:
                        clean_data[valid_key] = value
                NOW = datetime.now()
                clean_data['CS_INDEX'] = datetime.timestamp(NOW)
                # print(json.dumps(clean_data, indent=4, sort_keys=True))
                self.mongo['EUR_' + to_curr].create_index(
                    [('CS_INDEX', ASCENDING)], name='CS_INDEX', unique=True)
                self.mongo['EUR_' + to_curr].insert_one(clean_data)
            except Exception as e:
                print(
                    '-> Failed to save Alpha_Vantage data  => {0}'.format(e))

    def run(self):
        schedule.every(5).minutes.do(self.save_alpha_vantage_data)
        while 1:
            schedule.run_pending()
            time.sleep(1)


av_task = AlphaVantage_task()

# EUR -> USD

# ------------- RAW Data ------------->

# {
#     "1. From_Currency Code": "EUR",
#     "2. From_Currency Name": "Euro",
#     "3. To_Currency Code": "USD",
#     "4. To_Currency Name": "United States Dollar",
#     "5. Exchange Rate": "1.11380000",
#     "6. Last Refreshed": "2019-05-30 16:04:44",
#     "7. Time Zone": "UTC",
#     "8. Bid Price": "1.11370000",
#     "9. Ask Price": "1.11400000"
# }

# ------------- CLEANED Data (saved in mongodb) ------------->

# {
#     "Ask_Price": "1.11700000",
#     "Bid_Price": "1.11680000",
#     "Exchange_Rate": "1.11690000",
#     "From_Currency_Code": "EUR",
#     "From_Currency_Name": "Euro",
#     "Last_Refreshed": 1559397322.0,
#     "Time_Zone": "UTC",
#     "To_Currency_Code": "USD",
#     "To_Currency_Name": "United States Dollar"
# }

# ------------- Transform Data via Jupyterhub and save it back to mongodb ------------->

# From_Currency_Code + '/' + To_Currency_Code = LABEL
# Exchange_Rate = VALUE
# Last_Refreshed = DATE

# construct PREDICT_1H

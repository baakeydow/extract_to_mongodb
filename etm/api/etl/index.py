import pandas as pd
import numpy as np
import os
import json
from etm import app
from etm.api.logger import LOG
from etm.api.app import mongo


class XlExtractor():
    'provide operations to get data from excel files'
    metaData = dict()

    def __init__(self, file_name):
        self.file_name = file_name
        root_path = os.path.join(os.environ.get(
            'ROOT_PATH'), 'public/old/data_samples/')
        self.file_path = os.path.join(
            root_path + file_name)
        self.xl_file = pd.ExcelFile(self.file_path)
        self.df = pd.read_excel(self.file_path)
        file = self.xl_file
        sheets = file.sheet_names
        if self.file_name not in self.metaData:
            self.metaData[self.file_name] = sheets

    def getMetaData(self):
        return self.metaData[self.file_name]

    def df_to_json(self, df):
        data_json = json.loads(df.to_json())
        LOG.info(json.dumps(data_json, indent=4, sort_keys=True))
        return data_json

    def iter_rows(self, df, label):
        LOG.info(len(df.index))
        for row in df.itertuples():
            LOG.info(getattr(row, label))

    def parse_file(self):
        with app.app_context():
            sheets_array = self.metaData[self.file_name]
            LOG.info('sheets_array => {0}'.format(sheets_array))
            for sheet in sheets_array:
                if sheet.isdigit():
                    df_with_unnamed = pd.read_excel(
                        self.file_path, sheet_name=sheet)
                    df = df_with_unnamed.loc[:, ~df_with_unnamed.columns.str.contains('^Unnamed')]  # noqa
                    records = json.loads(df.T.to_json()).values()
                    mongo.db.data.insert(records)


xl_object = XlExtractor("fake_data.xlsx")
xl_object.parse_file()
exit()

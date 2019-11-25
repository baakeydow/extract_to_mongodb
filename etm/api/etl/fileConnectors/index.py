import numpy as np
import json
from etm.api import LOG


class FileExtractor():
    'provide operations to get data from files'

    def is_valid_json(self, data):
        if data is None:
            return True
        elif isinstance(data, (bool, int, float)):
            return True
        elif isinstance(data, (tuple, list)):
            return all(self.is_valid_json(x) for x in data)
        elif isinstance(data, dict):
            return all(isinstance(k, str) and self.is_valid_json(v) for k, v in data.items())
        return False

    def get_db_name_from_fs(self, file_path):
        folderName = file_path.split("data_sources/", 1)[1].split('/')[0]
        if folderName is None:
            return 'data'
        dirName = self.get_valid_string(folderName)
        return dirName.lower()

    def df_to_json(self, df):
        data_json = json.loads(df.to_json())
        LOG.info(json.dumps(data_json, indent=4, sort_keys=True))
        return data_json

    def iter_rows(self, df, label):
        LOG.info(len(df.index))
        for row in df.itertuples():
            LOG.info(getattr(row, label))

    def get_valid_string(self, text):
        if str(text).isdigit():
            return str(text)
        for ch in ['null', '\\', '/', '\"', ' ', '`', '*', '?', '{', '}', '[', ']', '(', ')', '>', '#', '+', '.', '!', '$', '\'']:
            if ch in text:
                print(
                    'replacing wrong string the bitch !!!!!! => {0}'.format(text))
                text = text.replace(ch, '-')
        return text

    def parse_keys(self, headers):
        for index, item in enumerate(headers):
            headers[index] = self.get_valid_string(item)
        return headers

    def keys_replacement(self, df):
        LOG.info('keys found: {0}'.format(df.columns.values))
        columns = self.parse_keys(df.columns.values)
        LOG.info('keys replaced: {0}'.format(df.columns.values))
        return columns


extractor = FileExtractor()

# self.iter_rows(df, 'DOMAIN') old way
# metadata
# LOG.error(df.keys)
# LOG.error(df.values)
# LOG.error(df.items)

''' common utils to manage mongodb '''

from termcolor import colored
from pymongo import MongoClient, errors
from etm.api.env_utils import env


class DbManager():
    'mongodb main class for common db management operations'

    def __init__(self, uri):
        self.uri = uri
        self.client = MongoClient(
            self.uri, serverSelectionTimeoutMS=5000)
        self.dbName = 'etm_db'

    def get_client(self):
        return self.client

    def set_dbName(self, dbName):
        self.dbName = dbName

    def get_dbName(self):
        return self.dbName

    def db_exists(self, dbName):
        db_names = self.client.database_names()
        if dbName in db_names:
            return True
        return False

    def coll_exists(self, dbName, collName):
        coll_names = self.client[dbName].list_collection_names()
        if collName in coll_names:
            return True
        return False

    def set_uri(self, uri):
        self.uri = uri

    def get_uri(self):
        return self.uri

    def get_default_client_instance(self):
        return self.client[self.dbName]

    def get_client_instance_with_db_name(self, dbName):
        return self.client[dbName]

    def check_connect(self):
        try:
            self.client.server_info()
            print('Successfully Connected to: ' + colored(self.uri, 'green'))
            print(colored(self.client.database_names(), 'white', 'on_blue'))
            self.client.close()
        except errors.ServerSelectionTimeoutError as err:
            print(colored("%s%s" %
                          ('failed to connect to mongodb database:', err), 'red'))
            exit()


db_manager = DbManager(env.getEnvVar('MONGODB_URI'))

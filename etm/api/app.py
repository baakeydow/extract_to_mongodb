''' flask app with mongo '''
import os
import json
import datetime
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from flask_jwt_extended import JWTManager
from flask_pymongo import PyMongo, MongoClient, pymongo
from flask import Flask


class JSONEncoder(json.JSONEncoder):
    ''' extend json-encoder class'''

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, set):
            return list(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


def check_connect(client_uri):
    try:
        client = MongoClient(
            client_uri, serverSelectionTimeoutMS=5000)
        client.server_info()
        print(client.database_names())
        client.close()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print("%s%s" % ('failed to connect to mongodb database:', err))
        exit()


check_connect(os.environ.get('DB'))

# create the flask object
app = Flask(__name__)
app.config['MONGO_URI'] = os.environ.get('DB')
app.config['JWT_SECRET_KEY'] = os.environ.get('SECRET')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = datetime.timedelta(days=1)
mongo = PyMongo(app)
flask_bcrypt = Bcrypt(app)
jwt = JWTManager(app)
app.json_encoder = JSONEncoder

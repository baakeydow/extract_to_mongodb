''' main app declaration'''
import os
import datetime
import json
from flask import Flask, jsonify, make_response, send_from_directory
from flask_jwt_extended import JWTManager
from flask_bcrypt import Bcrypt
from bson.objectid import ObjectId
from etm.api.env_utils import env

PUBLIC_PATH = os.environ.get('PUBLIC_PATH')


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


root_api = Flask('etm-api', static_folder=PUBLIC_PATH)
root_api.config['APPLICATION_ROOT'] = '/api'
root_api.config['SECRET'] = env.getEnvVar('SECRET_TOKEN')
root_api.config['JWT_ACCESS_TOKEN_EXPIRES'] = datetime.timedelta(days=1)

flask_bcrypt = Bcrypt(root_api)
jwt = JWTManager(root_api)

root_api.json_encoder = JSONEncoder

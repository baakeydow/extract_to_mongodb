import os
import sys
from flask import jsonify, request, make_response, send_from_directory
from flask_jwt_extended import jwt_required, jwt_refresh_token_required
from etm import app
from etm.api.app import jwt
from etm.api.logger import LOG

PUBLIC_PATH = os.environ.get('PUBLIC_PATH')

LOG.error(os.path.dirname(sys.modules['__main__'].__file__))


@app.route('/')
def index():
    """ static files serve """
    return send_from_directory(PUBLIC_PATH, 'index.html')


@jwt.unauthorized_loader
def unauthorized_response(callback):
    return jsonify({
        'ok': False,
        'message': 'Missing Authorization Header'
    }), 401


@app.errorhandler(404)
def not_found(error):
    """ error handler """
    LOG.error(error)
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/admin')
def not_here():
    """ gtfo m8 """
    return send_from_directory(PUBLIC_PATH, '404.html')


@app.route('/<path:path>')
def static_proxy(path):
    """ static folder serve """
    file_name = path.split('/')[-1]
    dir_name = os.path.join(PUBLIC_PATH, '/'.join(path.split('/')[:-1]))
    return send_from_directory(dir_name, file_name)

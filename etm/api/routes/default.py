
''' default routing entrypoints '''
import os
from pathlib import Path
from flask_jwt_extended import jwt_required, jwt_refresh_token_required
from flask import Blueprint, jsonify, make_response, send_from_directory
from etm.api import LOG
from etm.api.controllers.user import auth_user, registerNew, refresh, user
from etm.api.routes.index import jwt, flask_bcrypt, PUBLIC_PATH

main_api = Blueprint('main_api', __name__)


@main_api.route('/', strict_slashes=False)
def serve():
    """ serve index entrypoint """
    index_file = Path(PUBLIC_PATH + '/frontend/index.html')
    print(index_file)
    if index_file.is_file() == False:
        return send_from_directory(PUBLIC_PATH, 'resources/lol.html')
    else:
        return send_from_directory(PUBLIC_PATH, 'frontend/index.html')


@main_api.route('/<path:path>')
def serve_static_files(path):
    """ serve static folders and resources """
    print(path)
    file_name = path.split('/')[-1]
    dest_dir = PUBLIC_PATH + '/frontend'
    # if (path.find('resources') != -1):
        # dest_dir = PUBLIC_PATH + '/frontend'
    assets_folder = os.path.join(dest_dir, '/'.join(path.split('/')[:-1]))
    print('serving [{0}] static resources from => '.format(
        file_name) + assets_folder)
    return send_from_directory(assets_folder, file_name)


@main_api.route('/<path:path>')
def static_proxy(path):
    """ static folder serve """
    file_name = path.split('/')[-1]
    dir_name = os.path.join(PUBLIC_PATH, '/'.join(path.split('/')[:-1]))
    return send_from_directory(dir_name, file_name)


@main_api.route('/logger')
def logger():
    """ send log file """
    return send_from_directory(PUBLIC_PATH, 'etm-api.log')


@main_api.route('/auth', methods=['POST'])
def auth():
    """ auth user """
    auth_user(flask_bcrypt)


@main_api.route('/register', methods=['POST'])
def registerUser():
    """ register user """
    registerNew(flask_bcrypt)


@main_api.route('/refresh', methods=['POST'])
@jwt_refresh_token_required
def refreshToken():
    """ renew user token """
    refresh()


@main_api.route('/user', methods=['GET', 'DELETE', 'PATCH'])
@jwt_required
def userOperations():
    """ path user """
    user()


@jwt.unauthorized_loader
def unauthorized_response(callback):
    """ 401 """
    return jsonify({
        'ok': False,
        'message': 'Missing Authorization Header'
    }), 401


@main_api.route('/admin')
def not_here():
    """ gtfo m8 """
    return send_from_directory(PUBLIC_PATH, 'resources/lol.html'), 418


@main_api.errorhandler(404)
def not_found(error):
    """ error handler send fake index ;p """
    LOG.error(error)
    return send_from_directory(PUBLIC_PATH, 'resources/404.html')

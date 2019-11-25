''' Entrypoint to init modules && flask app declaration '''
import os
import sys
import threading
import pymongo
from flask import Flask, Blueprint
from termcolor import colored
from werkzeug.serving import run_simple
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from etm.api.env_utils import env

PUBLIC_PATH = os.environ.get('PUBLIC_PATH')

if __name__ == "etm":
    PORT = env.getEnvVar('PORT')
    MONGODB_URI = env.getEnvVar('MONGODB_URI')
    DEBUG = env.getEnvVar('PY_ENV') == 'development'
    print('init name: ' + colored(__name__, 'green'))
    print('init ROOT_PATH: ' + colored("{}".format(
        os.environ.get('ROOT_PATH')), 'green'))
    print('init DEBUG: ' + colored("{}".format(DEBUG), 'green'))
    print('init PORT: ' + colored("{}".format(PORT), 'green'))
    print('init PUBLIC_PATH: ' + colored("{}".format(PUBLIC_PATH), 'green'))
    print('init MONGODB_URI: ' + colored("{}".format(MONGODB_URI), 'green'))
    print('pymongo version: ' + colored("{}".format(pymongo.version), 'green'))
    print('Running Environment: ' + colored(env.getEnvVar('PY_ENV'), 'blue'))
    print('----------------------------------------')

    # Check database is up and shutdown app otherwise

    from etm.api import db_manager  # noqa
    db_manager.check_connect()

    # Create the main flask object

    application = Flask(__name__)

    # Import all modules

    from etm.api.common.directoryWatcher import dw_task  # noqa
    from etm.api.etl.apiConnectors.alphaVantage import av_task  # noqa
    from etm.api.etl.apiConnectors.quandl import qdl_task  # noqa
    from etm.api.routes.index import root_api  # noqa
    from etm.api.routes.default import main_api  # noqa
    from etm.api.routes.process import process_api  # noqa

    # Run application

    directory_thread = threading.Thread(name='dir_task', target=dw_task.run)
    directory_thread.start()

    alpha_task_thread = threading.Thread(name='alpha_task', target=av_task.run)
    alpha_task_thread.start()

    quandl_task_thread = threading.Thread(name='qdl_task', target=qdl_task.run)
    quandl_task_thread.start()

    PORT = env.getEnvVar('PORT')
    DEBUG = env.getEnvVar('PY_ENV') == 'development'

    bp = Blueprint('etm-api', __name__, template_folder=PUBLIC_PATH)

    root_api.register_blueprint(bp, url_prefix='/api')
    root_api.register_blueprint(main_api)
    root_api.register_blueprint(process_api)

    app = DispatcherMiddleware(application, {
        '/api': root_api,
    })

    run_simple('0.0.0.0', int(PORT), app, use_reloader=DEBUG, use_evalex=DEBUG,
               use_debugger=DEBUG, threaded=True)

    directory_thread.join()
    alpha_task_thread.join()
    quandl_task_thread.join()

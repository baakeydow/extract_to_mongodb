""" index file for REST APIs using Flask """
import os
import sys
from termcolor import colored
from flask_script import Manager, prompt_bool, Shell, Server

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
os.environ.update({'ROOT_PATH': ROOT_PATH})

sys.path.append(os.path.join(ROOT_PATH, 'etm'))

from etm import app  # noqa
from etm.api.logger import LOG  # noqa
from etm.api.routes import *  # noqa


PUBLIC_PATH = os.path.join(ROOT_PATH, 'public')
os.environ.update({'PUBLIC_PATH': PUBLIC_PATH})


manager = Manager(app)


def make_shell_context():
    return dict(app=app)


PORT = os.environ.get('PORT')
DEBUG = os.environ.get('ENV') == 'development'

manager.add_command('runserver', Server(host='127.0.0.1', port=PORT, use_debugger=DEBUG, use_reloader=DEBUG,
                                        threaded=False, processes=1, passthrough_errors=False))
manager.add_command('shell', Shell(make_context=make_shell_context))

if __name__ == '__main__':
    LOG.info('running environment: ' + colored(os.environ.get('ENV'), 'green'))
    manager.run()

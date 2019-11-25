''' wrapper around logging module '''
import os
import logging
import coloredlogs
from etm.api.env_utils import env


def get_root_logger(logger_name, filename=None):
    ''' get the logger object '''
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    debug = env.getEnvVar('PY_ENV') == 'development'
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    consoleFormatter = logging.Formatter(coloredlogs.install(
        fmt='%(asctime)s - %(msecs)03d - %(hostname)s - %(name)s[%(process)d] - %(levelname)s ------------------\n %(message)s'))

    fileFormatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] => %(message)s")

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(consoleFormatter)
    logger.addHandler(consoleHandler)

    if filename is not None:
        fh = logging.FileHandler(filename)
        fh.setFormatter(fileFormatter)
        logger.addHandler(fh)

    return logger


# Create a logger object to log the info and debug
PUBLIC_PATH = os.environ.get('PUBLIC_PATH')
LOG = get_root_logger(os.environ.get(
    'ROOT_LOGGER', 'root'), filename=os.path.join(PUBLIC_PATH, 'etm-api.log'))

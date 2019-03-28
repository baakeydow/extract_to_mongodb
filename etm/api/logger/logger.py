''' wrapper around logging module '''
import os
import logging
import coloredlogs


def get_root_logger(logger_name, filename=None):
    ''' get the logger object '''
    logger = logging.getLogger(logger_name)
    debug = os.environ.get('ENV', 'development') == 'development'
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    formatter = logging.Formatter(coloredlogs.install(
        fmt='%(asctime)s - %(msecs)03d - %(hostname)s - %(name)s[%(process)d] - %(levelname)s ------------------\n %(message)s'))

    if filename:
        fh = logging.FileHandler(filename)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def get_child_logger(root_logger, name):
    return logging.getLogger('.'.join([root_logger, name]))

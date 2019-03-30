# ''' Entrypoint to init modules  '''

from etm.api.app import app

if __name__ == "etm":
    from etm.api.etl import *  # noqa

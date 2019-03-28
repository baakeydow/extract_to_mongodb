from .logger import *

# Create a logger object to log the info and debug
ROOT_PATH = os.environ.get('ROOT_PATH')
LOG = get_root_logger(os.environ.get(
    'ROOT_LOGGER', 'root'), filename=os.path.join(ROOT_PATH, 'etm-api.log'))

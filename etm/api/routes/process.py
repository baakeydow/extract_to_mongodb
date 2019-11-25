''' process routing entrypoints '''
from flask import Blueprint
from etm.api.controllers.process import get_available_dbs, csv_test

process_api = Blueprint('process_api', __name__)


@process_api.route('/db/list', methods=['GET'])
def serve():
    """ return list of available dbs """
    return get_available_dbs()


@process_api.route('/test_csv', methods=['GET'])
def test_csv():
    """ return list of available dbs """
    return csv_test()

''' processing controller '''
from flask import jsonify
from etm.api import db_manager
from etm.api.etl.fileConnectors.csvSaver import CsvSaver


def get_available_dbs():
    client = db_manager.get_client()
    dbs = client.database_names()
    return jsonify({'success': True, 'data': dbs}), 200


def csv_test():
    return jsonify({'success': True, 'data': 'data'}), 200

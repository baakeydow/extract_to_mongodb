from pathlib import Path
from watchdog.events import FileSystemEventHandler
from termcolor import colored
from etm.api import LOG
from etm.api.etl.fileConnectors.csvSaver import CsvSaver
from etm.api.etl.fileConnectors.xlSaver import XlSaver
from etm.api.etl.fileConnectors.swiftSaver import SwiftSaver


class filesHandler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        print("===================FileHandler===================")
        if event.is_directory:
            print('Folder change detected - {0}'.format(event.src_path))
            print("===================================================")
            return None
        print('File change detected - {0}'.format(event.src_path))
        print('Check path is file ===> {0}'.format(
            Path(event.src_path).is_file()))
        print("===================================================")
        if event.event_type == 'created' or event.event_type == 'modified':
            LOG.info('File created - {0}'.format(event.src_path))
            process_file(event.src_path)
        if event.event_type == 'deleted':
            LOG.critical('File deleted - {0}'.format(event.src_path))


def file_needs_processing(file_path, ext):
    if (ext not in [".xls", ".xlsx", ".csv"]) and (ext == '.txt' and 'data_sources/mt940' not in file_path):
        return False
    return True


def process_file(file_path):
    p = Path(file_path)
    if p.is_file():
        ext = p.suffix
        if file_needs_processing(file_path, ext) == False:
            LOG.info('file not compatible... not saving')
            return
        LOG.info(
            colored('--------------- Processing => ' + file_path, 'blue'))
        try:
            parse_and_save_file(ext, file_path)
        except (OSError, IOError) as e:
            LOG.critical(e)


def parse_and_save_file(ext, file_path):
    try:
        error = 0
        if ext in [".xls", ".xlsx"]:
            xl_saver = XlSaver(file_path)
            xl_saver.handle_excel()
        elif ext == ".csv":
            cs_saver = CsvSaver(file_path)
            cs_saver.handle_csv()
        elif ext == ".txt" and ('data_sources/mt940' in file_path):
            mt940_saver = SwiftSaver(file_path)
            mt940_saver.handle_mt940()
    except Exception as e:
        error = e
        LOG.critical(
            'Failed to save File: {0}'.format(file_path))
        LOG.critical(
            'ETM_SAVE_FILE_ERROR -> {0}'.format(e))
        raise SaveFileError()
    finally:
        if error == 0:
            LOG.info(
                colored('--------------- File successfully processed without uncatched exceptions :) => ' + file_path, 'yellow'))


class SaveFileError(Exception):
    '''raise when failed to save the entire file'''

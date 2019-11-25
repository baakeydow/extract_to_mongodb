import os
import time
from watchdog.observers import Observer
from etm.api.common.fileHandler import filesHandler


class Watcher:
    DIRECTORY_TO_WATCH = os.path.join(os.environ.get(
        'ROOT_PATH'), 'public/data_sources')

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = filesHandler()
        self.observer.schedule(
            event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Error")
        self.observer.join()


dw_task = Watcher()

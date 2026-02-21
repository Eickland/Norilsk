import requests
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

class HTTPHandler(logging.Handler):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.executor = ThreadPoolExecutor(max_workers=2)
    def emit(self, record):
        try:
            log_entry = self.format(record)
            # submit() просто кладет задачу в очередь и мгновенно возвращает управление
            self.executor.submit(self._send, log_entry)
        except Exception:
            self.handleError(record)
    def _send(self, log_entry):
        try:
            requests.post(self.url, json={"log": log_entry}, timeout=0.5)
        except:
            pass

# В настройках

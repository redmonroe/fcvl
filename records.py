import logging

class RecordHandler:
    def __init__(self, log_file):
        # logging.basicConfig(filename='gitignorethislog.log', level = logging.DEBUG)
        # logger = logging.getLogger()
        self.log_file = log_file
        self.recorder = logging.getLogger(__name__)
        self.recorder.setLevel(logging.DEBUG)
        self.file_handler = logging.FileHandler(log_file)
        self.formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.recorder.addHandler(self.file_handler)

    def info(self, message):
        self.recorder.info(message)

    # def error(self, message):
    #     self.logger.error(message)

    # def warning(self, message):
    #     self.logger.warning(message)

    # def debug(self, message):
    #     self.logger.debug(message)

    # def critical(self, message):
    #     self.logger.critical(message)

def sum(a, b):
    record = RecordHandler('records.log')
    record.info('sum function called')
    return a + b

print(sum(2, 3))

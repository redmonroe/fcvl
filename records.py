import logging

import functools

logging.basicConfig(filename='record/record.log', level = logging.DEBUG)
logger = logging.getLogger()

def record(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # breakpoint()
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"function {func.__name__} called with args {signature}")
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e
    return wrapper

# class RecordHandler:
#     def __init__(self, log_file):
        # logging.basicConfig(filename='gitignorethislog.log', level = logging.DEBUG)
        # logger = logging.getLogger()
    #     self.log_file = log_file
    #     self.recorder = logging.getLogger(__name__)
    #     self.recorder.setLevel(logging.DEBUG)
    #     self.file_handler = logging.FileHandler(log_file)
    #     self.formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    #     self.file_handler.setFormatter(self.formatter)
    #     self.recorder.addHandler(self.file_handler)

    # def info(self, message):
    #     self.recorder.info(message)

    # def error(self, message):
    #     self.logger.error(message)

    # def warning(self, message):
    #     self.logger.warning(message)

    # def debug(self, message):
    #     self.logger.debug(message)

    # def critical(self, message):
    #     self.logger.critical(message)

# @record
# def sum(a, b):
#     # record = RecordHandler('records.log')
#     # record.info('sum function called')
#     return a + b

# print(sum(2, 3))

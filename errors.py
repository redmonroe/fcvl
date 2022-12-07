import time
from functools import wraps

from googleapiclient.errors import HttpError
from playwright._impl._api_types import TimeoutError as PlaywrightTimeoutError


# interesting link: https://stackoverflow.com/questions/50246304/using-python-decorators-to-retry-request

def retry_google_api(times, sleep1, exceptions):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """
    def decorator(func):
        @wraps(func)
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    if e.resp.status == exceptions:
                        print(f'\nException on {func}, attempt {attempt} of {times} | sleep={sleep1} | code={exceptions}\n')
                        time.sleep(sleep1)
                    attempt += 1
            return func(*args, **kwargs)
        return newfn
    return decorator

class Errors:
    
    @staticmethod
    def xlsx_permission_error(path, pandas_object):
        try:
            return pandas_object.ExcelWriter(path, engine='xlsxwriter')
        except PermissionError as e:
            decision = input('Exception caught in workbook.close(): %s\n'
                        "Please close the file if it is open in Excel.\n"
                        "Try to write file again? [Y/n]: " % e)
            if decision != 'n':
                return pandas_object.ExcelWriter(path, engine='xlsxwriter')
            else:
                raise

    @staticmethod
    def playwright_timeerror(func):
        @wraps(func)
        def error_wrapper(*args, **kwargs):
            print(f'running {func.__name__}')
            try:
                # breakpoint()
                func(*args, **kwargs)
                return 'ok'
            except PlaywrightTimeoutError as e:
                print('up hook')
                print('playwright aint succeeded in downloading file.  Please try manually downloading.')
                return 'error'
        return error_wrapper




        # def decorator(func):
        #     @wraps(func)
        #     def inner_pwtout_error(*args, **kwargs):
        #             pw_func
        #         except PlaywrightTimeoutError as e:
        #             print(e)
        #         return pw_func
        #     return inner_pwtout_error
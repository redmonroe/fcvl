import time
from functools import wraps

from googleapiclient.errors import HttpError

# interesting link: https://stackoverflow.com/questions/50246304/using-python-decorators-to-retry-request
# also tenacity
'''
error_codes = [429]
sleep1 = 2
MAX_RETRIES = 3
def http_error_handling_google(func):
    def trial(*args, num_retries=0, **kwargs):
        try:
            return func(*args, **kwargs)
        except HttpError as e:
            if e.resp.status == error_codes:
                if num_retries > MAX_RETRIES:
                    raise RuntimeError("Too many retries")
                else:
                    msg = f"rate limit reached. Waiting {sleep1} sec ..."
                    time.sleep(sleep1)
                    return trial(*args, num_retries=num_retries + 1, **kwargs)

                    print(f'rate limit reached; trying again with timeout of {sleep1} s')
                    time.sleep(sleep1)
          
    return trial
'''

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
                    # breakpoint()
                    return func(*args, **kwargs)
                except HttpError as e:
                    if e.resp.status == exceptions:
                        print('Exception thrown when attempting to run %s, attempt '
                            '%d of %d' % (func, attempt, times))
                        print(f'sleep time = {sleep1}, exception code: {exceptions}')
                        time.sleep(sleep1)
                    attempt += 1
            return func(*args, **kwargs)
        return newfn
    return decorator
'''
except HttpError as e:
        if e.resp.status == error_codes:
            print(f'trying again with timeout of {sleep1} s')
            time.sleep(sleep1)
        else:
            raise
'''

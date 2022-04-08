from googleapiclient.errors import HttpError

# interesting link: https://stackoverflow.com/questions/50246304/using-python-decorators-to-retry-request
# also tenacity



def retry(times, exceptions):
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
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    print(
                        'Exception thrown when attempting to run %s, attempt '
                        '%d of %d' % (func, attempt, times)
                    )
                    attempt += 1
            return func(*args, **kwargs)
        return newfn
    return decorator

@retry(times=3, exceptions=(ValueError, TypeError))
def foo1():
    print('Some code here ....')
    print('Oh no, we have exception')
    raise ValueError('Some error')

foo1()
import multiprocessing as mp
import requests
import time
from typing import Any, Callable
import urllib

from .constants import REQUEST_TIMEOUTS

LOG = mp.get_logger()


def attempt_until_success(func: Callable) -> Any:
    def make_attempts(url, *args, **kwargs):
        while True:
            try:
                func(url, *args, **kwargs)
            except Exception as e:
                LOG.warning(
                    f"Attempt to access {url} failed so retrying, error message: {e}"
                )
                time.sleep(REQUEST_TIMEOUTS)

    return make_attempts


@attempt_until_success
def download_until_success(url, file_path):
    """
    Keeps attempting download until successful
    """
    urllib.request.urlretrieve(url, file_path)
    return


@attempt_until_success
def get_until_success(url):
    """
    Keeps attempting GET request until successful
    """
    response = requests.get(url)
    return response


@attempt_until_success
def post_until_success(url, params):
    """
    Keeps attempting POST request until successful
    """
    response = requests.post(url, params)
    return response

import logging

import requests
from backoff import on_exception, expo
from ratelimit import limits, sleep_and_retry


def fatal_code(e):
    return 400 <= e.response.status_code < 500 and e.response.status_code != 429


@on_exception(lambda: expo(factor=10), (requests.HTTPError, requests.ConnectionError), max_tries=8, giveup=fatal_code)
@sleep_and_retry
@limits(calls=25, period=60)
def rest_get(url, **kwargs):
    r = _logged_request(url, **kwargs)
    return r.json()


@on_exception(lambda: expo(factor=10), (requests.HTTPError, requests.ConnectionError), max_tries=8, giveup=fatal_code)
@sleep_and_retry
@limits(calls=120, period=60)
def rest_get_express(url, **kwargs):
    r = _logged_request(url, **kwargs)
    return r.json()


def _logged_request(url, **kwargs):
    logging.info(url)
    r = requests.get(url, **kwargs)
    if not r.ok:
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            logging.warning(e.response)
            logging.warning(e.response.text)
            raise
    return r

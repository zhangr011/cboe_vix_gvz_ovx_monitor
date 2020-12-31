# encoding: UTF-8

import requests, traceback, json
from bs4 import BeautifulSoup
from .logger import logger


#----------------------------------------------------------------------
def get_content(url: str, parse: callable):
    try:
        response = requests.get(url)
        response.raise_for_status()
        rets = parse(response)
        return rets
    except Exception as e:
        logger.error(f"http requests failed. {traceback.format_exc(limit = 0)}")
        return False


#----------------------------------------------------------------------
def get_content_soup(url):
    return get_content(url, lambda response: BeautifulSoup(response.text, 'html.parser'))


#----------------------------------------------------------------------
def get_content_json(url):
    return get_content(url, lambda response: json.loads(response.text))

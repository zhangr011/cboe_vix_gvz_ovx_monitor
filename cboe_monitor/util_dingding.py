# encoding: UTF-8

import logging, configparser, os
import time, hmac, hashlib, base64, urllib.parse
import requests, threading, traceback, json

from .utilities import DATA_ROOT
from .logger import logger


ini_config = configparser.ConfigParser()
PUSH_CONFIG_PATH = os.path.join(DATA_ROOT, 'push.ini')
PUSH_SECTION = 'ddpusher'
ini_config.read(PUSH_CONFIG_PATH)

# dingding push
dd_url = ini_config.get(PUSH_SECTION, 'url')
dd_token = ini_config.get(PUSH_SECTION, 'token')
dd_token_enc = dd_token.encode('utf-8')

headers = {'Content-Type': 'application/json'}


#----------------------------------------------------------------------
def generate_timestamp():
    """生成时间戳"""
    return str(round(time.time() * 1000))


#----------------------------------------------------------------------
def generate_sign(timestamp: str):
    """生成签名"""
    string_to_sign = f'{timestamp}\n{dd_token}'
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(dd_token_enc, string_to_sign_enc,
                         digestmod = hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return sign


#----------------------------------------------------------------------
def do_send_msg(msg: str):
    """发送消息"""
    timestamp = generate_timestamp()
    sign = generate_sign(timestamp)
    try:
        response = requests.post(
            dd_url + f'&timestamp={timestamp}&sign={sign}',
            headers = headers, data = json.dumps(msg))
        res = json.loads(response.content)
        if int(res.get('errcode', 1)) != 0:
            logger.error(res)
        else:
            logger.info(res)
    except requests.exceptions.ConnectionError:
        logger.error(traceback.format_exc(limit = 1))


#----------------------------------------------------------------------
def send_msg(msg: str):
    """在线程中发送消息"""
    thread = threading.Thread(target = do_send_msg, args = (msg, ))
    thread.start()


#----------------------------------------------------------------------
def send_md_msg(title: str, content: str):
    """发送 md 信息"""
    content = f'### {title}  \n\n  --------------  \n\n  {content}  \n\n'
    msg = {'msgtype': "markdown",
           'markdown': {"title": title,
                        "text": content}}
    send_msg(msg)

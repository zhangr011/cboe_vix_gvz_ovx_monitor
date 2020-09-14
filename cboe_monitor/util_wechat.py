# -*- coding:utf-8 -*-
'''
通过wxpusher发送Weixin的消息
http://wxpusher.zjiecode.com/
开通步骤：
1、关注公众号，注册
2、通过公众号，获取UID
3、通过网站打开管理后台：http://wxpusher.zjiecode.com/admin/ 使用微信扫码登录，=》应用列表=》新建应用，如vnpy2，并获得APP_TOOKEN
4、应用列表=》应用（vnpy2）=》 关注.
'''

from threading import Lock, Thread
import sys, os, requests, traceback, configparser
from datetime import datetime
from functools import wraps
from .logger import logger
global wechat_lock
wechat_lock = Lock()

from .utilities import DATA_ROOT

ini_config = configparser.ConfigParser()
PUSH_CONFIG_PATH = os.path.join(DATA_ROOT, 'push.ini')
PUSH_SECTION = 'wxpusher'
ini_config.read(PUSH_CONFIG_PATH)

# uids for push
UIDS = ini_config.get(PUSH_SECTION, 'uids').split(',')
APP_TOKEN = ini_config.get(PUSH_SECTION, 'app_token')


#----------------------------------------------------------------------
class WxPusherThread(Thread):

    def __init__(self, uids: list, content: str, topic_ids: list = [], url: str = '', app_token=''):
        super(WxPusherThread, self).__init__(name = "WxPusherThread")
        self.request_url = "http://wxpusher.zjiecode.com/api/send/message"
        self.uids = uids
        self.content = content
        self.topic_ids = topic_ids
        self.url = url
        self.lock = wechat_lock
        self.app_token = app_token if len(app_token) > 0 else APP_TOKEN

    def run(self):
        if self.content is None or len(self.content) == 0:
            return
        params = {}
        params['appToken'] = self.app_token
        params['content'] = self.content
        params['contentType'] = 1
        params['topicIds'] = self.topic_ids
        params['uids'] = self.uids
        params['url'] = self.url
        # try to send the msg
        try:
            response = requests.post(self.request_url, json = params).json()
            if not response.get('success', False):
                logger.info(response)
                return
        except Exception as e:
            logger.error(f"wechat_thread sent failed! ex:{e}, trace:{traceback.format_exc(level = 0)}"),
            return
        logger.info("wechat_thread sent successful!")


#----------------------------------------------------------------------
def send_wx_msg(*args, **kwargs):
    """
    content: str - msg for pushing
    uids: list   - uids for pushing to
    app_token: str
    """
    content = kwargs.get('content', None)
    if content is None:
        if len(args) == 0:
            return
        content = args[0]
    if len(content) == 0:
        return
    # dict => str, none str => str
    if not isinstance(content, str):
        if isinstance(content, dict):
            content = '{}'.format(print_dict(content))
        else:
            content = str(content)
    uids = kwargs.get('uids', [])
    # use the default uids if no uids passed in
    if len(uids) == 0:
        uids.extend(UIDS)
    # use the default app_token if no token passed in
    app_token = kwargs.get('app_token', APP_TOKEN)
    t = WxPusherThread(uids = uids, content = content, app_token = app_token)
    t.daemon = False
    t.start()

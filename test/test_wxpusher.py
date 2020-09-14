# encoding: UTF-8

import unittest as ut

from cboe_monitor.util_wechat import send_wx_msg


class TestWxPusher(ut.TestCase):

    def testSendMsg(self):
        send_wx_msg('test for default send. ')
        send_wx_msg('test for send to fake uids', uids = ['abc', '123'])


if __name__ == '__main__':
    ut.main()

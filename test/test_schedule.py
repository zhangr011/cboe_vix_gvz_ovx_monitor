# encoding: UTF-8

import unittest as ut
from time import sleep
from cboe_monitor.schedule_manager import ScheduleManager
from cboe_monitor.logger import logger


#----------------------------------------------------------------------
class TestScheduleManager(ScheduleManager):
    _crontab = '28 * * * *'

    def do_timeout(self):
        """do sth every hour"""
        logger.info(f"schedule done. ")


#----------------------------------------------------------------------
class TestSchedule(ut.TestCase):

    def testSingleton(self):
        """"""
        mgr1 = TestScheduleManager()
        mgr2 = TestScheduleManager()
        self.assertEqual(mgr1, mgr2)

    def notestSchedule(self):
        """"""
        mgr = TestScheduleManager()
        while True:
            sleep(1)


if __name__ == '__main__':
    ut.main()

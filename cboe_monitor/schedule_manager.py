# encoding: UTF-8

from .singleton import Singleton
import threading
from crontab import CronTab


# crontab may cause an advance trigger, so we need to delay a little
CRONTAB_MIN_DELAY = 1          # delay for the schedule
CRONTAB_DOIT_MIN_DELAY = 10    # delay for the doing


#----------------------------------------------------------------------
def get_delay_time(cronTab):
    """get the next trigger time according to the crontab
    *  *  *  *  *
    |  |  |  |  |
    |  |  |  |  +---- day of week (0 - 6) (sunday = 0)
    |  |  |  +----- month (1 - 12)
    |  |  +------ day of month (1 - 31)
    |  +------- hour (0 - 23)
    +-------- min (0 - 59)
    """
    entry = CronTab(cronTab)
    return max(int(entry.next(default_utc = True)), CRONTAB_MIN_DELAY)


#----------------------------------------------------------------------
class ScheduleManager(metaclass = Singleton):
    # minute hour day month weekday
    # '0 0 * * *'
    _crontab = '0 0 * * *'
    _thread = None

    def __init__(self, doit: bool = False):
        """ Constructor """
        super(ScheduleManager, self).__init__()
        self.timeout(doit)

    def get_delay_time(self):
        """get the delay time for the next action"""
        return get_delay_time(self._crontab)

    def timeout(self, doit: bool = True):
        """when time out"""
        self.cancel_timer()
        delay = self.get_delay_time()
        if True == doit and delay > CRONTAB_DOIT_MIN_DELAY:
            if not self.do_timeout():
                # re_delay is 5 minutes
                delay = 300
        self._thread = threading.Timer(delay, self.timeout)
        self._thread.start()

    def cancel_timer(self):
        if self._thread:
            self._thread.cancel()
            self._thread = None

    def do_timeout(self):
        """we do sth in this function"""
        raise NotImplementedError

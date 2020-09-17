# encoding: UTF-8

from cboe_monitor.utilities import \
    run_over_time_frame, mk_notification, is_business_day
from cboe_monitor.data_manager import VIXDataManager
from cboe_monitor.schedule_manager import ScheduleManager
from cboe_monitor.util_wechat import send_wx_msg
from cboe_monitor.logger import logger
from datetime import datetime, timezone, timedelta
from time import sleep


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = '50 23 * * *'

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        delivery_dates, schedule_days = run_over_time_frame()
        last_day = datetime.now(tz = timezone.utc) - timedelta(days = 1)
        if not is_business_day(last_day, schedule_days):
            logger.info('last day is not a business day. ')
            return
        vdm = VIXDataManager(delivery_dates)
        vdm.download_raw_data()
        df = vdm.combine_all()
        percent, vix, gvz, ovx = vdm.analyze()
        msg = mk_notification(df, percent, vix, gvz, ovx)
        send_wx_msg(msg)
        logger.info('schedule task done. ')


if __name__ == '__main__':
    mgr = MonitorScheduleManager(False)
    while True:
        sleep(1)

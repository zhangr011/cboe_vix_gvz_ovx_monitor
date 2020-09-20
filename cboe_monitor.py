# encoding: UTF-8

from cboe_monitor.utilities import \
    run_over_time_frame, mk_notification, mk_notification_params, is_business_day
from cboe_monitor.data_manager import VIXDataManager, GVZDataManager, OVXDataManager
from cboe_monitor.schedule_manager import ScheduleManager
from cboe_monitor.util_wechat import send_wx_msg
from cboe_monitor.logger import logger
from datetime import datetime, timezone
from time import sleep


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = '50 23 * * *'

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        delivery_dates, schedule_days = run_over_time_frame()
        last_day = datetime.now(tz = timezone.utc)
        if not is_business_day(last_day, schedule_days):
            logger.info('last day is not a business day. ')
            return
        vdm = VIXDataManager(delivery_dates)
        vdm.download_raw_data()
        df = vdm.combine_all()
        rets_vix = vdm.analyze()
        # gvz futures are delisted
        gvzm = GVZDataManager([])
        gvzm.download_raw_data()
        rets_gvzm = gvzm.analyze()
        # ovx futures are delisted
        ovxm = OVXDataManager([])
        ovxm.download_raw_data()
        rets_ovxm = ovxm.analyze()
        params = mk_notification_params(df, delivery_dates, rets_vix, rets_gvzm, rets_ovxm)
        msg = mk_notification(**params)
        send_wx_msg(msg)
        logger.info('schedule task done. ')


if __name__ == '__main__':
    mgr = MonitorScheduleManager(False)
    logger.info('cboe monitor started. ')
    while True:
        sleep(1)

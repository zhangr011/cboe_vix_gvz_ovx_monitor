# encoding: UTF-8

from cboe_monitor.utilities import \
    run_over_time_frame, mk_notification, mk_notification_params, is_business_day
from cboe_monitor.data_manager import VIXDataManager, GVZDataManager, OVXDataManager
from cboe_monitor.schedule_manager import ScheduleManager
from cboe_monitor.util_dingding import send_md_msg
from cboe_monitor.logger import logger
from datetime import datetime, timezone
from time import sleep


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _crontab = '50 23 * * *'
    _day_index = None
    _day_vix_downloaded = False
    _day_gvz_downloaded = False
    _day_ovx_downloaded = False

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        delivery_dates, schedule_days = run_over_time_frame()
        last_day = datetime.now(tz = timezone.utc)
        if not is_business_day(last_day, schedule_days):
            logger.info('last day is not a business day. ')
            return self.clear_and_return_true()
        vdm = VIXDataManager(delivery_dates)
        vdm.download_raw_data(self._day_vix_downloaded)
        df = vdm.combine_all()
        rets_vix = vdm.analyze()
        if self._day_index is None:
            # mark the _day_index
            self._day_index = rets_vix['vix'].index[-1]
        if rets_vix['vix_diff'].index[-1] != self._day_index:
            # vix diff is not pulled, retry 10 minutes later
            logger.info("vix info download failed. ")
            return False
        # gvz futures are delisted
        gvzm = GVZDataManager([])
        gvzm.download_raw_data(self._day_gvz_downloaded)
        rets_gvzm = gvzm.analyze()
        if rets_gvzm['gvz'].index[-1] != self._day_index:
            logger.info("gvz info download failed. ")
            return False
        # ovx futures are delisted
        ovxm = OVXDataManager([])
        ovxm.download_raw_data(self._day_ovx_downloaded)
        rets_ovxm = ovxm.analyze()
        if rets_ovxm['ovx'].index[-1] != self._day_index:
            logger.info("ovx info download failed. ")
            return False
        params = mk_notification_params(df, delivery_dates, rets_vix, rets_gvzm, rets_ovxm)
        title, msg = mk_notification(**params)
        send_md_msg(title, msg)
        logger.info('schedule task done. ')
        return self.clear_and_return_true()

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        self._day_index = None
        self._day_vix_downloaded = False
        self._day_gvz_downloaded = False
        self._day_ovx_downloaded = False
        return True


if __name__ == '__main__':
    mgr = MonitorScheduleManager(True)
    logger.info('cboe monitor started. ')
    while True:
        sleep(1)

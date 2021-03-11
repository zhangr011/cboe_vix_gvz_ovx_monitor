# encoding: UTF-8

from cboe_monitor.utilities import \
    DATE_FORMAT, run_over_time_frame, DAILY_UPDATE_HOUR, get_last_day, \
    mk_notification, mk_notification_params, is_business_day
from cboe_monitor.data_manager import VIXDataManager, GVZDataManager, OVXDataManager
from cboe_monitor.schedule_manager import ScheduleManager
from cboe_monitor.util_http_bs4 import get_content_json
from cboe_monitor.util_cboe_vix_futures import \
    check_vix_intraday_warning, check_warning_info_same, VIX_FUTURES_URL, \
    mk_intraday_notification
from cboe_monitor.util_dingding import send_md_msg
from cboe_monitor.logger import logger
from datetime import datetime
from time import sleep

import argparse


#----------------------------------------------------------------------
class MonitorScheduleManager(ScheduleManager):

    # UTC+8
    _update_hour = DAILY_UPDATE_HOUR
    _crontab = f'50 {_update_hour} * * *'
    _last_day = None
    _day_index = None
    _day_vix_downloaded = False
    _day_gvz_downloaded = False
    _day_ovx_downloaded = False

    def __init__(self, immediately: bool = False, push_msg: bool = False):
        """"""
        self._immediately = immediately
        self._push_msg = push_msg
        super(MonitorScheduleManager, self).__init__(immediately)

    def do_timeout(self):
        """"""
        logger.info('start schedule task. ')
        delivery_dates, schedule_days = run_over_time_frame()
        self._last_day = get_last_day(self._update_hour)
        self._day_index = datetime.strftime(self._last_day, DATE_FORMAT)
        if not is_business_day(self._last_day, schedule_days):
            logger.info('last day is not a business day. ')
            return self.clear_and_return_true()
        vdm = VIXDataManager(delivery_dates)
        vdm.download_raw_data(self._day_vix_downloaded)
        df = vdm.combine_all()
        rets_vix = vdm.analyze()
        if rets_vix['vix_diff'].index[-1] != self._day_index or \
           self._day_index not in rets_vix['vix'].index:
            # vix diff is not pulled, retry 5 minutes later
            logger.info(f"last_day: {self._last_day}, index: {self._day_index}, vix_index: {rets_vix['vix'].index[-1]}, vix_diff_index: {rets_vix['vix_diff'].index[-1]}")
            logger.info("vix info download failed. ")
            return False
        elif df.iloc[-1][0] <= 1 or df.iloc[-1][1] <= 1:
            logger.info(f'vix info download failed due to vix 0 or 1 is zero. ')
            return False
        self._day_vix_downloaded = True
        # gvz futures are delisted
        gvzm = GVZDataManager([])
        gvzm.download_raw_data(self._day_gvz_downloaded)
        rets_gvzm = gvzm.analyze()
        if self._day_index not in rets_gvzm['gvz'].index:
            logger.info("gvz info download failed. ")
            return False
        self._day_gvz_downloaded = True
        # ovx futures are delisted
        ovxm = OVXDataManager([])
        ovxm.download_raw_data(self._day_ovx_downloaded)
        rets_ovxm = ovxm.analyze()
        if self._day_index not in rets_ovxm['ovx'].index:
            logger.info("ovx info download failed. ")
            return False
        self._day_ovx_downloaded = True
        params = mk_notification_params(df, delivery_dates, rets_vix, rets_gvzm, rets_ovxm)
        title, msg = mk_notification(**params)
        if self._push_msg:
            send_md_msg(title, msg)
        logger.info('schedule task done. ')
        return self.clear_and_return_true()

    def clear_and_return_true(self):
        """clear the _day_index and return True"""
        self._last_day = None
        self._day_index = None
        self._day_vix_downloaded = False
        self._day_gvz_downloaded = False
        self._day_ovx_downloaded = False
        self._push_msg = True
        self._immediately = False
        return True


#----------------------------------------------------------------------
class IntradayScheduleManager(ScheduleManager):

    # use interval
    _last_infos = []
    _counter = 0

    def get_delay_time(self):
        """do it every 900 seconds"""
        return 900

    #----------------------------------------------------------------------
    def do_timeout(self):
        """"""
        soup = get_content_json(VIX_FUTURES_URL)
        if False == soup:
            logger.info("intraday vix futures info fetch failed. ")
            return False
        rets = check_vix_intraday_warning(soup)
        if rets != [] and not check_warning_info_same(self._last_infos, rets):
            title, msg = mk_intraday_notification(rets)
            send_md_msg(title, msg)
            logger.info('intraday vix warning msg sended. ')
            self._last_infos = rets
        self._counter +=1
        # 192 = 4 * 48, about 2 day
        if self._counter >= 192:
            logger.info('intraday monitor is still aliving... ')
            self._counter = 0
        return True


#----------------------------------------------------------------------
if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--imm', type = bool, dest = 'immediately', default = False,
                            help = 'immediately analyze when started. ')
    arg_parser.add_argument('--push', type = bool, dest = 'push_msg', default = False,
                            help = 'push the message. ')
    args = arg_parser.parse_args()
    mgr = MonitorScheduleManager(args.immediately, args.push_msg)
    logger.info('cboe monitor started. ')
    intraday_mgr = IntradayScheduleManager(True)
    logger.info('cboe intraday monitor started. ')
    while True:
        sleep(1)

# encoding: UTF-8

import unittest as ut
from cboe_monitor.util_http_bs4 import get_content_json
from cboe_monitor.util_cboe_vix_futures import \
    check_vix_intraday_warning, VixIntradayState, \
    check_warning_info_same, VIX_FUTURES_URL, mk_intraday_notification


class TestHttpBs4(ut.TestCase):

    def testHttpCboeVix(self):
        soup = get_content_json(VIX_FUTURES_URL)
        rets = check_vix_intraday_warning(soup, True)
        self.assertNotEqual([], len(rets))
        v1_type, v1 = rets[0]
        self.assertEqual(VixIntradayState.PinUp, v1_type)
        title, msg = mk_intraday_notification(rets)
        # self.assertEqual('', title)
        self.assertEqual('', msg)
        soup2 = get_content_json(VIX_FUTURES_URL)
        rets2 = check_vix_intraday_warning(soup2, True)
        self.assertEqual(True, check_warning_info_same(rets, rets2))
        self.assertEqual({}, v1)


if __name__ == '__main__':
    ut.main()

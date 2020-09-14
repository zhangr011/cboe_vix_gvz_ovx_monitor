# encoding: UTF-8

import unittest as ut
import numpy as np
import pandas as pd
import os

from cboe_monitor.utilities import \
    TEST_DATA_ROOT, \
    run_over_time_frame, filter_delivery_dates, shift_delivery_dates, \
    generate_term_structure, load_futures_by_csv, combine_data, \
    combine_all, is_futures_file



def mk_datetime_key(date_str: str):
    return pd.datetime(date_str)


class TestAnalyze(ut.TestCase):

    def testDateCompare(self):
        """for date str compare"""
        self.assertEqual(True, '2020-09-10' > '2020-09-09')
        self.assertEqual(True, '2020-09-01' > '2020-08-31')
        self.assertEqual(True, '2020-01-01' > '2019-12-31')

    def testDeliveryDates(self):
        """"""
        delivery_dates, schedul_days = run_over_time_frame()
        flist = filter_delivery_dates(delivery_dates, '2020-09-10')
        np.testing.assert_array_equal(['2019-09-18', '2019-10-16',
                                       '2019-11-20', '2019-12-18',
                                       '2020-01-22', '2020-02-19',
                                       '2020-03-18', '2020-04-15',
                                       '2020-05-20', '2020-06-17',
                                       '2020-07-22', '2020-08-19'], flist)
        flist2 = shift_delivery_dates(delivery_dates, flist, -1)
        np.testing.assert_array_equal(['2019-08-21', '2019-09-18', '2019-10-16',
                                       '2019-11-20', '2019-12-18',
                                       '2020-01-22', '2020-02-19',
                                       '2020-03-18', '2020-04-15',
                                       '2020-05-20', '2020-06-17',
                                       '2020-07-22'], flist2)
        flist3 = shift_delivery_dates(delivery_dates, flist2, -1)
        np.testing.assert_array_equal(['2019-07-17', '2019-08-21',
                                       '2019-09-18', '2019-10-16',
                                       '2019-11-20', '2019-12-18',
                                       '2020-01-22', '2020-02-19',
                                       '2020-03-18', '2020-04-15',
                                       '2020-05-20', '2020-06-17'], flist3)
        flist4 = shift_delivery_dates(delivery_dates, flist, -2)
        np.testing.assert_array_equal(flist4, flist3)

    def testTermStucture(self):
        """for mask test"""
        delivery_dates, schedule_days = run_over_time_frame()
        futures_0916 = load_futures_by_csv(os.path.join(TEST_DATA_ROOT, '2020-09-16.csv'))
        term1 = generate_term_structure(
            delivery_dates, futures_0916, '2020-09-10')
        self.assertEqual((191, 12), term1.shape)
        np.testing.assert_array_equal([28.775, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term1.loc['2020-09-10'])
        np.testing.assert_array_equal([0, 26.225, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term1.loc['2020-08-19'])
        np.testing.assert_array_equal([0, 30.275, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term1.loc['2020-07-23'])
        np.testing.assert_array_equal([0, 0, 29.125, 0, 0, 0, 0, 0, 0, 0, 0, 0], term1.loc['2020-07-22'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 17.475, 0, 0, 0], term1.loc['2020-01-17'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 19.1, 0, 0], term1.loc['2019-12-12'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 19.2, 0, 0], term1.loc['2019-12-09'])
        # boundary test for deliver date
        futures_0819 = load_futures_by_csv(os.path.join(TEST_DATA_ROOT, '2020-08-19.csv'))
        term2 = generate_term_structure(
            delivery_dates, futures_0819, '2020-08-19')
        np.testing.assert_array_equal([21.71, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term2.loc['2020-08-19'])
        np.testing.assert_array_equal([28.775, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term2.loc['2020-07-23'])
        np.testing.assert_array_equal([0, 27.25, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term2.loc['2020-07-22'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 17.175, 0, 0, 0, 0], term2.loc['2020-01-17'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.6, 0, 0, 0], term2.loc['2019-12-12'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 19.075, 0, 0, 0], term2.loc['2019-12-09'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.75, 0, 0, 0], term2.loc['2019-11-25'])
        # test for combine
        term3 = combine_data(term1, term2)
        np.testing.assert_array_equal([28.775, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term3.loc['2020-09-10'])
        np.testing.assert_array_equal([21.71, 26.225, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term3.loc['2020-08-19'])
        np.testing.assert_array_equal([28.775, 30.275, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], term3.loc['2020-07-23'])
        np.testing.assert_array_equal([0, 27.25, 29.125, 0, 0, 0, 0, 0, 0, 0, 0, 0], term3.loc['2020-07-22'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 17.175, 17.475, 0, 0, 0], term3.loc['2020-01-17'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.6, 19.1, 0, 0], term3.loc['2019-12-12'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 19.075, 19.2, 0, 0], term3.loc['2019-12-09'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.75, 0, 0, 0], term3.loc['2019-11-25'])
        futures_0116 = load_futures_by_csv(os.path.join(TEST_DATA_ROOT, '2013-01-16.csv'))
        term4 = generate_term_structure(delivery_dates, futures_0116, '2013-01-16')
        np.testing.assert_array_equal([0], term4.loc['2013-01-16'])
        np.testing.assert_array_equal([14.2], term4.loc['2013-01-15'])
        np.testing.assert_array_equal([15.6], term4.loc['2013-01-02'])
        futures_0116 = load_futures_by_csv(os.path.join(TEST_DATA_ROOT, '2012-01-16.csv'))
        term5 = generate_term_structure(delivery_dates, futures_0116, '2012-01-16')
        self.assertEqual(None, term5)


    def testCombineAll(self):
        """"""
        self.assertEqual((True, '2020-08-19'), is_futures_file('2020-08-19.csv'))
        self.assertEqual((True, '2020-09-16'), is_futures_file('2020-09-16.csv'))
        self.assertEqual((True, '2020-08-19'), is_futures_file('./data/2020-08-19.csv'))
        self.assertEqual((False, None), is_futures_file('VIX.csv'))
        delivery_dates, schedule_days = run_over_time_frame()
        info = combine_all(delivery_dates, TEST_DATA_ROOT)
        self.assertEqual((211, 12), info.shape)
        np.testing.assert_array_equal([28.775, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2020-09-10'])
        np.testing.assert_array_equal([21.71, 26.225, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2020-08-19'])
        np.testing.assert_array_equal([28.775, 30.275, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2020-07-23'])
        np.testing.assert_array_equal([0, 27.25, 29.125, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2020-07-22'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 17.175, 17.475, 0, 0, 0], info.loc['2020-01-17'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.6, 19.1, 0, 0], info.loc['2019-12-12'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 19.075, 19.2, 0, 0], info.loc['2019-12-09'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 18.75, 0, 0, 0], info.loc['2019-11-25'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2013-01-16'])
        np.testing.assert_array_equal([14.2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2013-01-15'])
        np.testing.assert_array_equal([15.6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], info.loc['2013-01-02'])


if __name__ == '__main__':
    ut.main()

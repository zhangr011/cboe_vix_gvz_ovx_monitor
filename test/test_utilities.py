# encoding: UTF-8

import unittest as ut
import os

from cboe_monitor.utilities import \
    TEST_DATA_ROOT, DATE_FORMAT, get_day_index, \
    run_over_time_frame, get_file_path, \
    check_data_integrity, generate_csv_checksums, generate_futures_chain, \
    index_distribution_of_per

import pandas_datareader as pdr
import pandas as pd
from datetime import datetime


#----------------------------------------------------------------------
class TestUnititiesCase(ut.TestCase):

    #----------------------------------------------------------------------
    def testDayIndex(self):
        """test for get_day_index"""
        _last_day1 = datetime.strptime("2020-12-02 0", "%Y-%m-%d %H")
        _last_day2 = datetime.strptime("2020-12-02 22", "%Y-%m-%d %H")
        _last_day3 = datetime.strptime("2020-12-02 23", "%Y-%m-%d %H")
        self.assertEqual("2020-12-01", get_day_index(_last_day1, 23))
        self.assertEqual("2020-12-01", get_day_index(_last_day2, 23))
        self.assertEqual("2020-12-02", get_day_index(_last_day3, 23))

    #----------------------------------------------------------------------
    def testDataIntegrity(self):
        """test for check data's integrity"""
        filepath = os.path.join(TEST_DATA_ROOT, '2013-01-16.csv')
        self.assertEqual(True, check_data_integrity(filepath, '2013-01-16'))
        self.assertEqual(False, check_data_integrity(filepath, '2013-01-15'))
        # for empty file
        filepath = os.path.join(TEST_DATA_ROOT, '2012-01-16.csv')
        self.assertEqual(False, check_data_integrity(filepath, '2012-01-16'))

    #----------------------------------------------------------------------
    def testGenerateChecksums(self):
        """"""
        ret = generate_csv_checksums(TEST_DATA_ROOT)
        self.assertEqual([('2013-01-16.csv', '5177dab14a912f774a8478bfbefb9e4100023c45'),
                          ('2020-08-19.csv', '8b5b795edfea70bba2c183e6b198c769ea4dd8cb')], ret)

    #----------------------------------------------------------------------
    def testRunOverTimeFrame(self):
        """test for generating the delivery date"""
        seq, days = run_over_time_frame()
        self.assertEqual('2013-01-16', seq[0])
        self.assertEqual('2013-02-13', seq[1])
        self.assertEqual('2013-03-20', seq[2])
        self.assertEqual('2013-04-17', seq[3])
        self.assertEqual('2013-05-22', seq[4])
        self.assertEqual('2013-06-19', seq[5])
        self.assertEqual('2013-07-17', seq[6])
        self.assertEqual('2013-08-21', seq[7])
        self.assertEqual('2013-09-18', seq[8])
        self.assertEqual('2013-10-16', seq[9])
        self.assertEqual('2013-11-20', seq[10])
        self.assertEqual('2013-12-18', seq[11])
        self.assertEqual('2019-08-21', seq[79])
        self.assertEqual('2019-09-18', seq[80])
        self.assertEqual('2019-10-16', seq[81])
        self.assertEqual('2019-11-20', seq[82])
        self.assertEqual('2019-12-18', seq[83])
        self.assertEqual('2020-01-22', seq[84])
        self.assertEqual('2020-02-19', seq[85])
        self.assertEqual('2020-03-18', seq[86])
        self.assertEqual('2020-04-15', seq[87])
        self.assertEqual('2020-05-20', seq[88])
        self.assertEqual('2020-06-17', seq[89])
        self.assertEqual('2020-07-22', seq[90])
        self.assertEqual('2020-08-19', seq[91])
        self.assertEqual('2020-09-16', seq[92])
        self.assertEqual('2020-10-21', seq[93])
        self.assertEqual('2020-11-18', seq[94])
        self.assertEqual('2020-12-16', seq[95])

    #----------------------------------------------------------------------
    def testFuturesChain(self):
        """"""
        gcs = generate_futures_chain('GC', 'CMX', '2020-09-16')
        self.assertEqual(True, 'GCZ20.CMX' in gcs)
        self.assertEqual(True, 'GCX20.CMX' in gcs)
        self.assertEqual(True, 'GCU20.CMX' in gcs)
        self.assertEqual(True, 'GCU21.CMX' in gcs)
        cls = generate_futures_chain('CL', 'NYM', '2020-12-16')
        self.assertEqual(True, 'CLZ20.NYM' in cls)
        self.assertEqual(True, 'CLZ21.NYM' in cls)
        self.assertEqual(True, 'CLX21.NYM' in cls)

    #----------------------------------------------------------------------
    def testIndexDistribute(self):
        """"""
        self.assertEqual(list(range(1, 100)), index_distribution_of_per(100))
        self.assertEqual(list(range(2, 101)), index_distribution_of_per(101))
        target = list(range(3, 102))
        target[0] = 2
        self.assertEqual(target, index_distribution_of_per(102))
        target = list(range(2, 200, 2))
        self.assertEqual(target, index_distribution_of_per(199))
        self.assertEqual(target, index_distribution_of_per(200))


if __name__ == '__main__':
    ut.main()

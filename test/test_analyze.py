# encoding: UTF-8

import unittest as ut
import numpy as np
import pandas as pd

from cboe_monitor.utilities import \
    run_over_time_frame, \
    generate_term_structure_mask, shift_term_structure_mask



def mk_datetime_key(date_str: str):
    return pd.datetime(date_str)


class TestAnalyze(ut.TestCase):

    def testDateCompare(self):
        """for date str compare"""
        self.assertEqual(True, '2020-09-10' > '2020-09-09')
        self.assertEqual(True, '2020-09-01' > '2020-08-31')
        self.assertEqual(True, '2020-01-01' > '2019-12-31')

    def testDateMask(self):
        """for mask test"""
        delivery_dates, schedule_days = run_over_time_frame()
        mask = generate_term_structure_mask(
            delivery_dates, schedule_days, '2020-09-10')
        self.assertEqual((253, 12), mask.shape)
        np.testing.assert_array_equal([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-09-10'])
        np.testing.assert_array_equal([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-08-19'])
        np.testing.assert_array_equal([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-07-23'])
        np.testing.assert_array_equal([0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-07-22'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], mask.loc['2019-10-16'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], mask.loc['2019-09-19'])
        # boundary test for deliver date
        mask = generate_term_structure_mask(
            delivery_dates, schedule_days, '2020-09-16')
        np.testing.assert_array_equal([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-09-16'])
        np.testing.assert_array_equal([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-09-15'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], mask.loc['2019-10-16'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], mask.loc['2019-09-19'])
        mask = generate_term_structure_mask(
            delivery_dates, schedule_days, '2020-09-17')
        np.testing.assert_array_equal([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-09-17'])
        np.testing.assert_array_equal([0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], mask.loc['2020-09-16'])
        np.testing.assert_array_equal([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], mask.loc['2019-10-17'])
        # test the shift
        shift = shift_term_structure_mask(delivery_dates, schedule_days, mask)
        self.assertEqual((257, 12), shift.shape)


if __name__ == '__main__':
    ut.main()

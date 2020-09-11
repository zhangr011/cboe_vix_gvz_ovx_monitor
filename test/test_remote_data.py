#encoding: UTF-8

import unittest as ut
from cboe_monitor.remote_data import RemoteDataFactory, SYNC_DATA_MODE


class TestRemoteData(ut.TestCase):

    def testYahooData(self):
        """test for the yahoo data fetch"""
        data_fac = RemoteDataFactory('./test/data', None)
        rdata = data_fac.create('^VIX', '^VIX', SYNC_DATA_MODE.PANDAS_DATAREADER)
        res = rdata.sync_data()
        self.assertEqual(6, res.shape[1])
        # check index name changed
        self.assertEqual('Trade Date', res.index.name)


if __name__ == '__main__':
    ut.main()

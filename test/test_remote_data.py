#encoding: UTF-8

import unittest as ut
import pandas as pd
from cboe_monitor.utilities import load_vix_by_csv
from cboe_monitor.remote_data import RemoteDataFactory, SYNC_DATA_MODE


class TestRemoteData(ut.TestCase):

    def testCboeData(self):
        """"""
        data_fac = RemoteDataFactory('./test/data', None)
        rdata = data_fac.create('^VIX', '^VIX', SYNC_DATA_MODE.HTTP_DOWNLOAD_CBOE)
        res = rdata.sync_data()
        self.assertEqual(5, res.shape[1])
        rdata = data_fac.create('^GVZ',  '^GVZ', SYNC_DATA_MODE.HTTP_DOWNLOAD_CBOE)
        res = rdata.sync_data()
        self.assertEqual(5, res.shape[1])
        rdata = data_fac.create('^OVX',  '^OVX', SYNC_DATA_MODE.HTTP_DOWNLOAD_CBOE)
        res = rdata.sync_data()
        self.assertEqual(5, res.shape[1])
        li, df_old = rdata.get_last_index()
        # drop 2 need to update all day's info
        # rdata.drop_last_n_test(2)
        # drop 1 only need update the last trading day's info
        rdata.drop_last_n_test(1)
        li, df_drop = rdata.get_last_index()
        self.assertEqual(df_old.shape[0], df_drop.shape[0] + 1)
        rdata.sync_data()
        li, df_new = rdata.get_last_index()
        self.assertEqual(df_old.shape[0], df_new.shape[0])
        self.assertEqual(df_old.index[-1], df_new.index[-1])
        self.assertEqual(df_old.index[-2], df_new.index[-2])
        self.assertEqual(df_old.index[-3], df_new.index[-3])


    def notestInrementalUpdates(self):
        """"""
        data_fac = RemoteDataFactory('./test/data', None)
        rdata = data_fac.create('^VIX', '^VIX', SYNC_DATA_MODE.PANDAS_DATAREADER_YAHOO)
        res = rdata.sync_data()
        self.assertEqual(6, res.shape[1])
        # check index name changed
        self.assertEqual('Trade Date', res.index.name)
        # check the size
        local_path = rdata.get_local_path()
        # drop the last 5 rows
        lf = load_vix_by_csv(local_path)
        old_size = lf.shape[0]
        lf = lf.iloc[:-5]
        lf.to_csv(local_path)
        idx, df = rdata.get_last_index()
        self.assertNotEqual(None, idx)
        self.assertEqual(6, df.shape[1])
        res = rdata.sync_data()
        nf = load_vix_by_csv(local_path)
        self.assertEqual((old_size, 6), nf.shape)
        # reset all
        lf = lf.iloc[0:0]
        lf.to_csv(local_path)
        idx, df = rdata.get_last_index()
        self.assertEqual(None, idx)
        self.assertEqual(None, df)
        res = rdata.sync_data()
        nf = load_vix_by_csv(local_path)
        self.assertEqual((old_size, 6), nf.shape)


if __name__ == '__main__':
    ut.main()

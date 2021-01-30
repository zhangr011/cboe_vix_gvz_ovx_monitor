# encoding: UTF-8

from .utilities import \
    CHECK_SECTION, make_sure_dirs_exist, \
    get_file_path, generate_csv_checksums, combine_all, \
    analyze_diff_percent, load_futures_by_csv, load_vix_by_csv, \
    close_ma5_ma10_ma20, generate_futures_chain
from .remote_data import RemoteDataFactory, SYNC_DATA_MODE
from .logger import logger

import os, logging, configparser, threadpool
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    futures_link = ''
    symbols = []
    # futures target info
    futures_target = ''
    futures_chain_prefix = ''
    futures_chain_suffix = ''
    data_path = ''
    ini_path = ''

    pool_size = 10

    def __init__(self, delivery_dates: list = []):
        """Constructor"""
        self._delivery_dates = delivery_dates
        self.ini_parser = configparser.ConfigParser()
        self.ini_parser.read(self.ini_path)
        self.check_ini()

    #----------------------------------------------------------------------
    def download_raw_data(self, downloaded = False):
        """download the data"""
        if downloaded is True:
            return
        make_sure_dirs_exist(self.data_path)
        logger.info(f'start downloading data from {self.futures_link}')
        to_update = []
        data_fac = RemoteDataFactory(self.data_path, self.ini_parser)
        for sym in self.symbols:
            rdata = data_fac.create(
                sym, sym, SYNC_DATA_MODE.HTTP_DOWNLOAD_CBOE)
            to_update.append(rdata)
        # if self.futures_target:
        #     # add the futures target
        #     rdata = data_fac.create(
        #         self.futures_target, self.futures_target,
        #         SYNC_DATA_MODE.PANDAS_DATAREADER_YAHOO)
        #     to_update.append(rdata)
        # for sym in generate_futures_chain(self.futures_chain_prefix,
        #                                   self.futures_chain_suffix):
        #     # add the furtures chain
        #     rdata = data_fac.create(
        #         sym, sym, SYNC_DATA_MODE.PANDAS_DATAREADER)
        #     to_update.append(rdata)
        for expiration_date in self._delivery_dates:
            remote_path = os.path.join(self.futures_link, expiration_date)
            rdata = data_fac.create(
                expiration_date, remote_path, SYNC_DATA_MODE.HTTP_DOWNLOAD_FILE)
            # (None, dict_param: dict) for pass parameters by dict
            to_update.append(rdata)
        # do request in the threadpool
        requests = threadpool.makeRequests(lambda x: x.sync_data(), to_update)
        pool = threadpool.ThreadPool(self.pool_size)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        logger.info('all data downloaded. ')
        checksums = generate_csv_checksums(self.data_path)
        # save the local file's checksum
        self.save_checksums(checksums)

    #----------------------------------------------------------------------
    def combine_all(self, max_times: int = 12):
        """combine all futures' term structure"""
        self._term = combine_all(self._delivery_dates, self.data_path, max_times)
        # drop the first columns of 0, it's useless however
        self._term = self._term[self._term.iloc[:][0] > 0]
        return self._term

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def check_ini(self):
        """check ini"""
        if CHECK_SECTION not in self.ini_parser.sections():
            self.ini_parser.add_section(CHECK_SECTION)
            self.save_ini()

    #----------------------------------------------------------------------
    def save_checksums(self, checksums: list):
        """save csv files' checksum to the ini config file"""
        for csv_name, checksum in checksums:
            self.ini_parser.set(CHECK_SECTION, csv_name, checksum)
        self.save_ini()

    #----------------------------------------------------------------------
    def save_ini(self):
        """save ini"""
        self.ini_parser.write(open(self.ini_path, 'w'))


#----------------------------------------------------------------------
class VIXDataManager(DataManager):

    futures_link = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/VX/'
    symbols = ['^VIX']
    data_path = get_file_path('vix')
    ini_path = get_file_path('vix.ini')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        delta_p = analyze_diff_percent(self._term)
        # drop the first column, it's useless
        delta_p.drop(0, axis = 1, inplace = True)
        vix = load_vix_by_csv(os.path.join(self.data_path, 'VIX.csv'))
        vix.dropna(subset = ['Close'], inplace = True)
        vix.drop(columns = ['Volume'], inplace = True)
        close_ma5_ma10_ma20(vix)
        return {'vix_diff': delta_p, 'vix' : vix}


#----------------------------------------------------------------------
class GVZDataManager(DataManager):


    # deprecated due to the furtures are delisted
    futures_link = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/GV/'
    symbols = ['^GVZ']
    # GC=F, GCX20.CMX, GCZ20.CMX, GCH21.CMX etc
    futures_target = 'GC=F'
    futures_chain_prefix = 'GC'
    futures_chain_suffix = 'CMX'
    data_path = get_file_path('gvz')
    ini_path = get_file_path('gvz.ini')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        gvz = load_vix_by_csv(os.path.join(self.data_path, 'GVZ.csv'))
        gvz.dropna(subset = ['Close'], inplace = True)
        gvz.drop(columns = ['Volume'], inplace = True)
        close_ma5_ma10_ma20(gvz)
        return {'gvz': gvz}


#----------------------------------------------------------------------
class OVXDataManager(DataManager):

    # deprecated due to the furtures are delisted
    futures_link = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/OV/'
    symbols = ['^OVX']
    # CL=F, CLX20.NYM, CLH21.NYM
    futures_target = 'CL=F'
    futures_chain_prefix = 'CL'
    futures_chain_suffix = 'NYM'
    data_path = get_file_path('ovx')
    ini_path = get_file_path('ovx.ini')

    #----------------------------------------------------------------------
    def analyze(self):
        """analyze the data"""
        ovx = load_vix_by_csv(os.path.join(self.data_path, 'OVX.csv'))
        ovx.dropna(subset = ['Close'], inplace = True)
        ovx.drop(columns = ['Volume'], inplace = True)
        close_ma5_ma10_ma20(ovx)
        return {'ovx': ovx}

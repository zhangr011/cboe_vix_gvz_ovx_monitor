# encoding: UTF-8

from .utilities import \
    CHECK_SECTION, make_sure_dirs_exist, \
    get_file_path, generate_csv_checksums, combine_all
from .remote_data import RemoteDataFactory, SYNC_DATA_MODE
from .logger import logger

import os, logging, configparser, threadpool
import pandas as pd


#----------------------------------------------------------------------
class DataManager():

    futures_link = ''
    symbols = []
    data_path = ''
    ini_path = ''

    pool_size = 10

    def __init__(self, delivery_dates: list):
        """Constructor"""
        self._delivery_dates = delivery_dates
        self.ini_parser = configparser.ConfigParser()
        self.ini_parser.read(self.ini_path)
        self.check_ini()

    #----------------------------------------------------------------------
    def download_raw_data(self):
        """download the data"""
        make_sure_dirs_exist(self.data_path)
        logger.info(f'start downloading data from {self.futures_link}')
        to_update = []
        data_fac = RemoteDataFactory(self.data_path, self.ini_parser)
        for sym in self.symbols:
            rdata = data_fac.create(
                sym, sym, SYNC_DATA_MODE.PANDAS_DATAREADER)
            to_update.append(rdata)
        for expiration_date in self._delivery_dates:
            remote_path = os.path.join(self.futures_link, expiration_date)
            rdata = data_fac.create(
                expiration_date, remote_path, SYNC_DATA_MODE.HTTP_DOWNLOAD)
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
        df = combine_all(self._delivery_dates, self.data_path, max_times)
        return df

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
    symbols = ['^VIX', '^GVZ', '^OVX']
    data_path = get_file_path('vix')
    ini_path = get_file_path('vix.ini')


#----------------------------------------------------------------------
class GVZDataManager(DataManager):


    # deprecated due to the furtures are delisted
    futures_link = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/GV/'
    symbols = []
    data_path = get_file_path('gvz')
    ini_path = get_file_path('gvz.ini')


#----------------------------------------------------------------------
class OVXDataManager(DataManager):

    # deprecated due to the furtures are delisted
    futures_link = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/OV/'
    symbols = []
    data_path = get_file_path('ovx')
    ini_path = get_file_path('ovx.ini')

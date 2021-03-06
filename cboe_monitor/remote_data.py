#encoding: UTF-8

from .utilities import \
    CHECK_SECTION, INDEX_KEY, DATE_FORMAT, \
    check_file_integrity, load_vix_by_csv, get_recent_trading_days, \
    OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME
from .util_http_bs4 import get_content_json
from .logger import logger

from abc import abstractclassmethod, ABCMeta
from enum import Enum
import os, re, configparser, traceback, urllib, urllib3, requests, http, time
from datetime import datetime, timedelta
import dateutil.parser as date_parser
import pandas as pd
import pandas_datareader as pdr


#----------------------------------------------------------------------
class SYNC_DATA_MODE(Enum):
    HTTP_DOWNLOAD_FILE = 1
    HTTP_DOWNLOAD_YAHOO = 2
    HTTP_DOWNLOAD_CBOE = 3
    PANDAS_DATAREADER_YAHOO = 11


#----------------------------------------------------------------------
class CBOE_REMOTE_DATA_TYPE(Enum):
    HISTORY = 1
    QUOTES  = 2



FIX_FILE_PATTERN = re.compile(r'\^|\=')


#----------------------------------------------------------------------
class IRemoteData(metaclass = ABCMeta):

    def __init__(self, ini_parser: configparser.ConfigParser,
                 data_path: str, local: str, remote_path: str):
        """Constructor"""
        self.ini_parser = ini_parser
        self.data_path = data_path
        self.local = self.fix_file_name(local)
        self.remote_path = remote_path

    #----------------------------------------------------------------------
    def fix_file_name(self, local: str):
        """fix the local name"""
        res = FIX_FILE_PATTERN.subn('', local)
        return res[0]

    #----------------------------------------------------------------------
    def get_local_file(self):
        """get local file name"""
        return f'{self.local}.csv'

    #----------------------------------------------------------------------
    def get_local_path(self):
        """get the local file path"""
        return os.path.join(self.data_path, self.get_local_file())

    #----------------------------------------------------------------------
    def get_local_checksum(self):
        """get local file's checksum"""
        try:
            if self.ini_parser:
                return self.ini_parser.get(CHECK_SECTION, self.get_local_file())
            return None
        except configparser.NoOptionError:
            return None

    #----------------------------------------------------------------------
    def get_last_index(self):
        """get the local last index"""
        try:
            df = load_vix_by_csv(self.get_local_path())
            return df.index[-1], df
        except (FileNotFoundError, IndexError):
            return None, None

    #----------------------------------------------------------------------
    def sync_data(self):
        """sync the data if needed. """
        checksum = self.get_local_checksum()
        local_path = self.get_local_path()
        if not check_file_integrity(local_path, checksum):
            try:
                data = self.do_sync_data()
                logger.info(f'{self.get_local_path()} downloaded. ')
                return data
            except (http.client.RemoteDisconnected,
                    urllib.error.URLError,
                    urllib.error.HTTPError,
                    urllib3.exceptions.MaxRetryError,
                    requests.exceptions.ConnectionError,
                    pdr._utils.RemoteDataError):
                # for network error handling
                # logger.error(f'{self.remote_path} download failed: {traceback.format_exc()}')
                logger.error(f'{self.remote_path} download failed: {traceback.format_exc(limit = 0)}')
            except:
                logger.error(f'{self.remote_path} download failed: {traceback.format_exc()}')

    #----------------------------------------------------------------------
    @abstractclassmethod
    def do_sync_data(self):
        """do the sync"""
        pass

    #----------------------------------------------------------------------
    def drop_last_n_test(self, n: int = 1):
        """"""
        li, df = self.get_last_index()
        if li is None:
            return
        # df = df.iloc[:-n]
        df.drop(df.tail(n).index, inplace = True)
        df.to_csv(path_or_buf = self.get_local_path())


#----------------------------------------------------------------------
class RemoteHttpFileData(IRemoteData):

    #----------------------------------------------------------------------
    def do_sync_data(self):
        """sync the data"""
        data = pd.read_csv(self.remote_path)
        # without index
        data.to_csv(index = False, path_or_buf = self.get_local_path())
        return data


#----------------------------------------------------------------------
class IRemoteHttpData(IRemoteData):


    #----------------------------------------------------------------------
    def fix_data_index(self, data):
        data.index = data.index.strftime(DATE_FORMAT)

    #----------------------------------------------------------------------
    @abstractclassmethod
    def query_remote(self, start: str):
        """query the remote data"""
        raise NotImplementedError

    #----------------------------------------------------------------------
    def do_sync_data(self):
        """sync the data"""
        li, ldf = self.get_last_index()
        data = self.query_remote(li)
        data.index.rename(INDEX_KEY, inplace = True)
        # with index
        if ldf is None:
            data.to_csv(path_or_buf = self.get_local_path())
        else:
            # append data to the local path, this is not work due to the last
            # row is changed from time to time
            # data.to_csv(path_or_buf = self.get_local_path(), mode = 'a', header = False)
            self.fix_data_index(data)
            data = pd.concat([ldf, data])
            # drop the duplicated index rows
            data = data[~data.index.duplicated(keep = 'last')]
            data.to_csv(path_or_buf = self.get_local_path())
        return data


#----------------------------------------------------------------------
class RemotePDRYahooData(IRemoteHttpData):

    #----------------------------------------------------------------------
    def query_remote(self, start: str):
        """query the remote data"""
        data = pdr.get_data_yahoo(self.remote_path, start = start)
        return data


#----------------------------------------------------------------------
class RemoteHttpYahooData(IRemoteHttpData):

    download_url = "https://query1.finance.yahoo.com/v7/finance/download/"
    download_url_suffix = "interval=1d&events=history&includeAdjustedClose=true"

    #----------------------------------------------------------------------
    def query_remote(self, start: str):
        """download the data"""
        url = self.mk_url(start)
        logger.info(f'download url: {url}')
        data = pd.read_csv(url, index_col = 0)
        return data

    #----------------------------------------------------------------------
    def mk_url(self, start: str):
        """make the download url"""
        if start is None:
            start = '2010-01-01'
        # refresh the last 3 days data
        start_datetime = date_parser.parse(start) + timedelta(days = -3)
        start_utime = int(time.mktime(start_datetime.timetuple()))
        end_datetime = datetime.now().replace(hour = 23, minute = 59, second = 59)
        end_utime = int(time.mktime(end_datetime.timetuple()))
        # This needed because yahoo returns data shifted by 4 hours ago.
        four_hours_in_seconds = 14400
        start_utime += four_hours_in_seconds
        end_utime += four_hours_in_seconds
        return f"{self.download_url}{self.remote_path}?period1={start_utime}&period2={end_utime}&{self.download_url_suffix}"

    #----------------------------------------------------------------------
    def fix_data_index(self,  data):
        """no need to fix. """
        pass


#----------------------------------------------------------------------
class RemoteHttpCBOEData(IRemoteHttpData):

    history_url = "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_%s.json"
    quotes_url = "https://cdn.cboe.com/api/global/delayed_quotes/quotes/_%s.json"
    query_dates = []

    #----------------------------------------------------------------------
    def get_url(self, start: str):
        """"""
        self.query_dates = get_recent_trading_days()
        if start is None:
            return True, CBOE_REMOTE_DATA_TYPE.HISTORY, self.history_url % self.local
        else:
            if start in self.query_dates:
                if start == self.query_dates[-2]:
                    # we only need to download the last day's data
                    return True, CBOE_REMOTE_DATA_TYPE.QUOTES, self.quotes_url % self.local
                elif start == self.query_dates[-1]:
                    # no data download needed
                    return False
            # some data have been lost, we need to download all data again
            return True, CBOE_REMOTE_DATA_TYPE.HISTORY, self.history_url % self.local

    #----------------------------------------------------------------------
    def do_data_handle(self, params: tuple):
        """"""
        _check, query_type, url = params
        data = get_content_json(url)
        if CBOE_REMOTE_DATA_TYPE.HISTORY == query_type:
            data = self.do_history_data_handle(data)
        elif CBOE_REMOTE_DATA_TYPE.QUOTES == query_type:
            data = self.do_quotes_data_handle(data)
        else:
            raise NotImplementedError(f'not supported cboe remote type: {query_type}')
        data.index.rename(INDEX_KEY, inplace = True)
        logger.info(f'remote data from {url} downloaded. ')
        return data

    #----------------------------------------------------------------------
    def do_history_data_handle(self, data_dic: dict):
        """convert history json data to pandas dataframe"""
        data = data_dic['data']
        df = pd.DataFrame(data)
        df.set_index('date', inplace = True)
        df.rename({'open'  : OPEN_PRICE_NAME,
                   'high'  : HIGH_PRICE_NAME,
                   'low'   : LOW_PRICE_NAME,
                   'close' : CLOSE_PRICE_NAME,
                   'volume': VOLUME_NAME}, axis = 1, inplace = True)
        return df

    #----------------------------------------------------------------------
    def do_quotes_data_handle(self, data_dic: dict):
        """convert quotes json data to pandas dataframe"""
        data = data_dic['data']
        df = pd.DataFrame([data])
        df.set_index('last_trade_time', inplace = True)
        df.index = df.index.str.replace('T[0-9]{2}:[0-9]{2}:[0-9]{2}', '', regex = True)
        df.rename({'open' : OPEN_PRICE_NAME,
                   'high' : HIGH_PRICE_NAME,
                   'low'  : LOW_PRICE_NAME,
                   'close' : CLOSE_PRICE_NAME,
                   'volume' : VOLUME_NAME}, axis = 1, inplace = True)
        df = df[[OPEN_PRICE_NAME, HIGH_PRICE_NAME, LOW_PRICE_NAME, CLOSE_PRICE_NAME, VOLUME_NAME]]
        if df.index[-1] != self.query_dates[-1]:
            raise ValueError(f'date expected error: for {self.query_dates[-1]} but get {df.index[-1]}. ')
        return df


    #----------------------------------------------------------------------
    def query_remote(self, start: str):
        """download the data"""
        url_params = self.get_url(start)
        if url_params is False:
            return False
        data = self.do_data_handle(url_params)
        return data

    #----------------------------------------------------------------------
    def do_sync_data(self, index: int = 0):
        """sync the data"""
        if 0 == index:
            self._query_times = 0
        else:
            self._query_times += 1
        li, ldf = self.get_last_index()
        data = self.query_remote(li)
        if data is False or self._query_times >= 2:
            return ldf
        data.index.rename(INDEX_KEY, inplace = True)
        # with index
        if ldf is None:
            data.to_csv(path_or_buf = self.get_local_path())
        else:
            # append data to the local path, this is not work due to the last
            # row is changed from time to time
            # data.to_csv(path_or_buf = self.get_local_path(), mode = 'a', header = False)
            data = pd.concat([ldf, data])
            # drop the duplicated index rows
            data = data[~data.index.duplicated(keep = 'last')]
            data.to_csv(path_or_buf = self.get_local_path())
        # recursive call sync data
        time.sleep(1)
        return self.do_sync_data(1)


#----------------------------------------------------------------------
class RemoteDataFactory():

    data_path = ''
    ini_parser = None

    def __init__(self, data_path: str, ini_parser: configparser.ConfigParser):
        """Constructor"""
        self.data_path = data_path
        self.ini_parser = ini_parser

    #----------------------------------------------------------------------
    def create(self, local: str, remote: str, via: SYNC_DATA_MODE):
        """the creator of RemoteData"""
        if SYNC_DATA_MODE.HTTP_DOWNLOAD_FILE == via:
            return RemoteHttpFileData(
                self.ini_parser, self.data_path, local, remote)
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_CBOE == via:
            return RemoteHttpCBOEData(
                self.ini_parser, self.data_path, local, remote)
        elif SYNC_DATA_MODE.PANDAS_DATAREADER_YAHOO == via:
            return RemotePDRYahooData(
                self.ini_parser, self.data_path, local, remote)
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_YAHOO == via:
            return RemoteHttpYahooData(
                self.ini_parser, self.data_path, local, remote)
        raise NotImplementedError

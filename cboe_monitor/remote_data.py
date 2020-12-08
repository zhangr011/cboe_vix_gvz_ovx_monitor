#encoding: UTF-8

from .utilities import \
    CHECK_SECTION, INDEX_KEY, DATE_FORMAT, \
    check_file_integrity, load_vix_by_csv
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
    HTTP_DOWNLOAD = 1
    HTTP_DOWNLOAD_YAHOO = 2
    PANDAS_DATAREADER_YAHOO = 11


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
class RemoteHttpData(IRemoteData):

    #----------------------------------------------------------------------
    def do_sync_data(self):
        """sync the data"""
        data = pd.read_csv(self.remote_path)
        # without index
        data.to_csv(index = False, path_or_buf = self.get_local_path())
        return data


#----------------------------------------------------------------------
class RemotePDRYahooData(IRemoteData):

    #----------------------------------------------------------------------
    def get_last_index(self):
        """get the local last index"""
        try:
            df = load_vix_by_csv(self.get_local_path())
            return df.index[-1], df
        except (FileNotFoundError, IndexError):
            return None, None

    #----------------------------------------------------------------------
    def query_remote(self, start: str):
        """query the remote data"""
        data = pdr.get_data_yahoo(self.remote_path, start = start)
        return data

    #----------------------------------------------------------------------
    def fix_data_index(self, data):
        data.index = data.index.strftime(DATE_FORMAT)

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
class RemoteHttpYahooData(RemotePDRYahooData):

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
        if SYNC_DATA_MODE.HTTP_DOWNLOAD == via:
            return RemoteHttpData(
                self.ini_parser, self.data_path, local, remote)
        elif SYNC_DATA_MODE.PANDAS_DATAREADER_YAHOO == via:
            return RemotePDRYahooData(
                self.ini_parser, self.data_path, local, remote)
        elif SYNC_DATA_MODE.HTTP_DOWNLOAD_YAHOO == via:
            return RemoteHttpYahooData(
                self.ini_parser, self.data_path, local, remote)
        raise NotImplementedError

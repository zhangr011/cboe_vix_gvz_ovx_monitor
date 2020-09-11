#encoding: UTF-8

from .utilities import check_file_integrity, CHECK_SECTION, INDEX_KEY
from .logger import logger

from abc import abstractclassmethod, ABCMeta
from enum import Enum
import os, re, configparser, traceback, urllib
import pandas as pd
import pandas_datareader as pdr


#----------------------------------------------------------------------
class SYNC_DATA_MODE(Enum):
    HTTP_DOWNLOAD = 1
    PANDAS_DATAREADER = 2


FIX_FILE_PATTERN = re.compile(r'\^')


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
            except urllib.error.HTTPError:
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
class RemoteYahooData(IRemoteData):

    #----------------------------------------------------------------------
    def do_sync_data(self):
        """sync the data"""
        data = pdr.get_data_yahoo(self.remote_path)
        data.index.rename(INDEX_KEY, inplace = True)
        # with index
        data.to_csv(path_or_buf = self.get_local_path())
        return data


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
        elif SYNC_DATA_MODE.PANDAS_DATAREADER == via:
            return RemoteYahooData(
                self.ini_parser, self.data_path, local, remote)
        raise NotImplementedError

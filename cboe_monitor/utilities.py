# encoding: UTF-8

import os, datetime, hashlib
import pandas as pd
import pandas_market_calendars as market_cal

from .logger import logger

#Date format: Year-Month-Day
DATE_FORMAT ='%Y-%m-%d'

cboe_calendar = market_cal.get_calendar('CME')

ONE_DAY = datetime.timedelta(days = 1)
SEVEN_DAYS = datetime.timedelta(days = 7)
TZ_INFO = 'America/Chicago'

# data root
DATA_ROOT = './data'

# section name for ini parser
CHECK_SECTION = 'checksum'
# index key for file check
INDEX_KEY = 'Trade Date'


#----------------------------------------------------------------------
def is_business_day(input_date, schedule_days):
    input_date_str = input_date.strftime(DATE_FORMAT)
    holiday_check = cboe_calendar.open_at_time(schedule_days, pd.Timestamp(input_date_str + ' 12:00', tz=TZ_INFO))
    return holiday_check


#----------------------------------------------------------------------
def run_over_time_frame():
    """run over all the delivery dates"""
    logger.info("Calculating contract expiration dates...")
    futures_exp_dates = []
    # two years later
    end_time = datetime.datetime.now() + datetime.timedelta(days = 420)
    cur_time = datetime.datetime(2013, 1, 1)
    schedule_days = cboe_calendar.schedule(cur_time.strftime(DATE_FORMAT), end_time.strftime(DATE_FORMAT))
    while cur_time.weekday() != 4:
        # find the first friday
        cur_time += ONE_DAY
    month_weeks = 0
    while cur_time < end_time:
        month_weeks += 1
        if 3 == month_weeks:
            # find the month's 3rd friday
            delivery_time = cur_time
            while not is_business_day(delivery_time, schedule_days):
                # make sure it's a business day
                delivery_time -= ONE_DAY
            delivery_time -= datetime.timedelta(days = 30)
            futures_exp_dates.append(delivery_time.strftime(DATE_FORMAT))
        next_time = cur_time + SEVEN_DAYS
        if next_time.month != cur_time.month:
            # new month comes, reset the month_weeks
            month_weeks = 0
        cur_time = next_time
    futures_exp_dates.pop(0)
    logger.info("Expiration Dates Generated.")
    return futures_exp_dates


#----------------------------------------------------------------------
def make_sure_dirs_exist(path):
    """确保目录存在"""
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)
    if not os.path.exists(path):
        return False
    return True


#----------------------------------------------------------------------
def hash_file(filename):
    # make a hash object
    hash = hashlib.sha1()
    with open(filename,'rb') as file:
        # loop until end of file
        chunk = 0
        while chunk != b'':
            # read only 1024 bytes at a timedelta
            chunk = file.read(1024)
            hash.update(chunk)
        # return hex of digest
        return hash.hexdigest()


#----------------------------------------------------------------------
def get_file_path(filename: str) -> str:
    return os.path.join(DATA_ROOT, filename)


#----------------------------------------------------------------------
def check_file_integrity(path: str, checksum: str):
    """check the local path data """
    if checksum and hash_file(path) == checksum:
        return True
    return False


#----------------------------------------------------------------------
def check_data_integrity(path: str, date: str):
    """check data's integrity"""
    data = pd.read_csv(path)
    if not data.empty and data[INDEX_KEY].iloc[-1] == date:
        return True
    return False


#----------------------------------------------------------------------
def generate_csv_checksums(path: str):
    """generate all csv files' checksums"""
    checksums = []
    for root, dirs, filenames in os.walk(path):
        for fn in filenames:
            date, ext = os.path.splitext(fn)
            if '.csv' == ext:
                filepath = os.path.join(root, fn)
                if check_data_integrity(filepath, date):
                    checksums.append((fn, hash_file(filepath)))
    return checksums

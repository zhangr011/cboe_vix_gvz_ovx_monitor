# encoding: UTF-8

import os, datetime, hashlib, glob, re
import pandas as pd
import numpy as np
import pandas_market_calendars as market_cal
from functools import reduce

from .logger import logger


# Date format: Year-Month-Day
DATE_FORMAT ='%Y-%m-%d'
DATE_FORMAT_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')
# INIT_DATE for fix empty delivery dates
INIT_DATE = '1900-01-01'

cboe_calendar = market_cal.get_calendar('CME')

ONE_DAY = datetime.timedelta(days = 1)
SEVEN_DAYS = datetime.timedelta(days = 7)
TZ_INFO = 'America/Chicago'

# data root
DATA_ROOT = './data'
TEST_DATA_ROOT = './test/data'

# section name for ini parser
CHECK_SECTION = 'checksum'
# index key for file check
INDEX_KEY = 'Trade Date'
SETTLE_PRICE_NAME = 'Settle'
CLOSE_PRICE_NAME = 'Close'


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
    # about 14 months later
    end_time = datetime.datetime.now() + datetime.timedelta(days = 420)
    cur_time = datetime.datetime(2013, 1, 1)
    schedule_days = cboe_calendar.schedule(cur_time.strftime(DATE_FORMAT),
                                           end_time.strftime(DATE_FORMAT))
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
    return futures_exp_dates, schedule_days


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


#----------------------------------------------------------------------
def filter_deliver_date(deliver_date: list, end_date: str, times: int = 12):
    """filter the last n deliver date, default to one year """
    dates = []
    for item in deliver_date:
        if item < end_date:
            dates.append(item)
    # return the last n dates
    return dates[-times:]


#----------------------------------------------------------------------
def filter_delivery_dates(delivery_dates: list, end_date: str, times: int = 12):
    """fitler the last n delivery dates, default to one year"""
    dates = []
    for item in delivery_dates:
        if item < end_date:
            dates.append(item)
    # return the last n dates
    return dates[-times:]


#----------------------------------------------------------------------
def shift_delivery_dates(full_delivery_dates, delivery_dates,
                         times: int = 1, size: int = 12):
    """shift the delivery dates back, default back one month"""
    sdate = delivery_dates[0]
    idx = full_delivery_dates.index(sdate) + times
    return full_delivery_dates[idx:idx + size]


#----------------------------------------------------------------------
def dup_futures_info(futures_info: pd.DataFrame, sdate: str, edate: str):
    """duplicate a date schedule between sdate and edate"""
    return pd.DataFrame(futures_info[(futures_info.index > sdate) &
                                     (futures_info.index <= edate)],
                        columns = [CLOSE_PRICE_NAME, SETTLE_PRICE_NAME])


#----------------------------------------------------------------------
def mark_by_date(row: pd.Series, sdate: str, edate: str = None):
    """"""
    date = row.name
    if isinstance(date, datetime.datetime):
        date = datetime.datetime.strftime(row.name, DATE_FORMAT)
    if date > sdate and (edate is None or date <= edate):
        # use the settle price
        if row[SETTLE_PRICE_NAME]:
            return row[SETTLE_PRICE_NAME]
        else:
            return row[CLOSE_PRICE_NAME]
    else:
        return 0


#----------------------------------------------------------------------
def generate_term_structure(delivery_dates: list,
                            futures_info: pd.DataFrame, tdate: str):
    """generate tdate's mask of term structure"""
    if futures_info.empty:
        return None
    # remain last one year's trade date
    filtered_delivery = filter_delivery_dates(delivery_dates, tdate)
    if [] == filtered_delivery:
        filtered_delivery = [INIT_DATE]
    sdate = filtered_delivery[0]
    # duplicate the target frame
    term = dup_futures_info(futures_info, sdate, tdate)
    # generate the term structure according to tdate
    reversed_delivery = reversed(filtered_delivery)
    edate = None
    for idx, sdate in enumerate(reversed_delivery):
        term.insert(loc = idx, column = idx,
                    value = term.apply(
                        lambda x: mark_by_date(x, sdate, edate), axis = 1))
        # mark the end date for the next item check
        edate = sdate
    # drop the settle price column
    term.drop(columns = [CLOSE_PRICE_NAME, SETTLE_PRICE_NAME], inplace = True)
    return term


#----------------------------------------------------------------------
def combine_data(futures_info_a: pd.DataFrame, futures_info_b: pd.DataFrame):
    """combine futures close price according to the front to later"""
    if futures_info_b is None or futures_info_b.empty:
        return futures_info_a
    return futures_info_a.add(futures_info_b, fill_value = 0)


#----------------------------------------------------------------------
def combine_all(delivery_dates: list, path: str, max_times: int = 12):
    """combine the last n futures info, default to the last 12 futures"""
    paths = sorted(glob.glob(os.path.join(path, '*.csv')), reverse = True)
    filtered_infos = []
    times = 0
    for path in paths:
        res, date = is_futures_file(path)
        if res:
            info = load_futures_by_csv(path)
            term = generate_term_structure(delivery_dates, info, date)
            if term is None:
                continue
            filtered_infos.append(term)
            times += 1
            if times >= max_times:
                break
    final = reduce(combine_data, filtered_infos)
    return final.fillna(0)


#----------------------------------------------------------------------
def load_futures_by_csv(path: str):
    """load futures info by csv"""
    df = pd.read_csv(path, index_col = False)
    df.set_index(INDEX_KEY, inplace = True)
    return df


#----------------------------------------------------------------------
def is_futures_file(path: str):
    """check the file then return the delivery date"""
    res = DATE_FORMAT_PATTERN.search(path)
    if res:
        return True, res.group(1)
    return False, None

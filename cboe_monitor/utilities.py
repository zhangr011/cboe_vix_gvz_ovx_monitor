# encoding: UTF-8

import os, sys, datetime, hashlib, glob, re
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


def set_data_root(paths: str):
    global DATA_ROOT
    DATA_ROOT = paths


# section name for ini parser
CHECK_SECTION = 'checksum'
# index key for file check
INDEX_KEY = 'Trade Date'
SETTLE_PRICE_NAME = 'Settle'
OPEN_PRICE_NAME  = 'Open'
HIGH_PRICE_NAME  = 'High'
LOW_PRICE_NAME   = 'Low'
CLOSE_PRICE_NAME = 'Close'
VOLUME_NAME      = 'Volume'

# about 3 years
HV_DISTRIBUTION_PERIODS = 260 * 3

FUTURES_CHAIN = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']

# markdown head and \n replace
MD_HEAD_PATTERN = re.compile(r'\|(:?-{3,}:?\|){4,}\n')
MD_FORMAT_PATTERN = re.compile(r'\n')
MD_FORMAT_TO = r'\n\n'


#----------------------------------------------------------------------
def is_business_day(input_date: datetime.datetime, schedule_days):
    if isinstance(input_date, datetime.datetime):
        input_date_str = input_date.strftime(DATE_FORMAT)
    else:
        input_date_str = input_date
    holiday_check = cboe_calendar.open_at_time(schedule_days, pd.Timestamp(input_date_str + ' 12:00', tz=TZ_INFO))
    return holiday_check


#----------------------------------------------------------------------
def get_day_index(last_day: datetime, hour: int):
    if last_day.hour < hour:
        last_day = last_day + datetime.timedelta(days = -1)
    return datetime.datetime.strftime(last_day, DATE_FORMAT)


#----------------------------------------------------------------------
def run_over_time_frame():
    """run over all the delivery dates"""
    logger.info("Calculating contract expiration dates...")
    futures_exp_dates = []
    # about 14 months later
    end_time = datetime.datetime.now(tz = cboe_calendar.tz) + datetime.timedelta(days = 420)
    cur_time = datetime.datetime(2013, 1, 1, tzinfo = cboe_calendar.tz)
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
def get_recent_trading_days(delta: int = 10, current: datetime = None):
    """get the last 5 trading days"""
    if current is None:
        current = datetime.datetime.now(tz = cboe_calendar.tz)
    start = current + datetime.timedelta(days = -delta)
    recent = cboe_calendar.schedule(start_date = start.strftime(DATE_FORMAT),
                                    end_date = current.strftime(DATE_FORMAT))
    days = market_cal.date_range(recent, frequency = '1D')
    days = days.strftime(DATE_FORMAT)
    # just worked, not good
    fdays = [is_business_day(x, recent) for x in days]
    return days[fdays]


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
    # combine the all infos
    final = reduce(combine_data, filtered_infos)
    return final.fillna(0)


#----------------------------------------------------------------------
def analyze_diff_percent(info: pd.DataFrame):
    """calculate the diff percent"""
    delta = info.diff(periods = 1, axis = 1)
    delta_p = delta.div(info.shift(periods = 1, axis = 1))
    return delta_p


#----------------------------------------------------------------------
def close_ma5_ma10_ma20(df: pd.DataFrame):
    """calculate the ma5 ma10 and ma20 of the close price"""
    close_seq = df[CLOSE_PRICE_NAME]
    ma5 = close_seq.rolling(5).mean()
    ma10 = close_seq.rolling(10).mean()
    ma20 = close_seq.rolling(20).mean()
    df['ma5'] = ma5
    df['ma10'] = ma10
    df['ma20'] = ma20
    return df


#----------------------------------------------------------------------
def index_distribution_of_per(size: int):
    """calculate the index to split a list"""
    min_delta = int(np.floor(size / 100))
    remain = size - min_delta * 100
    # remain to the bottom
    li = []
    for idx in range(99):
        delta = min_delta
        if remain > 0:
            delta += 1
            remain -= 1
        if 0 == idx:
            li.append(delta)
        else:
            li.append(li[-1] + delta)
    return li


#----------------------------------------------------------------------
def percent_distribution_list(hvs: pd.Series):
    """calculate the percentage value list"""
    sorted_hvs = hvs.dropna().sort_values()
    size = sorted_hvs.shape[0]
    if size < HV_DISTRIBUTION_PERIODS:
        # not enough size,  return empty list
        return []
    else:
        sorted_hvs = sorted_hvs[-HV_DISTRIBUTION_PERIODS:]
        idxes = index_distribution_of_per(HV_DISTRIBUTION_PERIODS)
        return map(lambda idx: sorted_hvs.iloc[idx], idxes)


#----------------------------------------------------------------------
def percent_distribution(vix: pd.Series, val: float = None):
    """calculate the percentage of the value at"""
    dis = percent_distribution_list(vix)
    if [] == dis:
        # not enough distribution, return 50
        return 50
    if val is None:
        val = vix.iloc[-1]
    ret = 0
    for tval in dis:
        if val > tval:
            ret += 1
        else:
            break
    return ret


#----------------------------------------------------------------------
def generate_futures_chain(symbol: str, suffix: str, date: str = None):
    """generate the futures chain of symbol"""
    if not symbol or not suffix:
        return []
    if date is None:
        now = datetime.datetime.now()
        month = now.month
        year = now.year
    else:
        now = datetime.datetime.strptime(date, DATE_FORMAT)
        month = now.month
        year = now.year
    chain = []
    for idx, mflag in enumerate(FUTURES_CHAIN):
        if idx + 1 <= month:
            chain.append(f'{symbol}{mflag}{(year + 1) % 100}.{suffix}')
        if idx + 1 >= month:
            chain.append(f'{symbol}{mflag}{year % 100}.{suffix}')
    return chain


#----------------------------------------------------------------------
def format_index(df: pd.DataFrame, delivery_dates: list = []):
    """format the index of DataFrame"""
    if delivery_dates != []:
        df['d'] = df.apply(lambda row: 'd' if row.name in delivery_dates else '', axis = 1)
    df.index = df.index.str.replace(r'\d{4}-', '', regex = True)
    df.index = df.index.str.replace('-', '', regex = True)
    df.index.rename('Date', inplace = True)


#----------------------------------------------------------------------
def calc_percentage(vx: pd.DataFrame):
    """calculate the percentage"""
    historical_max_min_per(vx)
    vx['per'] = vx.Close.rolling(HV_DISTRIBUTION_PERIODS).apply(lambda rows: percent_distribution(rows))
    vx_51 = vx.iloc[-5:].loc[:, [CLOSE_PRICE_NAME, 'mper', 'per']]
    format_index(vx_51)
    return vx_51, vx.iloc[-1].loc['Max'], vx.iloc[-1].loc['Min']


#----------------------------------------------------------------------
def historical_max_min_per(df: pd.DataFrame):
    """mark the historical max an min in the dataframe"""
    # according to:
    # https://stackoverflow.com/questions/61759149/apply-a-function-on-a-dataframe-that-depend-on-the-previous-row-values
    # NOTE: I rename the variables with _ to avoid using builtin method names
    max_ = sys.float_info.min
    min_ = sys.float_info.max
    # list for the results
    l_res = []
    for value in df.Close.to_numpy():
        # iterate over the values
        if value >= max_:
            max_ = value
        if value <= min_:
            min_ = value
        # append the results in the list
        ratio = 100
        if max_ - min_ > 0:
            ratio = round((value - min_) * 100 / (max_ - min_))
        l_res.append([max_, min_, ratio])
    # create the three columns outside of the loop
    df[['Max', 'Min', 'mper']] = pd.DataFrame(l_res, index = df.index)


#----------------------------------------------------------------------
def mk_notification_params(vix_futures: pd.DataFrame,
                           delivery_dates: list,
                           rets_vix: dict,
                           rets_gvz: dict, rets_ovx: dict):
    """make the notification's params"""
    rets = {'vix_futures': vix_futures, 'delivery_dates': delivery_dates}
    rets.update(rets_vix)
    rets.update(rets_gvz)
    rets.update(rets_ovx)
    return rets


#----------------------------------------------------------------------
def notify_format(df: pd.DataFrame):
    """format the dataframe for notification"""
    return df.to_markdown()


#----------------------------------------------------------------------
def notify_format_content(content: str):
    """format the string"""
    # clear the table head
    content = MD_HEAD_PATTERN.subn('', content)[0]
    # make sure two space besides the \n
    return MD_FORMAT_PATTERN.subn(MD_FORMAT_TO, content)[0]


#----------------------------------------------------------------------
def mk_notification(vix_futures: pd.DataFrame,
                    delivery_dates: list,
                    vix_diff: pd.DataFrame,
                    vix: pd.DataFrame,
                    gvz: pd.DataFrame = None,
                    ovx: pd.DataFrame = None):
    """make the notification msg from the params"""
    if np.alltrue(vix_diff.iloc[-5:][1] > 0.02):
        per_msg = 'vix 2/1 is safe now. '
    elif np.any(vix_diff.iloc[-5:][1] < -0.02):
        per_msg = 'vix 2/1 warning!!!! '
    else:
        per_msg = 'vix is ok. '
    # combine the result
    futures_521 = vix_futures.iloc[-5:, [0, 1]]
    vix_diff_51 = vix_diff.iloc[-5:, [0]]
    futures_521 = futures_521.applymap(lambda x: f"{x:.1f}")
    futures_521['f2/1'] = vix_diff_51[1].apply(lambda x: f"{x:.1%}")
    # clear the year info of Trade Date
    format_index(futures_521, delivery_dates)
    # calculate the vix percentage
    vix_51, vmax, vmin = calc_percentage(vix)
    # calculate the gvz percentage
    gvz_51, gmax, gmin = calc_percentage(gvz)
    # calculate the ovx percentage
    ovx_51, omax, omin = calc_percentage(ovx)
    content = f"""{notify_format(futures_521)}
------------------------
#### **vix:** {vmin:.2f} - {vmax:.2f}
{notify_format(vix_51)}
------------------------
#### **gvz:** {gmin:.2f} - {gmax:.2f}
{notify_format(gvz_51)}
------------------------
#### **ovx:** {omin:.2f} - {omax:.2f}
{notify_format(ovx_51)}"""
    return per_msg, notify_format_content(content)


#----------------------------------------------------------------------
def load_futures_by_csv(path: str):
    """load futures info by csv"""
    df = pd.read_csv(path, index_col = False)
    df.set_index(INDEX_KEY, inplace = True)
    return df


#----------------------------------------------------------------------
def load_vix_by_csv(path: str):
    """load vix info by csv"""
    df = pd.read_csv(path)
    df.set_index(INDEX_KEY, inplace = True)
    return df


#----------------------------------------------------------------------
def is_futures_file(path: str):
    """check the file then return the delivery date"""
    res = DATE_FORMAT_PATTERN.search(path)
    if res:
        return True, res.group(1)
    return False, None

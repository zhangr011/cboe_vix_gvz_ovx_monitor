# encoding: UTF-8

import json
from enum import Enum


#----------------------------------------------------------------------
class VixIntradayState(Enum):
    # state of the intraday
    Normal = 0
    High = 1
    Low = 2
    PinUp = 3
    PinDown = 4


VIX_FUTURES_URL = "https://markets.cboe.com/us/futures/api/get_quotes_combined/?symbol=VX&rootsymbol=VIX"


#----------------------------------------------------------------------
def check_unusual(prev_close: float, high: float, low: float, close: float,
                  per_aa: float, min_aa: float, test: bool = False):
    if test:
        delta_aa = 0.1
    else:
        delta_aa = max(prev_close * per_aa, min_aa)
    pin_aa = delta_aa * 2 / 3
    if high - prev_close >= delta_aa:
        # up to at least aa, this is unusal
        if high - close >= pin_aa:
            # close is back down
            return VixIntradayState.PinUp
        # not back down
        return VixIntradayState.High
    if abs(prev_close - low) >= delta_aa:
        # down to at least aa, this is unusal too
        if close - low >= pin_aa:
            # close is back up
            return VixIntradayState.PinDown
        # not back up
        return VixIntradayState.Low
    return VixIntradayState.Normal


#----------------------------------------------------------------------
def check_vix_intraday_warning(rets, test: bool = False):
    values = rets.get('data')
    infos = []
    for item in values:
        if item.get('volume') == '-' or int(item.get('volume')) < 100:
            # no volume info or volume is too low, pass it
            continue
        high = float(item.get('high', 0))
        low = float(item.get('low', 0))
        close = float(item.get('last_price', 0))
        # item.get('settlement')
        prev_close = float(item.get('prev_settlement', 0))
        check_ret = check_unusual(prev_close, high, low, close, 0.1, 5, test)
        if check_ret != VixIntradayState.Normal:
            item['high'] = high
            item['low'] = low
            item['prev_settlement'] = prev_close
            item['last_price'] = close
            infos.append((check_ret, item))
    return infos


#----------------------------------------------------------------------
def mk_intraday_notification(infos):
    """make the intraday vix warning info"""
    title = "intraday vix warning!!! "
    content = ""
    for state, item in infos:
        msg = f"""#### **{item.get('symbol')}:** {state}
open: {item.get('prev_settlement')}
high: {item.get('high')}
low: {item.get('low')}
close: {item.get('last_price')}
------------------------
"""
        content += msg
    return title, content


#----------------------------------------------------------------------
def check_warning_info_same(infos1, infos2):
    if len(infos1) != len(infos2):
        return False
    for idx, (state, info) in enumerate(infos1):
        state2, info2 = infos2[idx]
        if state != state2 or info.get("symbol", 1) != info2.get("symbol", 2):
            return False
    return True

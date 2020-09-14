# encoding: UTF-8

from cboe_monitor.utilities import run_over_time_frame
from cboe_monitor.data_manager import VIXDataManager, GVZDataManager, OVXDataManager


def main():
    delivery_dates, schedule_days = run_over_time_frame()
    vdm = VIXDataManager(delivery_dates)
    vdm.download_raw_data()
    df = vdm.combine_all()
    percent, vix, gvz, ovx = vdm.analyze()


if __name__ == '__main__':
    main()

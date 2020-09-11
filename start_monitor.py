# encoding: UTF-8

from cboe_monitor.utilities import run_over_time_frame
from cboe_monitor.data_manager import VIXDataManager, GVZDataManager, OVXDataManager


def main():
    delivery_dates = run_over_time_frame()
    vdm = VIXDataManager()
    vdm.download_raw_data(delivery_dates)


if __name__ == '__main__':
    main()

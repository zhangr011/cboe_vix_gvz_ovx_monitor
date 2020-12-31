# CBOE VIX（恐慌指数）、GVZ（黄金 VIX）、OVX（原油 VIX）监控
CBOE 的 VIX GVZ OVX 是反应公众恐慌程度的三个重要指标。
本项目旨在监测期货市场的波动率，以预估当前投资市场整体环境。

# cboe_vix_gvz_ovx_monitor
A monitor for the cboe futures of vix, gvz and ovx, which are the most important market's index to show the public's panic.

# how to use
```bash
pip3 install -r ./requirements.txt
pip3 install .
# copy and edit your push.ini if needed.
cp ./data/push.back.ini ./data/push.ini
# start the monitor at back
bash ./start_monitor.sh restart&
# check if any error occurred
tail ./nohup.out
```

## 特别感谢 Thanks for:
#### https://github.com/kostasmetaxas/vixfuturesdatabridge.git
#### https://github.com/black-swan-2/VIX_Master.git
#### https://github.com/datasets/finance-vix.git

# schedule
## done:
### 0.2.0 - 数据下载（data download）
### 0.3.0 - vix 期货升贴水结构识别（contango or backwardation of vix futures）
### 0.4.0 - 通知（notification with wxpush）
### 0.5.0 - 任务调度（task schedule）
### 0.6.0 - bug fixed & features enhancement
### 0.7.0 - 钉钉通知（notification with dingding）
### 0.8.0 - 数据从 yahoo http 下载（use yahoo http download instead of pandas_datareader）
由于数据格式的问题，需要删除已经下载的 ./data/vix/VIX.csv ./data/gvz/GVZ.csv ./data/ovx/OVX.csv
due to the data source changed, you maybe be need to delete the ./data/vix/VIX.csv ./data/gvz/GVZ.csv ./data/ovx/OVX.csv manually.
### 0.9.0 - 日内 vix 异常波动预警（intraday vix warning）
## update schedule:
### 0.9.1 - 数据从 cboe http 下载（use cboe http download instead of yahoo）

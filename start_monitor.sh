#!/bin/bash

SERVICE_NAME=cboe_monitor
PID=$SERVICE_NAME.pid
ProcNumber=`ps -ef | grep -w $SERVICE_NAME | grep -v grep | wc -l`

case "$1" in
    start)
        if [ -f ./$PID ]; then
            echo "$SERVICE_NAME is started, please use the restart option. "
        else
            nohup python3 ./cboe_monitor.py 2>&1 &
            echo $! > ./$PID
            echo "==== start $SERVICE_NAME ===="
        fi
        ;;
    stop)
        kill -9 `cat ./$PID`
        rm -rf ./$PID
        echo "==== stop $SERVICE_NAME ===="
        ;;
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    *)
        echo "Usage: bash start_monitor.sh [start|stop|restart]"
        ;;
esac
exit 0

#!/bin/bash

SERVICE_NAME=cboe_monitor
PID=$SERVICE_NAME.pid

pyimm=
imm=
pypush=
push=

while getopts "m:ipr" opt; do
    case ${opt} in
        m)
            mode=$OPTARG
            ;;
        i)
            pyimm='--imm=True'
            imm='-i'
            ;;
        p)
            pypush='--push=True'
            push='-p'
            ;;
        *)
            echo 'unknown argument. '
    esac
done


case "$mode" in
    start)
        if [ -f ./$PID ]; then
            echo "$SERVICE_NAME is started, please use the restart option. "
        else
            nohup python3 ./cboe_monitor.py $pyimm $pypush 2>&1 &
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
        $0 -m stop
        sleep 2
        $0 -m start $imm $push
        ;;
    *)
        echo "Usage: bash start_monitor.sh -m [start|stop|restart]"
        echo `python3 ./cboe_monitor.py --help`
        ;;
esac
exit 0

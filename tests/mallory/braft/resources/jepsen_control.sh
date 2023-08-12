#!/bin/bash

ulimit -n 10000
ulimit -c unlimited

node_hostnames="n1 n2 n3 n4 n5"
port="8700"
# self_ip=$(ifconfig | grep "mask" | grep -v "127.0.0.1" | awk '{print $2}')\
self_hostname=$(hostname)
self_ip=$(getent ahosts "${self_hostname}" | awk '{ print $1 }' | head -n1)
self_node="${self_ip}:${port}"

peers="" # of the form ip:port,ip:port
exclude_peers=""
# gen peers and exclude_peers
for host in $node_hostnames
do
    ip=$(getent ahosts "${host}" | awk '{ print $1 }' | head -n1)
    node="${ip}:${port}"
    #echo $node
    if [ -z "$peers" ];then
        peers=$node
    else
        peers=$peers","$node
    fi

    if [ "$node" == "$self_node" ];then
        continue
    fi
    if [ -z "$exclude_peers" ];then
        exclude_peers=$node
    else
        exclude_peers=$exclude_peers","$node
    fi
done

echo "Peers: $peers"

function help {
    echo "Usage: jepsen_control.sh boot|start|stop|kill|restart|join|leave"
    exit 1
}

if [ $# -ne 1 ];then
    help
fi


case $1 in
    boot)
        echo "boot atomic_server ${self_node}"
        killall -9 atomic_server || true
        rm -rf log data run.log core.* && mkdir log
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &
        
        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 >> run.log 2>&1 &
    sleep 1
    braft_cli reset_peer --group=Atomic --peer="${self_node}" --new_peers="${peers}"
        ;;
    start)
        echo "start atomic_server ${self_node}"
        killall -9 atomic_server || true
        # rm -rf log data core.* && mkdir log
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &

        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 >> run.log 2>&1 &
        ;;
    kill)
        echo "stop atomic_server ${self_node}"
        killall -9 atomic_server || true
        ;;
    stop)
        echo "stop atomic_server ${self_node}"
        killall -15 atomic_server || true
        ;;
    restart)
        echo "restart atomic_server ${self_node}"
        killall -9 atomic_server || true
        #./atomic_server -raft_sync=true -bthread_concurrency=24 -crash_on_fatal_log=true -port=8700 > run.log 2>&1 &

        ./atomic_server -raft_sync=true -bthread_concurrency=24 --log_dir=log -port=8700 >> run.log 2>&1 &
        ;;
    join)
        echo "join atomic_server ${self_node}"
        braft_cli add_peer --group=Atomic --peer="${self_node}" --conf="${exclude_peers}"
        ;;
    leave)
        echo "leave atomic_server ${self_node}"
        braft_cli remove_peer --group=Atomic --peer="${self_node}" --conf="${peers}"
        ;;
    *)
        help
        ;;
esac


#!/bin/bash

pid_file=secret_api.pid

# Check if already running
if [[ -f $pid_file ]]; then
    pid=$(cat $pid_file)
    ps -p $pid > /dev/null 2>& 1
    if [[ $? -eq 0 ]]; then
        echo "Error PID file already exists: $pid_file"
        exit 1
    else
        echo "Cleaning up PID file."
        rm -f $pid_file
    fi
fi

# Startup
log_file=${1:-/dev/null}
echo "Starting API"
nohup python api.py > $log_file 2>&1 &
apipid=$!
echo $apipid | tee $pid_file

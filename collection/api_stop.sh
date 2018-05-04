#!/bin/bash

# Get PID
pid_file=secret_api.pid
if [[ ! -f $pid_file ]]; then
    echo "Cannot find pid file: $pid_file"
    exit 1
fi

pid=$(cat $pid_file)
pid_list="$(pgrep -P $pid) $pid"

# Shutdown
echo "Killing API processes: $pid_list"
kill $pid_list
rm -f $pid_file


#!/usr/bin/env bash

if [ -a "options.sh" ]; then
    source "options.sh"
fi

export DEBUG=pipeline*

nohup node server/pipelineSchedulerApp.js &

sleep 3

chmod 775 nohup.out

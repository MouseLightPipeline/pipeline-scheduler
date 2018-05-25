#!/usr/bin/env bash

logName=$(date '+%Y-%m-%d%H-%M-%S');

export DEBUG=pipeline*

node server/pipelineSchedulerApp.js &> /var/log/pipeline/scheduler-${logName}.log

# sleep infinity

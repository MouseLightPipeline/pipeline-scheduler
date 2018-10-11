#!/usr/bin/env bash

logName=$(date '+%Y-%m-%d_%H-%M-%S');

mkdir -p /var/log/pipeline

export DEBUG=pipeline*

node pipelineSchedulerApp.js &> /var/log/pipeline/scheduler-${logName}.log

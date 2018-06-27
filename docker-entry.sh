#!/usr/bin/env bash

logName=$(date '+%Y-%m-%d_%H-%M-%S');

export DEBUG=pipeline*

mkdir -p /var/log/pipeline

node pipelineSchedulerApp.js &> /var/log/pipeline/scheduler-${logName}.log

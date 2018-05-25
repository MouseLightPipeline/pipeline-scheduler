#!/usr/bin/env bash

logName=$(date '+%Y-%m-%d%H-%M-%S');

if [ ! -z "${PIPELINE_PERFORM_MIGRATION}" ]; then
    ./migrate.sh &> /var/log/pipeline/coordinator-${logName}.log
fi

export DEBUG=pipeline*

node server/pipelineApiApp.js &> /var/log/pipeline/coordinator-${logName}.log

# sleep infinity

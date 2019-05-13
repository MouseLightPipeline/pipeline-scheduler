#!/usr/bin/env bash

logName=$(date '+%Y-%m-%d_%H-%M-%S')

logPrefix="scheduler"

logBase=/var/log/pipeline

logFile=${logPrefix}-${logName}.log

logPath=${logBase}/${logFile}

mkdir -p ${logBase}

touch ${logPath}

chown mluser:mousebrainmicro ${logPath}

export DEBUG=pipeline*

node pipelineSchedulerApp.js >> ${logPath} 2>&1

import * as express from "express";
import * as bodyParser from "body-parser";

const debug = require("debug")("pipeline:coordinator-api:server");

import {SchedulerHub} from "./schedulers/schedulerHub";
import {ServiceOptions} from "./options/serverOptions";
import {MetricsConnector} from "./data-access/metrics/metricsConnector";

const useChildProcessWorkers = (parseInt(process.env.USE_CHILD_PROCESS_WORKERS) === 1) || false;

MetricsConnector.Instance().initialize().then();

const app = express();

app.use(bodyParser.urlencoded({extended: true}));

app.use(bodyParser.json());

SchedulerHub.Run(useChildProcessWorkers).then(() => {
    app.listen(ServiceOptions.port, () => {
        debug(`running on http://localhost:${ServiceOptions.port}`);
    });
});

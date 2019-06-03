import * as express from "express";
import * as bodyParser from "body-parser";

const debug = require("debug")("pipeline:scheduler:server");

import {SchedulerHub} from "./schedulers/schedulerHub";
import {ServiceOptions} from "./options/serverOptions";
import {MetricsConnector} from "./data-access/metrics/metricsConnector";
import {MainQueue} from "./message-queue/mainQueue";

start().then().catch((err) => debug(err));

async function start() {
    const useChildProcessWorkers = (parseInt(process.env.USE_CHILD_PROCESS_WORKERS) === 1) || false;

    await SchedulerHub.Run(useChildProcessWorkers);

    await MainQueue.Instance.connect();

    await MetricsConnector.Instance().initialize();

    const app = express();

    app.use(bodyParser.urlencoded({extended: true}));

    app.use(bodyParser.json());

    app.get("/healthcheck", (req, res) =>{
        res.sendStatus(200);
    });

    app.listen(ServiceOptions.port, () => {
        debug(`running on http://localhost:${ServiceOptions.port}`);
    });
}

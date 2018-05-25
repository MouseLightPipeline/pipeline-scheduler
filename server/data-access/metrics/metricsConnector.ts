const Influx = require("influx");

import {MetricsOptions} from "../../options/coreServicesOptions";
import {CompletionResult} from "../../data-model/taskExecution";

const debug = require("debug")("ndb:search:database-connector");

export class MetricsConnector {
    private static _instance: MetricsConnector = null;
    private taskExecutionDatabase = null;

    public static Instance(): MetricsConnector {
        if (!this._instance) {
            this._instance = new MetricsConnector();
        }

        return this._instance;
    }

    public async initialize() {
        await this.migrate();
        this.createTaskExecutionConnection();
    }

    public async writeTaskExecution(completion_status_code: CompletionResult, duration: number) {
        try {
            if (this.taskExecutionDatabase) {
                this.taskExecutionDatabase.writePoints([
                    {
                        measurement: "task_execution",
                        tags: {
                            workerId: "unknown",
                            workerName: "unknown"
                        },
                        fields: {
                            completion_status_code,
                            duration
                        },
                    }
                ]).then();
            }
        } catch (err) {
            debug("loq query failed.");
            debug(err);
        }
    }

    private createTaskExecutionConnection() {
        this.taskExecutionDatabase = new Influx.InfluxDB({
            host: MetricsOptions.host,
            port: MetricsOptions.port,
            database: MetricsOptions.taskDatabase,
            schema: [
                {
                    measurement: "task_execution",
                    fields: {
                        completion_status_code: Influx.FieldType.INTEGER,
                        duration: Influx.FieldType.INTEGER
                    },
                    tags: [
                        "workerId",
                        "workerName"
                    ]
                }
            ]
        });
    }

    private async migrate() {
        const influx = new Influx.InfluxDB({
            host: MetricsOptions.host,
            port: MetricsOptions.port
        });

        const names = await influx.getDatabaseNames();

        if (!names.includes(MetricsOptions.taskDatabase)) {
            await influx.createDatabase(MetricsOptions.taskDatabase);
        }
    }
}
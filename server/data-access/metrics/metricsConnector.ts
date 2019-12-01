import {PipelineWorker} from "../../data-model/pipelineWorker";

const Influx = require("influx");

import {MetricsOptions} from "../../options/coreServicesOptions";
import {TaskExecution} from "../../data-model/taskExecution";
import {WorkerTaskExecution} from "../../data-model/workerTaskExecution";

const debug = require("debug")("pipeline:coordinator-api:metrics-database");

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

    public async writeTaskExecution(taskExecution: TaskExecution | WorkerTaskExecution) {
        try {
            if (this.taskExecutionDatabase) {
                const duration_minutes = taskExecution.completed_at && taskExecution.started_at ? (taskExecution.completed_at.valueOf() - taskExecution.started_at.valueOf()) / 60000 : null;
                const pending_duration_minutes = taskExecution.started_at && taskExecution.submitted_at ? (taskExecution.started_at.valueOf() - taskExecution.submitted_at.valueOf()) / 60000 : null;

                const fields = {
                    local_work_units: taskExecution.local_work_units,
                    cluster_work_units: taskExecution.cluster_work_units,
                    queue_type: taskExecution.queue_type,
                    completion_status_code: taskExecution.completion_status_code,
                    exit_code: taskExecution.exit_code,
                    cpu_time_seconds: taskExecution.cpu_time_seconds,
                    max_cpu_percent: taskExecution.max_cpu_percent,
                    max_memory_mb: taskExecution.max_memory_mb,
                    duration_minutes,
                    pending_duration_minutes
                };

                const worker = await PipelineWorker.findByPk(taskExecution.worker_id);

                const tags =  {
                    worker_id: taskExecution.worker_id,
                        worker_name: worker.name,
                        task_id: taskExecution.task_definition_id,
                        pipeline_stage_id: taskExecution.pipeline_stage_id
                };

                await this.taskExecutionDatabase.writePoints([
                    {
                        measurement: "task_execution",
                        tags,
                        fields
                    }
                ]);

                debug("wrote task execution");
            }
        } catch (err) {
            debug("write task execution metrics failed.");
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
                        local_work_units: Influx.FieldType.INTEGER,
                        cluster_work_units: Influx.FieldType.INTEGER,
                        queue_type: Influx.FieldType.INTEGER,
                        completion_status_code: Influx.FieldType.INTEGER,
                        exit_code: Influx.FieldType.INTEGER,
                        cpu_time_seconds: Influx.FieldType.FLOAT,
                        max_cpu_percent: Influx.FieldType.FLOAT,
                        max_memory_mb: Influx.FieldType.FLOAT,
                        duration_minutes: Influx.FieldType.FLOAT,
                        pending_duration_minutes: Influx.FieldType.FLOAT
                    },
                    tags: [
                        "worker_id",
                        "worker_name",
                        "task_id",
                        "pipeline_stage_id",
                    ]
                }
            ]
        });
    }

    private async migrate() {
        return new Promise<void>(async (resolve) => {
            try {
                const influx = new Influx.InfluxDB({
                    host: MetricsOptions.host,
                    port: MetricsOptions.port
                });

                const names = await influx.getDatabaseNames();

                if (!names.includes(MetricsOptions.taskDatabase)) {
                    await influx.createDatabase(MetricsOptions.taskDatabase);
                }

                debug(`successful connection to metrics database`);

                resolve();
            } catch {
                debug("failed to connect to metrics database; delaying 10 seconds");
                setTimeout(() => this.migrate(), 10000);
            }
        });
    }
}

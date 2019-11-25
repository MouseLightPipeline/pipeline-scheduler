import * as  path from "path";

const debug = require("debug")("pipeline:scheduler:stage-pipeline-scheduler");

import {PipelineWorkerClient} from "../graphql/pipelineWorkerClient";
import {
    IToProcessTile,
    StageTableConnector, ToProcessTile
} from "../data-access/sequelize/stageTableConnector";
import {BasePipelineScheduler, DefaultPipelineIdKey, TilePipelineStatus} from "./basePipelineScheduler";
import {ProjectDatabaseConnector} from "../data-access/sequelize/projectDatabaseConnector";
import {createTaskExecutionWithInput, ExecutionStatus, ITaskExecution} from "../data-model/taskExecution";
import {PipelineStage} from "../data-model/pipelineStage";
import {Project} from "../data-model/project";
import {PipelineWorker} from "../data-model/pipelineWorker";
import {TaskDefinition} from "../data-model/taskDefinition";

const MAX_KNOWN_INPUT_SKIP_COUNT = 1;
const MAX_ASSIGN_PER_ITERATION = 50;

export abstract class StagePipelineScheduler extends BasePipelineScheduler {

    protected _pipelineStage: PipelineStage;

    protected _inputStageConnector: StageTableConnector;

    private _knownInputSkipCheckCount: number = 0;

    protected constructor(pipelineStage: PipelineStage, project: Project) {
        super(project, pipelineStage);

        this._pipelineStage = pipelineStage;
    }

    public async getSource(): Promise<Project | PipelineStage> {
        return PipelineStage.findByPk(this._sourceId);
    }

    protected async createOutputStageConnector(connector: ProjectDatabaseConnector): Promise<StageTableConnector> {
        return await connector.connectorForStage(this._pipelineStage);
    }

    protected async createTables(connector: ProjectDatabaseConnector) {
        if (await super.createTables(connector)) {
            const project = await this.getProject();
            if (this._pipelineStage.previous_stage_id === null) {
                this._inputStageConnector = await connector.connectorForProject();
            } else {
                const stage = await PipelineStage.findByPk(this._pipelineStage.previous_stage_id);
                this._inputStageConnector = await connector.connectorForStage(stage);
            }
        } else {
            return false;
        }

        return true;
    }

    protected async refreshTileStatus(): Promise<boolean> {
        // Check and update the status of anything in-process
        // await this.updateInProcessStatus(); -- Handled by message queue now

        // Look if anything is already in the to-process queue
        let available: boolean = (await this._outputStageConnector.countToProcess()) > 0;

        // If not, search database for newly available to-process and put in to-process queue.  Skip count is used
        // to periodically force an update even if there are some in queue so that displayed counts get updated.
        if (!available || this._knownInputSkipCheckCount >= MAX_KNOWN_INPUT_SKIP_COUNT) {
            let knownInput = await this._inputStageConnector.loadTiles();

            // Update the database with the completion status of tiles from the previous stage.  This essentially
            // converts this_stage_status from the previous stage id table to prev_stage_status for this stage.
            // Load all tiles to find ones new, changed, and ones that have been removed upstream.
            await this.refreshWithKnownInput(knownInput);

            available = await this.updateToProcessQueue();

            this._knownInputSkipCheckCount = 0;
        } else {
            const source = await this.getSource();

            debug(`${source.name}: skipping new to queue check with available to process (skip count ${this._knownInputSkipCheckCount} of ${MAX_KNOWN_INPUT_SKIP_COUNT})`);
            this._knownInputSkipCheckCount++;
        }

        return available;
    }

    protected async performProcessing(): Promise<void> {
        const source = await this.getSource();

        const allWorkers = await PipelineWorker.findAll({});

        const workers = allWorkers.filter(worker => worker.is_in_scheduler_pool);

        if (workers.length === 0) {
            debug(`${source.name}: no available workers to schedule (of ${allWorkers.length} known)`);
            return;
        }

        const project = await this.getProject();

        let src_path = project.root_path;

        if (this._pipelineStage.previous_stage_id) {
            let previousStage: PipelineStage = await PipelineStage.findByPk(this._pipelineStage.previous_stage_id);

            src_path = previousStage.dst_path;
        }

        // The promise returned for each queued item should be true to continue through the list, false to exit the
        // promise chain and not complete the list.
        //
        // The goal is to fill a worker completely before moving on to the next worker.
        await this.queue(workers, async (worker: PipelineWorker) => {
            const real_time_worker = await PipelineWorkerClient.Instance().queryWorker(worker);

            if (real_time_worker.local_task_load < 0 && real_time_worker.cluster_task_load < 0) {
                debug(`worker ${worker.name} skipped (unknown/unreported task loads)`);
                return true;
            }

            const task = await TaskDefinition.findByPk(this._pipelineStage.task_id);

            if (((real_time_worker.local_task_load + task.local_work_units - 0.0001) > real_time_worker.local_work_capacity) && ((real_time_worker.cluster_task_load + task.cluster_work_units - 0.0001) > real_time_worker.cluster_work_capacity)) {
                debug(`${source.name}: worker ${worker.name} has insufficient capacity, ignoring worker [${real_time_worker.local_task_load.toFixed(1)} load of ${real_time_worker.local_work_capacity.toFixed(1)}, ${real_time_worker.cluster_task_load.toFixed(1)} load of ${real_time_worker.cluster_work_capacity.toFixed(1)}]`);
                return true;
            }

            let waitingToProcess = await this._outputStageConnector.loadToProcess(MAX_ASSIGN_PER_ITERATION);

            if (!waitingToProcess || waitingToProcess.length === 0) {
                return false;
            }

            debug(`${source.name}: scheduling worker from available ${waitingToProcess.length} pending`);

            // Will continue through all tiles until the worker reaches full capacity
            let stillLookingForTilesForWorker = await this.queue(waitingToProcess, async (toProcessTile: ToProcessTile) => {
                // Return true to continue searching for an available worker and false if the task is launched.
                try {
                    const pipelineTile = await this._outputStageConnector.loadTile({relative_path: toProcessTile[DefaultPipelineIdKey]});

                    const inputTile = await this._inputStageConnector.loadTile({relative_path: toProcessTile[DefaultPipelineIdKey]});

                    // Verify the state is still complete.  The previous stage status is only update once every N times
                    // through scheduling of work.
                    if (inputTile.this_stage_status !== TilePipelineStatus.Complete) {
                        // Will eventually get cleaned up in overall tile update.
                        debug("input tile is no longer marked complete");
                        return true;
                    }

                    let outputPath = path.join(this._pipelineStage.dst_path, pipelineTile.relative_path);

                    const log_root_path = project.log_root_path || this._pipelineStage.dst_path || `/tmp/${this._pipelineStage.id}`;

                    const logFile = path.join(log_root_path, pipelineTile.relative_path, ".log", `${task.log_prefix}-${pipelineTile.tile_name}`);

                    let taskExecutionInput: ITaskExecution = await createTaskExecutionWithInput(worker, task, {
                        pipelineStageId: this._pipelineStage.id,
                        tileId: pipelineTile.relative_path,
                        outputPath,
                        logFile
                    });

                    let args = [src_path, this._pipelineStage.dst_path, pipelineTile.relative_path, pipelineTile.tile_name];

                    const context = await this.getTaskContext(pipelineTile);

                    args = args.concat(this.mapTaskArguments(project, task, taskExecutionInput, worker, pipelineTile, context));

                    taskExecutionInput.resolved_script_args = JSON.stringify(args);

                    let startTaskResponse = await PipelineWorkerClient.Instance().startTaskExecution(worker, taskExecutionInput);

                    if (startTaskResponse != null && startTaskResponse.taskExecution != null) {
                        const responseExecution = startTaskResponse.taskExecution;
                        const now = new Date();

                        if (startTaskResponse.taskExecution.execution_status_code === ExecutionStatus.Completed) {
                            // Failed immediately - do not put in inProcess table or list as processing.  Expecting
                            // a complete message in message queue that has likely already arrived.
                            pipelineTile.this_stage_status = TilePipelineStatus.Failed;

                            await pipelineTile.save();

                            debug(`${source.name}: returned failed task start from worker ${worker.name}`);
                        } else {
                            let taskExecution = await this._outputStageConnector.createTaskExecution(Object.assign(taskExecutionInput, {
                                worker_task_execution_id: responseExecution.id,
                                queue_type: responseExecution.queue_type,
                                resolved_script_args: responseExecution.resolved_script_args, // Worker may have substituted, e.g., IS_CLUSTER_JOB
                                local_work_units: responseExecution.local_work_units,
                                cluster_work_units: responseExecution.cluster_work_units,
                                submitted_at: responseExecution.submitted_at,
                                started_at: responseExecution.started_at,
                                completed_at: responseExecution.completed_at
                            }));

                            await this._outputStageConnector.insertInProcessTile({
                                relative_path: pipelineTile.relative_path,
                                worker_id: worker.id,
                                worker_last_seen: now,
                                task_execution_id: taskExecution.id,
                                worker_task_execution_id: responseExecution.id
                            });

                            pipelineTile.this_stage_status = TilePipelineStatus.Processing;

                            await pipelineTile.save();

                            await this._outputStageConnector.deleteToProcessTile(toProcessTile);

                            debug(`${source.name}: started task on worker ${worker.name} with execution id ${taskExecution.id}`);
                        }

                        return true;
                    } else {
                        if (startTaskResponse != null) {
                            if (((startTaskResponse.localTaskLoad + task.local_work_units) > real_time_worker.local_work_capacity) && ((startTaskResponse.clusterTaskLoad + task.cluster_work_units) > real_time_worker.cluster_work_capacity)) {
                                debug(`${source.name}: worker ${worker.name} rejected for insufficient capacity for further tasks [${startTaskResponse.localTaskLoad} load of ${real_time_worker.local_work_capacity}, ${startTaskResponse.clusterTaskLoad} load of ${real_time_worker.cluster_work_capacity}]`);
                            } else {
                                debug(`${source.name}: start task did not error, however returned null taskExecution`);
                            }
                        } else {
                            debug(`${source.name}: start task did not error, however returned null`);
                        }
                        return false;
                    }
                } catch (err) {
                    debug(`${source.name}: worker ${worker.name} with error starting execution ${err}`);
                    return false;
                }
            });

            if (!this.IsProcessingRequested || this.IsExitRequested) {
                debug(`${source.name}: cancel requested - exiting stage worker`);
                return false;
            }

            // If result is true, a worker was never found for the last tile so short circuit be returning a promise
            // that resolves to false.  Otherwise, the tile task was launched, so try the next one.

            return Promise.resolve(!stillLookingForTilesForWorker);
        });
    }

    /*
     * Somewhat generic serialized list queue.  Doesn't really belong here.  The queue or related object in async should
     * be able to handle this.
     */
    protected async queue(list, queueFunction) {
        return await list.reduce((promiseChain, item) => this.createQueueFunctionPromise(promiseChain, queueFunction, item), Promise.resolve(true));
    }

    private createQueueFunctionPromise(promiseChain, queueFunction, item) {
        return promiseChain.then((result) => {
            if (result) {
                return queueFunction(item);
            } else {
                return Promise.resolve(false);
            }
        });
    }
}

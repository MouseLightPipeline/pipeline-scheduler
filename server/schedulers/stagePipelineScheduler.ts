import * as  path from "path";

const fse = require("fs-extra");
const debug = require("debug")("pipeline:scheduler:stage-pipeline-scheduler");

import {PipelineWorkerClient} from "../graphql/pipelineWorkerClient";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";
import {IProject} from "../data-model/sequelize/project";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";
import {IPipelineStage} from "../data-model/sequelize/pipelineStage";
import {
    IToProcessTileAttributes,
    StageTableConnector
} from "../data-access/sequelize/project-connectors/stageTableConnector";
import {BasePipelineScheduler, DefaultPipelineIdKey, TilePipelineStatus} from "./basePipelineScheduler";
import {ProjectDatabaseConnector} from "../data-access/sequelize/project-connectors/projectDatabaseConnector";
import {updatePipelineStageCounts} from "../data-model/sequelize/pipelineStagePerformance";

const MAX_KNOWN_INPUT_SKIP_COUNT = 1;
const MAX_ASSIGN_PER_ITERATION = 50;

export abstract class StagePipelineScheduler extends BasePipelineScheduler {

    protected _pipelineStage: IPipelineStage;

    protected _inputStageConnector: StageTableConnector;

    private _knownInputSkipCheckCount: number = 0;

    protected constructor(pipelineStage: IPipelineStage, project: IProject) {
        super(project, pipelineStage);

        this._pipelineStage = pipelineStage;
    }

    protected async createOutputStageConnector(connector: ProjectDatabaseConnector): Promise<StageTableConnector> {
        return await connector.connectorForStage(this._pipelineStage);
    }

    protected async createTables(connector: ProjectDatabaseConnector) {
        if (await super.createTables(connector)) {
            if (this._pipelineStage.previous_stage_id === null) {
                this._inputStageConnector = await connector.connectorForProject(this._project);
            } else {
                const stage = await PersistentStorageManager.Instance().PipelineStages.findById(this._pipelineStage.previous_stage_id);
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
            debug(`skipping new to queue check with available to process (skip count ${this._knownInputSkipCheckCount} of ${MAX_KNOWN_INPUT_SKIP_COUNT})`);
            this._knownInputSkipCheckCount++;
        }

        updatePipelineStageCounts(this.StageId, await this._outputStageConnector.countInProcess(), await this._outputStageConnector.countToProcess());

        return available;
    }

    protected async performProcessing(): Promise<void> {
        let pipelineStages = PersistentStorageManager.Instance().PipelineStages;

        let allWorkers = await PersistentStorageManager.Instance().getPipelineWorkers();

        // Use cluster proxies as last resort when behind.
        let workers = allWorkers.filter(worker => worker.is_in_scheduler_pool).sort((a, b) => {
            if ((a.cluster_work_capacity <= 0) === (b.cluster_work_capacity <= 0)) {
                return 0;
            }

            return a.cluster_work_capacity <= 0 ? 1 : -1;
        });

        if (workers.length === 0) {
            debug(`no available workers to schedule (of ${allWorkers.length} known)`);
            return;
        }

        let src_path = this._project.root_path;

        if (this._pipelineStage.previous_stage_id) {
            let previousStage: IPipelineStage = await pipelineStages.findById(this._pipelineStage.previous_stage_id);

            src_path = previousStage.dst_path;
        }

        // The promise returned for each queued item should be true to continue through the list, false to exit the
        // promise chain and not complete the list.
        //
        // The goal is to fill a worker completely before moving on to the next worker.
        await this.queue(workers, async (worker: IPipelineWorker) => {

            const real_time_worker = await PipelineWorkerClient.Instance().queryWorker(worker);

            const isClusterProxy = real_time_worker.cluster_work_capacity > 0;

            let taskLoad = real_time_worker ? (isClusterProxy ? real_time_worker.cluster_task_load : real_time_worker.local_task_load) : -1;

            if (taskLoad < 0) {
                debug(`worker ${worker.name} skipped (unknown/unreported task load)`);
                return true;
            }

            const task = await PersistentStorageManager.Instance().TaskDefinitions.findById(this._pipelineStage.task_id);

            // TODO Get worker value for work units for this task, if applicable.
            const workUnits = isClusterProxy ? task.cluster_work_units : task.local_work_units;

            let capacity = (isClusterProxy ? real_time_worker.cluster_work_capacity : real_time_worker.local_work_capacity);

            let availableLoad = capacity - taskLoad;

            if ((availableLoad + 0.000001) < workUnits) {
                debug(`worker ${worker.name} has insufficient capacity: ${availableLoad} of ${capacity}`);
                return true;
            }

            debug(`worker ${worker.name} has load ${taskLoad} of capacity ${capacity}`);

            let waitingToProcess = await this._outputStageConnector.loadToProcess(MAX_ASSIGN_PER_ITERATION);

            if (!waitingToProcess || waitingToProcess.length === 0) {
                return false;
            }

            debug(`scheduling worker from available ${waitingToProcess.length} pending`);

            // Will continue through all tiles until the worker reaches full capacity
            let stillLookingForTilesForWorker = await this.queue(waitingToProcess, async (toProcessTile: IToProcessTileAttributes) => {
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

                    // fse.ensureDirSync(outputPath);
                    // fse.chmodSync(outputPath, 0o775);

                    const log_root_path = this._project.log_root_path || this._pipelineStage.dst_path || `/tmp/${this._pipelineStage.id}`;

                    const logFile = path.join(log_root_path, pipelineTile.relative_path, ".log", `${task.log_prefix}-${pipelineTile.tile_name}`);

                    let taskExecution = await this._outputStageConnector.createTaskExecution(worker, task, {
                        pipelineStageId: this._pipelineStage.id,
                        tileId: pipelineTile.relative_path,
                        outputPath,
                        logFile
                    });

                    let args = [src_path, this._pipelineStage.dst_path, pipelineTile.relative_path, pipelineTile.tile_name];

                    const context = await this.getTaskContext(pipelineTile);

                    args = args.concat(this.mapTaskArguments(task, taskExecution, worker, pipelineTile, context));

                    await taskExecution.update({resolved_script_args: JSON.stringify(args)});

                    let taskResponse = await PipelineWorkerClient.Instance().startTaskExecution(worker, taskExecution.get({plain: true}));

                    if (taskResponse != null) {
                        let now = new Date();

                        await this._outputStageConnector.insertInProcessTile({
                            relative_path: pipelineTile.relative_path,
                            worker_id: worker.id,
                            worker_last_seen: now,
                            task_execution_id: taskExecution.id,
                            worker_task_execution_id: taskResponse.id,
                            created_at: now,
                            updated_at: now
                        });

                        await taskExecution.update({
                            submitted_at: taskResponse.submitted_at,
                            started_at: taskResponse.started_at,
                            completed_at: taskResponse.completed_at
                        });

                        // TODO: Should use value returned from taskExecution in case it is worker-dependent
                        // In order to do that, workers must be updated to return the right value when a cluster
                        // worker (i.e., 1 per job).  Currently they only return the actual value.
                        taskLoad += workUnits;

                        if (isClusterProxy) {
                            worker.cluster_task_load = taskLoad;
                        } else {
                            worker.local_task_load = taskLoad;
                        }

                        pipelineTile.this_stage_status = TilePipelineStatus.Processing;

                        await pipelineTile.save();

                        await this._outputStageConnector.deleteToProcessTile(toProcessTile);

                        debug(`started task on worker ${worker.name} with execution id ${taskExecution.id}`);

                        availableLoad = isClusterProxy ? worker.cluster_work_capacity : worker.local_work_capacity - taskLoad;

                        // Does this worker have enough capacity to handle more tiles from this task given the work units
                        // per task on this worker.
                        if ((availableLoad + 0.00001) < workUnits) {
                            debug(`worker ${worker.name} has insufficient capacity ${availableLoad} of ${capacity} for further tasks`);
                            return false;
                        }

                        return true;
                    } else {
                        debug("start task did not error, however returned null");
                    }
                } catch (err) {
                    debug(`worker ${worker.name} with error starting execution ${err}`);
                    return false;
                }

                if (!this.IsProcessingRequested || this.IsExitRequested) {
                    debug("cancel requested - exiting stage worker");
                    return false;
                }

                // Did not start due to unavailability or error starting.  Return true to keep looking for a worker.
                return true;
            });

            if (!this.IsProcessingRequested || this.IsExitRequested) {
                debug("cancel requested - exiting stage worker");
                return false;
            }

            // debug(`worker search for tile ${toProcessTile[DefaultPipelineIdKey]} resolves with stillLookingForTilesForWorker: ${stillLookingForTilesForWorker}`);

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

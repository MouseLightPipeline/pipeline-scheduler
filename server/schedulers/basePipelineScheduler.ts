import * as  path from "path";
import {isNullOrUndefined} from "util";
import * as _ from "lodash";

const fse = require("fs-extra");
const debug = require("debug")("pipeline:coordinator-api:base-pipeline-scheduler");

import {updatePipelineStagePerformance} from "../data-model/sequelize/pipelineStagePerformance";
import {ISchedulerInterface} from "./schedulerHub";
import {PipelineWorkerClient} from "../graphql/client/pipelineWorkerClient";
import {PipelineServerContext} from "../graphql/pipelineServerContext";
import {IProject} from "../data-model/sequelize/project";
import {CompletionResult, ExecutionStatus, ITaskExecution} from "../data-model/taskExecution";
import {connectorForProject, ProjectDatabaseConnector} from "../data-access/sequelize/projectDatabaseConnector";
import {
    IInProcessTileAttributes, IPipelineTile, IPipelineTileAttributes, IToProcessTileAttributes,
    StageTableConnector
} from "../data-access/sequelize/stageTableConnector";
import {ITaskArgument, ITaskDefinition, TaskArgumentType} from "../data-model/sequelize/taskDefinition";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";

export const DefaultPipelineIdKey = "relative_path";

/**
 * Internal state of a tile within a pipeline stage - used for the state of the previous stage
 * as well as the state in the current stage.
 */
export enum TilePipelineStatus {
    DoesNotExist = 0,
    Incomplete = 1,
    Queued = 2,
    Processing = 3,
    Complete = 4,
    Failed = 5,
    Canceled = 6
}


export interface IMuxTileLists {
    toInsert: IPipelineTileAttributes[],
    toUpdate: IPipelineTile[],
    toReset: IPipelineTile[],
    toDelete: string[]
}

class InProcessModifyList {
    private _toDelete: string[] = [];
    private _toUpdate: Map<TilePipelineStatus, string[]> = new Map<TilePipelineStatus, string[]>();

    public get ToDelete(): string[] {
        return this._toDelete;
    }

    public get ToUpdate(): Map<TilePipelineStatus, string[]> {
        return this._toUpdate;
    }

    public remove(id: string) {
        this._toDelete.push(id);
    }

    public update(id: string, status: TilePipelineStatus) {
        let list = this._toUpdate.get(status);

        if (!list) {
            list = [];
            this._toUpdate.set(status, list);
        }

        list.push(id);
    }
}

export abstract class BasePipelineScheduler implements ISchedulerInterface {
    protected _project: IProject;

    protected _outputStageConnector: StageTableConnector;

    private _isCancelRequested: boolean;
    private _isProcessingRequested: boolean;

    private _isInitialized: boolean = false;

    protected constructor(project: IProject) {
        this._project = project;

        this.IsExitRequested = false;

        this.IsProcessingRequested = false;
    }

    public set IsExitRequested(b: boolean) {
        this._isCancelRequested = b;
    }

    public get IsExitRequested() {
        return this._isCancelRequested;
    }

    public set IsProcessingRequested(b: boolean) {
        // TODO When set to true, reload the stage b/c it may have been edited.  Same for project & project scheduler.
        this._isProcessingRequested = b;
    }

    public get IsProcessingRequested() {
        return this._isProcessingRequested;
    }

    public get OutputPath(): string {
        return this.getOutputPath();
    }

    protected abstract getOutputPath(): string;

    protected get StageId() {
        return this.getStageId();
    }

    protected abstract getStageId(): string;

    protected abstract getDepth(): number;

    public async run() {
        if (this._isInitialized) {
            return;
        }

        this.transitionToInitialized();
    }

    public async loadTileThumbnailPath(x: number, y: number, z: number): Promise<string> {
        try {
            const tile = await this._outputStageConnector.loadTileThumbnailPath(x, y, z);

            if (tile) {
                return path.join(this.OutputPath, tile.relative_path);
            }
        } catch (err) {
            debug(err);
        }

        return null;
    }

    public async loadTileStatusForPlane(zIndex: number) {

        let tiles = await this._outputStageConnector.loadTileStatusForPlane(zIndex);

        tiles = tiles.map(tile => {
            tile["stage_id"] = this.StageId;
            tile["depth"] = this.getDepth();
            return tile;
        });

        return tiles;
    }

    protected async updateToProcessQueue(): Promise<boolean> {
        //
        // TODO There is an infrequent bug where a tile stays as 2, but not in To_Process table.
        // Pending finding the bug, should do a sweep of tiles that are marked queued (2), but that are not in the
        // table and remark them incomplete (1) and reprocess them just to be safe
        //

        debug("looking for new to-process");

        let toProcessInsert: IToProcessTileAttributes[] = [];

        const unscheduled = await this._outputStageConnector.loadUnscheduled();

        debug(`found ${unscheduled.length} unscheduled`);

        if (unscheduled.length > 0) {
            let waitingToProcess = await this._outputStageConnector.loadToProcess();

            debug(`found ${waitingToProcess.length} waitingToProcess`);

            // Only items that are ready to queue, but aren't actually in the toProcess table yet.  There appear to be
            // some resubmit situations where these are out of sync temporarily.
            const notAlreadyInToProcessTable = _.differenceBy(unscheduled, waitingToProcess, "relative_path");

            // Items that are already queued in toProcess table, but for some reason are listed as incomplete rather
            // than queued in the main table.
            let alreadyInToProcessTable = _.intersectionBy(unscheduled, waitingToProcess, "relative_path");

            let toSchedule = notAlreadyInToProcessTable.filter(tile => {
                if (!isNullOrUndefined(this._project.region_x_min) && tile.lat_x < this._project.region_x_min) {
                    return false;
                }

                if (!isNullOrUndefined(this._project.region_x_max) && tile.lat_x > this._project.region_x_max) {
                    return false;
                }

                if (!isNullOrUndefined(this._project.region_y_min) && tile.lat_y < this._project.region_y_min) {
                    return false;
                }

                if (!isNullOrUndefined(this._project.region_y_max) && tile.lat_y > this._project.region_y_max) {
                    return false;
                }

                if (!isNullOrUndefined(this._project.region_z_min) && tile.lat_z < this._project.region_z_min) {
                    return false;
                }

                return !(!isNullOrUndefined(this._project.region_z_max) && tile.lat_z > this._project.region_z_max);
            });

            debug(`have ${toSchedule.length} unscheduled after region filtering`);

            toSchedule = toSchedule.map(obj => {
                obj.this_stage_status = TilePipelineStatus.Queued;
                return obj;
            });

            let now = new Date();

            toProcessInsert = toSchedule.map(obj => {
                return {
                    relative_path: obj.relative_path,
                    created_at: now,
                    updated_at: now
                };
            });

            // Update that are already in the toProcess table.
            alreadyInToProcessTable = alreadyInToProcessTable.map(obj => {
                obj.this_stage_status = TilePipelineStatus.Queued;
                return obj
            });

            await this._outputStageConnector.insertToProcess(toProcessInsert);

            const updateList = toSchedule.concat(alreadyInToProcessTable);

            await this._outputStageConnector.updateTiles(updateList);
        }

        return toProcessInsert.length > 0;
    }

    protected async updateInProcessStatus() {
        let inProcess = await this._outputStageConnector.loadInProcess();

        let workerManager = new PipelineServerContext();

        if (inProcess.length > 0) {
            debug(`updating status of ${inProcess.length} in process tiles`);

            const updateList = new InProcessModifyList();

            await Promise.all(inProcess.map(tile => this.updateOneExecutingTile(workerManager, tile, updateList)));

            await this._outputStageConnector.updateTileStatus(updateList.ToUpdate);

            await this._outputStageConnector.deleteInProcess(updateList.ToDelete);

            debug(`updated status of ${inProcess.length} in process tiles`);
        } else {
            debug(`no in process tiles to update`);
        }
    }

    private async updateOneExecutingTile(serverContext: PipelineServerContext, tile: IInProcessTileAttributes, updateList: InProcessModifyList): Promise<void> {
        let workerForTask = await serverContext.getPipelineWorker(tile.worker_id);

        const executionStatus = await PipelineWorkerClient.Instance().queryTaskExecution(workerForTask, tile.worker_task_execution_id);

        if (executionStatus.workerResponded) {
            const executionInfo = executionStatus.taskExecution;

            if (executionInfo != null) {
                const localTaskExecution = await this._outputStageConnector.loadTaskExecution(tile.task_execution_id);

                const update = Object.assign({}, {
                    job_id: executionInfo.job_id,
                    job_name: executionInfo.job_name,
                    execution_status_code: executionInfo.execution_status_code,
                    completion_status_code: executionInfo.completion_status_code,
                    last_process_status_code: executionInfo.last_process_status_code,
                    max_memory: executionInfo.max_memory,
                    max_cpu: executionInfo.max_cpu,
                    exit_code: executionInfo.exit_code,
                    submitted_at: executionInfo.submitted_at,
                    started_at: executionInfo.started_at,
                    completed_at: executionInfo.completed_at,
                    sync_status: executionInfo.sync_status,
                    synchronized_at: executionInfo.synchronized_at
                });

                await localTaskExecution.update(update);

                if (executionInfo.execution_status_code === ExecutionStatus.Completed || executionInfo.execution_status_code === ExecutionStatus.Zombie) {
                    let tileStatus = TilePipelineStatus.Queued;

                    switch (executionInfo.completion_status_code) {
                        case CompletionResult.Success:
                            tileStatus = TilePipelineStatus.Complete;
                            break;
                        case CompletionResult.Error:
                            tileStatus = TilePipelineStatus.Failed; // Do not queue again
                            break;
                        case CompletionResult.Cancel:
                            tileStatus = TilePipelineStatus.Canceled; // Could return to incomplete to be queued again
                            break;
                    }

                    // Tile should be marked with status and not be present in any intermediate tables.
                    updateList.update(tile[DefaultPipelineIdKey], tileStatus);

                    if (tileStatus === TilePipelineStatus.Complete) {
                        updatePipelineStagePerformance(this.StageId, executionInfo);

                        fse.appendFileSync(`${executionInfo.resolved_log_path}-done.txt`, `Complete ${(new Date()).toUTCString()}`);
                    }

                    // await this.inProcessTable.where(DefaultPipelineIdKey, tile[DefaultPipelineIdKey]).del();
                    updateList.remove(tile[DefaultPipelineIdKey])
                }
            } else {
                // If the worker responded and has no knowledge of this task id, it may have cleared on the worker side
                // prematurely via the worker UI.  Mark as canceled since the status is unknown.
                updateList.update(tile[DefaultPipelineIdKey], TilePipelineStatus.Canceled);

                updateList.remove(tile[DefaultPipelineIdKey])
            }
        }
    }

    /***
     * This is the opportunity to prepare any scheduler-specific information that are mapped from parameter arguments.
     * In particular, anything that requires an async/await call.
     *
     * @param {IPipelineTileAttributes} tile
     * @returns {Promise<any>}
     */
    protected async getTaskContext(tile: IPipelineTileAttributes): Promise<any> {
        return null;
    }

    protected mapTaskArgumentParameter(value: string, task: ITaskDefinition, taskExecution: ITaskExecution, worker: IPipelineWorker, tile: IPipelineTileAttributes, context: any): string {
        let result = value;

        switch (result.toUpperCase()) {
            case "PROJECT_NAME":
                return this._project.name;
            case "PROJECT_ROOT":
                return this._project.root_path;
            case "LOG_FILE":
                return taskExecution.resolved_log_path;
            case "X":
                return isNullOrUndefined(tile.lat_x) ? value : tile.lat_x.toString();
            case "Y":
                return isNullOrUndefined(tile.lat_y) ? value : tile.lat_y.toString();
            case "Z":
                return isNullOrUndefined(tile.lat_z) ? value : tile.lat_z.toString();
            case "STEP_X":
                return isNullOrUndefined(tile.step_x) ? value : tile.step_x.toString();
            case "STEP_Y":
                return isNullOrUndefined(tile.step_y) ? value : tile.step_y.toString();
            case "STEP_Z":
                return isNullOrUndefined(tile.step_z) ? value : tile.step_z.toString();
            case "EXPECTED_EXIT_CODE":
                return isNullOrUndefined(task.expected_exit_code) ? value : task.expected_exit_code.toString();
            case "IS_CLUSTER_JOB":
                return worker.is_cluster_proxy ? "1" : "0";
            case "TASK_ID":
                return taskExecution.id;
        }

        return result;
    }

    protected mapTaskArguments(task: ITaskDefinition, taskExecution: ITaskExecution, worker: IPipelineWorker, tile: IPipelineTileAttributes, context: any): string[] {
        const scriptsArgs: ITaskArgument[] = task.user_arguments;

        return scriptsArgs.map(arg => {
            if (arg.type === TaskArgumentType.Literal) {
                return arg.value;
            }

            let value = arg.value;

            if (value.length > 3) {
                value = value.substring(2, value.length - 1);
            }

            // Stop-gap to make sure we never return an empty return that causes arguments in the shell script to go out
            // of order.
            return this.mapTaskArgumentParameter(value, task, taskExecution, worker, tile, context) || value;
        });
    }

    protected async muxInputOutputTiles(knownInput, knownOutput: IPipelineTile[]): Promise<IMuxTileLists> {
        return {
            toInsert: [],
            toUpdate: [],
            toReset: [],
            toDelete: []
        };
    }

    protected async refreshWithKnownInput(knownInput: any[]) {
        if (knownInput.length > 0) {
            debug(`updating known input tiles`);

            let knownOutput = await this._outputStageConnector.loadTiles({attributes: [DefaultPipelineIdKey, "prev_stage_status", "this_stage_status"]});

            debug(`muxing tiles`);
            let sorted = await this.muxInputOutputTiles(knownInput, knownOutput);

            debug(`${sorted.toInsert.length} to insert`);
            await this._outputStageConnector.insertTiles(sorted.toInsert);

            debug(`${sorted.toUpdate.length} to update`);
            await this._outputStageConnector.updateTiles(sorted.toUpdate);

            debug(`${sorted.toDelete.length} to delete`);
            await this._outputStageConnector.deleteTiles(sorted.toDelete);

            debug(`${sorted.toDelete.length} to delete in to process`);
            // Also remove any queued to process.
            await this._outputStageConnector.deleteToProcess(sorted.toDelete);

            // Remove any queued whose previous stage have been reverted.

            debug(`${sorted.toReset.length} to reset`);
            if (sorted.toReset.length > 0) {
                debug(`${sorted.toReset.length} tiles have reverted their status and should be removed from to-process`);
                await this._outputStageConnector.deleteToProcess(sorted.toReset.map(t => t.relative_path));
            }

            debug(`update complete`);
            /*
             const previousStageRegression = sorted.toUpdate.filter(t => t.prev_stage_status !== TilePipelineStatus.Complete && t.this_stage_status === TilePipelineStatus.Queued).map(t => t[DefaultPipelineIdKey]);

             if (previousStageRegression.length > 0) {
                 debug(`${previousStageRegression.length} tiles have reverted their status and should be removed from to-process`);
                 await this._outputStageConnector.deleteToProcess(previousStageRegression);
             }
             */
        } else {
            debug("no input from previous stage");
        }
    }

    protected async performProcessing(): Promise<void> {
    }

    protected async refreshTileStatus(): Promise<boolean> {
        return false;
    }

    protected async performWork() {
        if (this.IsExitRequested) {
            debug("cancel requested - exiting stage worker");
            return;
        }

        try {
            // Update tiles.
            const available = await this.refreshTileStatus();

            // If there is any to-process, try to fill worker capacity.
            if (this.IsProcessingRequested && available) {
                await this.performProcessing();
            }
        } catch (err) {
            debug(err);
        }

        setTimeout(() => this.performWork(), 30 * 1000)
    }

    protected abstract async createOutputStageConnector(connector: ProjectDatabaseConnector): Promise<StageTableConnector>;

    protected async createTables(connector: ProjectDatabaseConnector) {
        this._outputStageConnector = await this.createOutputStageConnector(connector);

        return this._outputStageConnector !== null;
    }

    /*
     * State transitions
     */
    private transitionToInitialized() {
        try {
            this._isInitialized = true;

            setImmediate(() => this.transitionToEstablishDataConnection());
        } catch (err) {
            debug(err);
        }
    }

    private async transitionToEstablishDataConnection() {
        try {
            if (this.IsExitRequested) {
                return;
            }

            const connector = await connectorForProject(this._project);

            const connected = await this.createTables(connector);

            if (connected) {
                await this.transitionToProcessStage()
            } else {
                setTimeout(() => this.transitionToEstablishDataConnection(), 15 * 1000);
            }
        } catch (err) {
            debug(err);
        }
    }

    private async transitionToProcessStage() {
        try {
            if (this.IsExitRequested) {
                return;
            }

            await this.performWork();
        } catch (err) {
            debug(err);
        }
    }
}

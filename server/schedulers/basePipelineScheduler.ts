import * as _ from "lodash";

const debug = require("debug")("pipeline:scheduler:base-pipeline-scheduler");

import {ISchedulerInterface} from "./schedulerHub";
import {IProject} from "../data-model/sequelize/project";
import {
    CompletionResult,
    ExecutionStatus,
    ITaskExecutionAttributes,
    IWorkerTaskExecutionAttributes
} from "../data-model/taskExecution";
import {
    connectorForProject,
    ProjectDatabaseConnector
} from "../data-access/sequelize/project-connectors/projectDatabaseConnector";
import {
    IPipelineTile, IPipelineTileAttributes, IToProcessTileAttributes,
    StageTableConnector
} from "../data-access/sequelize/project-connectors/stageTableConnector";
import {ITaskArgument, ITaskDefinition, TaskArgumentType} from "../data-model/sequelize/taskDefinition";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";
import {IPipelineStage} from "../data-model/sequelize/pipelineStage";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";

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

export abstract class BasePipelineScheduler implements ISchedulerInterface {
    // protected _project: IProject;
    private readonly  _projectId: string;

    protected readonly _sourceId: string;
    // protected _source: IProject | IPipelineStage;

    protected _outputStageConnector: StageTableConnector;

    private _isCancelRequested: boolean;
    private _isProcessingRequested: boolean;

    private _isInitialized: boolean = false;

    protected constructor(project: IProject, source: IProject | IPipelineStage) {
        this._projectId = project.id;
        this._sourceId = source.id;

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

    public async getProject(): Promise<IProject> {
        return await PersistentStorageManager.Instance().Projects.findById(this._projectId);
    }

    public abstract async getSource(): Promise<IProject | IPipelineStage>;

    public async run(): Promise<void> {
        if (this._isInitialized) {
            return;
        }

        return this.transitionToEstablishDataConnection();
    }

    protected async updateToProcessQueue(): Promise<boolean> {
        //
        // TODO There is an infrequent bug where a tile stays as 2, but not in To_Process table.
        // Pending finding the bug, should do a sweep of tiles that are marked queued (2), but that are not in the
        // table and remark them incomplete (1) and reprocess them just to be safe
        //

        const project = await this.getProject();

        const source = await this.getSource();

        let toProcessInsert: IToProcessTileAttributes[] = [];

        let unscheduled = await this._outputStageConnector.loadUnscheduled();

        debug(`${source.name}: found ${unscheduled.length} unscheduled`);

        if (unscheduled.length > 0) {
            let waitingToProcess = await this._outputStageConnector.loadToProcess();

            const initialLength = waitingToProcess.length;

            debug(`${source.name}: found ${initialLength} waitingToProcess`);

            // Only items that are ready to queue, but aren't actually in the toProcess table yet.  There appear to be
            // some resubmit situations where these are out of sync temporarily.
            const notAlreadyInToProcessTable = _.differenceBy(unscheduled, waitingToProcess, "relative_path");

            // Items that are already queued in toProcess table, but for some reason are listed as incomplete rather
            // than queued in the main table.
            let alreadyInToProcessTable = _.intersectionBy(unscheduled, waitingToProcess, "relative_path");

            let toSchedule = notAlreadyInToProcessTable.filter(tile => {
                if (project.region_x_min != null && tile.lat_x < project.region_x_min) {
                    return false;
                }

                if (project.region_x_max != null && tile.lat_x > project.region_x_max) {
                    return false;
                }

                if (project.region_y_min != null && tile.lat_y < project.region_y_min) {
                    return false;
                }

                if (project.region_y_max != null && tile.lat_y > project.region_y_max) {
                    return false;
                }

                if (project.region_z_min != null && tile.lat_z < project.region_z_min) {
                    return false;
                }

                return !(project.region_z_max != null && tile.lat_z > project.region_z_max);
            });

            if (initialLength !== toSchedule.length) {
                debug(`${source.name}: have ${toSchedule.length} unscheduled after region filtering`);
            }

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

    public async onTaskExecutionUpdate(executionInfo: IWorkerTaskExecutionAttributes): Promise<void> {
        const localTaskExecution = await this._outputStageConnector.loadTaskExecution(executionInfo.remote_task_execution_id);

        if (localTaskExecution == null || localTaskExecution.execution_status_code > ExecutionStatus.Running) {
            // Don't update in the event this is processed after the completion message (separate queues).
            // Also, could get an update before response from start is processed (no entry yet).
            return;
        }

        const update = Object.assign({}, {
            job_id: executionInfo.job_id,
            job_name: executionInfo.job_name,
            execution_status_code: executionInfo.execution_status_code,
            last_process_status_code: executionInfo.last_process_status_code,
            cpu_time_seconds: executionInfo.cpu_time_seconds,
            max_cpu_percent: executionInfo.max_cpu_percent,
            max_memory_mb: executionInfo.max_memory_mb,
            submitted_at: executionInfo.submitted_at,
            started_at: executionInfo.started_at
        });

        // TODO nothing stopping the async here and next method from having this complete after an update goes through.
        await localTaskExecution.update(update);
    }

    public async onTaskExecutionComplete(executionInfo: IWorkerTaskExecutionAttributes): Promise<void> {
        const localTaskExecution = await this._outputStageConnector.loadTaskExecution(executionInfo.remote_task_execution_id);

        if (localTaskExecution == null) {
            // There is no record of this task execution - two possibilities:
            // 1) It is considered running by the scheduler - something went wrong, reset the tile.
            // 2) It never started because the worker errored trying to start - an execution was never stored because
            //    there was a good chance this message would come before it was even added.
            const tile = await this._outputStageConnector.loadTileById(executionInfo.tile_id);

            if (tile !== null && tile.this_stage_status === TilePipelineStatus.Processing) {
                await this._outputStageConnector.updateTileStatus(executionInfo.tile_id, TilePipelineStatus.Incomplete);
            }

            // Remove from in process if there.
            await this._outputStageConnector.deleteInProcess([executionInfo.tile_id]);

            return;
        }

        const update = Object.assign({}, {
            job_id: executionInfo.job_id,
            job_name: executionInfo.job_name,
            execution_status_code: executionInfo.execution_status_code,
            completion_status_code: executionInfo.completion_status_code,
            last_process_status_code: executionInfo.last_process_status_code,
            cpu_time_seconds: executionInfo.cpu_time_seconds,
            max_cpu_percent: executionInfo.max_cpu_percent,
            max_memory_mb: executionInfo.max_memory_mb,
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

            await this._outputStageConnector.updateTileStatus(executionInfo.tile_id, tileStatus);

            await this._outputStageConnector.deleteInProcess([executionInfo.tile_id]);
        }
    }

    /***
     * This is the opportunity to prepare any scheduler-specific information that is mapped from parameter arguments.
     * In particular, anything that requires an async/await call.
     *
     * @param {IPipelineTileAttributes} tile
     * @returns {Promise<any>}
     */
    protected async getTaskContext(tile: IPipelineTileAttributes): Promise<any> {
        return null;
    }

    private static mapUserParameter(valueLowerCase: string, userParameters: Map<string, string>): string {
        return userParameters.has(valueLowerCase) ? userParameters.get(valueLowerCase) : null;
    }

    protected mapTaskArgumentParameter(project: IProject, valueLowerCase: string, task: ITaskDefinition, taskExecution: ITaskExecutionAttributes, worker: IPipelineWorker, tile: IPipelineTileAttributes, context: any): string {
        switch (valueLowerCase) {
            case "project_name":
                return project.name;
            case "project_root":
                return project.root_path;
            case "log_file":
                return taskExecution.resolved_log_path;
            case "x":
                return tile.lat_x == null ? null : tile.lat_x.toString();
            case "y":
                return tile.lat_y == null ? null : tile.lat_y.toString();
            case "z":
                return tile.lat_z == null ? null : tile.lat_z.toString();
            case "step_x":
                return tile.step_x == null ? null : tile.step_x.toString();
            case "step_y":
                return tile.step_y == null ? null : tile.step_y.toString();
            case "step_z":
                return tile.step_z == null ? null : tile.step_z.toString();
            case "expected_exit_code":
                return task.expected_exit_code ? null : task.expected_exit_code.toString();
            case "is_cluster_job":
                return "is_cluster_job"; // Will be filled in by the worker
            case "task_id":
                return taskExecution.id;
        }

        return null;
    }

    protected mapTaskArguments(project: IProject, task: ITaskDefinition, taskExecution: ITaskExecutionAttributes, worker: IPipelineWorker, tile: IPipelineTileAttributes, context: any): string[] {
        const scriptsArgs: ITaskArgument[] = task.user_arguments;

        if (scriptsArgs.length === 0) {
            return [];
        }

        const userParameters = new Map<string, string>();

        const userParametersObj = JSON.parse(project.user_parameters);

        Object.keys(userParametersObj).forEach(prop => userParameters.set(prop.toLowerCase(), userParametersObj[prop]));

        return scriptsArgs.map(arg => {
            if (arg.type === TaskArgumentType.Literal) {
                return arg.value;
            }

            let value = arg.value.toLowerCase();

            if (value.length > 3) {
                value = value.substring(2, value.length - 1);
            }

            // Never return an empty result that causes arguments in the shell script to go out of order.  Use the
            // parameter name as a last resort.
            //
            // User-defined parameters from project or stage (not implemented) input override task parameter with same
            // name.
            return BasePipelineScheduler.mapUserParameter(value, userParameters) || this.mapTaskArgumentParameter(project, value, task, taskExecution, worker, tile, context) || value;
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
        const source = await this.getSource();

        if (knownInput.length > 0) {
            let knownOutput = await this._outputStageConnector.loadTiles({attributes: [DefaultPipelineIdKey, "prev_stage_status", "this_stage_status"]});

            let sorted = await this.muxInputOutputTiles(knownInput, knownOutput);

            await this._outputStageConnector.insertTiles(sorted.toInsert);

            await this._outputStageConnector.updateTiles(sorted.toUpdate);

            await this._outputStageConnector.deleteTiles(sorted.toDelete);

            // Also remove any queued to process.
            await this._outputStageConnector.deleteToProcess(sorted.toDelete);

            // Remove any queued whose previous stage have been reverted.
            if (sorted.toReset.length > 0) {
                debug(`${sorted.toReset.length} tiles have reverted their status and should be removed from to-process`);
                await this._outputStageConnector.deleteToProcess(sorted.toReset.map(t => t.relative_path));
            }

            debug(`${source.name}: ${sorted.toInsert.length} insert, ${sorted.toUpdate.length} update, ${sorted.toDelete.length} delete, ${sorted.toReset.length} reset`);
        } else {
            debug(`${source.name}: no input from previous stage`);
        }
    }

    protected async performProcessing(): Promise<void> {
    }

    protected async refreshTileStatus(): Promise<boolean> {
        return false;
    }

    protected async performWork() {
        if (this.IsExitRequested) {
            const source = await this.getSource();

            debug(`${source.name}: cancel requested - exiting stage worker`);
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

    private async transitionToEstablishDataConnection(): Promise<void> {
        try {
            this._isInitialized = true;

            if (this.IsExitRequested) {
                return;
            }

            const project = await this.getProject();

            const connector = await connectorForProject(project);

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

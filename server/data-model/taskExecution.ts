import {FindOptions, Instance, Model, Sequelize} from "sequelize";

import {isNullOrUndefined} from "util";
import {IPipelineWorker, QueueType} from "./sequelize/pipelineWorker";
import {ITaskDefinition} from "./sequelize/taskDefinition";

export interface IStartTaskInput {
    pipelineStageId: string;
    tileId: string;
    logFile: string;
}

export enum ExecutionStatus {
    Undefined = 0,
    Initializing = 1,
    Running = 2,
    Zombie = 3,   // Was marked initialized/running but can not longer find in process manager list/cluster jobs
    Orphaned = 4, // Found in process manager with metadata that associates to worker, but no linked task in database
    Completed = 5
}

export enum CompletionResult {
    Unknown = 0,
    Incomplete = 1,
    Cancel = 2,
    Success = 3,
    Error = 4,
    Resubmitted = 5
}


export enum SyncStatus {
    Never = 0,
    InProgress = 1,
    Complete = 2,
    Expired = 3
}

export interface ITaskExecutionAttributes {
    id?: string;
    task_definition_id?: string;
    pipeline_stage_id?: string;
    tile_id?: string;
    resolved_script?: string;
    resolved_interpreter?: string;
    resolved_script_args?: string;
    resolved_cluster_args?: string;
    resolved_log_path?: string;
    expected_exit_code?: number;
    worker_id?: string;
    work_units?: number;
    cluster_work_units?: number;
    queue_type?: number;
    job_id?: number;
    job_name?: string;
    execution_status_code?: ExecutionStatus;
    completion_status_code?: CompletionResult;
    last_process_status_code?: number;
    max_memory?: number;
    max_cpu?: number;
    exit_code?: number;
    submitted_at?: Date;
    started_at?: Date;
    completed_at?: Date;
    sync_status?: SyncStatus;
    synchronized_at?: Date;
    created_at?: Date;
    updated_at?: Date;
    deleted_at?: Date;
}

export interface ITaskExecution extends Instance<ITaskExecutionAttributes>, ITaskExecutionAttributes {
}

export interface ITaskExecutionModel extends Model<ITaskExecution, ITaskExecutionAttributes> {
    createTaskExecution(worker: IPipelineWorker, taskDefinition: ITaskDefinition, startTaskInput: IStartTaskInput): Promise<ITaskExecution>;
    getPage(reqOffset: number, reqLimit: number, completionCode: CompletionResult): Promise<ITaskExecution[]>;
}

export function createTaskExecutionTable(sequelize: Sequelize, tableName: string): any {
    const DataTypes = sequelize.Sequelize;

    return sequelize.define(tableName, {
        id: {
            primaryKey: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        task_definition_id: {
            type: DataTypes.UUID
        },
        pipeline_stage_id: {
            type: DataTypes.UUID
        },
        tile_id: {
            type: DataTypes.TEXT
        },
        resolved_script: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        resolved_interpreter: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        resolved_script_args: {
            type: DataTypes.TEXT
        },
        resolved_cluster_args: {
            type: DataTypes.TEXT
        },
        resolved_log_path: {
            type: DataTypes.TEXT
        },
        expected_exit_code: {
            type: DataTypes.INTEGER
        },
        worker_id: {
            type: DataTypes.UUID
        },
        work_units: {
            type: DataTypes.INTEGER
        },
        cluster_work_units: {
            type: DataTypes.INTEGER
        },
        queue_type: {
            type: DataTypes.INTEGER
        },
        job_id: {
            type: DataTypes.INTEGER
        },
        job_name: {
            type: DataTypes.TEXT
        },
        execution_status_code: {
            type: DataTypes.INTEGER
        },
        completion_status_code: {
            type: DataTypes.INTEGER
        },
        last_process_status_code: {
            type: DataTypes.INTEGER
        },
        max_memory: {
            type: DataTypes.FLOAT
        },
        max_cpu: {
            type: DataTypes.FLOAT
        },
        exit_code: {
            type: DataTypes.INTEGER
        },
        submitted_at: {
            type: DataTypes.DATE
        },
        started_at: {
            type: DataTypes.DATE
        },
        completed_at: {
            type: DataTypes.DATE
        },
        sync_status: {
            type: DataTypes.INTEGER
        },
        synchronized_at: {
            type: DataTypes.DATE
        }
    }, {
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: false,
        indexes: [{
            fields: ["worker_id"]
        }]
    });

    /*
    TaskExecutionModel.associate = models => {
        TaskExecutionModel.belongsTo(models.TaskDefinitions, {foreignKey: "task_definition_id"});
        TaskExecutionModel.belongsTo(models.PipelineStages, {foreignKey: "pipeline_stage_id"});
    };
    */
}

export function augmentTaskExecutionModel(Model: ITaskExecutionModel) {
    Model.getPage = async (reqOffset: number, reqLimit: number, completionCode: CompletionResult): Promise<ITaskExecution[]> => {
        const options: FindOptions<ITaskExecutionAttributes> = {
            offset: reqOffset,
            limit: reqLimit
        };

        if (!isNullOrUndefined(completionCode)) {
            options.where = {completion_status_code: completionCode};
        }

        return Model.findAll(options);
    };


    Model.createTaskExecution = async function (worker: IPipelineWorker, taskDefinition: ITaskDefinition, startTaskInput: IStartTaskInput): Promise<ITaskExecution> {
        let taskExecution = await createTaskExecutionWithInput(worker, taskDefinition, startTaskInput);

        return this.create(taskExecution);
    };
}

async function createTaskExecutionWithInput(worker: IPipelineWorker, taskDefinition: ITaskDefinition, startTaskInput: IStartTaskInput): Promise<ITaskExecutionAttributes> {
    const queueType: QueueType = worker.is_cluster_proxy ? QueueType.Cluster : QueueType.Local;

    return {
        task_definition_id: taskDefinition.id,
        pipeline_stage_id: startTaskInput.pipelineStageId,
        tile_id: startTaskInput.tileId,
        worker_id: worker.id,
        work_units: queueType === QueueType.Local ? taskDefinition.work_units : null,
        cluster_work_units: queueType === QueueType.Cluster ? taskDefinition.cluster_work_units : null,
        resolved_script: await taskDefinition.getFullScriptPath(false),
        resolved_interpreter: taskDefinition.interpreter,
        resolved_script_args: null, // Will be filled later b/c may include execution id created after this is saved. JSON.stringify(startTaskInput.scriptArgs),
        resolved_cluster_args: queueType === QueueType.Cluster ? JSON.parse(taskDefinition.cluster_args).arguments[0] : null,
        resolved_log_path: startTaskInput.logFile,
        expected_exit_code: taskDefinition.expected_exit_code,
        queue_type: queueType,
        job_id: null,
        job_name: null,
        execution_status_code: ExecutionStatus.Initializing,
        completion_status_code: CompletionResult.Incomplete,
        last_process_status_code: null,
        max_memory: NaN,
        max_cpu: NaN,
        exit_code: null,
        submitted_at: null,
        started_at: null,
        completed_at: null,
        sync_status: SyncStatus.Never,
        synchronized_at: null,
        created_at: null,
        updated_at: null,
        deleted_at: null
    };
}

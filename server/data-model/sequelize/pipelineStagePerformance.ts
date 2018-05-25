const AsyncLock = require("async");

import {CompletionResult, ITaskExecutionAttributes} from "../taskExecution";

enum PerformanceQueueActions {
    Reset = 0,
    UpdateStatistics = 1,
    UpdateCounts = 2
}

interface IPerformanceQueueItem {
    action: PerformanceQueueActions;
    context: any
}

interface IUpdateCountsQueueItem {
    pipeline_stage_id: string;
    inProcess: number;
    waiting: number;
}

interface IUpdateStatsQueueItem {
    pipeline_stage_id: string;
    status: CompletionResult;
    memory_mb: number;
    cpu_percent: number;
    duration_ms: number;
}

export interface IPipelineStagePerformance {
    id?: string;
    pipeline_stage_id: string;
    num_in_process: number;
    num_ready_to_process: number;
    num_execute: number;
    num_complete: number;
    num_error: number;
    num_cancel: number;
    cpu_average: number;
    cpu_high: number;
    cpu_low: number;
    memory_average: number;
    memory_high: number;
    memory_low: number;
    duration_average: number;
    duration_high: number;
    duration_low: number;
    created_at: Date;
    updated_at: Date;
    deleted_at: Date;
}

export const TableName = "PipelineStagePerformances";

let PipelineStagePerformanceAccess = null;

export function sequelizeImport(sequelize, DataTypes) {
    const PipelineStagePerformance = sequelize.define(TableName, {
        id: {
            primaryKey: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        num_in_process: {
            type: DataTypes.INTEGER,
        },
        num_ready_to_process: {
            type: DataTypes.INTEGER,
        },
        num_execute: {
            type: DataTypes.INTEGER,
            defaultValue: ""
        },
        num_complete: {
            type: DataTypes.INTEGER,
            defaultValue: ""
        },
        num_error: {
            type: DataTypes.INTEGER,
            defaultValue: ""
        },
        num_cancel: {
            type: DataTypes.INTEGER,
        },
        duration_average: {
            type: DataTypes.FLOAT,
        },
        duration_high: {
            type: DataTypes.FLOAT,
        },
        duration_low: {
            type: DataTypes.FLOAT,
        },
        cpu_average: {
            type: DataTypes.FLOAT,
        },
        cpu_high: {
            type: DataTypes.FLOAT,
        },
        cpu_low: {
            type: DataTypes.FLOAT,
        },
        memory_average: {
            type: DataTypes.FLOAT,
        },
        memory_high: {
            type: DataTypes.FLOAT
        },
        memory_low: {
            type: DataTypes.FLOAT
        }
    }, {
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: false
    });

    PipelineStagePerformance.associate = models => {
        PipelineStagePerformance.belongsTo(models.PipelineStages, {foreignKey: "pipeline_stage_id"});
    };

    PipelineStagePerformance.createForStage = async function (pipelineStageId: string): Promise<IPipelineStagePerformance> {
        let pipelineStagePerformance = create(pipelineStageId);

        return PipelineStagePerformance.create(pipelineStagePerformance);
    };

    PipelineStagePerformance.queue = AsyncLock.queue(async (updateItem: IPerformanceQueueItem, callback) => {
        try {
            switch (updateItem.action) {
                case PerformanceQueueActions.Reset:
                    await PipelineStagePerformance.reset(true);
                    break;
                case PerformanceQueueActions.UpdateStatistics:
                    await PipelineStagePerformance.updateForPipelineStage(updateItem.context);
                    break;
                case PerformanceQueueActions.UpdateCounts:
                    await PipelineStagePerformance.updateCountsForPipelineStage(updateItem.context);
                    break;
            }
        } catch (err) {
            console.log(err);
        }

        callback();
    }, 1);

    PipelineStagePerformance.queue.error = (err) => {
        console.log("queue error");
    };

    PipelineStagePerformance.updateCountsForPipelineStage = async function (updateTask: IUpdateCountsQueueItem) {
        let performance = await this.findOne({where: {pipeline_stage_id: updateTask.pipeline_stage_id}});

        if (!performance) {
            performance = await this.createForStage(updateTask.pipeline_stage_id);
        }

        performance.num_in_process = updateTask.inProcess;
        performance.num_ready_to_process = updateTask.waiting;

        await performance.save();
    };

    PipelineStagePerformance.updateForPipelineStage = async function (updateTask: IUpdateStatsQueueItem) {
        let performance = await this.findOne({where: {pipeline_stage_id: updateTask.pipeline_stage_id}});

        if (!performance) {
            performance = await this.createForStage(updateTask.pipeline_stage_id);
        }

        switch (updateTask.status) {
            case CompletionResult.Cancel:
                performance.num_cancel++;
                break;
            case CompletionResult.Error:
                performance.num_error++;
                break;
            case CompletionResult.Success:
                updatePerformanceStatistics(performance, updateTask.cpu_percent, updateTask.memory_mb, updateTask.duration_ms);
                performance.num_complete++;
                break;
            default:
                return; // Should only be updated on completion of some form.
        }

        performance.num_execute++;

        await performance.save();
    };

    PipelineStagePerformance.reset = async function (now: boolean = false) {
        if (now) {
            await PipelineStagePerformance.destroy({where: {}, truncate: true});
        } else {
            PipelineStagePerformance.queue.push({
                action: PerformanceQueueActions.Reset,
                context: null
            }, (err) => {
                console.log(err);
            });
        }

        return 0;
    };

    PipelineStagePerformanceAccess = PipelineStagePerformance;

    return PipelineStagePerformance;
}

function create(pipelineStageId: string): IPipelineStagePerformance {
    return {
        pipeline_stage_id: pipelineStageId,
        num_in_process: 0,
        num_ready_to_process: 0,
        num_execute: 0,
        num_complete: 0,
        num_error: 0,
        num_cancel: 0,
        cpu_average: 0,
        cpu_high: -Infinity,
        cpu_low: Infinity,
        memory_average: 0,
        memory_high: -Infinity,
        memory_low: Infinity,
        duration_average: 0,
        duration_high: -Infinity,
        duration_low: Infinity,
        created_at: null,
        updated_at: null,
        deleted_at: null
    };
}

function updatePerformanceStatistics(performance: IPipelineStagePerformance, cpu: number, mem: number, duration_ms: number) {
    updatePerformance(performance, "cpu", cpu);
    updatePerformance(performance, "memory", mem);
    updatePerformance(performance, "duration", duration_ms / 1000 / 3600);
}

function updatePerformance(performance: IPipelineStagePerformance, statName: string, latestPerformance: number) {
    if (latestPerformance == null || isNaN(latestPerformance)) {
        return;
    }

    performance[statName + "_average"] = updateAverage(performance[statName + "_average"], performance.num_complete, latestPerformance);
    performance[statName + "_high"] = Math.max(performance[statName + "_high"], latestPerformance);
    performance[statName + "_low"] = Math.min(performance[statName + "_low"], latestPerformance);
}

function updateAverage(existing_average: number, existing_count: number, latestValue: number) {
    return ((existing_average * existing_count) + latestValue ) / (existing_count + 1);
}

export function updatePipelineStageCounts(pipelineStageId: string, inProcess: number, waiting: number) {
    if (!PipelineStagePerformanceAccess) {
        return;
    }

    PipelineStagePerformanceAccess.queue.push({
        action: PerformanceQueueActions.UpdateCounts,
        context: {
            pipeline_stage_id: pipelineStageId,
            inProcess: inProcess,
            waiting: waiting,
        }
    }, (err) => {
    });
}

export function updatePipelineStagePerformance(pipelineStageId: string, taskExecution: ITaskExecutionAttributes) {
    if (!PipelineStagePerformanceAccess) {
        return;
    }

    let duration_ms = null;

    if (taskExecution.completed_at && taskExecution.started_at) {
        duration_ms = taskExecution.completed_at.valueOf() - taskExecution.started_at.valueOf();
    }

    PipelineStagePerformanceAccess.queue.push({
        action: PerformanceQueueActions.UpdateStatistics,
        context: {
            pipeline_stage_id: pipelineStageId,
            status: taskExecution.completion_status_code,
            cpu_percent: taskExecution.max_cpu,
            memory_mb: taskExecution.max_memory,
            duration_ms: duration_ms
        }
    }, (err) => {
    });
}


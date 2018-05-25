import {Instance, Model, Sequelize} from "sequelize";

const debug = require("debug")("pipeline:coordinator-api:stage-database-connector");

import {TilePipelineStatus} from "../../schedulers/basePipelineScheduler";
import {
    augmentTaskExecutionModel,
    createTaskExecutionTable, IStartTaskInput, ITaskExecution,
    ITaskExecutionModel
} from "../../data-model/taskExecution";
import {IPipelineWorker} from "../../data-model/sequelize/pipelineWorker";
import {ITaskDefinition} from "../../data-model/sequelize/taskDefinition";

export function generatePipelineCustomTableName(pipelineStageId: string, tableName) {
    return pipelineStageId + "_" + tableName;
}

function generatePipelineStageInProcessTableName(pipelineStageId: string) {
    return generatePipelineCustomTableName(pipelineStageId, "InProcess");
}

function generatePipelineStageToProcessTableName(pipelineStageId: string) {
    return generatePipelineCustomTableName(pipelineStageId, "ToProcess");
}

function generatePipelineStageTaskExecutionTableName(pipelineStageId: string) {
    return generatePipelineCustomTableName(pipelineStageId, "TaskExecutions");
}

export interface IPipelineStageTileCounts {
    incomplete: number;
    queued: number;
    processing: number;
    complete: number;
    failed: number;
    canceled: number;
}

export interface IPipelineTileAttributes {
    relative_path?: string;
    index?: number;
    tile_name?: string;
    prev_stage_status?: TilePipelineStatus;
    this_stage_status?: TilePipelineStatus;
    lat_x?: number;
    lat_y?: number;
    lat_z?: number;
    step_x?: number;
    step_y?: number;
    step_z?: number;
    duration?: number;
    cpu_high?: number;
    memory_high?: number;
    created_at?: Date;
    updated_at?: Date;
}

export interface IInProcessTileAttributes {
    relative_path: string;
    worker_id?: string;
    worker_last_seen?: Date;
    task_execution_id?: string;
    worker_task_execution_id?: string;
    created_at?: Date;
    updated_at?: Date;
}

export interface IToProcessTileAttributes {
    relative_path: string;
    created_at?: Date;
    updated_at?: Date;
}

export interface IPipelineTile extends Instance<IPipelineTileAttributes>, IPipelineTileAttributes {
}

export interface IPipelineTileModel extends Model<IPipelineTile, IPipelineTileAttributes> {
}

export interface IInProcessTile extends Instance<IInProcessTileAttributes>, IInProcessTileAttributes {
}

export interface IInProcessTileModel extends Model<IInProcessTile, IInProcessTileAttributes> {
}

export interface IToProcessTile extends Instance<IToProcessTileAttributes>, IToProcessTileAttributes {
}

export interface IToProcessTileModel extends Model<IToProcessTile, IToProcessTileAttributes> {
}

const CreateChunkSize = 100;
const UpdateChunkSize = 100;

export class StageTableConnector {

    protected _connection: Sequelize;
    protected _tableBaseName: string;

    private _tileTable: IPipelineTileModel = null;
    private _toProcessTable: IInProcessTileModel = null;
    private _inProcessTable: IToProcessTileModel = null;
    private _taskExecutionTable: ITaskExecutionModel = null;

    public constructor(connection: Sequelize, id: string) {
        this._connection = connection;
        this._tableBaseName = id;
    }

    public async initialize() {
        this.defineTables();

        return this._connection.sync();
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadTiles(options = null): Promise<IPipelineTile[]> {
        // TODO this is leaking sequelize specifics to callers.
        if (options) {
            return this._tileTable.findAll(options);
        } else {
            return this._tileTable.findAll();
        }
    }

    public async loadTile(where: any): Promise<IPipelineTile> {
        return this._tileTable.findOne({where});
    }

    public async loadUnscheduled(): Promise<IPipelineTile[]> {
        return this._tileTable.findAll({
            where: {
                prev_stage_status: TilePipelineStatus.Complete,
                this_stage_status: TilePipelineStatus.Incomplete
            }
        });
    }

    public async loadTileThumbnailPath(x: number, y: number, z: number): Promise<IPipelineTile> {
        return this._tileTable.findOne({where: {lat_x: x, lat_y: y, lat_z: z}});
    }

    public async loadTileStatusForPlane(zIndex: number): Promise<IPipelineTile[]> {
        return this._tileTable.findAll({where: {lat_z: zIndex}});
    }

    public async loadInProcess(): Promise<IInProcessTile[]> {
        return this._inProcessTable.findAll();
    }

    public async loadToProcess(limit: number = null): Promise<IToProcessTile[]> {
        return this._toProcessTable.findAll({order: [["relative_path", "ASC"]], limit: limit});
    }

    public async loadTaskExecution(id: string): Promise<ITaskExecution> {
        return this._taskExecutionTable.findById(id);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async countTiles(where: any): Promise<number> {
        if (where) {
            return this._tileTable.count(where);
        } else {
            return this._tileTable.count();
        }
    }

    public async countInProcess(): Promise<number> {
        return this._inProcessTable.count();
    }

    public async countToProcess(): Promise<number> {
        return this._toProcessTable.count();
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertTiles(tiles: IPipelineTileAttributes[]) {
        return StageTableConnector.bulkCreate(this._tileTable, tiles);
    }

    public async updateTiles(objArray: IPipelineTile[]) {
        if (!objArray || objArray.length === 0) {
            return;
        }

        debug(`bulk update ${objArray.length} items`);

        // Operate on a shallow copy since splice is going to be destructive.
        const toUpdate = objArray.slice();

        while (toUpdate.length > 0) {
            try {
                const next = toUpdate.splice(0, UpdateChunkSize);
                await this.bulkUpdate(next);
            } catch (err) {
                debug(err);
            }
        }
    }

    public async updateTileStatus(toUpdate: Map<TilePipelineStatus, string[]>) {
        if (!toUpdate) {
            return;
        }

        return Promise.all(Array.from(toUpdate.keys()).map(async (status) => {
            await this._tileTable.update({this_stage_status: status},
                {where: {relative_path: {$in: toUpdate.get(status)}}});
        }));
    }

    public async deleteTiles(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._tileTable.destroy({where: {relative_path: {$in: toDelete}}});
    }

    public async getTileCounts(): Promise<IPipelineStageTileCounts> {
        const incomplete = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Incomplete}});
        const queued = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Queued}});
        const processing = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Processing}});
        const complete = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Complete}});
        const failed = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Failed}});
        const canceled = await this._tileTable.count({where: {this_stage_status: TilePipelineStatus.Canceled}});

        return {
            incomplete,
            queued,
            processing,
            complete,
            failed,
            canceled
        }
    }

    public async setTileStatus(tileIds: string[], status: TilePipelineStatus): Promise<IPipelineTileAttributes[]> {
        const [affectedCount, affectedRows] = await this._tileTable.update({this_stage_status: status}, {
            where: {relative_path: {$in: tileIds}},
            returning: true
        });

        return affectedRows;
    }

    public async convertTileStatus(currentStatus: TilePipelineStatus, desiredStatus: TilePipelineStatus): Promise<IPipelineTileAttributes[]> {
        const [affectedCount, affectedRows] = await this._tileTable.update({this_stage_status: desiredStatus}, {
            where: {this_stage_status: currentStatus},
            returning: true
        });

        return affectedRows;
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertToProcess(toProcess: IToProcessTileAttributes[]) {
        return StageTableConnector.bulkCreate(this._toProcessTable, toProcess);
    }

    public async deleteToProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: {$in: toDelete}}});
    }

    public async deleteToProcessTile(toProcess: IToProcessTileAttributes) {
        if (!toProcess) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: toProcess.relative_path}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertInProcessTile(inProcess: IInProcessTileAttributes) {
        return this._inProcessTable.create(inProcess);
    }

    public async deleteInProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._inProcessTable.destroy({where: {relative_path: {$in: toDelete}}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async createTaskExecution(worker: IPipelineWorker, taskDefinition: ITaskDefinition, startTaskInput: IStartTaskInput): Promise<ITaskExecution> {
        return this._taskExecutionTable.createTaskExecution(worker, taskDefinition, startTaskInput);
    };

    // -----------------------------------------------------------------------------------------------------------------

    protected static async bulkCreate(table: any, objArray: any[]) {
        if (!objArray || objArray.length === 0) {
            return;
        }

        debug(`bulk create ${objArray.length} items`);

        // Operate on a shallow copy since splice is going to be destructive.
        const toInsert = objArray.slice();

        while (toInsert.length > 0) {
            try {
                await table.bulkCreate(toInsert.splice(0, CreateChunkSize));
            } catch (err) {
                debug(err);
            }
        }
    }

    protected async bulkUpdate(objArray: any[]) {
        return this._connection.transaction(t => {
            return Promise.all(objArray.map(obj => {
                obj.save({transaction: t});
            }));
        });
    }

    // -----------------------------------------------------------------------------------------------------------------

    protected defineTables() {
        this._tileTable = this.defineTileTable(this._connection.Sequelize);

        this._toProcessTable = this.defineToProcessTable(this._connection.Sequelize);

        this._inProcessTable = this.defineInProcessTable(this._connection.Sequelize);

        this._taskExecutionTable = createTaskExecutionTable(this._connection, generatePipelineStageTaskExecutionTableName(this._tableBaseName));
        augmentTaskExecutionModel(this._taskExecutionTable);
    }

    private defineTileTable(DataTypes): any {
        return this._connection.define(this._tableBaseName, {
            relative_path: {
                primaryKey: true,
                unique: true,
                type: DataTypes.TEXT
            },
            index: {
                type: DataTypes.INTEGER
            },
            tile_name: {
                type: DataTypes.TEXT
            },
            prev_stage_status: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            this_stage_status: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            lat_x: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            lat_y: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            lat_z: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            step_x: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            step_y: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            step_z: {
                type: DataTypes.INTEGER,
                defaultValue: null
            },
            duration: {
                type: DataTypes.DOUBLE,
                defaultValue: 0
            },
            cpu_high: {
                type: DataTypes.DOUBLE,
                defaultValue: 0
            },
            memory_high: {
                type: DataTypes.DOUBLE,
                defaultValue: 0
            },
            user_data: {
                type: DataTypes.TEXT,
                defaultValue: null
            }
        }, {
            timestamps: true,
            createdAt: "created_at",
            updatedAt: "updated_at",
            paranoid: false,
            indexes: [{
                fields: ["prev_stage_status"]
            }, {
                fields: ["this_stage_status"]
            }]
        });
    }

    private defineToProcessTable(DataTypes): any {
        return this._connection.define(generatePipelineStageToProcessTableName(this._tableBaseName), {
            relative_path: {
                primaryKey: true,
                unique: true,
                type: DataTypes.TEXT
            }
        }, {
            timestamps: true,
            createdAt: "created_at",
            updatedAt: "updated_at",
            paranoid: false
        });
    }

    private defineInProcessTable(DataTypes): any {
        return this._connection.define(generatePipelineStageInProcessTableName(this._tableBaseName), {
            relative_path: {
                primaryKey: true,
                unique: true,
                type: DataTypes.TEXT
            },
            task_execution_id: {
                type: DataTypes.UUID,
                defaultValue: null
            },
            worker_id: {
                type: DataTypes.UUID,
                defaultValue: null
            },
            worker_task_execution_id: {
                type: DataTypes.UUID,
                defaultValue: null
            },
            worker_last_seen: {
                type: DataTypes.DATE,
                defaultValue: null
            }
        }, {
            timestamps: true,
            createdAt: "created_at",
            updatedAt: "updated_at",
            paranoid: false,
            indexes: [{
                fields: ["worker_id"]
            }]
        });
    }
}

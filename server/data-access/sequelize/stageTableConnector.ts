import {BuildOptions, CountOptions, Model, Sequelize, Op} from "sequelize";

const debug = require("debug")("pipeline:scheduler:stage-database-connector");

import {
    createTaskExecutionTable, ITaskExecution,
    TaskExecution,
    TaskExecutionStatic
} from "../../data-model/taskExecution";
import {TilePipelineStatus} from "../../data-model/TilePipelineStatus";

const CreateChunkSize = 100;
const UpdateChunkSize = 100;

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

export interface IPipelineTile {
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
    task_executions?: TaskExecution[];
}

export class PipelineTile extends Model implements IPipelineTile {
    relative_path: string;
    index: number;
    tile_name: string;
    prev_stage_status: TilePipelineStatus;
    this_stage_status: TilePipelineStatus;
    lat_x: number;
    lat_y: number;
    lat_z: number;
    step_x: number;
    step_y: number;
    step_z: number;
    task_executions: TaskExecution[];
    created_at: Date;
    updated_at: Date;
}

export type PipelineTileStatic = typeof Model & {
    new(values?: object, options?: BuildOptions): PipelineTile;
}

export class IInProcessTile  {
    public relative_path: string;
    public worker_id: string;
    public worker_last_seen: Date;
    public task_execution_id: string;
    public worker_task_execution_id: string;
}

export class InProcessTile extends Model implements IInProcessTile {
    public relative_path: string;
    public worker_id: string;
    public worker_last_seen: Date;
    public task_execution_id: string;
    public worker_task_execution_id: string;

    public readonly created_at: Date;
    public readonly updated_at: Date;
}

export type InProcessTileStatic = typeof Model & {
    new(values?: object, options?: BuildOptions): InProcessTile;
}


export interface IToProcessTile {
    relative_path: string;
    lat_x: number;
    lat_y: number;
    lat_z: number;
}

export class ToProcessTile extends Model implements IToProcessTile {
    public relative_path: string;
    public lat_x: number;
    public lat_y: number;
    public lat_z: number;

    public readonly created_at: Date;
    public readonly updated_at: Date;
}

export type ToProcessTileStatic = typeof Model & {
    new(values?: object, options?: BuildOptions): ToProcessTile;
}

export class StageTableConnector {

    protected _connection: Sequelize;
    protected _tableBaseName: string;

    private _tileTable: PipelineTileStatic = null;
    private _toProcessTable: ToProcessTileStatic = null;
    private _inProcessTable: InProcessTileStatic = null;
    private _taskExecutionTable: TaskExecutionStatic = null;

    public constructor(connection: Sequelize, id: string) {
        this._connection = connection;
        this._tableBaseName = id;
    }

    public async initialize(): Promise<void> {
        this.defineTables();

        // Do not perform mode/table updates from the API server, only the scheduler.
        // this._connection.sync();
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadTiles(options = null): Promise<PipelineTile[]> {
        // TODO this is leaking sequelize specifics to callers.
        if (options) {
            return this._tileTable.findAll(options);
        } else {
            return this._tileTable.findAll();
        }
    }

    public async loadTile(where: any): Promise<PipelineTile> {
        return this._tileTable.findOne({where});
    }

    public async loadTileById(id: string): Promise<IPipelineTile> {
        return this._tileTable.findOne({where: {relative_path: id}});
    }

    public async loadUnscheduled(): Promise<PipelineTile[]> {
        return this._tileTable.findAll({
            where: {
                prev_stage_status: TilePipelineStatus.Complete,
                this_stage_status: TilePipelineStatus.Incomplete
            }
        });
    }

    public async loadInProcess(): Promise<InProcessTile[]> {
        return this._inProcessTable.findAll();
    }

    public async loadToProcess(limit: number = null): Promise<ToProcessTile[]> {
        return this._toProcessTable.findAll({order: [["relative_path", "ASC"]], limit: limit});
    }

    public async dequeueForZPlanes(planes: number[]) {
        if (!planes || planes.length === 0) {
            return;
        }

        await this._toProcessTable.destroy({where: {lat_z: {[Op.in]: planes}}});

        await this._tileTable.update({this_stage_status: TilePipelineStatus.Incomplete}, {
            where: {
                [Op.and]: [{
                    lat_z: {[Op.in]: planes},
                    this_stage_status: TilePipelineStatus.Queued
                }]
            }
        });
    }

    public async loadTileThumbnailPath(x: number, y: number, z: number): Promise<PipelineTile> {
        return this._tileTable.findOne({where: {lat_x: x, lat_y: y, lat_z: z}});
    }

    public async loadTileStatusForPlane(zIndex: number): Promise<PipelineTile[]> {
        return this._tileTable.findAll({where: {lat_z: zIndex}});
    }


    // -----------------------------------------------------------------------------------------------------------------

    public async countTiles(options: CountOptions): Promise<number> {
        return options ? this._tileTable.count(options) : this._tileTable.count();
    }

    public async countInProcess(): Promise<number> {
        return this._inProcessTable.count();
    }

    public async countToProcess(): Promise<number> {
        return this._toProcessTable.count();
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertTiles(tiles: IPipelineTile[]) {
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

    public async updateTileStatus(relative_path: string, status: TilePipelineStatus) {
        const tile = await this.loadTile({relative_path: relative_path});

        if (tile) {
            await tile.update({this_stage_status: status});
        }
    }

    public async deleteTiles(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

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

    public async setTileStatus(tileIds: string[], status: TilePipelineStatus): Promise<PipelineTile[]> {
        const [affectedCount, affectedRows] = await this._tileTable.update({this_stage_status: status}, {
            where: {relative_path: {[Op.in]: tileIds}},
            returning: true
        });

        return affectedRows;
    }

    public async convertTileStatus(currentStatus: TilePipelineStatus, desiredStatus: TilePipelineStatus): Promise<PipelineTile[]> {
        const [affectedCount, affectedRows] = await this._tileTable.update({this_stage_status: desiredStatus}, {
            where: {this_stage_status: currentStatus},
            returning: true
        });

        return affectedRows;
    }

    public async taskExecutionsForTile(id: string): Promise<TaskExecution[]> {
        return this._taskExecutionTable.findAll({where: {tile_id: id}, order: ["created_at"]});
    }

    public async taskExecutionForId(id: string): Promise<TaskExecution> {
        return this._taskExecutionTable.findByPk(id);
    }

    public async loadTaskExecution(id: string): Promise<TaskExecution> {
        return this._taskExecutionTable.findByPk(id);
    }

    public async removeTaskExecution(id: string): Promise<boolean> {
        try {
            const taskExecution = await this.taskExecutionForId(id);

            if (taskExecution != null) {
                await taskExecution.destroy();
                return true;
            }
        } catch {
        }

        return false;
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertToProcess(toProcess: IToProcessTile[]) {
        return StageTableConnector.bulkCreate(this._toProcessTable, toProcess);
    }

    public async deleteToProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: {$in: toDelete}}});
    }

    public async deleteToProcessTile(toProcess: ToProcessTile) {
        if (!toProcess) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: toProcess.relative_path}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertInProcessTile(inProcess: IInProcessTile) {
        return this._inProcessTable.create(inProcess);
    }

    public async deleteInProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._inProcessTable.destroy({where: {relative_path: {$in: toDelete}}});
    }


    // -----------------------------------------------------------------------------------------------------------------

    public async createTaskExecution(task: ITaskExecution): Promise<TaskExecution> {
        return this._taskExecutionTable.create(task);
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
    }

    private defineTileTable(DataTypes): PipelineTileStatic {
        return <PipelineTileStatic>this._connection.define(this._tableBaseName, {
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
            user_data: {
                type: DataTypes.TEXT,
                defaultValue: null
            }
        }, {
            tableName: this._tableBaseName,
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

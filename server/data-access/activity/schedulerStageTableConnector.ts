import {Op, Sequelize} from "sequelize";

import {StageTableConnector} from "./stageTableConnector";

import {TilePipelineStatus} from "../../data-model/activity/TilePipelineStatus";

import {
    ITaskExecution,
    TaskExecution,
} from "../../data-model/activity/taskExecution";
import {
    IPipelineTile,
    PipelineTile
} from "../../data-model/activity/pipelineTile";
import {
    IInProcessTile
} from "../../data-model/activity/inProcessTile";
import {
    IToProcessTile
} from "../../data-model/activity/toProcessTile";
import {AdjacentTile, IAdjacentTile} from "../../data-model/activity/adjacentTileMap";


const debug = require("debug")("pipeline:scheduler:stage-database-connector");

const UpdateChunkSize = 100;

export class SchedulerStageTableConnector extends StageTableConnector{

    public constructor(connection: Sequelize, stage_id: string, prev_stage_id) {
        super(connection, stage_id, prev_stage_id);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadTilesWithAttributes(attributes: string[]): Promise<PipelineTile[]> {
        // TODO this is leaking system specifics to callers.
        return this._tileTable.findAll({where: {stage_id: this._stage_id}, attributes});
    }

    public async loadTileWithOptions(whereOptions = {}): Promise<PipelineTile> {
        whereOptions["stage_id"] = this._stage_id;

        return this._tileTable.findOne({where: whereOptions});
    }

    public async loadTileById(id: string): Promise<PipelineTile> {
        return this._tileTable.findOne({where: {stage_id: this._stage_id, relative_path: id}});
    }

    public async loadInputTile(whereOptions): Promise<PipelineTile> {
        whereOptions["stage_id"] = this._prev_stage_id;

        return this._tileTable.findOne({where: whereOptions});
    }

    public async loadUnscheduled(): Promise<PipelineTile[]> {
        return this._tileTable.findAll({
            where: {
                [Op.and]: {
                    [Op.and]: {
                        stage_id: this._prev_stage_id,
                        stage_status: TilePipelineStatus.Complete
                    },
                    [Op.and]: {
                        stage_id: this._stage_id,
                        stage_status: TilePipelineStatus.Incomplete
                    }
                }
            }
        });
    }

    public async loadToProcess(limit: number = null): Promise<IToProcessTile[]> {
        return this._toProcessTable.findAll({
            where: {stage_id: this._stage_id},
            order: [["relative_path", "ASC"]],
            limit: limit
        });
    }

    public async dequeueForZPlanes(planes: number[]) {
        if (!planes || planes.length === 0) {
            return;
        }

        await this._toProcessTable.destroy({where: {stage_id: this._stage_id, lat_z: {[Op.in]: planes}}});

        await this._tileTable.update({stage_status: TilePipelineStatus.Incomplete}, {
            where: {
                [Op.and]: [{
                    stage_id: this._stage_id,
                    lat_z: {[Op.in]: planes},
                    stage_status: TilePipelineStatus.Queued
                }]
            }
        });
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async countToProcess(): Promise<number> {
        return this._toProcessTable.count({where: {stage_id: this._stage_id}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertTiles(tiles: IPipelineTile[]) {
        tiles = tiles.map(t => {
            t.stage_id = this._stage_id;
            return t;
        });

        return SchedulerStageTableConnector.bulkCreate(this._tileTable, tiles);
    }

    public async updateTiles(objArray: IPipelineTile[]) {
        if (!objArray || objArray.length === 0) {
            return;
        }

        // debug(`bulk update ${objArray.length} items`);

        // Operate on a shallow copy since splice is going to be destructive.
        const toUpdate = objArray.map(t => {
            t.stage_id = this._stage_id;
            return t;
        }).slice();

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
        const tile = await this.loadTileWithOptions({relative_path: relative_path});

        if (tile) {
            await tile.update({stage_status: status});
        }
    }

    public async deleteTiles(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadAdjacentTile(id: string): Promise<AdjacentTile> {
        return this._adjacentTileModel.findOne({where: {relative_path: id, stage_id: this._stage_id}});
    }

    public async loadAdjacentTiles(): Promise<AdjacentTile[]> {
        return this._adjacentTileModel.findAll({where: {stage_id: this._stage_id}});
    }

    public async insertAdjacent(toProcess: IAdjacentTile[]) {
        toProcess = toProcess.map(t => {
            t.stage_id = this._stage_id;
            return t;
        });

        return SchedulerStageTableConnector.bulkCreate(this._adjacentTileModel, toProcess);
    }

    public async deleteAdjacent(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._adjacentTileModel.destroy({where: {stage_id: this._stage_id, relative_path: {[Op.in]: toDelete}}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadTaskExecution(id: string): Promise<TaskExecution> {
        return this._taskExecutionTable.findByPk(id);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async deleteToProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: {[Op.in]: toDelete}}});
    }

    public async deleteToProcessTile(toProcess: IToProcessTile) {
        if (!toProcess) {
            return;
        }

        return this._toProcessTable.destroy({where: {relative_path: toProcess.relative_path}});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertInProcessTile(inProcess: IInProcessTile) {
        return this._inProcessTable.create(inProcess);
    }


    // -----------------------------------------------------------------------------------------------------------------

    public async createTaskExecution(task: ITaskExecution): Promise<TaskExecution> {
        return this._taskExecutionTable.create(task);
    };

    protected async bulkUpdate(objArray: any[]) {
        return this._connection.transaction(t => {
            return Promise.all(objArray.map(obj => {
                obj.save({transaction: t});
            }));
        });
    }
}

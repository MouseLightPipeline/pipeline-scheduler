import {AdjacentTileStatic} from "../../data-model/activity/adjacentTileMap";

import * as path from "path";
import {Op, Sequelize} from "sequelize";

import {
    TaskExecution,
    TaskExecutionStatic
} from "../../data-model/activity/taskExecution";
import {
    IPipelineStageTileCounts,
    IPipelineTile,
    PipelineTile,
    PipelineTileStatic
} from "../../data-model/activity/pipelineTile";
import {
    InProcessTileStatic
} from "../../data-model/activity/inProcessTile";
import {
    IToProcessTile,
    ToProcessTileStatic
} from "../../data-model/activity/toProcessTile";

const debug = require("debug")("pipeline:scheduler:stage-database-connector");

const CreateChunkSize = 100;

export class StageTableConnector {
    protected _connection: Sequelize;
    protected _stage_id: string;
    protected _prev_stage_id: string;

    protected _tileTable: PipelineTileStatic = null;
    protected _toProcessTable: ToProcessTileStatic = null;
    protected _inProcessTable: InProcessTileStatic = null;
    protected _adjacentTileModel: AdjacentTileStatic = null;
    protected _taskExecutionTable: TaskExecutionStatic = null;

    public constructor(connection: Sequelize, stage_id: string, prev_stage_id) {
        this._connection = connection;
        this._stage_id = stage_id;
        this._prev_stage_id = prev_stage_id;
    }

    public async initialize(migrateIfNeeded: boolean = false): Promise<void> {
        this.loadModels(path.normalize(path.join(__dirname, "..", "..", "data-model/activity")));

        // Do not perform model/table updates from the API server, only the scheduler.
        if (migrateIfNeeded) {
            this._connection.sync();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async loadTiles(): Promise<IPipelineTile[]> {
        return this._tileTable.findAll({where: {stage_id: this._stage_id}});
    }

    public async loadInputTiles(whereOptions = {}): Promise<PipelineTile[]> {
        whereOptions["stage_id"] = this._prev_stage_id;

        return this._tileTable.findAll({where: whereOptions});
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async taskExecutionForId(id: string): Promise<TaskExecution> {
        return this._taskExecutionTable.findByPk(id);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async insertToProcess(toProcess: IToProcessTile[]) {
        return StageTableConnector.bulkCreate(this._toProcessTable, toProcess);
    }

    // -----------------------------------------------------------------------------------------------------------------

    public async deleteInProcess(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._inProcessTable.destroy({where: {relative_path: {[Op.in]: toDelete}}});
    }

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

    // -----------------------------------------------------------------------------------------------------------------

    private loadModels(modelLocation: string) {
        this._tileTable = this.loadModel(path.join(modelLocation, "pipelineTile"));
        this._toProcessTable = this.loadModel(path.join(modelLocation, "toProcessTile"));
        this._inProcessTable = this.loadModel(path.join(modelLocation, "inProcessTile"));
        this._adjacentTileModel = this.loadModel(path.join(modelLocation, "adjacentTileMap"));
        this._taskExecutionTable = this.loadModel(path.join(modelLocation, "taskExecution"));

        this._tileTable.hasMany(this._taskExecutionTable, {foreignKey: "tile_id", as: {singular: "taskExecution", plural: "taskExecutions"}});
        this._inProcessTable.belongsTo(this._tileTable, {foreignKey: "tile_id"});
        this._taskExecutionTable.belongsTo(this._tileTable, {foreignKey: "tile_id"});
        this._toProcessTable.belongsTo(this._tileTable, {foreignKey: "tile_id"});
    }

    private loadModel(moduleLocation: string) {
        let modelModule = require(moduleLocation);
        return modelModule.modelInit(this._connection);
    }
}

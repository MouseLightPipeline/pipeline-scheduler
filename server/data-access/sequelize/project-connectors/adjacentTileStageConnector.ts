import {Instance, Model} from "sequelize";

import {generatePipelineCustomTableName, IToProcessTileAttributes, StageTableConnector} from "./stageTableConnector";

export interface IAdjacentTileAttributes {
    relative_path: string,
    adjacent_relative_path: string;
    adjacent_tile_name: string;
}

export interface IAdjacentTile extends Instance<IAdjacentTileAttributes>, IAdjacentTileAttributes {
}

function generatePipelineStageAdjacentTileTableName(pipelineStageId: string) {
    return generatePipelineCustomTableName(pipelineStageId, "Adjacent");
}

export class AdjacentTileStageConnector extends StageTableConnector {
    private _adjacentTileModel: Model<IAdjacentTile, IAdjacentTileAttributes> = null;

    public async loadAdjacentTile(id: string): Promise<IAdjacentTile> {
        return this._adjacentTileModel.findOne({where: {relative_path: id}});
    }

    public async loadAdjacentTiles(): Promise<IAdjacentTile[]> {
        return this._adjacentTileModel.findAll();
    }

    public async insertAdjacent(toProcess: IToProcessTileAttributes[]) {
        return StageTableConnector.bulkCreate(this._adjacentTileModel, toProcess);
    }

    public async deleteAdjacent(toDelete: string[]) {
        if (!toDelete || toDelete.length === 0) {
            return;
        }

        return this._adjacentTileModel.destroy({where: {relative_path: {$in: toDelete}}});
    }

    protected defineTables() {
        super.defineTables();

        this._adjacentTileModel = this.defineAdjacentTileModel();
    }

    private defineAdjacentTileModel(): any {
        return this._connection.define(generatePipelineStageAdjacentTileTableName(this._tableBaseName), {
            relative_path: {
                primaryKey: true,
                unique: true,
                type: this._connection.Sequelize.TEXT
            },
            adjacent_relative_path: {
                type: this._connection.Sequelize.TEXT,
                defaultValue: null
            },
            adjacent_tile_name: {
                type: this._connection.Sequelize.TEXT,
                defaultValue: null
            }
        }, {
            timestamps: true,
            createdAt: "created_at",
            updatedAt: "updated_at",
            paranoid: false
        });
    }
}

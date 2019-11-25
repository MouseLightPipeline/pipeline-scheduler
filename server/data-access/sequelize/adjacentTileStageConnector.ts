import {BuildOptions, Model, DataTypes} from "sequelize";

import {generatePipelineCustomTableName, StageTableConnector, ToProcessTile} from "./stageTableConnector";

export interface IAdjacentTile {
    relative_path: string,
    adjacent_relative_path: string;
    adjacent_tile_name: string;
}

export class AdjacentTile extends Model implements IAdjacentTile {
    public relative_path: string;
    public adjacent_relative_path: string;
    public adjacent_tile_name: string;

    public readonly created_at: Date;
    public readonly updated_at: Date;
}

export type AdjacentTileStatic = typeof Model & {
    new (values?: object, options?: BuildOptions): AdjacentTile;
}

function generatePipelineStageAdjacentTileTableName(pipelineStageId: string) {
    return generatePipelineCustomTableName(pipelineStageId, "Adjacent");
}

export class AdjacentTileStageConnector extends StageTableConnector {
    private _adjacentTileModel: AdjacentTileStatic = null;


    public async loadAdjacentTile(id: string): Promise<AdjacentTile> {
        return this._adjacentTileModel.findOne({where: {relative_path: id}});
    }

    public async loadAdjacentTiles(): Promise<AdjacentTile[]> {
        return this._adjacentTileModel.findAll();
    }

    public async insertAdjacent(toProcess: IAdjacentTile[]) {
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

    private defineAdjacentTileModel(): AdjacentTileStatic {
        return <AdjacentTileStatic>this._connection.define(generatePipelineStageAdjacentTileTableName(this._tableBaseName), {
            relative_path: {
                primaryKey: true,
                unique: true,
                type: DataTypes.TEXT
            },
            adjacent_relative_path: {
                type: DataTypes.TEXT,
                defaultValue: null
            },
            adjacent_tile_name: {
                type: DataTypes.TEXT,
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

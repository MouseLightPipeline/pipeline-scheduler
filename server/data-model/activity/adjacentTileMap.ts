import {BuildOptions, DataTypes, Model, Sequelize} from "sequelize";

export interface IAdjacentTile {
    id: string;
    stage_id: string;
    tile_id: string;
    adjacent_tile_id: string;
    relative_path: string,
    adjacent_relative_path: string;
    adjacent_tile_name: string;
}

export class AdjacentTile extends Model implements IAdjacentTile {
    public id: string;
    public stage_id: string;
    public tile_id: string;
    public adjacent_tile_id: string;
    public relative_path: string;
    public adjacent_relative_path: string;
    public adjacent_tile_name: string;

    public readonly created_at: Date;
    public readonly updated_at: Date;
}

export type AdjacentTileStatic = typeof Model & {
    new(values?: object, options?: BuildOptions): AdjacentTile;
}

export function generatePipelineStageAdjacentTileTableName() {
    return "Adjacent";
}

export const modelInit = (sequelize: Sequelize): AdjacentTileStatic => {
    return <AdjacentTileStatic>sequelize.define(generatePipelineStageAdjacentTileTableName(), {
        id: {
            primaryKey: true,
            unique: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        stage_id: {
            type: DataTypes.UUID
        },
        relative_path: {
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
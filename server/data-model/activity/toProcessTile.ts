import {BuildOptions, DataTypes, Model, Sequelize} from "sequelize";

import {PipelineTile} from "./pipelineTile";

function generatePipelineStageToProcessTableName() {
    return "ToProcess";
}

export interface IToProcessTile {
    id?: string;
    stage_id: string;
    tile_id: string;
    relative_path: string;
    lat_x: number;
    lat_y: number;
    lat_z: number;
}

export class ToProcessTile extends Model implements IToProcessTile {
    public id: string;
    public stage_id: string;
    public tile_id: string;
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

export const modelInit = (sequelize: Sequelize): ToProcessTileStatic => {
    return <ToProcessTileStatic>sequelize.define(generatePipelineStageToProcessTableName(), {
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

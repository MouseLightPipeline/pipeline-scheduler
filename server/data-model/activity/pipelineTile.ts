import {BuildOptions, DataTypes, Model, Sequelize} from "sequelize";

import {TilePipelineStatus} from "./TilePipelineStatus";
import {TaskExecution} from "./taskExecution";

export interface IPipelineStageTileCounts {
    incomplete: number;
    queued: number;
    processing: number;
    complete: number;
    failed: number;
    canceled: number;
}

export interface IPipelineTile {
    id?: string;
    stage_id: string;
    relative_path?: string;
    index?: number;
    tile_name?: string;
    stage_status?: TilePipelineStatus;
    lat_x?: number;
    lat_y?: number;
    lat_z?: number;
    step_x?: number;
    step_y?: number;
    step_z?: number;
    task_executions?: TaskExecution[];
}

export class PipelineTile extends Model implements IPipelineTile {
    id: string;
    stage_id: string;
    relative_path: string;
    index: number;
    tile_name: string;
    stage_status: TilePipelineStatus;
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

function generatePipelineStageTileTableName() {
    return "Tile";
}

export type PipelineTileStatic = typeof Model & {
    new(values?: object, options?: BuildOptions): PipelineTile;
}

export const modelInit = (sequelize: Sequelize): PipelineTileStatic => {
    return <PipelineTileStatic>sequelize.define(generatePipelineStageTileTableName(), {
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
        index: {
            type: DataTypes.INTEGER
        },
        tile_name: {
            type: DataTypes.TEXT
        },
        stage_status: {
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
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        paranoid: false,
        indexes: [{
            fields: ["stage_status"]
        }]
    });
}

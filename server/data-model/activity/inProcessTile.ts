import {BuildOptions, DataTypes, Model, Sequelize} from "sequelize";

import {PipelineTile} from "./pipelineTile";

function generatePipelineStageInProcessTableName() {
    return "InProcess";
}

export interface IInProcessTile {
    id?: string;
    stage_id: string;
    tile_id: string;
    relative_path: string;
    worker_id: string;
    worker_last_seen: Date;
    task_execution_id: string;
    worker_task_execution_id: string;
}

export class InProcessTile extends Model implements IInProcessTile {
    public id: string;
    public stage_id: string;
    public tile_id: string;
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

export const modelInit = (sequelize: Sequelize): InProcessTileStatic => {
    return <InProcessTileStatic>sequelize.define(generatePipelineStageInProcessTableName(), {
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

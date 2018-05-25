import {IToProcessTile, IToProcessTileAttributes} from "../../data-access/sequelize/stageTableConnector";
import {Instance, Model} from "sequelize";
import {ITaskDefinition, ITaskDefinitionAttributes} from "./taskDefinition";

export const NO_BOUND: number = null;
export const NO_SAMPLE: number = -1;

export interface IProjectGridRegion {
    x_min: number;
    x_max: number;
    y_min: number;
    y_max: number;
    z_min: number;
    z_max: number;
}

export interface IProjectInput {
    id?: string;
    name?: string;
    description?: string;
    root_path?: string;
    is_processing?: boolean;
    sample_number?: number;
    region_bounds?: IProjectGridRegion;
}

export interface IProjectAttributes {
    id?: string;
    name?: string;
    description?: string;
    root_path?: string;
    log_root_path?: string;
    sample_number?: number;
    sample_x_min?: number;
    sample_x_max?: number;
    sample_y_min?: number;
    sample_y_max?: number;
    sample_z_min?: number;
    sample_z_max?: number;
    region_x_min?: number;
    region_x_max?: number;
    region_y_min?: number;
    region_y_max?: number;
    region_z_min?: number;
    region_z_max?: number;
    is_processing?: boolean;
    created_at?: Date;
    updated_at?: Date;
    deleted_at?: Date;
}

export interface IProject extends Instance<IProjectAttributes>, IProjectAttributes {}

export interface IProjectModel extends Model<IProject, IProjectAttributes> {
}

export const TableName = "Projects";

export function sequelizeImport(sequelize, DataTypes) {
    const Project = sequelize.define(TableName, {
        id: {
            primaryKey: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        name: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        description: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        root_path: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        log_root_path: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        sample_number: {
            type: DataTypes.INTEGER,
            defaultValue: 0
        },
        sample_x_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        sample_x_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        sample_y_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        sample_y_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        sample_z_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        sample_z_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_x_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_x_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_y_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_y_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_z_min: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        region_z_max: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        is_processing: {
            type: DataTypes.BOOLEAN,
            defaultValue: false
        }
    }, {
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: true
    });

    Project.associate = models => {
        Project.hasMany(models.PipelineStages, {foreignKey: "project_id"});
    };

    return Project;
}

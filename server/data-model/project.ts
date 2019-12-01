import {Sequelize, Model, DataTypes, HasManyGetAssociationsMixin, Transaction} from "sequelize";

import {PipelineStage} from "./pipelineStage";

export const NO_BOUND: number = null;
export const NO_SAMPLE: number = -1;

export enum ProjectInputSourceState {
    Unknown = 0,
    BadLocation = 1,
    Missing = 2,
    Dashboard = 3,
    Pipeline = 4,
    Disappeared = 5
}

export interface IProjectGridRegion {
    x_min: number;
    x_max: number;
    y_min: number;
    y_max: number;
    z_min: number;
    z_max: number;
}

// TODO pass user_parameters as ab object and stringify at database boundary rather than API
// TODO allow full x, y, z, skip plane configuration.
export interface IProjectInput {
    id?: string;
    name?: string;
    description?: string;
    root_path?: string;
    is_processing?: boolean;
    sample_number?: number;
    region_bounds?: IProjectGridRegion;
    user_parameters?: string;
    zPlaneSkipIndices?: number[];
    input_source_state?: ProjectInputSourceState;
    last_seen_input_source?: number;
    last_checked_input_source?: number;
}

export class Project extends Model {
    public id: string;
    public name: string;
    public description: string;
    public root_path: string;
    public log_root_path: string;
    public sample_number: number;
    public sample_x_min: number;
    public sample_x_max: number;
    public sample_y_min: number;
    public sample_y_max: number;
    public sample_z_min: number;
    public sample_z_max: number;
    public region_x_min: number;
    public region_x_max: number;
    public region_y_min: number;
    public region_y_max: number;
    public region_z_min: number;
    public region_z_max: number;
    public user_parameters: string;
    public plane_markers: string;
    public is_processing: boolean;
    public input_source_state: ProjectInputSourceState;
    public last_seen_input_source: Date;
    public last_checked_input_source: Date;

    public readonly created_at: Date;
    public readonly updated_at: Date;
    public readonly deleted_at: Date;

    public getStages!: HasManyGetAssociationsMixin<PipelineStage>;

    public get zPlaneSkipIndices(): number[] {
        return JSON.parse(this.plane_markers).z;
    }

    public set zPlaneSkipIndices(value: number[]) {
        this.setDataValue("plane_markers", JSON.stringify(Object.assign({}, JSON.parse(this.plane_markers), {z: value})));
    }
}

const TableName = "Projects";

export const modelInit = (sequelize: Sequelize) => {
    Project.init({
        id: {
            primaryKey: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        name: {
            type: DataTypes.TEXT
        },
        description: {
            type: DataTypes.TEXT
        },
        root_path: {
            type: DataTypes.TEXT
        },
        log_root_path: {
            type: DataTypes.TEXT
        },
        sample_number: {
            type: DataTypes.INTEGER
        },
        sample_x_min: {
            type: DataTypes.DOUBLE
        },
        sample_x_max: {
            type: DataTypes.DOUBLE
        },
        sample_y_min: {
            type: DataTypes.DOUBLE
        },
        sample_y_max: {
            type: DataTypes.DOUBLE
        },
        sample_z_min: {
            type: DataTypes.DOUBLE
        },
        sample_z_max: {
            type: DataTypes.DOUBLE
        },
        region_x_min: {
            type: DataTypes.DOUBLE
        },
        region_x_max: {
            type: DataTypes.DOUBLE
        },
        region_y_min: {
            type: DataTypes.DOUBLE
        },
        region_y_max: {
            type: DataTypes.DOUBLE
        },
        region_z_min: {
            type: DataTypes.DOUBLE
        },
        region_z_max: {
            type: DataTypes.DOUBLE
        },
        user_parameters: {
            type: DataTypes.TEXT,
        },
        plane_markers: {
            type: DataTypes.TEXT,
        },
        is_processing: {
            type: DataTypes.BOOLEAN
        },
        input_source_state: {
            type: DataTypes.INTEGER
        },
        last_seen_input_source: {
            type: DataTypes.DATE
        },
        last_checked_input_source: {
            type: DataTypes.DATE
        },
    }, {
        tableName: TableName,
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: true,
        sequelize
    });
};

export const modelAssociate = () => {
    Project.hasMany(PipelineStage, {foreignKey: "project_id", as: {singular: "stage", plural: "stages"}});
};

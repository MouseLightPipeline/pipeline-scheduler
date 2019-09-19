import {Instance, Model} from "sequelize";

export enum ProjectInputSourceState {
    Unknown = 0,
    BadLocation = 1,
    Missing = 2,
    Dashboard = 3,
    Pipeline = 4
}

export interface IProjectInput {
    id: string;
    input_source_state?: ProjectInputSourceState;
    last_seen_input_source?: Date | number;
    last_checked_input_source?: Date | number;
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
    user_parameters?: string;
    is_processing?: boolean;
    created_at?: Date;
    updated_at?: Date;
    deleted_at?: Date;
}

export interface IProject extends Instance<IProjectAttributes>, IProjectAttributes {
    planeMarkers: any;
    zPlaneSkipIndices: number[];
}

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
        user_parameters: {
            type: DataTypes.TEXT,
            defaultValue: "{}"
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
        paranoid: true,
        getterMethods: {
            planeMarkers: function() {
                return JSON.parse(this.plane_markers);
            },
            zPlaneSkipIndices: function() {
                return this.planeMarkers.z;
            }
        },
        setterMethods: {
            planeMarkers: function(value) {
                this.setDataValue("plane_markers", JSON.stringify(value));
            },
            zPlaneSkipIndices: function(value) {
                this.setDataValue("plane_markers", JSON.stringify(Object.assign({}, this.planeMarkers, {z: value})));
            }
        }
    });

    Project.associate = models => {
        Project.hasMany(models.PipelineStages, {foreignKey: "project_id"});
    };

    return Project;
}

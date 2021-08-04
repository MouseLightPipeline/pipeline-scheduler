import {Sequelize, Model, DataTypes, HasManyGetAssociationsMixin} from "sequelize";

import {TaskDefinition} from "./taskDefinition";

export interface ITaskRepositoryInput {
    id?: string;
    name: string;
    description?: string;
    location: string;
}

export class TaskRepository extends Model {
    public id: string;
    public name: string;
    public description: string;
    public location: string;
    public readonly created_at: Date;
    public readonly updated_at: Date;
    public readonly deleted_at: Date;

    public getTasks!: HasManyGetAssociationsMixin<TaskDefinition>;
}

const TableName = "TaskRepositories";

export const modelInit = (sequelize: Sequelize) => {
    TaskRepository.init({

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
        location: {
            type: DataTypes.TEXT,
            defaultValue: ""
        }
    }, {
        tableName: TableName,
        freezeTableName: true,
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: true,
        sequelize
    });
};

export const modelAssociate = () => {
    TaskRepository.hasMany(TaskDefinition, {foreignKey: "task_repository_id", as: "tasks"});
};

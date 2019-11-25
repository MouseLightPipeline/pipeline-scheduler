import * as path from "path";
import {Sequelize, Model, DataTypes, HasManyGetAssociationsMixin, BelongsToGetAssociationMixin, Op} from "sequelize"

import {TaskRepository} from "./taskRepository";
import {PipelineStage} from "./pipelineStage";

export enum TaskArgumentType {
    Literal = 0,
    Parameter = 1
}

export interface ITaskArgument {
    value: string;
    type: TaskArgumentType;
}

export interface ITaskDefinitionInput {
    id?: string;
    name?: string;
    description?: string;
    script?: string;
    interpreter?: string;
    script_args?: string;
    cluster_args?: string;
    expected_exit_code?: number;
    local_work_units?: number;
    cluster_work_units?: number;
    log_prefix?: string;
    task_repository_id?: string;
}

const TableName = "TaskDefinitions";

export class TaskDefinition extends Model {
    public id: string;
    public name: string;
    public description: string;
    public script: string;
    public interpreter: string;
    public script_args: string;
    public cluster_args: string;
    public expected_exit_code: number;
    public local_work_units: number;
    public cluster_work_units: number;
    public log_prefix: string;
    public task_repository_id: string;
    public readonly created_at: Date;
    public readonly updated_at: Date;
    public readonly deleted_at: Date;

    public getStages!: HasManyGetAssociationsMixin<PipelineStage>;
    public getTaskRepository!: BelongsToGetAssociationMixin<TaskRepository>;

    /**
     * Find all tasks in repos that have not been deleted.
     */
    public static async getAll(): Promise<TaskDefinition[]> {
        const repos = await TaskRepository.findAll();

        return TaskDefinition.findAll({where: {task_repository_id: {[Op.in]: repos.map(p => p.id)}}});
    }

    public async getFullScriptPath(resolveRelative: boolean): Promise<string> {
        let scriptPath = this.script;

        if (this.task_repository_id) {
            const repo = await TaskRepository.findByPk(this.task_repository_id);

            scriptPath = path.resolve(path.join(repo.location, scriptPath));
        } else {
            if (resolveRelative && !path.isAbsolute(scriptPath)) {
                scriptPath = path.join(process.cwd(), scriptPath);
            }
        }

        return scriptPath;
    };
}

export const modelInit = (sequelize: Sequelize) => {
    TaskDefinition.init({
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
        script: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        interpreter: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        script_args: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        cluster_args: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        expected_exit_code: {
            type: DataTypes.INTEGER,
            defaultValue: 0
        },
        local_work_units: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        cluster_work_units: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        log_prefix: {
            type: DataTypes.TEXT,
            defaultValue: ""
        }
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
    TaskDefinition.belongsTo(TaskRepository, {foreignKey: "task_repository_id"});
    TaskDefinition.hasMany(PipelineStage, {foreignKey: "task_id", as: {singular: "stage", plural: "stages"}});
};

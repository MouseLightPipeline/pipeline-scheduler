export enum PipelineStageMethod {
    DashboardProjectRefresh = 1,
    MapTile = 2,
    XAdjacentTileComparison = 3,
    YAdjacentTileComparison = 4,
    ZAdjacentTileComparison = 5
}

export interface IPipelineStage {
    id?: string;
    name?: string;
    description?: string;
    dst_path?: string;
    function_type?: PipelineStageMethod;
    depth?: number;
    is_processing?: boolean;
    project_id?: string;
    previous_stage_id?: string;
    task_id?: string;
    created_at?: Date;
    updated_at?: Date;
    deleted_at?: Date;
}

export const TableName = "PipelineStages";

export function sequelizeImport(sequelize, DataTypes) {
    const PipelineStage = sequelize.define(TableName, {
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
        dst_path: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        function_type: {
            type: DataTypes.INTEGER,
            defaultValue: 0
        },
        depth: {
            type: DataTypes.INTEGER,
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

    PipelineStage.associate = models => {
        PipelineStage.belongsTo(models.Projects, {foreignKey: "project_id"});
        PipelineStage.belongsTo(models.PipelineStages, {foreignKey: "previous_stage_id"});
        PipelineStage.belongsTo(models.TaskDefinitions, {foreignKey: "task_id"});
    };

    PipelineStage.createFromInput = async (stageInput: IPipelineStage): Promise<IPipelineStage> => {
        let previousDepth = 0;

        if (stageInput.previous_stage_id) {
            let previousStage = await PipelineStage.findById(stageInput.previous_stage_id);

            if (previousStage) {
                previousDepth = previousStage.depth;
            }
        }

        let pipelineStage = {
            name: stageInput.name,
            description: stageInput.description,
            project_id: stageInput.project_id,
            task_id: stageInput.task_id,
            previous_stage_id: stageInput.previous_stage_id,
            dst_path: stageInput.dst_path,
            is_processing: false,
            function_type: stageInput.function_type,
            depth: previousDepth + 1
        };

        return PipelineStage.create(pipelineStage);
    };

    PipelineStage.getForProject = (project_id: string): IPipelineStage[] => project_id ? PipelineStage.findAll({where: {project_id}}) : [];

    PipelineStage.getForTask = (task_id: string): IPipelineStage[] => task_id ? PipelineStage.findAll({where: {task_id}}) : [];

    return PipelineStage;
}

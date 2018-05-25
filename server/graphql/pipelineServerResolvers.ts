import {ITaskRepository} from "../data-model/sequelize/taskRepository";

import {IPipelineStagePerformance} from "../data-model/sequelize/pipelineStagePerformance";
import {
    IPipelineStageDeleteOutput, IPipelineStageMutationOutput,
    IProjectDeleteOutput,
    IProjectMutationOutput, ISimplePage, ITaskDefinitionDeleteOutput,
    ITaskDefinitionMutationOutput,
    ITaskRepositoryDeleteOutput,
    ITaskRepositoryMutationOutput, ITilePage, IWorkerMutationOutput, PipelineServerContext
} from "./pipelineServerContext";
import {ITaskDefinition, ITaskDefinitionAttributes} from "../data-model/sequelize/taskDefinition";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";
import {IProjectAttributes, IProjectInput} from "../data-model/sequelize/project";
import {IPipelineStage} from "../data-model/sequelize/pipelineStage";
import {CompletionResult, ITaskExecutionAttributes} from "../data-model/taskExecution";
import {IPipelineStageTileCounts, IPipelineTileAttributes} from "../data-access/sequelize/stageTableConnector";
import {TilePipelineStatus} from "../schedulers/basePipelineScheduler";

interface IIdOnlyArgument {
    id: string;
}

interface IUpdateWorkerArguments {
    worker: IPipelineWorker;
}

interface ITaskDefinitionIdArguments {
    task_definition_id: string;
}

interface ICreateProjectArguments {
    project: IProjectInput;

}

interface IUpdateProjectArguments {
    project: IProjectInput;
}

interface ICreatePipelineStageArguments {
    pipelineStage: IPipelineStage;
}

interface IUpdatePipelineStageArguments {
    pipelineStage: IPipelineStage;
}

interface IMutateRepositoryArguments {
    taskRepository: ITaskRepository;
}

interface IMutateTaskDefinitionArguments {
    taskDefinition: ITaskDefinitionAttributes;
}

interface IPipelinePlaneStatusArguments {
    project_id: string;
    plane: number;
}

interface IActiveWorkerArguments {
    id: string;
    shouldBeInSchedulerPool: boolean;
}

interface ITaskExecutionPageArguments {
    offset: number;
    limit: number;
    status: CompletionResult;
}

interface ITileStatusArguments {
    pipelineStageId: string;
    status: TilePipelineStatus;
    offset: number;
    limit: number;
}

interface ISetTileStatusArgs {
    pipelineStageId: string;
    tileIds: string[];
    status: TilePipelineStatus;
}

interface IConvertTileStatusArgs {
    pipelineStageId: string;
    currentStatus: TilePipelineStatus;
    desiredStatus: TilePipelineStatus;
}

let resolvers = {
    Query: {
        pipelineWorker(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IPipelineWorker> {
            return context.getPipelineWorker(args.id);
        },
        pipelineWorkers(_, __, context: PipelineServerContext): Promise<IPipelineWorker[]> {
            return context.getPipelineWorkers();
        },
        project(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IProjectAttributes> {
            return context.getProject(args.id);
        },
        projects(_, __, context: PipelineServerContext): Promise<IProjectAttributes[]> {
            return context.getProjects();
        },
        pipelineStage(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IPipelineStage> {
            return context.getPipelineStage(args.id);
        },
        pipelineStages(_, __, context: PipelineServerContext): Promise<IPipelineStage[]> {
            return context.getPipelineStages();
        },
        pipelineStagesForProject(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IPipelineStage[]> {
            return context.getPipelineStagesForProject(args.id);
        },
        taskDefinition(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<ITaskDefinitionAttributes> {
            return context.getTaskDefinition(args.id);
        },
        taskDefinitions(_, __, context: PipelineServerContext): Promise<ITaskDefinitionAttributes[]> {
            return context.getTaskDefinitions();
        },
        taskRepository(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<ITaskRepository> {
            return context.getTaskRepository(args.id);
        },
        taskRepositories(_, __, context: PipelineServerContext): Promise<ITaskRepository[]> {
            return context.getTaskRepositories();
        },
        taskExecution(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<ITaskExecutionAttributes> {
            return context.getTaskExecution(args.id);
        },
        taskExecutions(_, __, context: PipelineServerContext): Promise<ITaskExecutionAttributes[]> {
            return context.getTaskExecutions();
        },
        taskExecutionsPage(_, args: ITaskExecutionPageArguments, context: PipelineServerContext): Promise<ISimplePage<ITaskExecutionAttributes>> {
            return context.getTaskExecutionsPage(args.offset, args.limit, args.status);
        },
        pipelineStagePerformance(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IPipelineStagePerformance> {
            return context.getPipelineStagePerformance(args.id);
        },
        pipelineStagePerformances(_, __, context: PipelineServerContext): Promise<IPipelineStagePerformance[]> {
            return context.getPipelineStagePerformances();
        },
        projectPlaneTileStatus(_, args: IPipelinePlaneStatusArguments, context: PipelineServerContext): Promise<any> {
            return PipelineServerContext.getProjectPlaneTileStatus(args.project_id, args.plane);
        },
        tilesForStage(_, args: ITileStatusArguments, context: PipelineServerContext): Promise<ITilePage> {
            return context.tilesForStage(args.pipelineStageId, args.status, args.offset, args.limit);
        },
        scriptContents(_, args: ITaskDefinitionIdArguments, context: PipelineServerContext): Promise<string> {
            return context.getScriptContents(args.task_definition_id);
        },
        pipelineVolume(): string {
            return process.env.PIPELINE_VOLUME || "";
        }
    },
    Mutation: {
        createProject(_, args: ICreateProjectArguments, context: PipelineServerContext): Promise<IProjectMutationOutput> {
            return context.createProject(args.project);
        },
        updateProject(_, args: IUpdateProjectArguments, context: PipelineServerContext): Promise<IProjectMutationOutput> {
            return context.updateProject(args.project);
        },
        duplicateProject(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IProjectMutationOutput> {
            return context.duplicateProject(args.id);
        },
        deleteProject(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IProjectDeleteOutput> {
            return context.deleteProject(args.id);
        },
        createPipelineStage(_, args: ICreatePipelineStageArguments, context: PipelineServerContext): Promise<IPipelineStageMutationOutput> {
            return context.createPipelineStage(args.pipelineStage);
        },
        updatePipelineStage(_, args: IUpdatePipelineStageArguments, context: PipelineServerContext): Promise<IPipelineStageMutationOutput> {
            return context.updatePipelineStage(args.pipelineStage);
        },
        deletePipelineStage(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<IPipelineStageDeleteOutput> {
            return context.deletePipelineStage(args.id);
        },
        createTaskRepository(_, args: IMutateRepositoryArguments, context: PipelineServerContext): Promise<ITaskRepositoryMutationOutput> {
            return context.createTaskRepository(args.taskRepository);
        },
        updateTaskRepository(_, args: IMutateRepositoryArguments, context: PipelineServerContext): Promise<ITaskRepositoryMutationOutput> {
            return context.updateTaskRepository(args.taskRepository);
        },
        deleteTaskRepository(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<ITaskRepositoryDeleteOutput> {
            return context.deleteTaskRepository(args.id);
        },
        createTaskDefinition(_, args: IMutateTaskDefinitionArguments, context: PipelineServerContext): Promise<ITaskDefinitionMutationOutput> {
            return context.createTaskDefinition(args.taskDefinition);
        },
        updateTaskDefinition(_, args: IMutateTaskDefinitionArguments, context: PipelineServerContext): Promise<ITaskDefinitionMutationOutput> {
            return context.updateTaskDefinition(args.taskDefinition);
        },
        deleteTaskDefinition(_, args: IIdOnlyArgument, context: PipelineServerContext): Promise<ITaskDefinitionDeleteOutput> {
            return context.deleteTaskDefinition(args.id);
        },
        updateWorker(_, args: IUpdateWorkerArguments, context: PipelineServerContext): Promise<IWorkerMutationOutput> {
            return context.updateWorker(args.worker);
        },
        setWorkerAvailability(_, args: IActiveWorkerArguments, context: PipelineServerContext) {
            return context.setWorkerAvailability(args.id, args.shouldBeInSchedulerPool);
        },
        setTileStatus(_, args: ISetTileStatusArgs, context: PipelineServerContext): Promise<IPipelineTileAttributes[]> {
            return context.setTileStatus(args.pipelineStageId, args.tileIds, args.status);
        },
        convertTileStatus(_, args: IConvertTileStatusArgs, context: PipelineServerContext): Promise<IPipelineTileAttributes[]> {
            return context.convertTileStatus(args.pipelineStageId, args.currentStatus, args.desiredStatus);
        }
    },
    Project: {
        stages(project, _, context: PipelineServerContext): any {
            return context.getPipelineStagesForProject(project.id);
        },
        dashboard_json_status(project: IProjectAttributes, _, context: PipelineServerContext): boolean {
            return PipelineServerContext.getDashboardJsonStatusForProject(project);
        }
    },
    PipelineStage: {
        performance(stage, _, context: PipelineServerContext): any {
            return context.getForStage(stage.id);
        },
        task(stage, _, context: PipelineServerContext): any {
            return context.getTaskDefinition(stage.task_id);
        },
        project(stage, _, context: PipelineServerContext): any {
            return context.getProject(stage.project_id);
        },
        previous_stage(stage, _, context: PipelineServerContext): Promise<IPipelineStage> {
            return context.getPipelineStage(stage.previous_stage_id);
        },
        child_stages(stage, _, context: PipelineServerContext): Promise<IPipelineStage[]> {
            return context.getPipelineStageChildren(stage.id);
        },
        tile_status(stage, _, context: PipelineServerContext): Promise<IPipelineStageTileCounts> {
            return context.getPipelineStageTileStatus(stage.id);
        }
    },
    TaskRepository: {
        task_definitions(repository: ITaskRepository, _, context: PipelineServerContext): any {
            return context.getRepositoryTasks(repository.id);
        }
    },
    TaskDefinition: {
        task_repository(taskDefinition: ITaskDefinition, _, context: PipelineServerContext): any {
            if (taskDefinition.task_repository_id) {
                return context.getTaskRepository(taskDefinition.task_repository_id);
            }

            return null;
        },
        pipeline_stages(taskDefinition: ITaskDefinition, _, context: PipelineServerContext): any {
            return context.getPipelineStagesForTaskDefinition(taskDefinition.id);
        },
        script_status(taskDefinition: ITaskDefinition, _, context: PipelineServerContext): any {
            return PipelineServerContext.getScriptStatusForTaskDefinition(taskDefinition);
        }
    },
    TaskExecution: {
        task_definition(taskExecution, _, context: PipelineServerContext) {
            return context.getTaskDefinition(taskExecution.task_definition_id);
        }
    }
};

export default resolvers;

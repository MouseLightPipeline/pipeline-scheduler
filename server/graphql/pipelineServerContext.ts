import * as path from "path";
import * as fs from "fs";

import {IPipelineStagePerformance} from "../data-model/sequelize/pipelineStagePerformance";
import {SchedulerHub} from "../schedulers/schedulerHub";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";
import {ITaskDefinition, ITaskDefinitionAttributes} from "../data-model/sequelize/taskDefinition";
import {ITaskRepository} from "../data-model/sequelize/taskRepository";
import {IPipelineWorker} from "../data-model/sequelize/pipelineWorker";
import {IProject, IProjectAttributes, IProjectInput, NO_BOUND, NO_SAMPLE} from "../data-model/sequelize/project";
import {IPipelineStage} from "../data-model/sequelize/pipelineStage";
import {PipelineWorkerClient} from "./client/pipelineWorkerClient";
import {CompletionResult, ITaskExecutionAttributes} from "../data-model/taskExecution";
import {IPipelineStageTileCounts, IPipelineTileAttributes} from "../data-access/sequelize/stageTableConnector";
import {TilePipelineStatus} from "../schedulers/basePipelineScheduler";
import {connectorForStage} from "../data-access/sequelize/projectDatabaseConnector";

export interface IWorkerMutationOutput {
    worker: IPipelineWorker;
    error: string;
}

export interface IProjectMutationOutput {
    project: IProject;
    error: string;
}

export interface IProjectDeleteOutput {
    id: string;
    error: string;
}

export interface IPipelineStageMutationOutput {
    pipelineStage: IPipelineStage;
    error: string;
}

export interface IPipelineStageDeleteOutput {
    id: string;
    error: string;
}

export interface ITaskRepositoryMutationOutput {
    taskRepository: ITaskRepository;
    error: string;
}

export interface ITaskRepositoryDeleteOutput {
    id: string;
    error: string;
}

export interface ITaskDefinitionMutationOutput {
    taskDefinition: ITaskDefinitionAttributes;
    error: string;
}

export interface ITaskDefinitionDeleteOutput {
    id: string;
    error: string;
}

export interface ISimplePage<T> {
    offset: number;
    limit: number;
    totalCount: number;
    hasNextPage: boolean;
    items: T[]
}

export type ITilePage = ISimplePage<IPipelineTileAttributes>;

export class PipelineServerContext {
    private _persistentStorageManager: PersistentStorageManager = PersistentStorageManager.Instance();

    public getPipelineWorker(id: string): Promise<IPipelineWorker> {
        return this._persistentStorageManager.PipelineWorkers.findById(id);
    }

    public getPipelineWorkers(): Promise<IPipelineWorker[]> {
        return this._persistentStorageManager.PipelineWorkers.findAll({});
    }

    public async updateWorker(workerInput: IPipelineWorker): Promise<IWorkerMutationOutput> {
        try {
            let row = await this._persistentStorageManager.PipelineWorkers.findById(workerInput.id);

            let output = await PipelineWorkerClient.Instance().updateWorker(Object.assign({}, {
                id: workerInput.id,
                work_unit_capacity: workerInput.work_unit_capacity,
                is_cluster_proxy: workerInput.is_cluster_proxy
            }, {
                name: row.name,
                address: row.address,
                port: row.port
            }));

            if (output.error !== null) {
                return output;
            }

            row = await row.update({
                work_unit_capacity: output.worker.work_capacity,
                is_cluster_proxy: output.worker.is_cluster_proxy
            });

            console.log(row.dataValues);

            return {worker: row, error: ""};
        } catch (err) {
            return {worker: null, error: err.message}
        }
    }

    public async setWorkerAvailability(id: string, shouldBeInSchedulerPool: boolean): Promise<IPipelineWorker> {
        const worker = await this._persistentStorageManager.PipelineWorkers.findById(id);

        await worker.update({is_in_scheduler_pool: shouldBeInSchedulerPool});

        return this._persistentStorageManager.PipelineWorkers.findById(id);
    }

    public static getDashboardJsonStatusForProject(project: IProjectAttributes): boolean {
        return fs.existsSync(path.join(project.root_path, "dashboard.json"));
    }

    public async getProject(id: string): Promise<IProject> {
        return this._persistentStorageManager.Projects.findById(id);
    }

    public async getProjects(): Promise<IProject[]> {
        return this._persistentStorageManager.Projects.findAll({order: [["sample_number", "ASC"], ["name", "ASC"]]});
    }

    public async createProject(projectInput: IProjectInput): Promise<IProjectMutationOutput> {
        try {
            const region = projectInput.region_bounds || {
                x_min: NO_BOUND,
                x_max: NO_BOUND,
                y_min: NO_BOUND,
                y_max: NO_BOUND,
                z_min: NO_BOUND,
                z_max: NO_BOUND
            };

            const project = {
                name: projectInput.name || "",
                description: projectInput.description || "",
                root_path: projectInput.root_path || "",
                sample_number: projectInput.sample_number || NO_SAMPLE,
                sample_x_min: NO_BOUND,
                sample_x_max: NO_BOUND,
                sample_y_min: NO_BOUND,
                sample_y_max: NO_BOUND,
                sample_z_min: NO_BOUND,
                sample_z_max: NO_BOUND,
                region_x_min: region.x_min,
                region_x_max: region.x_max,
                region_y_min: region.y_min,
                region_y_max: region.y_max,
                region_z_min: region.z_min,
                region_z_max: region.z_max,
                is_processing: false
            };

            const result = await this._persistentStorageManager.Projects.create(project);

            return {project: result, error: ""};
        } catch (err) {
            return {project: null, error: err.message}
        }
    }

    public async updateProject(projectInput: IProjectInput): Promise<IProjectMutationOutput> {
        try {
            let row = await this._persistentStorageManager.Projects.findById(projectInput.id);

            let project = projectInput.region_bounds ?
                Object.assign(projectInput, {
                    region_x_min: projectInput.region_bounds.x_min,
                    region_x_max: projectInput.region_bounds.x_max,
                    region_y_min: projectInput.region_bounds.y_min,
                    region_y_max: projectInput.region_bounds.y_max,
                    region_z_min: projectInput.region_bounds.z_min,
                    region_z_max: projectInput.region_bounds.z_max
                }) : projectInput;

            await row.update(project);

            row = await this._persistentStorageManager.Projects.findById(project.id);

            return {project: row, error: ""};
        } catch (err) {
            return {project: null, error: err.message}
        }
    }

    public async duplicateProject(id: string): Promise<IProjectMutationOutput> {
        try {
            const input = (await this._persistentStorageManager.Projects.findById(id)).toJSON();

            input.id = undefined;
            input.name += " copy";
            input.root_path += "copy";
            input.created_at = new Date();
            input.updated_at = input.created_at;

            const project = await this._persistentStorageManager.Projects.create(input);

            const inputStages = await this._persistentStorageManager.PipelineStages.findAll({
                where: {project_id: id},
                order: [["depth", "ASC"]]
            });

            const duplicateMap = new Map<string, IPipelineStage>();

            const dupeStage = async (inputStage): Promise<IPipelineStage> => {
                const stageData: IPipelineStage = inputStage.toJSON();

                stageData.project_id = project.id;
                if (inputStage.previous_stage_id !== null) {
                    stageData.previous_stage_id = duplicateMap.get(inputStage.previous_stage_id).id;
                } else {
                    stageData.previous_stage_id = null;
                }
                stageData.dst_path += "copy";

                const stage = await this._persistentStorageManager.PipelineStages.createFromInput(stageData);

                duplicateMap.set(inputStage.id, stage);

                return stage;
            };

            await inputStages.reduce(async (promise, stage) => {
                await promise;
                return dupeStage(stage);
            }, Promise.resolve());

            return {project, error: ""};
        } catch (err) {
            console.log(err);
            return {project: null, error: err.message}
        }
    }

    public async deleteProject(id: string): Promise<IProjectDeleteOutput> {
        try {
            const affectedRowCount = await this._persistentStorageManager.Projects.destroy({where: {id}});

            if (affectedRowCount > 0) {
                return {id, error: ""};
            } else {
                return {id: null, error: "Could not delete repository (no error message)"};
            }
        } catch (err) {
            return {id: null, error: err.message}
        }
    }

    public getPipelineStage(id: string): Promise<IPipelineStage> {
        return this._persistentStorageManager.PipelineStages.findById(id);
    }

    public getPipelineStages(): Promise<IPipelineStage[]> {
        return this._persistentStorageManager.PipelineStages.findAll({});
    }

    public getPipelineStagesForProject(id: string): Promise<IPipelineStage[]> {
        return this._persistentStorageManager.PipelineStages.getForProject(id);
    }

    public getPipelineStagesForTaskDefinition(id: string): Promise<IPipelineStage[]> {
        return this._persistentStorageManager.PipelineStages.getForTask(id);
    }

    public getPipelineStageChildren(id: string): Promise<IPipelineStage[]> {
        return this._persistentStorageManager.PipelineStages.findAll({where: {previous_stage_id: id}});
    }

    public async createPipelineStage(pipelineStage: IPipelineStage): Promise<IPipelineStageMutationOutput> {
        try {
            const result: IPipelineStage = await this._persistentStorageManager.PipelineStages.createFromInput(pipelineStage);

            return {pipelineStage: result, error: ""};
        } catch (err) {
            return {pipelineStage: null, error: err.message};
        }
    }

    public async updatePipelineStage(pipelineStage: IPipelineStage): Promise<IPipelineStageMutationOutput> {
        try {
            let row = await this._persistentStorageManager.PipelineStages.findById(pipelineStage.id);

            if (row.previous_stage_id === null) {
                pipelineStage.depth = 1;
            } else {
                const stage = await this._persistentStorageManager.PipelineStages.findById(row.previous_stage_id);
                pipelineStage.depth = stage.depth + 1;
            }

            await row.update(pipelineStage);

            row = await this._persistentStorageManager.PipelineStages.findById(pipelineStage.id);

            return {pipelineStage: row, error: ""};
        } catch (err) {
            return {pipelineStage: null, error: err.message}
        }
    }

    public async deletePipelineStage(id: string): Promise<IPipelineStageDeleteOutput> {
        try {
            const affectedRowCount = await this._persistentStorageManager.PipelineStages.destroy({where: {id}});

            if (affectedRowCount > 0) {
                return {id, error: ""};
            } else {
                return {id: null, error: "Could not delete repository (no error message)"};
            }
        } catch (err) {
            return {id: null, error: err.message}
        }
    }

    public getTaskRepository(id: string): Promise<ITaskRepository> {
        return this._persistentStorageManager.TaskRepositories.findById(id);
    }

    public getTaskRepositories(): Promise<ITaskRepository[]> {
        return this._persistentStorageManager.TaskRepositories.findAll({});
    }

    public async getRepositoryTasks(id: string): Promise<ITaskDefinitionAttributes[]> {
        return this._persistentStorageManager.TaskDefinitions.findAll({where: {task_repository_id: id}});
    }

    public async createTaskRepository(taskRepository: ITaskRepository): Promise<ITaskRepositoryMutationOutput> {
        try {
            const result = await this._persistentStorageManager.TaskRepositories.create(taskRepository);

            return {taskRepository: result, error: null};

        } catch (err) {
            return {taskRepository: null, error: err.message}
        }
    }

    public async updateTaskRepository(taskRepository: ITaskRepository): Promise<ITaskRepositoryMutationOutput> {
        try {
            let row = await this._persistentStorageManager.TaskRepositories.findById(taskRepository.id);

            await row.update(taskRepository);

            row = await this._persistentStorageManager.TaskRepositories.findById(taskRepository.id);

            return {taskRepository: row, error: null};
        } catch (err) {
            return {taskRepository: null, error: err.message}
        }
    }

    public async deleteTaskRepository(id: string): Promise<ITaskRepositoryDeleteOutput> {
        try {
            const affectedRowCount = await this._persistentStorageManager.TaskRepositories.destroy({where: {id}});

            if (affectedRowCount > 0) {
                return {id, error: null};
            } else {
                return {id: null, error: "Could not delete repository (no error message)"};
            }
        } catch (err) {
            return {id: null, error: err.message}
        }
    }

    public async getTaskDefinition(id: string): Promise<ITaskDefinition> {
        return await this._persistentStorageManager.TaskDefinitions.findById(id);
    }

    public async getTaskDefinitions(): Promise<ITaskDefinition[]> {
        return await this._persistentStorageManager.TaskDefinitions.findAll({});
    }

    public async createTaskDefinition(taskDefinition: ITaskDefinitionAttributes): Promise<ITaskDefinitionMutationOutput> {
        try {
            const result = await this._persistentStorageManager.TaskDefinitions.create(taskDefinition);

            return {taskDefinition: result, error: null};
        } catch (err) {
            return {taskDefinition: null, error: err.message}
        }
    }

    public async updateTaskDefinition(taskDefinition: ITaskDefinitionAttributes): Promise<ITaskDefinitionMutationOutput> {
        try {
            let row = await this._persistentStorageManager.TaskDefinitions.findById(taskDefinition.id);

            await row.update(taskDefinition);

            row = await this._persistentStorageManager.TaskDefinitions.findById(taskDefinition.id);

            return {taskDefinition: row, error: null};
        } catch (err) {
            return {taskDefinition: null, error: err.message}
        }
    }

    public async deleteTaskDefinition(id: string): Promise<ITaskDefinitionDeleteOutput> {
        try {
            const affectedRowCount = await this._persistentStorageManager.TaskDefinitions.destroy({where: {id}});

            if (affectedRowCount > 0) {
                return {id, error: null};
            } else {
                return {id: null, error: "Could not delete task definition (no error message)"};
            }
        } catch (err) {
            return {id: null, error: err.message}
        }
    }

    public static async getScriptStatusForTaskDefinition(taskDefinition: ITaskDefinition): Promise<boolean> {
        const scriptPath = await taskDefinition.getFullScriptPath(true);

        return fs.existsSync(scriptPath);
    }

    public async getScriptContents(taskDefinitionId: string): Promise<string> {
        const taskDefinition = await this.getTaskDefinition(taskDefinitionId);

        console.log(taskDefinition.user_arguments);

        if (taskDefinition) {
            const haveScript = await PipelineServerContext.getScriptStatusForTaskDefinition(taskDefinition);

            if (haveScript) {
                const scriptPath = await taskDefinition.getFullScriptPath(true);

                return fs.readFileSync(scriptPath, "utf8");
            }
        }

        return null;
    }

    public getTaskExecution(id: string): Promise<ITaskExecutionAttributes> {
        // return this._persistentStorageManager.TaskExecutions.findById(id);
        return null;
    }

    public getTaskExecutions(): Promise<ITaskExecutionAttributes[]> {
        // return this._persistentStorageManager.TaskExecutions.findAll({});
        return Promise.resolve([]);
    }

    public async getTaskExecutionsPage(reqOffset: number, reqLimit: number, completionCode: CompletionResult): Promise<ISimplePage<ITaskExecutionAttributes>> {

        let offset = 0;
        let limit = 10;

        if (reqOffset !== null && reqOffset !== undefined) {
            offset = reqOffset;
        }

        if (reqLimit !== null && reqLimit !== undefined) {
            limit = reqLimit;
        }
        /*
        const count = await this._persistentStorageManager.TaskExecutions.count();

        if (offset > count) {
            return {
                offset,
                limit,
                totalCount: count,
                hasNextPage: false,
                items: []
            };
        }

        const nodes: ITaskExecutionAttributes[] = await this._persistentStorageManager.TaskExecutions.getPage(offset, limit, completionCode);

        return {
            offset,
            limit,
            totalCount: count,
            hasNextPage: offset + limit < count,
            items: nodes
        };
        */
        return {
            offset,
            limit,
            totalCount: 0,
            hasNextPage: false,
            items: []
        };
    }

    public getPipelineStagePerformance(id: string): Promise<IPipelineStagePerformance> {
        return this._persistentStorageManager.PipelineStagePerformances.findById(id);
    }

    public getPipelineStagePerformances(): Promise<IPipelineStagePerformance[]> {
        return this._persistentStorageManager.PipelineStagePerformances.findAll({});
    }

    public async getForStage(pipeline_stage_id: string): Promise<IPipelineStagePerformance> {
        return this._persistentStorageManager.PipelineStagePerformances.findOne({where: {pipeline_stage_id}});
    }

    public static getProjectPlaneTileStatus(project_id: string, plane: number): Promise<any> {
        return SchedulerHub.Instance.loadTileStatusForPlane(project_id, plane);
    }

    public async tilesForStage(pipelineStageId: string, status: TilePipelineStatus, reqOffset: number, reqLimit: number): Promise<ITilePage> {
        const pipelineStage = await this._persistentStorageManager.PipelineStages.findById(pipelineStageId);

        if (!pipelineStage) {
            return {
                offset: reqOffset,
                limit: reqLimit,
                totalCount: 0,
                hasNextPage: false,
                items: []
            };
        }

        let offset = 0;
        let limit = 10;

        if (reqOffset !== null && reqOffset !== undefined) {
            offset = reqOffset;
        }

        if (reqLimit !== null && reqLimit !== undefined) {
            limit = reqLimit;
        }

        // TODO Use findAndCount
        const stageConnector = await connectorForStage(pipelineStage);

        const totalCount = await stageConnector.countTiles({
            where: {
                prev_stage_status: TilePipelineStatus.Complete,
                this_stage_status: status
            }
        });

        const items = await stageConnector.loadTiles({
            where: {
                prev_stage_status: TilePipelineStatus.Complete,
                this_stage_status: status
            },
            offset,
            limit
        });

        return {
            offset: offset,
            limit: limit,
            totalCount,
            hasNextPage: offset + limit < totalCount,
            items
        }
    }

    public async getPipelineStageTileStatus(pipelineStageId: string): Promise<IPipelineStageTileCounts> {
        try {
            const pipelineStage = await this._persistentStorageManager.PipelineStages.findById(pipelineStageId);

            if (!pipelineStage) {
                return null;
            }

            const stageConnector = await connectorForStage(pipelineStage);

            return stageConnector.getTileCounts();
        } catch (err) {
            return PipelineStageStatusUnavailable;
        }
    }

    /***
     * Set specific tiles (by id) to a specific status.
     *
     * @param {string} pipelineStageId
     * @param {string[]} tileIds
     * @param {TilePipelineStatus} status
     * @returns {Promise<IPipelineTileAttributes[]>}
     */
    public async setTileStatus(pipelineStageId: string, tileIds: string[], status: TilePipelineStatus): Promise<IPipelineTileAttributes[]> {
        const pipelineStage = await this._persistentStorageManager.PipelineStages.findById(pipelineStageId);

        if (!pipelineStage) {
            return null;
        }

        const stageConnector = await connectorForStage(pipelineStage);

        return stageConnector.setTileStatus(tileIds, status);
    }

    /***
     * Set all tiles with one status to another status.
     *
     * @param {string} pipelineStageId
     * @param {TilePipelineStatus} currentStatus
     * @param {TilePipelineStatus} desiredStatus
     * @returns {Promise<IPipelineTileAttributes[]>}
     */
    public async convertTileStatus(pipelineStageId: string, currentStatus: TilePipelineStatus, desiredStatus: TilePipelineStatus): Promise<IPipelineTileAttributes[]> {
        const pipelineStage = await this._persistentStorageManager.PipelineStages.findById(pipelineStageId);

        if (!pipelineStage) {
            return null;
        }

        const stageConnector = await connectorForStage(pipelineStage);

        return stageConnector.convertTileStatus(currentStatus, desiredStatus);
    }
}

const PipelineStageStatusUnavailable: IPipelineStageTileCounts = {
    incomplete: 0,
    queued: 0,
    processing: 0,
    complete: 0,
    failed: 0,
    canceled: 0
};

import {startAdjacentPipelineStageWorker} from "./pipelineAdjacentSchedulerChildProcess";

const path = require("path");
const child_process = require("child_process");

const debug = require("debug")("pipeline:scheduler:scheduler-hub");

import {startTileStatusFileWorker} from "./projectPipelineSchedulerChildProcess";
import {startMapPipelineStageWorker} from "./pipelineMapSchedulerChildProcess";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";
import {IProjectAttributes} from "../data-model/sequelize/project";
import {IPipelineStage, PipelineStageMethod} from "../data-model/sequelize/pipelineStage";
import {ITaskExecutionAttributes, IWorkerTaskExecutionAttributes} from "../data-model/taskExecution";

export interface ISchedulerInterface {
    IsExitRequested: boolean;
    IsProcessingRequested: boolean;

    onTaskExecutionComplete(executionInfo: ITaskExecutionAttributes): Promise<void>;
}

export class SchedulerHub {
    private static _instance: SchedulerHub = null;

    public static async Run(useChildProcessWorkers: boolean = false): Promise<SchedulerHub> {
        if (!this._instance) {
            this._instance = new SchedulerHub(useChildProcessWorkers);

            await this._instance.start();
        }

        return this._instance;
    }

    public static get Instance(): SchedulerHub {
        return this._instance;
    }

    public async onTaskExecutionComplete(taskExecution: IWorkerTaskExecutionAttributes) {
        try {
            const worker = this._pipelineStageWorkers.get(taskExecution.pipeline_stage_id);

            if (worker) {
                await worker.onTaskExecutionComplete(taskExecution);
            }
        } catch (err) {
            debug(err);
        }
    }

    private readonly _useChildProcessWorkers: boolean;

    private _pipelineStageWorkers = new Map<string, ISchedulerInterface>();

    private constructor(useChildProcessWorkers: boolean = false) {
        this._useChildProcessWorkers = useChildProcessWorkers;
    }

    private async start() {
        await this.manageAllSchedulers();
    }

    private _pidCount = 0;

    private async manageAllSchedulers() {
        try {
            if (PersistentStorageManager.Instance() && PersistentStorageManager.Instance().IsConnected) {
                const projectsManager = PersistentStorageManager.Instance().Projects;

                const projects: IProjectAttributes[] = await projectsManager.findAll({});

                const pipelineStagesManager = PersistentStorageManager.Instance().PipelineStages;

                // Turn stage workers off for projects that have been turned off.
                const pausedProjects = projects.filter(item => (item.is_processing || 0) === 0);

                await Promise.all(pausedProjects.map(project => this.pauseStagesForProject(pipelineStagesManager, project)));

                // Turn stage workers on (but not necessarily processing) for projects that are active for stats.
                // Individual stage processing is maintained in the next step.
                const resumedProjects = projects.filter(item => item.is_processing === true);

                await Promise.all(resumedProjects.map(project => this.resumeStagesForProject(pipelineStagesManager, project)));

                // Refresh processing state for active workers.
                await this.manageStageProcessingFlag();
            }

            this._pidCount++;

            if (this._pidCount >= 6) {
                debug(`process id: ${process.pid}`);
                this._pidCount = 0;
            }
        } catch (err) {
            debug(`exception (manageAllWorkers): ${err}`);
        }

        setTimeout(() => this.manageAllSchedulers(), 10 * 1000);
    }

    private async resumeStagesForProject(pipelineStagesManager: any, project: IProjectAttributes) {
        const stages: IPipelineStage[] = await pipelineStagesManager.getForProject(project.id);

        await this.addWorker(project, startTileStatusFileWorker, "/projectPipelineSchedulerChildProcess.js");

        await Promise.all(stages.map(stage => this.resumeStage(stage)));
    }

    private async resumeStage(stage: IPipelineStage): Promise<boolean> {
        if (stage.function_type !== PipelineStageMethod.MapTile) {
            return this.addWorker(stage, startAdjacentPipelineStageWorker, "/pipelineAdjacentSchedulerChildProcess.js");
        } else {
            return this.addWorker(stage, startMapPipelineStageWorker, "/pipelineMapSchedulerChildProcess.js");
        }
    }

    private async pauseStagesForProject(pipelineStagesManager: any, project: IProjectAttributes) {
        const stages: IPipelineStage[] = await pipelineStagesManager.getForProject(project.id);

        await this.removeWorker(project);

        await Promise.all(stages.map(stage => this.pauseStage(stage)));
    }

    private async pauseStage(stage: any): Promise<boolean> {
        stage.update({is_processing: false});

        return this.removeWorker(stage);
    }

    private async manageStageProcessingFlag() {
        const pipelineStagesManager = PersistentStorageManager.Instance().PipelineStages;

        const stages: IPipelineStage[] = await pipelineStagesManager.findAll({});

        return stages.map(stage => {
            let worker = this._pipelineStageWorkers.get(stage.id);

            if (worker) {
                worker.IsProcessingRequested = stage.is_processing;
            }
        });
    }

    private async addWorker(item: IProjectAttributes | IPipelineStage, inProcessFunction, childProcessModuleName): Promise<boolean> {
        let worker = this._pipelineStageWorkers.get(item.id);

        if (!worker) {
            debug(`add worker for ${item.id}`);

            worker = await this.startWorker(inProcessFunction, childProcessModuleName, [item.id]);
            worker.IsProcessingRequested = item.is_processing;

            if (worker) {
                this._pipelineStageWorkers.set(item.id, worker);
            }

            return true;
        }

        return false;
    }

    private removeWorker(item: IProjectAttributes | IPipelineStage): boolean {
        const worker = this._pipelineStageWorkers.get(item.id);

        if (worker) {
            debug(`remove worker for ${item.id}`);

            worker.IsExitRequested = true;

            this._pipelineStageWorkers.delete(item.id);

            return true;
        }

        return false;
    }

    private startWorkerChildProcess(moduleName: string, args: string[]) {
        // Options
        //   silent - pumps stdio back through this parent process
        //   execArv - remove possible $DEBUG flag on parent process causing address in use conflict
        const worker_process = child_process.fork(path.join(__dirname, moduleName), args, {silent: true, execArgv: []});

        worker_process.stdout.on("data", data => console.log(`${data.toString().slice(0, -1)}`));

        worker_process.stderr.on("data", data => console.log(`${data.toString().slice(0, -1)}`));

        worker_process.on("close", code => console.log(`child process exited with code ${code}`));

        return worker_process;
    }

    private async startWorker(inProcessFunction, childProcessModule: string, args: Array<any> = []) {
        if (this._useChildProcessWorkers) {
            debug("starting worker using child processes");
            return new Promise((resolve) => {
                let worker_process = this.startWorkerChildProcess(childProcessModule, args);
                resolve(worker_process);
            });
        } else {
            debug("starting worker within parent process");
            return await inProcessFunction(...args);
        }
    }
}

import {startAdjacentPipelineStageWorker} from "./pipelineAdjacentSchedulerChildProcess";

const path = require("path");
const child_process = require("child_process");

const debug = require("debug")("pipeline:scheduler:scheduler-hub");

import {startTileStatusFileWorker} from "./projectPipelineSchedulerChildProcess";
import {startMapPipelineStageWorker} from "./pipelineMapSchedulerChildProcess";
import {Project} from "../data-model/system/project";
import {PipelineStage, PipelineStageMethod} from "../data-model/system/pipelineStage";
import {WorkerTaskExecution} from "../data-model/system/workerTaskExecution";

export interface ISchedulerInterface {
    IsExitRequested: boolean;
    IsProcessingRequested: boolean;

    onTaskExecutionComplete(executionInfo: WorkerTaskExecution): Promise<void>;
    // onTaskExecutionUpdate(executionInfo: WorkerTaskExecution): Promise<void>;
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

    public async onTaskExecutionUpdate(taskExecution: WorkerTaskExecution) {
        if (taskExecution === null) {
            console.log("null task execution");
        }

        if (taskExecution.execution_status_code === null) {
            console.log("null execution_status_code");
            console.log(taskExecution);
        }

        try {
            const worker = this._pipelineStageWorkers.get(taskExecution.stage_id);

            if (worker) {
                // await worker.onTaskExecutionUpdate(taskExecution);
                return true;
            }
        } catch (err) {
            debug(err);
        }

        return false;
    }

    public async onTaskExecutionComplete(taskExecution: WorkerTaskExecution) {
        try {
            const worker = this._pipelineStageWorkers.get(taskExecution.stage_id);

            if (worker) {
                await worker.onTaskExecutionComplete(taskExecution);
                return true;
            } else {
                debug(`stage worker for pipeline stage ${taskExecution.stage_id} missing for task completion`)
            }
        } catch (err) {
            debug(err);
        }

        return false;
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
            const projects: Project[] = await Project.findAll({});

            // Turn stage workers off for projects that have been turned off.
            const pausedProjects = projects.filter(item => (item.is_processing || 0) === 0);

            await Promise.all(pausedProjects.map(project => this.pauseStagesForProject(project)));

            // Turn stage workers on (but not necessarily processing) for projects that are active for stats.
            // Individual stage processing is maintained in the next step.
            const resumedProjects = projects.filter(item => item.is_processing === true);

            await Promise.all(resumedProjects.map(project => this.resumeStagesForProject(project)));

            // Refresh processing state for active workers.
            await this.manageStageProcessingFlag();

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

    private async resumeStagesForProject(project: Project) {
        const stages = await project.getStages();

        await this.addWorker(project, startTileStatusFileWorker, "/projectPipelineSchedulerChildProcess.js");

        await Promise.all(stages.map(stage => this.resumeStage(stage)));
    }

    private async resumeStage(stage: PipelineStage): Promise<boolean> {
        if (stage.function_type !== PipelineStageMethod.MapTile) {
            return this.addWorker(stage, startAdjacentPipelineStageWorker, "/pipelineAdjacentSchedulerChildProcess.js");
        } else {
            return this.addWorker(stage, startMapPipelineStageWorker, "/pipelineMapSchedulerChildProcess.js");
        }
    }

    private async pauseStagesForProject(project: Project) {
        const stages = await project.getStages();

        await this.removeWorker(project);

        await Promise.all(stages.map(stage => this.pauseStage(stage)));
    }

    private async pauseStage(stage: any): Promise<boolean> {
        stage.update({is_processing: false});

        return this.removeWorker(stage);
    }

    private async manageStageProcessingFlag() {
        const stages = await PipelineStage.findAll({});

        return stages.map(stage => {
            let worker = this._pipelineStageWorkers.get(stage.id);

            if (worker) {
                worker.IsProcessingRequested = stage.is_processing;
            }
        });
    }

    private async addWorker(item: Project | PipelineStage, inProcessFunction, childProcessModuleName): Promise<boolean> {
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

    private removeWorker(item: Project | PipelineStage): boolean {
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

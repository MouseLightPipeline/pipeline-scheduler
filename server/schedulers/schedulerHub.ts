import {startAdjacentPipelineStageWorker} from "./pipelineAdjacentSchedulerChildProcess";

const path = require("path");
const child_process = require("child_process");

const debug = require("debug")("pipeline:coordinator-api:scheduler-hub");

import {startTileStatusFileWorker} from "./projectPipelineSchedulerChildProcess";
import {startMapPipelineStageWorker} from "./pipelineMapSchedulerChildProcess";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";
import {IProjectAttributes} from "../data-model/sequelize/project";
import {IPipelineStage, PipelineStageMethod} from "../data-model/sequelize/pipelineStage";
import {isNullOrUndefined} from "util";

export interface ISchedulerInterface {
    OutputPath: string;

    IsExitRequested: boolean;
    IsProcessingRequested: boolean;

    loadTileStatusForPlane(zIndex: number);
    loadTileThumbnailPath(x: number, y: number, z: number): Promise<string>
}

const kEmptyTileMap = {
    max_depth: 0,
    x_min: 0,
    x_max: 0,
    y_min: 0,
    y_max: 0,
    tiles: []
};

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

    private _useChildProcessWorkers: boolean;

    private _pipelineStageWorkers = new Map<string, ISchedulerInterface>();

    public async thumbnailPath(stageId: string, x, y, z): Promise<string> {
        const worker = this._pipelineStageWorkers.get(stageId);

        if (worker) {
            return worker.loadTileThumbnailPath(x, y, z);
        }

        return null;
    }

    public async loadTileStatusForPlane(project_id: string, plane: number): Promise<any> {
        try {
            if (plane == null) {
                debug("plane not defined");
                return kEmptyTileMap;
            }

            const projectsManager = PersistentStorageManager.Instance().Projects;

            const project = await projectsManager.findById(project_id);

            if (!project) {
                debug("project not defined");
                return kEmptyTileMap;
            }

            const pipelineStagesManager = PersistentStorageManager.Instance().PipelineStages;

            const stages: IPipelineStage[] = await pipelineStagesManager.getForProject(project_id);

            if (stages.length === 0) {
                debug("no stages for project");
                return kEmptyTileMap;
            }

            const maxDepth = stages.reduce((current, stage) => Math.max(current, stage.depth), 0);

            const stageWorkers = stages.map(stage => this._pipelineStageWorkers.get(stage.id)).filter(worker => worker != null);

            const projectTileWorker = this._pipelineStageWorkers.get(project.id);

            if (!isNullOrUndefined((projectTileWorker))) {
                stageWorkers.unshift(projectTileWorker);
            }

            if (stageWorkers.length === 0) {
                return kEmptyTileMap;
            }

            const promises = stageWorkers.map(worker => {
                return worker.loadTileStatusForPlane(plane);
            });

            const tilesAllStages = await Promise.all(promises);

            const tileArray = tilesAllStages.reduce((source, next) => source.concat(next), []);

            if (tileArray.length === 0) {
                debug("no tiles across all stages");
                return kEmptyTileMap;
            }

            let tiles = {};

            let x_min = 1e7, x_max = 0, y_min = 1e7, y_max = 0;

            tileArray.map(tile => {
                x_min = Math.min(x_min, tile.lat_x);
                x_max = Math.max(x_max, tile.lat_x);
                y_min = Math.min(y_min, tile.lat_y);
                y_max = Math.max(y_max, tile.lat_y);

                let t = tiles[`${tile.lat_x}_${tile.lat_y}`];

                if (!t) {
                    t = {
                        x_index: tile.lat_x,
                        y_index: tile.lat_y,
                        stages: []
                    };

                    tiles[`${tile.lat_x}_${tile.lat_y}`] = t;
                }

                // Duplicate tiles exist.  Use whatever is further along (i.e., a repeat of an incomplete that is complete
                // and processing supersedes.

                const existing = t.stages.filter(s => s.depth === tile.depth);

                if (existing.length === 0) {
                    t.stages.push({
                        relative_path: tile.relative_path,
                        stage_id: tile.stage_id,
                        depth: tile.depth,
                        status: tile.this_stage_status
                    });
                } else if (tile.this_stage_status > existing.status) {
                    existing.relative_path = tile.relative_path;
                    existing.stage_id = tile.stage_id;
                    existing.depth = tile.depth;
                    // This is not strictly correct as failed enum > complete and complete is probably what you want
                    // to know.
                    existing.status = tile.this_stage_status;
                }
            });

            let output = [];

            // I forget what I am trying to drop here?
            for (let prop in tiles) {
                if (tiles.hasOwnProperty(prop)) {
                    output.push(tiles[prop]);
                }
            }

            return {
                max_depth: maxDepth,
                x_min: project.sample_x_min >= 0 ? project.sample_x_min : x_min,
                x_max: project.sample_x_max >= 0 ? project.sample_x_min : x_max,
                y_min: project.sample_y_min >= 0 ? project.sample_y_min : x_min,
                y_max: project.sample_y_max >= 0 ? project.sample_y_min : x_max,
                tiles: output
            };
        } catch (err) {
            console.log(err);
            return {};
        }
    }

    private constructor(useChildProcessWorkers: boolean = false) {
        this._useChildProcessWorkers = useChildProcessWorkers;
    }

    private async start() {
        await this.manageAllWorkers();
    }

    private _pidCount = 0;

    private async manageAllWorkers() {
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

        setTimeout(() => this.manageAllWorkers(), 10 * 1000);
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

        await this.removeWorker(project/*, this._tileStatusWorkers*/);

        await Promise.all(stages.map(stage => this.pauseStage(stage)));
    }

    private async pauseStage(stage: any): Promise<boolean> {
        stage.update({is_processing: false});

        return this.removeWorker(stage/*, this._pipelineStageWorkers*/);
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

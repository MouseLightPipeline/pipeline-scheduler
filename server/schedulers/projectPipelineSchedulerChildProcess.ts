import {ProjectPipelineScheduler} from "./projectPipelineScheduler";
import {Project} from "../data-model/project";

const debug = require("debug")("pipeline:scheduler:tile-status-worker-process");

let projectId = process.argv.length > 2 ? process.argv[2] : null;

if (projectId) {
    startWorkerForProcess(projectId).then(() => {
        debug(`started tile status child process for project ${projectId}`);
    }).catch(err => {
        debug(`failed to start tile status process for ${projectId}: ${err}`);
    });
}

async function startWorkerForProcess(projectId) {
    let worker = await startTileStatusFileWorker(projectId);

    process.on("message", msg => {
        if (msg && msg.isCancelRequest) {
            worker.IsProcessingRequested = true;
        }

        process.disconnect();
    });

    debug("completed tile status child process");
}

export async function startTileStatusFileWorker(projectId: string) {

    let project = await Project.findByPk(projectId);

    let tileStatusWorker = new ProjectPipelineScheduler(project);

    await tileStatusWorker.run();

    return tileStatusWorker;
}

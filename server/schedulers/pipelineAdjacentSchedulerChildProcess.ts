import {PipelineAdjacentScheduler} from "./pipelineAdjacentScheduler";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";
import {IPipelineStage} from "../data-model/sequelize/pipelineStage";

const debug = require("debug")("pipeline:coordinator-api:pipeline-z-comparison-worker-process");

let stageId = process.argv.length > 2 ? process.argv[2] : null;

if (stageId) {
    debug("started pipeline z comparison child process");

    startWorkerForProcess(stageId).then(() => {
        debug(`started pipeline z comparison child process for stage ${stageId}`);
    }).catch(err => {
        debug(`failed to start pipeline z comparison stage for ${stageId}: ${err}`);
    });
}

async function startWorkerForProcess(stageId) {
    let worker = await startAdjacentPipelineStageWorker(stageId);

    process.on("message", msg => {
        if (msg && msg.isCancelRequest) {
            worker.IsProcessingRequested = true;
        }

        process.disconnect();
    });

    debug("completed pipeline z comparison child process");
}

export async function startAdjacentPipelineStageWorker(stageId) {
    let pipelineWorker = null;

    try {
        let stage: IPipelineStage = await PersistentStorageManager.Instance().PipelineStages.findById(stageId);

        if (stage) {
            let project = await PersistentStorageManager.Instance().Projects.findById(stage.project_id);

            pipelineWorker = new PipelineAdjacentScheduler(stage, project);

            await pipelineWorker.run();
        }
    } catch (err) {
        debug(err);
    }

    return pipelineWorker;
}
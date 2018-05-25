import * as path from "path";
const Sequelize = require("sequelize");

const debug = require("debug")("pipeline:coordinator-api:database-connector");

import {loadModels} from "./modelLoader";
import {SequelizeOptions} from "../../options/coreServicesOptions";
import {IProjectModel} from "../../data-model/sequelize/project";
import {ITaskDefinitionModel} from "../../data-model/sequelize/taskDefinition";

export interface IPipelineModels {
    TaskDefinitions?: ITaskDefinitionModel;
    TaskRepositories?: any;
    // TaskExecutions?: any;
    PipelineWorkers?: any;
    Projects?: IProjectModel;
    PipelineStages?: any;
    PipelineStagePerformances?: any;
}

export interface ISequelizeDatabase<T> {
    connection: any;
    models: T;
    isConnected: boolean;
}

export class PersistentStorageManager {

    private pipelineDatabase: ISequelizeDatabase<IPipelineModels>;

    public static Instance(): PersistentStorageManager {
        return _manager;
    }

    public get IsConnected() {
        return this.pipelineDatabase && this.pipelineDatabase.isConnected;
    }

    public get PipelineConnection() {
        return this.pipelineDatabase.connection;
    }

    public get TaskRepositories() {
        return this.pipelineDatabase.models.TaskRepositories;
    }

    public get TaskDefinitions() {
        return this.pipelineDatabase.models.TaskDefinitions;
    }
    /*
    public get TaskExecutions() {
        return this.pipelineDatabase.models.TaskExecutions;
    }
    */
    public get PipelineWorkers() {
        return this.pipelineDatabase.models.PipelineWorkers;
    }

    public get Projects() {
        return this.pipelineDatabase.models.Projects;
    }

    public get PipelineStages() {
        return this.pipelineDatabase.models.PipelineStages;
    }

    public get PipelineStagePerformances() {
        return this.pipelineDatabase.models.PipelineStagePerformances;
    }

    public async initialize() {
        this.pipelineDatabase = await createConnection({});
        await authenticate(this.pipelineDatabase, "pipeline");
    }
}

async function authenticate(database, name) {
    try {
        await database.connection.authenticate();

        database.isConnected = true;

        debug(`successful database connection: ${name}`);

        Object.keys(database.models).map(modelName => {
            if (database.models[modelName].prepareContents) {
                database.models[modelName].prepareContents(database.models);
            }
        });
    } catch (err) {
        debug(`failed database connection: ${name}`);
        debug(err);

        setTimeout(() => authenticate(database, name), 5000);
    }
}

async function createConnection<T>(models: T) {
    let db: ISequelizeDatabase<T> = {
        connection: null,
        models: models,
        isConnected: false
    };

    db.connection = new Sequelize(SequelizeOptions.database, SequelizeOptions.username, SequelizeOptions.password, SequelizeOptions);

    return await loadModels(db, path.normalize(path.join(__dirname, "..", "..", "data-model", "sequelize")));
}

const _manager: PersistentStorageManager = new PersistentStorageManager();

_manager.initialize().then(() => {
});

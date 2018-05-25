import {PersistentStorageManager} from "./databaseConnector";

const sequelize = require("sequelize");
const asyncUtils = require("async");
import {Sequelize} from "sequelize";

const debug = require("debug")("pipeline:coordinator-api:project-database-connector");

import {IProjectAttributes} from "../../data-model/sequelize/project";
import {IPipelineStage, PipelineStageMethod} from "../../data-model/sequelize/pipelineStage";
import {StageTableConnector} from "./stageTableConnector";
import {SequelizeOptions} from "../../options/coreServicesOptions";
import {AdjacentTileStageConnector} from "./adjacentTileStageConnector";

interface IAccessQueueToken {
    project: IProjectAttributes;
    resolve: any;
    reject: any;
}

interface IStageQueueToken {
    projectConnector: ProjectDatabaseConnector;
    stage: IPipelineStage;
    resolve: any;
    reject: any;
}

interface IProjectQueueToken {
    projectConnector: ProjectDatabaseConnector;
    project: IProjectAttributes;
    resolve: any;
    reject: any;
}

export class ProjectDatabaseConnector {
    private _isConnected: boolean;
    private _connection: Sequelize;
    private _project: IProjectAttributes;
    private _databaseName: string;

    private _stageConnectors = new Map<string, StageTableConnector>();
    private _stageConnectorQueueAccess = asyncUtils.queue(accessStageQueue, 1);
    private _projectConnectorQueueAccess = asyncUtils.queue(accessProjectQueue, 1);

    public async initialize(project: IProjectAttributes) {
        this._project = project;
        this._databaseName = this._project.id;

        await this.ensureDatabase();

        const databaseConfig = Object.assign({}, SequelizeOptions, {
            database: this._databaseName
        });

        this._connection = new sequelize(databaseConfig.database, databaseConfig.username, databaseConfig.password, databaseConfig);

        await this._connection.authenticate();

        this._isConnected = true;
    }

    public get IsConnected(): boolean {
        return this._isConnected;
    }

    public async connectorForStage(stage: IPipelineStage): Promise<StageTableConnector> {
        if (this._stageConnectors.has(stage.id)) {
            return this._stageConnectors.get(stage.id);
        }

        // Serialize access to queue for a non-existent connector so only one is created.
        return new Promise<StageTableConnector>((resolve, reject) => {
            this._stageConnectorQueueAccess.push({
                projectConnector: this,
                stage,
                resolve,
                reject
            });
        });
    }

    public async internalConnectorForStage(stage: IPipelineStage) {
        // This method can only be called serially despite async due to async queue.  Could arrive to find
        if (!this._stageConnectors.has(stage.id)) {
            const method: PipelineStageMethod = stage.function_type;

            const connector = method === PipelineStageMethod.MapTile ? new StageTableConnector(this._connection, stage.id) : new AdjacentTileStageConnector(this._connection, stage.id);

            await connector.initialize();

            this._stageConnectors.set(stage.id, connector);
        }

        return this._stageConnectors.get(stage.id);
    }

    public async connectorForProject(project: IProjectAttributes): Promise<StageTableConnector> {
        if (this._stageConnectors.has(project.id)) {
            return this._stageConnectors.get(project.id);
        }

        // Serialize access to queue for a non-existent connector so only one is created.
        return new Promise<StageTableConnector>((resolve, reject) => {
            this._projectConnectorQueueAccess.push({
                projectConnector: this,
                project,
                resolve,
                reject
            });
        });
    }

    public async internalConnectorForProject(project: IProjectAttributes): Promise<StageTableConnector> {
        if (!this._stageConnectors.has(project.id)) {
            const connector = new StageTableConnector(this._connection, project.id);

            await connector.initialize();

            this._stageConnectors.set(project.id, connector);
        }

        return this._stageConnectors.get(project.id);
    }

    private async ensureDatabase() {
        const databaseConfig = Object.assign({}, SequelizeOptions, {
            database: "postgres"
        });

        const connection = new sequelize(databaseConfig.database, databaseConfig.username, databaseConfig.password, databaseConfig);

        const result = await connection.query(`SELECT 1 FROM pg_database WHERE datname = '${this._databaseName}';`);

        if (result.length < 2 || result[0].length < 1) {
            await connection.query(`CREATE DATABASE "${this._databaseName}"`)
        }
    }
}

const connectionMap = new Map<string, ProjectDatabaseConnector>();

const connectorQueueAccess = asyncUtils.queue(accessQueueWorker, 1);

export async function connectorForStage(pipelineStage: IPipelineStage): Promise<StageTableConnector> {
    const project = await PersistentStorageManager.Instance().Projects.findById(pipelineStage.project_id);

    const connector = await connectorForProject(project);

    return connector.connectorForStage(pipelineStage);
}

export async function connectorForProject(project: IProjectAttributes): Promise<ProjectDatabaseConnector> {
    if (connectionMap.has(project.id)) {
        return connectionMap.get(project.id);
    }

    // Serialize access to queue for a non-existent connector so only one is created.
    return new Promise<ProjectDatabaseConnector>((resolve, reject) => {
        connectorQueueAccess.push({
            project,
            resolve,
            reject
        });
    });
}

async function accessQueueWorker(token: IAccessQueueToken, completeCallback) {
    try {
        // This function has serialized access through AsyncQueue.  If it is the first one in the pipeline through before
        // the connector is created it can create it knowing no other call will also.  If you find yourself here and the
        // connection has been created, you were not first in the queue.
        if (!connectionMap.has(token.project.id)) {
            debug(`creating connector for ${token.project.name} (${token.project.id})`);

            const connector = new ProjectDatabaseConnector();

            await connector.initialize(token.project);

            debug(`successful database connection: ${token.project.id}`);

            connectionMap.set(token.project.id, connector);
        }

        token.resolve(connectionMap.get(token.project.id));
    } catch (err) {
        debug(`failed database connection: ${token.project.id}`);
        debug(err);
        token.reject(err);
    }

    completeCallback();
}

async function accessStageQueue(token: IStageQueueToken, completeCallback) {
    try {
        const connector = await token.projectConnector.internalConnectorForStage(token.stage);

        token.resolve(connector);
    } catch (err) {
        debug(`failed database connection: ${token.stage.id}`);
        debug(err);
        token.reject(err);
    }

    completeCallback();
}

async function accessProjectQueue(token: IProjectQueueToken, completeCallback) {
    try {
        const connector = await token.projectConnector.internalConnectorForProject(token.project);

        token.resolve(connector);
    } catch (err) {
        debug(`failed database connection: ${token.project.id}`);
        debug(err);
        token.reject(err);
    }

    completeCallback();
}

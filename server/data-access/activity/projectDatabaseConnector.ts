const sequelize = require("sequelize");
const asyncUtils = require("async");
import {Sequelize} from "sequelize";

const debug = require("debug")("pipeline:pipeline-api:project-database-connector");

import {Project} from "../../data-model/system/project";
import {PipelineStage} from "../../data-model/system/pipelineStage";
import {SchedulerStageTableConnector} from "./schedulerStageTableConnector";
import {SequelizeOptions} from "../../options/coreServicesOptions";

interface IAccessQueueToken {
    project: Project;
    resolve: any;
    reject: any;
}

interface IStageQueueToken {
    projectConnector: ProjectDatabaseConnector;
    stage: PipelineStage;
    resolve: any;
    reject: any;
}

interface IProjectQueueToken {
    projectConnector: ProjectDatabaseConnector;
    resolve: any;
    reject: any;
}

export class ProjectDatabaseConnector {
    private _isConnected: boolean;
    private _connection: Sequelize;
    private _project: Project;
    private _databaseName: string;

    private _stageConnectors = new Map<string, SchedulerStageTableConnector>();
    private _stageConnectorQueueAccess = asyncUtils.queue(accessStageQueue, 1);
    private _projectConnectorQueueAccess = asyncUtils.queue(accessProjectQueue, 1);

    public async initialize(project: Project) {
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

    public get Project(): Project {
        return this._project;
    }

    public async connectorForStage(stage: PipelineStage): Promise<SchedulerStageTableConnector> {
        if (this._stageConnectors.has(stage.id)) {
            return this._stageConnectors.get(stage.id);
        }

        // Serialize access to queue for a non-existent connector so only one is created.
        return new Promise<SchedulerStageTableConnector>((resolve, reject) => {
            this._stageConnectorQueueAccess.push({
                projectConnector: this,
                stage,
                resolve,
                reject
            });
        });
    }

    public async internalConnectorForStage(stage: PipelineStage) {
        // This method can only be called serially despite async due to async queue.  Could arrive to find
        if (!this._stageConnectors.has(stage.id)) {
            const connector = new SchedulerStageTableConnector(this._connection, stage.id, stage.previous_stage_id ?? stage.project_id);

            await connector.initialize(true);

            this._stageConnectors.set(stage.id, connector);
        }

        return this._stageConnectors.get(stage.id);
    }

    public async connectorForProject(): Promise<SchedulerStageTableConnector> {
        if (this._stageConnectors.has(this._project.id)) {
            return this._stageConnectors.get(this._project.id);
        }

        // Serialize access to queue for a non-existent connector so only one is created.
        return new Promise<SchedulerStageTableConnector>((resolve, reject) => {
            this._projectConnectorQueueAccess.push({
                projectConnector: this,
                resolve,
                reject
            });
        });
    }

    public async internalConnectorForProject(): Promise<SchedulerStageTableConnector> {
        if (!this._stageConnectors.has(this._databaseName)) {
            const connector = new SchedulerStageTableConnector(this._connection, this._databaseName, null);

            await connector.initialize(true);

            this._stageConnectors.set(this._databaseName, connector);
        }

        return this._stageConnectors.get(this._databaseName);
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

export async function connectorForStage(pipelineStage: PipelineStage): Promise<SchedulerStageTableConnector> {
    const project = await Project.findByPk(pipelineStage.project_id);

    const connector = await connectorForProject(project);

    return connector.connectorForStage(pipelineStage);
}

export async function connectorForProject(project: Project): Promise<ProjectDatabaseConnector> {
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
        const connector = await token.projectConnector.internalConnectorForProject();

        token.resolve(connector);
    } catch (err) {
        debug(`failed database connection: ${token.projectConnector.Project.id}`);
        debug(err);
        token.reject(err);
    }

    completeCallback();
}

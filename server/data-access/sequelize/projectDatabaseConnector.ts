const sequelize = require("sequelize");
const asyncUtils = require("async");
import {Sequelize} from "sequelize";

const debug = require("debug")("pipeline:pipeline-api:project-database-connector");

import {Project} from "../../data-model/project";
import {PipelineStage, PipelineStageMethod} from "../../data-model/pipelineStage";
import {StageTableConnector} from "./stageTableConnector";
import {SequelizeOptions} from "../../options/coreServicesOptions";
import {AdjacentTileStageConnector} from "./adjacentTileStageConnector";

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

    private _stageConnectors = new Map<string, StageTableConnector>();
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

    public async connectorForStage(stage: PipelineStage): Promise<StageTableConnector> {
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

    public async internalConnectorForStage(stage: PipelineStage) {
        // This method can only be called serially despite async due to async queue.  Could arrive to find
        if (!this._stageConnectors.has(stage.id)) {

            // The API does not create stage tables, only the scheduler does.
            // let haveTables = (await this._connection.query(`SELECT to_regclass('public.${stage.id}');`, {type: sequelize.QueryTypes.SELECT})).some(r => r.to_regclass !== null);

            const test = await this._connection.query<any>(`SELECT to_regclass('public.${stage.id}');`, {type: sequelize.QueryTypes.SELECT});

            const haveTables = (await this._connection.query<any>(`SELECT to_regclass('public.${stage.id}');`, {type: sequelize.QueryTypes.SELECT})).some(r => r.to_regclass !== null);

            if (!haveTables) {
                return null;
            }

            const method: PipelineStageMethod = stage.function_type;

            const connector = method === PipelineStageMethod.MapTile ? new StageTableConnector(this._connection, stage.id) : new AdjacentTileStageConnector(this._connection, stage.id);

            await connector.initialize();

            this._stageConnectors.set(stage.id, connector);
        }

        return this._stageConnectors.get(stage.id);
    }

    public async connectorForProject(): Promise<StageTableConnector> {
        if (this._stageConnectors.has(this._project.id)) {
            return this._stageConnectors.get(this._project.id);
        }

        // Serialize access to queue for a non-existent connector so only one is created.
        return new Promise<StageTableConnector>((resolve, reject) => {
            this._projectConnectorQueueAccess.push({
                projectConnector: this,
                resolve,
                reject
            });
        });
    }

    public async internalConnectorForProject(): Promise<StageTableConnector> {
        if (!this._stageConnectors.has(this._project.id)) {
            // The API does not create stage tables, only the scheduler does.aa
            if (!this._connection.isDefined(this._project.id)) {
                return null;
            }

            const connector = new StageTableConnector(this._connection, this._project.id);

            await connector.initialize();

            this._stageConnectors.set(this._project.id, connector);
        }

        return this._stageConnectors.get(this._project.id);
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

export async function connectorForStage(pipelineStage: PipelineStage): Promise<StageTableConnector> {
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

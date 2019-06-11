import {HttpLink} from "apollo-link-http";
import {ApolloClient} from "apollo-client";
import {InMemoryCache} from "apollo-cache-inmemory";

const gql = require("graphql-tag");

require("isomorphic-fetch");

const debug = require("debug")("pipeline:scheduler:pipeline-worker-client");

import {IPipelineWorker, PipelineWorkerStatus} from "../data-model/sequelize/pipelineWorker";
import {ITaskExecutionAttributes} from "../data-model/taskExecution";
import {PersistentStorageManager} from "../data-access/sequelize/databaseConnector";

export interface IStartTaskResponse {
    taskExecution: ITaskExecutionAttributes;
    localTaskLoad: number;
    clusterTaskLoad: number;
}

export interface IClientWorker {
    id: string;
    local_work_capacity?: number;
    cluster_work_capacity?: number;
    local_task_load?: number;
    cluster_task_load?: number;
}

export class PipelineWorkerClient {
    private static _instance: PipelineWorkerClient = null;

    public static Instance(): PipelineWorkerClient {
        if (PipelineWorkerClient._instance === null) {
            PipelineWorkerClient._instance = new PipelineWorkerClient();
        }

        return PipelineWorkerClient._instance;
    }

    private _idClientMap = new Map<string, any>();

    private getClient(worker: IPipelineWorker): any {
        if (worker === null) {
            return null;
        }

        let client = this._idClientMap[worker.id];

        let uri = null;

        if (client == null) {
            try {
                uri = `http://${worker.address}:${worker.port}/graphql`;

                debug(`creating apollo client with uri ${uri}`);

                const client = new ApolloClient({
                    link: new HttpLink({uri}),
                    cache: new InMemoryCache()
                });

                this._idClientMap[worker.id] = client;
            } catch (err) {
                debug(`failed to create apollo client with uri ${uri}`);

                client = null;
            }
        }

        return client;
    }

    public async startTaskExecution(worker: IPipelineWorker, taskInput: ITaskExecutionAttributes): Promise<IStartTaskResponse> {
        const client = this.getClient(worker);

        if (client === null) {
            return null;
        }

        try {
            let response = await client.mutate({
                mutation: gql`
                mutation startTask($taskInput: String!) {
                    startTask(taskInput: $taskInput) {
                      taskExecution {
                        id
                        queue_type
                        resolved_script_args
                        completion_status_code
                        execution_status_code
                        last_process_status_code
                        local_work_units
                        cluster_work_units
                        submitted_at
                        started_at
                        completed_at
                      }
                      localTaskLoad
                      clusterTaskLoad
                    }
                }`,
                variables: {
                    taskInput: JSON.stringify(taskInput)
                }
            });

            return response.data.startTask;
        } catch (err) {
            await PipelineWorkerClient.markWorkerUnavailable(worker);
            throw(err);
        }
    }

    public async queryWorker(worker: IPipelineWorker): Promise<IClientWorker> {
        const client = this.getClient(worker);

        if (client === null) {
            debug("Could not connect to worker");
            return {id: worker.id, local_task_load: -1, cluster_task_load: -1};
        }

        try {
            const response: any = await client.query({
                query: gql`
                query {
                    worker {
                        id
                        local_work_capacity
                        cluster_work_capacity
                        local_task_load
                        cluster_task_load
                    }
                }`
            });

            return response.data.worker;
        } catch (err) {
            await PipelineWorkerClient.markWorkerUnavailable(worker);
            this._idClientMap.delete(worker.id);
            debug(`error requesting worker update ${worker.name}`);

            return {id: worker.id, local_task_load: -1, cluster_task_load: -1};
        }
    }

    private static async markWorkerUnavailable(worker: IPipelineWorker): Promise<void> {
        const row = await PersistentStorageManager.Instance().getPipelineWorker(worker.id);

        row.status = PipelineWorkerStatus.Unavailable;
    }
}

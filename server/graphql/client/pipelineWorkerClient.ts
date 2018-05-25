import ApolloClient, {createNetworkInterface} from "apollo-client";
import gql from "graphql-tag";
import "isomorphic-fetch";

const debug = require("debug")("pipeline:coordinator-api:pipeline-worker-client");

import {IPipelineWorker, PipelineWorkerStatus} from "../../data-model/sequelize/pipelineWorker";
import {ITaskExecutionAttributes} from "../../data-model/taskExecution";
import {PersistentStorageManager} from "../../data-access/sequelize/databaseConnector";

export interface ITaskExecutionStatus {
    workerResponded: boolean;
    taskExecution: ITaskExecutionAttributes;
}

export interface IClientWorker {
    id: string;
    work_capacity: number;
    is_cluster_proxy: boolean;
}

export interface IClientUpdateWorkerOutput {
    worker: IClientWorker;
    error: string;
}

export class PipelineWorkerClient {
    private static _instance: PipelineWorkerClient = null;

    public static Instance(): PipelineWorkerClient {
        if (PipelineWorkerClient._instance === null) {
            PipelineWorkerClient._instance = new PipelineWorkerClient();
        }

        return PipelineWorkerClient._instance;
    }

    private _idClientMap = new Map<string, ApolloClient>();

    private getClient(worker: IPipelineWorker): ApolloClient {
        if (worker === null) {
            return null;
        }

        let client = this._idClientMap[worker.id];

        let uri = null;

        if (client == null) {
            try {
                uri = `http://${worker.address}:${worker.port}/graphql`;

                debug(`creating apollo client with uri ${uri}`);
                const networkInterface = createNetworkInterface({uri});

                client = new ApolloClient({
                    networkInterface
                });

                this._idClientMap[worker.id] = client;
            } catch (err) {
                debug(`failed to create apollo client with uri ${uri}`);

                client = null;
            }
        }

        return client;
    }

    private static async markWorkerUnavailable(worker: IPipelineWorker): Promise<void> {
        const row = await PersistentStorageManager.Instance().getPipelineWorker(worker.id);

        row.status = PipelineWorkerStatus.Unavailable;
    }

    public async queryTaskExecution(worker: IPipelineWorker, executionId: string): Promise<ITaskExecutionStatus> {
        const taskExecutionStatus = {
            workerResponded: false,
            taskExecution: null
        };

        const client = this.getClient(worker);

        if (client === null) {
            return taskExecutionStatus;
        }

        try {
            let response: any = await client.query({
                query: gql`
                query($id: String!) {
                    taskExecution(id: $id) {
                        id
                        last_process_status_code
                        completion_status_code
                        execution_status_code
                        max_cpu
                        max_memory
                        work_units
                        resolved_log_path
                        started_at
                        completed_at
                    }
                }`,
                variables: {
                    id: executionId
                },
                fetchPolicy: "network-only"
            });

            taskExecutionStatus.taskExecution = response.data.taskExecution;
            taskExecutionStatus.workerResponded = true;
        } catch (err) {
            await PipelineWorkerClient.markWorkerUnavailable(worker);
            debug(`error querying task status for worker ${worker.name}`);
        }

        return taskExecutionStatus;
    }

    public async startTaskExecution(worker: IPipelineWorker, taskInput: ITaskExecutionAttributes): Promise<ITaskExecutionAttributes> {
        const client = this.getClient(worker);

        if (client === null) {
            return null;
        }

        try {
            let response = await client.mutate({
                mutation: gql`
                mutation startTask($taskInput: String!) {
                    startTask(taskInput: $taskInput) {
                        id
                        completion_status_code
                        execution_status_code
                        last_process_status_code
                        work_units
                        submitted_at
                        started_at
                        completed_at
                    }
                }`,
                variables: {
                    taskInput: JSON.stringify(taskInput)
                }
            });

            return response.data.startTask;
        } catch (err) {
            await PipelineWorkerClient.markWorkerUnavailable(worker);
            debug(`error submitting task to worker ${worker.name}`);
        }

        return null;
    }

    public async updateWorker(worker: IPipelineWorker): Promise<IClientUpdateWorkerOutput> {
        const client = this.getClient(worker);

        if (client === null) {
            return {worker: null, error: "Could not connect to worker"};
        }

        try {
            let response = await client.mutate({
                mutation: gql`
                mutation UpdateWorkerMutation($worker: WorkerInput) {
                    updateWorker(worker: $worker) {
                        id
                        work_capacity
                        is_cluster_proxy
                    }
                }`,
                variables: {
                    worker: Object.assign({}, {id: worker.id, work_capacity: worker.work_unit_capacity})
                }
            });

            return {worker: response.data.updateWorker, error: null};
        } catch (err) {
            await PipelineWorkerClient.markWorkerUnavailable(worker);
            debug(`error submitting update to worker ${worker.name}`);

            return {worker: null, error: err};
        }
    }
}
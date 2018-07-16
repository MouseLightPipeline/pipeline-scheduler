import ApolloClient, {createNetworkInterface} from "apollo-client";
import gql from "graphql-tag";
import "isomorphic-fetch";

import {ApiServerOptions} from "../options/coreServicesOptions";
import {IProjectInput, ProjectInputSourceState} from "../data-model/sequelize/project";

const debug = require("debug")("pipeline:scheduler:pipeline-api-client");


export class PipelineApiClient {
    private static _instance: PipelineApiClient = null;

    public static Instance(): PipelineApiClient {
        if (PipelineApiClient._instance === null) {
            PipelineApiClient._instance = new PipelineApiClient();
        }

        return PipelineApiClient._instance;
    }

    private _client: ApolloClient = null;

    private get Client(): ApolloClient {
        let uri = null;

        if (this._client == null) {
            try {
                uri = `http://${ApiServerOptions.host}:${ApiServerOptions.port}/graphql`;

                debug(`creating apollo client with uri ${uri}`);
                const networkInterface = createNetworkInterface({uri});

                this._client = new ApolloClient({
                    networkInterface
                });
            } catch (err) {
                debug(`failed to create apollo api client with uri ${uri}`);

                this._client = null;
            }
        }

        return this._client;
    }

    public async updateProject(id: string, state: ProjectInputSourceState): Promise<void> {
        const client = this.Client;

        if (client === null) {
            return;
        }

        const projectInput: IProjectInput = {
            id,
            input_source_state: state,
            last_checked_input_source: new Date()
        };

        if (state >= ProjectInputSourceState.Dashboard) {
            Object.assign(projectInput, {last_seen_input_source: projectInput.last_checked_input_source})
        }

        try {
            await client.mutate({
                mutation: gql`
                mutation updateProject($projectInput: ProjectInput) {
                    startTask(projectInput: $projectInput) {
                      project {
                        id
                      }
                    }
                }`,
                variables: {
                    projectInput: JSON.stringify(projectInput)
                }
            });
        } catch (err) {

        }
    }
}

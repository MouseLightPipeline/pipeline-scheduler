import {HttpLink} from "apollo-link-http";
import {ApolloClient} from "apollo-client";
import {InMemoryCache} from "apollo-cache-inmemory";

const gql = require("graphql-tag");

require("isomorphic-fetch");

import {ApiServerOptions} from "../options/coreServicesOptions";
import {IProjectInput, ProjectInputSourceState} from "../data-model/project";

const debug = require("debug")("pipeline:scheduler:pipeline-api-client");


export class PipelineApiClient {
    private static _instance: PipelineApiClient = null;

    public static Instance(): PipelineApiClient {
        if (PipelineApiClient._instance === null) {
            PipelineApiClient._instance = new PipelineApiClient();
        }

        return PipelineApiClient._instance;
    }

    private _client: any = null;

    private get Client(): any {
        let uri = null;

        if (this._client == null) {
            try {
                uri = `http://${ApiServerOptions.host}:${ApiServerOptions.port}/graphql`;

                debug(`creating apollo client with uri ${uri}`);

                this._client = new ApolloClient({
                    link: new HttpLink({uri}),
                    cache: new InMemoryCache()
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
                mutation updateProject($project: ProjectInput) {
                    updateProject(project: $project) {
                      project {
                        id
                      }
                    }
                }`,
                variables: {
                    project: projectInput
                }
            });
        } catch (err) {
            debug(err);
        }
    }
}

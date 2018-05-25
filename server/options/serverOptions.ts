interface IServiceOptions {
    port: number;
    graphQlEndpoint: string;
    graphiQlEndpoint: string;
    machineId: string;
}

interface IServerEnvDefinitions {
    production: IServiceOptions;
}

const configurations: IServerEnvDefinitions = {
    production: {
        port: 6001,
        graphQlEndpoint: "/graphql",
        graphiQlEndpoint: "/graphiql",
        machineId: "1BCC812D-97CE-4B14-AD48-5C3C9B9B416E".toLocaleLowerCase()
    }
};

function loadConfiguration(): IServiceOptions {
    const options = configurations.production;

    options.port = parseInt(process.env.PIPELINE_API_PORT) || options.port;

    return options;
}

export const ServiceOptions = loadConfiguration();


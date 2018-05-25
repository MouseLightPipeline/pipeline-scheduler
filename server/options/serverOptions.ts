interface IServiceOptions {
    port: number;
}

interface IServerEnvDefinitions {
    production: IServiceOptions;
}

const configurations: IServerEnvDefinitions = {
    production: {
        port: 6002
    }
};

function loadConfiguration(): IServiceOptions {
    const options = configurations.production;

    options.port = parseInt(process.env.PIPELINE_SCHEDULER_PORT) || options.port;

    return options;
}

export const ServiceOptions = loadConfiguration();


interface IServiceOptions {
    port: number;
    messageService: {
        host: string;
        port: number;
    }
}

const configurations: IServiceOptions = {
    port: 6002,
    messageService: {
        host: "pipeline-message-queue",
        port: 5672
    }
};

function loadConfiguration(): IServiceOptions {
    const options = configurations;

    options.port = parseInt(process.env.PIPELINE_SCHEDULER_PORT) || options.port;

    options.messageService.host = process.env.PIPELINE_CORE_SERVICES_HOST || options.messageService.host;
    options.messageService.port = parseInt(process.env.PIPELINE_MESSAGE_PORT) || options.messageService.port;

    return options;
}

export const ServiceOptions = loadConfiguration();


export interface IDriveMapping {
    local: string;
    remote: string;
}

export interface IServiceOptions {
    port: number;
    driveMapping: IDriveMapping[];
}

const configurations: IServiceOptions = {
    port: 6002,
    driveMapping: JSON.parse("[]")
};

function loadConfiguration(): IServiceOptions {
    const options = configurations;

    options.port = parseInt(process.env.PIPELINE_SCHEDULER_PORT) || options.port;
    options.driveMapping = JSON.parse(process.env.PIPELINE_DRIVE_MAPPING || null) || options.driveMapping;

    return options;
}

export const ServiceOptions = loadConfiguration();

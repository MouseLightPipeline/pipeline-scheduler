interface IPerformanceOptions {
    pipelineSchedulerIntervalSeconds: number;
    regenTileStatusSqliteChunkSize: number;
}

interface IPerformanceEnvDefinitions {
    production: IPerformanceOptions;
}

const configurations: IPerformanceEnvDefinitions = {
    production: {
        pipelineSchedulerIntervalSeconds: 30,
        regenTileStatusSqliteChunkSize: 50
    }
};

export default function (): IPerformanceOptions {
    return configurations.production;
}

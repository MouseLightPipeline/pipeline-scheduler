const coreServicesOptions = {
    database: {
        host: "pipeline-db",    // Default container name for when this is in a container orchestration
        port: 5432,             // Default port name for when this is in a container orchestration
        dialect: "postgres",
        database: "pipeline_production",
        username: "postgres",
        password: "pgsecret",
        logging: null,
        pool: {
            max: 30,
            min: 5,
            acquire: 20000,
            idle: 20000
        }
    },
    metricsDatabase: {
            host: "pipeline-metrics",
            port: 8086,
            taskDatabase: "task_metrics_db"
    },
    messageQueue: {

    }
};

function loadMetricsDatabaseOptions(options: any): any {
    // When outside a pure container environment.
    options.host = process.env.PIPELINE_CORE_SERVICES_HOST || options.host;
    options.port = parseInt(process.env.PIPELINE_INFLUX_PORT) || options.port;

    return options;
}

function loadDatabaseOptions(options: any): any {
    // When outside a pure container environment.
    options.host = process.env.PIPELINE_CORE_SERVICES_HOST || options.host;
    options.port = parseInt(process.env.PIPELINE_DATABASE_PORT) || options.port;
    options.username = process.env.PIPELINE_DATABASE_USER || options.username;
    options.password = process.env.PIPELINE_DATABASE_PASS || options.password;

    return options;
}

function loadOptions() {
    const options = Object.assign({}, coreServicesOptions);

    // When outside a pure container environment.
    options.database = loadDatabaseOptions(options.database);
    options.metricsDatabase = loadMetricsDatabaseOptions(options.metricsDatabase);

    return options;
}

export const CoreServicesOptions = loadOptions();

export const SequelizeOptions = CoreServicesOptions.database;

export const MetricsOptions = CoreServicesOptions.metricsDatabase;

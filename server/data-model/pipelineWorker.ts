import {Sequelize, Model, DataTypes} from "sequelize";

export enum PipelineWorkerStatus {
    Unavailable = 0,
    Connected,
    Idle,
    Processing
}

type WorkerDynamicProperties = {
    status: PipelineWorkerStatus;
    local_task_load: number;
    cluster_task_load: number;
}

export class PipelineWorker extends Model {
    public id: string;
    public worker_id: string;
    public name: string;
    public address: string;
    public port: number;
    public os_type: string;
    public platform: string;
    public arch: string;
    public release: string;
    public cpu_count: number;
    public total_memory: number;
    public free_memory: number;
    public load_average: number;
    public local_work_capacity: number;
    public cluster_work_capacity: number;
    public last_seen: Date;
    public is_in_scheduler_pool: boolean;

    public readonly created_at: Date;
    public readonly updated_at: Date;
    public readonly deleted_at: Date;

    private static _dynamicProperties = new Map<string, WorkerDynamicProperties>();

    private get dynamic_props(): WorkerDynamicProperties {
        let props = PipelineWorker._dynamicProperties[this.id];

        if (!props) {
            props = {
                status: PipelineWorkerStatus.Unavailable,
                local_task_load: -1,
                cluster_task_load: -1
            };

            PipelineWorker._dynamicProperties[this.id] = props;
        }

        return props;
    }

    public get status(): PipelineWorkerStatus {
        if (this.dynamic_props.status !== PipelineWorkerStatus.Unavailable && this.last_seen != null && (Date.now().valueOf() - this.last_seen.valueOf() > 60000)) {
            this.dynamic_props.status = PipelineWorkerStatus.Unavailable;
        }

        return this.dynamic_props.status;
    }

    public set status(status: PipelineWorkerStatus) {
        this.dynamic_props.status = status;
    }

    public get local_task_load(): number {
        return this.dynamic_props.local_task_load;
    }

    public set local_task_load(load: number) {
        this.dynamic_props.local_task_load = load;
    }

    public get cluster_task_load(): number {
        return this.dynamic_props.cluster_task_load;
    }

    public set cluster_task_load(load: number) {
        this.dynamic_props.cluster_task_load = load;
    }

    public static async getForWorkerId(workerId: string): Promise<PipelineWorker> {
        let worker = await PipelineWorker.findOne({where: {worker_id: workerId}});

        if (!worker) {
            worker = await PipelineWorker.create({worker_id: workerId});
        }

        return worker;
    }
}

const TableName = "PipelineWorkers";

export const modelInit = (sequelize: Sequelize) => {
    PipelineWorker.init({
        id: {
            primaryKey: true,
            type: DataTypes.UUID,
            defaultValue: DataTypes.UUIDV4
        },
        worker_id: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        name: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        address: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        port: {
            type: DataTypes.INTEGER,
            defaultValue: 0
        },
        os_type: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        platform: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        arch: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        release: {
            type: DataTypes.TEXT,
            defaultValue: ""
        },
        cpu_count: {
            type: DataTypes.INTEGER,
            defaultValue: 0
        },
        total_memory: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        free_memory: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        load_average: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        local_work_capacity: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        cluster_work_capacity: {
            type: DataTypes.DOUBLE,
            defaultValue: 0
        },
        last_seen: {
            type: DataTypes.DATE
        },
        is_in_scheduler_pool: {
            type: DataTypes.BOOLEAN,
            defaultValue: false
        }
    }, {
        tableName: TableName,
        timestamps: true,
        createdAt: "created_at",
        updatedAt: "updated_at",
        deletedAt: "deleted_at",
        paranoid: true,
        sequelize
    });
};

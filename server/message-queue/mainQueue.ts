import * as amqp from "amqplib";

import {MessageQueueOptions} from "../options/coreServicesOptions";
import {Connection, Channel} from "amqplib";
import {IWorkerTaskExecutionAttributes} from "../data-model/taskExecution";
import {SchedulerHub} from "../schedulers/schedulerHub";

const debug = require("debug")("pipeline:main-queue");

const TaskExecutionUpdateQueue = "TaskExecutionUpdateQueue";

export class MainQueue {
    private static instance: MainQueue = new MainQueue();

    private connection: Connection = null;
    private channel: Channel = null;

    public static get Instance() {
        return this.instance;
    }

    public async Connect(): Promise<void> {
        const url = `amqp://${MessageQueueOptions.host}:${MessageQueueOptions.port}`;

        debug(`main queue url: ${url}`);

        try {
            this.connection = await amqp.connect(url);

            this.channel = await this.connection.createChannel();

            await this.channel.assertQueue(TaskExecutionUpdateQueue, {durable: true});
        } catch (err) {
            debug("failed to connect");
            debug(err);
            return;
        }

        await this.channel.consume(TaskExecutionUpdateQueue, async (msg) => {
            const taskExecution: IWorkerTaskExecutionAttributes = JSON.parse(msg.content.toString());
            await SchedulerHub.Instance.onTaskExecutionComplete(taskExecution);
            this.channel.ack(msg);
        }, {noAck: false});

        debug(`main queue ready`);
    }
}

import { Kafka, Consumer, LogEntry } from "kafkajs";
import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import { ILogger } from "winston-logger-kafka";
import {KafkaConnectorConfig, KafkaConnector, KafkaListener, KafkaProducer, ListenerConfig} from "./kafka_connector"

export type TItem = {
    [key: string]: any
}

export interface KafkaAppConfig {
    connectorConfig: KafkaConnectorConfig;
    messageType: TItem;
}

export class KafkaApp {
    public config: KafkaAppConfig;
    private readonly _kafkaConnector: KafkaConnector;
    private _kafkaListener: KafkaListener | any;
    private _kafkaProducer: KafkaProducer | any;
    private _event_map: TItem;
    private readonly _logger: kafka.Logger;

    constructor(config: KafkaAppConfig) {
        this.config = config;
        this._kafkaConnector = new KafkaConnector(this.config.connectorConfig);
        this._logger = this._kafkaConnector.logger;
        this._event_map = {};
    }

    private async processMessage(payload: kafka.EachMessagePayload) {
        const msg_type = this.config.messageType[payload.topic]
        if (payload.message.value) {

        }
    }

    public async init() {
        this._kafkaListener = await this._kafkaConnector.getListener();
        this._kafkaProducer = await this._kafkaConnector.getProducer();
    }

    public static async create(config: KafkaAppConfig) {
        const newObject = new KafkaApp(config);
        await newObject.init();
        return newObject;
    }

    public async run() {
        await this._kafkaListener.listen()
    }

    public on(eventName: string, cb: void) {
        this._event_map[eventName] = cb;
    }

    public async emit(record: kafka.ProducerRecord) {
        await this._kafkaProducer.send(record)
    }
}


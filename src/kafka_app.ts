import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import { KafkaConnector, KafkaListener, KafkaProducer } from "./kafka_connector"

export type TItem = {
    [key: string]: any
}

export interface ConnectorConfig {
    clientConfig: kafka.KafkaConfig;
    producerConfig: kafka.ProducerConfig;
    consumerConfig: kafka.ConsumerConfig;
    topics: kafka.ConsumerSubscribeTopic[];
    loggerConfig: {
        loggerConfig: Logger.LoggerConfig;
        sinks?: Logger.Sink[];
    };
}

export interface KafkaAppConfig {
    connectorConfig: ConnectorConfig;
    processMessageCb?: (payload: kafka.EachMessagePayload) => void;
}

export interface Error {
    msg: string
}

export interface Message {
    event: string;
    request_id: string
    payload?: any[];
    error?: Error
    metadata?: TItem
}

export class KafkaApp {
    public readonly config: KafkaAppConfig;
    private readonly _kafkaConnector: KafkaConnector;
    public kafkaListener!: KafkaListener;
    public kafkaProducer!: KafkaProducer;
    public eventMap: TItem;
    public readonly logger: kafka.Logger;

    constructor(config: KafkaAppConfig) {
        this.config = config;
        const consumerRunConfig: kafka.ConsumerRunConfig = {
            autoCommit: true,
            eachMessage: this.processMessage.bind(this)
        }
        this._kafkaConnector = new KafkaConnector({
            clientConfig: this.config.connectorConfig.clientConfig,
            producerConfig: this.config.connectorConfig.producerConfig,
            consumerConfig: this.config.connectorConfig.consumerConfig,
            consumerRunConfig: consumerRunConfig,
            topics: this.config.connectorConfig.topics,
            loggerConfig: this.config.connectorConfig.loggerConfig
        });
        this.logger = this._kafkaConnector.logger;
        this.eventMap = {};
    }

    private async init() {
        this.kafkaListener = await this._kafkaConnector.getListener();
        this.kafkaProducer = await this._kafkaConnector.getProducer();
    }

    public static async create(config: KafkaAppConfig) {
        const newObject = new KafkaApp(config);
        await newObject.init();
        return newObject;
    }

    private async processMessage(payload: kafka.EachMessagePayload) {
        try {
            if(this.config.processMessageCb) {
                this.config.processMessageCb(payload);
            }
            const _value = payload.message.value
            if (_value) {

                const receivedMessage: Message = JSON.parse(_value.toString());
                if (receivedMessage.event) {
                    this.logger.info(`Received message: event: ${receivedMessage.event}, payload: ${receivedMessage.payload}`);
                    const key = [payload.topic, receivedMessage.event].join('.')
                    const cb = this.eventMap[key];
                    if(cb.constructor.name === "AsyncFunction") {
                        await cb(receivedMessage);
                    } else {
                        cb(receivedMessage);
                    }
                    // this.eventMap[receivedMessage.event](receivedMessage);
                }
                // else {
                //     this.logger.error('Received message is missing property "event".');
                // }
            }

            // await this.kafkaListener.kafkaConsumer.commitOffsets([{
            //     topic: payload.topic,
            //     partition: payload.partition,
            //     offset: (Number(payload.message.offset) + 1).toString()
            // }]);
        }
        catch (e) {
            console.log(e)
        }

    }

    public async run() {
        await this.kafkaListener.listen();
    }

    public on(
        eventName: string,
        cb: (message: Message, kwargs?: TItem) => void,
        topic?: string
    ) {
        if (topic) {
            const key = [topic, eventName].join('.')
            this.eventMap[key] = cb;
        } else {
            this.config.connectorConfig.topics.forEach(topicConf => {
                const key = [topicConf.topic, eventName].join('.')
                this.eventMap[key] = cb;
            })
        }
    }

    public async emit(record: kafka.ProducerRecord) {
        await this.kafkaProducer.send(record);
    }

    public async close() {
        this.logger.info('Closing application...')
        this.kafkaProducer.close().then(() => {
            this.kafkaListener.close().then(() => {
                this.logger.info("Application closed.");
            });
        });
    }
}


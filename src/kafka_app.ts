import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import {KafkaConnector, KafkaListener, ListenerConfig as ConnectorListenerConfig,
    KafkaProducer, ProducerConfig, ILogger, } from "./kafka_connector"

const path = require("path");

export interface ListenerConfig extends ConnectorListenerConfig {
    keyDeserializer?: (key: Buffer) => string
    valueDeserializer?: (key: Buffer) => any
}

export interface KafkaAppConfig {
    appName?: string
    clientConfig: kafka.KafkaConfig
    producerConfig?: ProducerConfig
    listenerConfig: ListenerConfig
    // topics: kafka.ConsumerSubscribeTopic[]
    messageKeyAsEvent?: boolean
    middlewareBatchCb?: (payload: kafka.EachBatchPayload) => void
    logger?: ILogger
    kafkaLogger?: ILogger

}

export interface KafkaMessageMetadata {
    topic: string
    partition: number
    highWatermark: string
    timestamp: string
    size: number
    attributes: number
    offset: string
    headers?: kafka.IHeaders
}

export class KafkaApp {
    public readonly config: KafkaAppConfig;
    private readonly _kafkaConnector: KafkaConnector;
    public kafkaListener!: KafkaListener;
    public kafkaProducer!: KafkaProducer;
    public eventMap: Record<string, any>;
    public readonly logger: ILogger;

    constructor(config: KafkaAppConfig) {
        this.config = config;
        if (!this.config.appName) {
            this.config.appName = 'Kafka application JS'
        }

        this.config.listenerConfig.eachBatch = this.processBatch.bind(this)
        if (this.config.logger) {
            this.logger = this.config.logger;
        } else {
            this.logger = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: this.config.appName,
                level: Logger.Levels.DEBUG
            })
        }

        this._kafkaConnector = new KafkaConnector({
            clientConfig: this.config.clientConfig,
            producerConfig: this.config.producerConfig,
            listenerConfig: this.config.listenerConfig,
            logger: ((kafkaLogger) => {
                if (kafkaLogger) {
                    return kafkaLogger;
                } else {
                    return Logger.getDefaultLogger({
                        module: path.basename(__filename),
                        component: `${this.config.appName}_kafka_client`,
                        level: Logger.Levels.DEBUG
                    })
                }
            })(this.config.kafkaLogger)
        });

        this.eventMap = {};
    }

    private async init() {
        this.kafkaListener = await this._kafkaConnector.getListener();
        this.kafkaProducer = await this._kafkaConnector.getProducer();
        this.logger.info(`${this.config.appName} is up and running.`);
    }

    public static async create(config: KafkaAppConfig) {
        const app = new KafkaApp(config);
        await app.init();
        return app;
    }

    private getEventCallback(topic: string, messageValue: any, messageKey: string | null) {
        if (messageValue) {
            let cbKey: string;
            let cb: (message: any, kwargs?: Record<string, any>) => void;

            if (this.config.messageKeyAsEvent) {
                if (!messageKey) {
                    return null
                }

                cbKey = [topic, messageKey].join('.');
                cb = this.eventMap[cbKey]

            } else {
                const event = messageValue['event'];
                if (!event) {
                    throw Error('"event" property is missing in message.value object. ' +
                        'Provide "event" property or set "message_key_as_event" option to True ' +
                        'to use message.key as event name.')
                }

                cbKey = [topic, event].join('.');
                cb = this.eventMap[cbKey]
            }

            return {messageKey, cb}
        } else {
            return null
        }
    }

    private async processBatch(payload: kafka.EachBatchPayload) {
        if (this.config.middlewareBatchCb) {
            this.config.middlewareBatchCb(payload);
        }

        for (const message of payload.batch.messages) {
            const messageValue = (val => {
                if (val) {
                    if (this.config.listenerConfig?.valueDeserializer) {
                        return this.config.listenerConfig.valueDeserializer(val)
                    } else {
                        return JSON.parse(val.toString())
                    }
                } else {
                    return null
                }
            })(message.value);

            const messageKey = (val => {
                if (val) {
                    if (this.config.listenerConfig?.keyDeserializer) {
                        return this.config.listenerConfig.keyDeserializer(val)
                    } else {
                        return val.toString()
                    }
                } else {
                    return null
                }
            })(message.key);

            const eventCallback = this.getEventCallback(payload.batch.topic, messageValue, messageKey)

            const messageMetadata: KafkaMessageMetadata = {
                topic: payload.batch.topic,
                partition: payload.batch.partition,
                highWatermark: payload.batch.highWatermark,
                timestamp: message.timestamp,
                size: message.size,
                attributes: message.attributes,
                offset: message.offset,
                headers: message.headers
            };

            if (eventCallback) {
                if (eventCallback.cb.constructor.name === "AsyncFunction") {
                    await eventCallback.cb(messageValue, messageMetadata);
                } else {
                    eventCallback.cb(messageValue, messageMetadata);
                }
            }

            const listenerConfig = this.config.listenerConfig
            if (!listenerConfig?.autoCommit && !listenerConfig?.eachBatchAutoResolve) {
                payload.resolveOffset(message.offset);
                await payload.commitOffsetsIfNecessary();
                await payload.heartbeat();
            }
        }
    }

    public async run() {
        await this.kafkaListener.listen();
    }

    public on(
        eventName: string,
        cb: (message: any, metadata: KafkaMessageMetadata) => void,
        topic?: string
    ) {
        if (topic) {
            const key = [topic, eventName].join('.')
            this.eventMap[key] = cb;
        } else {
            this.config.listenerConfig?.topics.forEach(topicConf => {
                const key = [topicConf.topic, eventName].join('.')
                this.eventMap[key] = cb;
            })
        }
    }

    public async emit(record: kafka.ProducerRecord) {
        return await this.kafkaProducer.send(record);
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


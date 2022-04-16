import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import {KafkaConnector, KafkaListener, ListenerConfig, KafkaProducer, ProducerConfig, ILogger, } from "./kafka_connector"

export type TItem = {
    [key: string]: any
}

const path = require("path");

// export interface ConsumerConfig extends kafka.ConsumerConfig {
//     autoCommit?: boolean
//     autoCommitInterval?: number | null
//     autoCommitThreshold?: number | null
//     eachBatchAutoResolve?: boolean
//     partitionsConsumedConcurrently?: number
//     keyDeserializer?: (key: Buffer) => string
//     valueDeserializer?: (key: Buffer) => any
// }

export interface AppListenerConfig extends ListenerConfig {
    keyDeserializer?: (key: Buffer) => string
    valueDeserializer?: (key: Buffer) => any
}

export interface KafkaAppConfig {
    appName?: string
    clientConfig: kafka.KafkaConfig
    producerConfig?: ProducerConfig
    listenerConfig: AppListenerConfig
    // topics: kafka.ConsumerSubscribeTopic[]
    messageKeyAsEvent?: boolean
    middlewareBatchCb?: (payload: kafka.EachBatchPayload) => void
    logger?: ILogger
    kafkaLogger?: ILogger

}

export interface KafkaMessageMetadata {
    topic: string
    partition: number
    highWatermark?: string
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
    public eventMap: TItem;
    public readonly logger: ILogger;

    constructor(config: KafkaAppConfig) {
        this.config = config;
        if (!this.config.appName) {
            this.config.appName = 'Kafka application JS'
        }
        // const consumerRunConfig: kafka.ConsumerRunConfig = {
        //     autoCommit: this.config.consumerConfig?.autoCommit,
        //     autoCommitInterval: this.config.consumerConfig?.autoCommitInterval,
        //     autoCommitThreshold: this.config.consumerConfig?.autoCommitThreshold,
        //     eachBatchAutoResolve: this.config.consumerConfig?.eachBatchAutoResolve,
        //     partitionsConsumedConcurrently: this.config.consumerConfig?.partitionsConsumedConcurrently,
        //     eachBatch: this.processBatch.bind(this)
        // }
        this.config.listenerConfig.eachBatch = this.processBatch.bind(this)
        if (this.config.logger) {
            this.logger = this.config.logger;
        } else {
            // let component = 'Kafka application JS';
            // if (this.config.appName) {
            //     component = this.config.appName;
            // }

            this.logger = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: this.config.appName,
                level: Logger.Levels.DEBUG
            })
        }

        if (this.config.kafkaLogger) {

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

    private getEventCallback(topic: string, message_value: any, message_key: string | null) {
        if (message_value) {
            let cbKey: string;
            let cb: (message: any, kwargs?: TItem) => void;

            if (this.config.messageKeyAsEvent) {
                if (!message_key) {
                    return null
                }

                cbKey = [topic, message_key].join('.');
                cb = this.eventMap[cbKey]

            } else {
                const event = message_value['event'];
                if (!event) {
                    throw Error('"event" property is missing in message.value object. ' +
                        'Provide "event" property or set "message_key_as_event" option to True ' +
                        'to use message.key as event name.')
                }

                cbKey = [topic, event].join('.');
                cb = this.eventMap[cbKey]
            }

            return {message_key, cb}
        } else {
            return null
        }
    }

    private async processBatch(payload: kafka.EachBatchPayload) {
        if (this.config.middlewareBatchCb) {
            this.config.middlewareBatchCb(payload);
        }

        for (let message of payload.batch.messages) {
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
                // if (consumerConf?.autoCommitInterval || consumerConf?.autoCommitThreshold) {
                //     console.log('commitOffsetsIfNecessary()')
                //     await payload.commitOffsetsIfNecessary();
                //     await payload.heartbeat();
                // } else {
                //     console.log('resolveOffset()')
                //     payload.resolveOffset(message.offset);
                //     await payload.commitOffsetsIfNecessary();
                //     await payload.heartbeat();
                // }

                payload.resolveOffset(message.offset);
                await payload.commitOffsetsIfNecessary();
                await payload.heartbeat();
            }
        }
    }

    // private async processMessage(payload: kafka.EachMessagePayload) {
    //     if (this.config.middlewareMessageCb) {
    //         this.config.middlewareMessageCb(payload);
    //     }
    //
    //     const message = payload.message;
    //
    //     const messageValue = (val => {
    //         if (val) {
    //             if (this.config.consumerConfig && this.config.consumerConfig.valueDeserializer) {
    //                 return this.config.consumerConfig.valueDeserializer(val)
    //             } else {
    //                 return JSON.parse(val.toString())
    //             }
    //         } else {
    //             return null
    //         }
    //     })(message.value);
    //
    //     const messageKey = (val => {
    //         if (val) {
    //             if (this.config.consumerConfig && this.config.consumerConfig.keyDeserializer) {
    //                 return this.config.consumerConfig.keyDeserializer(val)
    //             } else {
    //                 return val.toString()
    //             }
    //         } else {
    //             return null
    //         }
    //     })(message.key);
    //
    //     const eventCallback = this.getEventCallback(payload.topic, messageValue, messageKey)
    //
    //     const messageMetadata: KafkaMessageMetadata = {
    //         topic: payload.topic,
    //         partition: payload.partition,
    //         timestamp: message.timestamp,
    //         size: message.size,
    //         attributes: message.attributes,
    //         offset: message.offset,
    //         headers: message.headers
    //     }
    //
    //     if (eventCallback) {
    //         if (eventCallback.cb.constructor.name === "AsyncFunction") {
    //             await eventCallback.cb(messageValue, messageMetadata);
    //         } else {
    //             eventCallback.cb(messageValue, messageMetadata);
    //         }
    //     }
    // }

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


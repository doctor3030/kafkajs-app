import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import {v4 as uuidv4} from 'uuid';
import {
    KafkaConnector, KafkaListener, ListenerConfig as ConnectorListenerConfig,
    KafkaProducer, ProducerConfig, ILogger,
} from "./kafka_connector"
import {MessagePipeline} from "./kafka_app_pipeline"
import {KafkaMessage, Offsets, PartitionOffset} from "kafkajs";
import {createHash} from "crypto"
import {log} from "async";


const path = require("path");

export interface ListenerConfig extends ConnectorListenerConfig {
    keyDeserializer?: (key: Buffer) => string
    valueDeserializer?: (key: Buffer) => any
}

interface EmitWithResponseOptions {
    topicEventList: [string, string][];
    cacheClient: any;
    returnEventTimeout: number; // milliseconds
}

export interface KafkaAppConfig {
    appName?: string
    appId?: string
    clientConfig: kafka.KafkaConfig
    producerConfig?: ProducerConfig
    listenerConfig: ListenerConfig
    messageKeyAsEvent?: boolean
    middlewareBatchCb?: (payload: kafka.EachBatchPayload) => void
    emitWithResponseOptions?: EmitWithResponseOptions
    pipelinesMap?: Record<string, MessagePipeline>
    maxConcurrentTasks?: number
    maxConcurrentPipelines?: number
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

export interface SyncTask {
    handle: (message: any, metadata: KafkaMessageMetadata) => void,
    message: any,
    metadata: KafkaMessageMetadata
}

export interface AsyncTask {
    handle: (message: any, metadata: KafkaMessageMetadata) => Promise<void>,
    message: any,
    metadata: KafkaMessageMetadata
}

export interface PipelineTask {
    pipeline: MessagePipeline,
    message: any,
    metadata: KafkaMessageMetadata
}

export class KafkaApp {
    public appId: string;
    public appName: string;
    public readonly config: KafkaAppConfig;
    private readonly _kafkaConnector: KafkaConnector;
    public kafkaListener!: KafkaListener;
    public kafkaProducer!: KafkaProducer;
    public readonly eventMap: Record<string, ((message: any, metadata: KafkaMessageMetadata) => void) | ((message: any, metadata: KafkaMessageMetadata) => Promise<void>)>;
    public pipelinesMap: Record<string, MessagePipeline>
    public maxConcurrentTasks: number;
    public maxConcurrentPipelines: number
    public readonly logger: ILogger;

    private syncTasksQueue: SyncTask[]
    private asyncTasksQueue: AsyncTask[]
    private pipelinesQueue: PipelineTask[]
    private cachingQueue: PipelineTask[]
    private stop: boolean;

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
                level: Logger.Levels.INFO
            })
        }

        if (this.config.appId) {
            this.appId = this.config.appId;
        } else {
            this.appId = uuidv4();
        }

        if (this.config.appName) {
            this.appName = this.config.appName;
        } else {
            this.appName = 'Kafka application'
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
        this.pipelinesMap = {};

        if (this.config.pipelinesMap) {
            this.pipelinesMap = this.config.pipelinesMap;
            const cachingPipelinesMap: Record<string, MessagePipeline> = {};

            Object.values(this.pipelinesMap).forEach((pipeline) => {
                pipeline.appId = this.appId;

                pipeline.transactions.forEach((txn) => {
                    if (txn.pipeResultOptions && txn.pipeResultOptions.withResponseOptions) {
                        this.registerCachingPipeline(
                            txn.pipeResultOptions.withResponseOptions.responseFromTopic,
                            txn.pipeResultOptions.withResponseOptions.responseEventName,
                            txn.pipeResultOptions.withResponseOptions.cacheClient,
                            cachingPipelinesMap
                        );
                    } else if (txn.pipeResultOptionsCustom && txn.pipeResultOptionsCustom.withResponseOptions) {
                        txn.pipeResultOptionsCustom.withResponseOptions.responseEventTopicKeys.forEach((k) => {
                            this.registerCachingPipeline(
                                k[0],
                                k[1],
                                txn.pipeResultOptionsCustom!.withResponseOptions!.cacheClient,
                                cachingPipelinesMap
                            );
                        })
                    }
                })

                this.pipelinesMap = Object.assign({}, this.pipelinesMap, cachingPipelinesMap);
            })
        }

        if (this.config.emitWithResponseOptions) {
            this.config.emitWithResponseOptions.topicEventList.forEach((topicEventPair) => {
                this.registerCachingPipeline(
                    topicEventPair[0],
                    topicEventPair[1],
                    this.config.emitWithResponseOptions?.cacheClient,
                    this.pipelinesMap
                )
            })
        }

        this.syncTasksQueue = [];
        this.asyncTasksQueue = [];
        this.pipelinesQueue = [];
        this.cachingQueue = [];

        this.maxConcurrentTasks = this.config.maxConcurrentTasks ? this.config.maxConcurrentTasks : 100;
        this.maxConcurrentPipelines = this.config.maxConcurrentPipelines ? this.config.maxConcurrentPipelines : 100;

        this.stop = false;
    }

    private async init() {
        this.kafkaListener = await this._kafkaConnector.getListener();
        this.kafkaProducer = await this._kafkaConnector.getProducer();
        // this.logger.info(`${this.config.appName} is up and running.`);
    }

    public static async create(config: KafkaAppConfig) {
        const app = new KafkaApp(config);
        await app.init();
        return app;
    }

    private registerCachingPipeline(
        topic: string,
        event: string,
        cacheClient: any,
        cachingPipelinesMap: Record<string, MessagePipeline>
    ) {
        const key = `${topic}.${event}`
        cachingPipelinesMap[key] = new MessagePipeline(
            {
                name: `caching.${key}`,
                transactions: [{
                    fnc: this.cachePipeResponse.bind(this),
                    args: {"cache_client": cacheClient}
                }],
                logger: this.logger,
                appId: this.appId
            }
        )
    }

    private async cachePipeResponse(
        message: any,
        logger: any,
        // cacheClient: any,
        kwargs: Record<string, any>
    ): Promise<any> {
        try {
            const eventName = this.config.messageKeyAsEvent ? kwargs.key : message.event;
            const headers = kwargs.headers;
            const cacheClient = kwargs.cache_client
            if (headers) {
                Object.values(headers).map((val: any) => Buffer.isBuffer(val) ? new TextDecoder('utf-8').decode(val) : val);
            }

            logger.info(
                `--------> CACHING PIPE RESPONSE: name: ${eventName}; event_id: ${headers.event_id}`
            );

            const key = this.getEventIdHash(headers.event_id);
            await cacheClient.set(key, JSON.stringify(message));
            await cacheClient.expire(key, 300);

            return message;
        } catch (e: any) {
            const msg = `cachePipeResponse => Exception: message: ${e.message}; stack: ${e.stack};`
            logger.error(msg)
        }
    }

    private getEventIdHash(eventId: string): string {
        return createHash('sha256')
            .update(eventId)
            .digest('hex')
    }

    // private getEventCallback(topic: string, messageValue: any, messageKey: string | null) {
    //     if (messageValue) {
    //         let cbKey: string;
    //         let cb: (message: any, metadata: KafkaMessageMetadata) => void;
    //
    //         if (this.config.messageKeyAsEvent) {
    //             if (!messageKey) {
    //                 return null
    //             }
    //
    //             cbKey = [topic, messageKey].join('.');
    //             cb = this.eventMap[cbKey]
    //
    //         } else {
    //             const event = messageValue['event'];
    //             if (!event) {
    //                 throw Error('"event" property is missing in message.value object. ' +
    //                     'Provide "event" property or set "message_key_as_event" option to True ' +
    //                     'to use message.key as event name.')
    //             }
    //
    //             cbKey = [topic, event].join('.');
    //             cb = this.eventMap[cbKey]
    //         }
    //
    //         return {messageKey, cb}
    //     } else {
    //         return null
    //     }
    // }

    // private counters = {
    //     nBatch: 0,
    //     nMsg: 0
    // }

    private async processBatch(payload: kafka.EachBatchPayload) {
        try {
            // this.counters.nBatch++;

            if (this.config.middlewareBatchCb) {
                this.config.middlewareBatchCb(payload);
            }

            await Promise.all(
                payload.batch.messages.map(async (msg) => {
                    payload.resolveOffset(msg.offset);

                    const metadata: KafkaMessageMetadata = {
                        topic: payload.batch.topic,
                        partition: payload.batch.partition,
                        highWatermark: payload.batch.highWatermark,
                        timestamp: msg.timestamp,
                        size: msg.size,
                        attributes: msg.attributes,
                        offset: msg.offset,
                        headers: msg.headers
                    };

                    await this.processMessage(msg, metadata);

                    const listenerConfig = this.config.listenerConfig
                    if (!listenerConfig?.autoCommit && !listenerConfig?.eachBatchAutoResolve) {
                        payload.resolveOffset(msg.offset);

                        const offsets: Offsets = {
                            topics: [{
                                topic: payload.batch.topic,
                                partitions: [{
                                    partition: payload.batch.partition,
                                    offset: (Number(msg.offset) + 1).toString()
                                }]
                            }]
                        };
                        await payload.commitOffsetsIfNecessary(offsets);
                        await payload.heartbeat();
                    }
                })
            )

            // for (const message of payload.batch.messages) {
            //     const messageValue = (val => {
            //         if (val) {
            //             if (this.config.listenerConfig?.valueDeserializer) {
            //                 return this.config.listenerConfig.valueDeserializer(val)
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
            //             if (this.config.listenerConfig?.keyDeserializer) {
            //                 return this.config.listenerConfig.keyDeserializer(val)
            //             } else {
            //                 return val.toString()
            //             }
            //         } else {
            //             return null
            //         }
            //     })(message.key);
            //
            //     const eventCallback = this.getEventCallback(payload.batch.topic, messageValue, messageKey)
            //
            //     const messageMetadata: KafkaMessageMetadata = {
            //         topic: payload.batch.topic,
            //         partition: payload.batch.partition,
            //         highWatermark: payload.batch.highWatermark,
            //         timestamp: message.timestamp,
            //         size: message.size,
            //         attributes: message.attributes,
            //         offset: message.offset,
            //         headers: message.headers
            //     };
            //
            //     if (eventCallback) {
            //         if (eventCallback.cb.constructor.name === "AsyncFunction") {
            //             await eventCallback.cb(messageValue, messageMetadata);
            //         } else {
            //             eventCallback.cb(messageValue, messageMetadata);
            //         }
            //     }
            //
            //     const listenerConfig = this.config.listenerConfig
            //     if (!listenerConfig?.autoCommit && !listenerConfig?.eachBatchAutoResolve) {
            //         payload.resolveOffset(message.offset);
            //
            //         const offsets: Offsets = {
            //             topics: [{
            //                 topic: payload.batch.topic,
            //                 partitions: [{
            //                     partition: payload.batch.partition,
            //                     offset: (Number(message.offset) + 1).toString()
            //                 }]
            //             }]
            //         };
            //         await payload.commitOffsetsIfNecessary(offsets);
            //         await payload.heartbeat();
            //     }
            // }

            // const listenerConfig = this.config.listenerConfig
            // if (!listenerConfig?.autoCommit && !listenerConfig?.eachBatchAutoResolve) {
            //     // payload.resolveOffset(message.offset);
            //
            //     const offsets: Offsets = {
            //         topics: [{
            //             topic: payload.batch.topic,
            //             partitions: payload.batch.messages.map((msg) => {
            //                 return {
            //                     partition: payload.batch.partition,
            //                     offset: (Number(msg.offset) + 1).toString()
            //                 } as PartitionOffset
            //             })
            //         }]
            //     };
            //     await payload.commitOffsetsIfNecessary(offsets);
            //     await payload.heartbeat();
            // }

        } catch (e: any) {
            const msg = `processBatch => Exception: 
            message: ${e.message}; 
            stack: ${e.stack};`;
            this.logger.error(msg);
        }
    }

    private async processMessage(message: KafkaMessage, metadata: KafkaMessageMetadata) {
        try {
            // this.counters.nMsg++

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
            let pipeline: MessagePipeline | undefined;
            let handle: any | undefined;

            if (this.config.messageKeyAsEvent && messageKey) {
                if (this.pipelinesMap[messageKey]) {
                    pipeline = this.pipelinesMap[messageKey];
                } else if (this.pipelinesMap[`${metadata.topic}.${messageKey}`]) {
                    pipeline = this.pipelinesMap[`${metadata.topic}.${messageKey}`];
                }

                handle = this.eventMap[`${metadata.topic}.${messageKey}`];

            } else {
                const event = messageValue.event;
                if (!event) {
                    throw new Error(
                        '"event" property is missing in message.value object. ' +
                        'Provide "event" property or set "message_key_as_event" option ' +
                        'to True to use message.key as event name.'
                    );
                }

                if (this.pipelinesMap[event]) {
                    pipeline = this.pipelinesMap[event];
                } else if (this.pipelinesMap[`${metadata.topic}.${event}`]) {
                    pipeline = this.pipelinesMap[`${metadata.topic}.${event}`];
                }

                handle = this.eventMap[`${metadata.topic}.${event}`];
            }

            if (handle) {
                if (handle.constructor.name === "AsyncFunction") {
                    this.asyncTasksQueue.push({
                        handle: handle,
                        message: messageValue,
                        metadata: metadata
                    });
                } else {
                    this.syncTasksQueue.push({
                        handle: handle,
                        message: messageValue,
                        metadata: metadata
                    });
                }
            }

            if (pipeline) {
                if ((pipeline.transactions[0].fnc as (...args: any[]) => any).name === "cachePipeResponse") {
                    this.cachingQueue.push({pipeline: pipeline, message: messageValue, metadata: metadata});
                } else {
                    this.pipelinesQueue.push({pipeline: pipeline, message: messageValue, metadata: metadata});
                }
            }

        } catch (e: any) {
            const msg = `processMessage => Exception: 
            message: ${e.message}; 
            stack: ${e.stack};`;
            this.logger.error(msg);
        }
    }

    public async run() {
        await this.kafkaListener.listen();
        await this.processSyncTasks();
        await this.processAsyncTasks();
        await this.processPipelines();
        await this.processCaching();

        this.logger.info(`${this.appName} is up and running.`)
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

    public async emitWithResponse(record: kafka.ProducerRecord): Promise<{payload: any, error: string | null}> {
        try {
            if (!this.config.emitWithResponseOptions) {
                throw Error('Please provide emitWithResponseOptions in the application config.')
            }

            const eventId = uuidv4();
            const headers = record.messages[0].headers ? record.messages[0].headers : {};
            headers.event_id = eventId;
            record.messages[0].headers = headers;

            this.logger.info(
                `<-------- PERFORMING EMIT WITH RESPONSE: to: ${record.topic}; event_id: ${eventId}`
            )

            await this.emit(record);

            const timeUp = Date.now();

            const result: any = await new Promise((resolve, reject) => {
                const interval = setInterval(
                    async () => {
                        const response = await this.config.emitWithResponseOptions?.cacheClient.get(this.getEventIdHash(eventId));

                        if (response) {
                            clearInterval(interval);
                            resolve(JSON.parse(response));
                        }

                        if (Date.now() - timeUp > this.config.emitWithResponseOptions?.returnEventTimeout!) {
                            clearInterval(interval);
                            reject(
                                new Error(
                                    `Emit event with response timeout: to: ${record.topic}; event_id: ${eventId}`
                                )
                            );
                        }

                    },
                    1);
            });

            // let result: any | null;
            // const interval = setInterval(
            //     async () => {
            //         console.log('AAAAAAAAAAAAAAA')
            //         const response = await this.config.emitWithResponseOptions?.cacheClient.get(this.getEventIdHash(eventId));
            //
            //         if (response) {
            //             clearInterval(interval);
            //             result = JSON.parse(response);
            //         }
            //
            //         if (Date.now() - timeUp > this.config.emitWithResponseOptions?.returnEventTimeout!) {
            //             clearInterval(interval);
            //             throw new Error(
            //                 `Emit event with response timeout: to: ${record.topic}; event_id: ${eventId}`
            //             )
            //         }
            //
            //     },
            //     1);

            return {payload: result, error: null};
        } catch (e: any) {
            const msg = `emit_with_response => Exception: message: ${e.message}; stack: ${e.stack};`;
            this.logger.error(msg);
            return {payload: null, error: msg};
        }
    }


    private async processSyncTasks() {
        // while (true) {
        //     if (this.stop && this.syncTasksQueue.length === 0) {
        //         break; // Exit the loop if stop is true and queue is empty
        //     }
        //
        //     // Pick task from the queue
        //     const task = this.syncTasksQueue.shift();
        //     if (task) {
        //         const {handle, message, metadata} = task;
        //         // Execute transaction function of the task
        //         handle(message, metadata);
        //     }
        // }

        const interval = setInterval(
            () => {
                // console.log(`processSyncTasks: stop = ${this.stop}; syncTasksQueue.length = ${this.syncTasksQueue.length === 0}`)
                if (this.stop && this.syncTasksQueue.length === 0) {
                    clearInterval(interval); // Exit the loop if stop is true and queue is empty
                }

                const task = this.syncTasksQueue.shift();
                // console.log(`this.syncTasksQueue.length = ${this.syncTasksQueue.length}`)
                // console.log(JSON.stringify(task))
                // console.log(JSON.stringify(this.counters))
                if (task) {
                    const {handle, message, metadata} = task;
                    // Execute transaction function of the task
                    handle(message, metadata);
                }

            },
            1);
    }

    private async processAsyncTasks() {
        // while (true) {
        //     if (this.stop && this.asyncTasksQueue.length === 0) {
        //         break; // Exit the loop if stop is true and queue is empty
        //     }
        //
        //     // Get tasks batch
        //     const batchSize = Math.min(this.maxConcurrentTasks, this.asyncTasksQueue.length);
        //     const batch: AsyncTask[] = [];
        //
        //     for (let i = 0; i < batchSize; i++) {
        //         const task = this.asyncTasksQueue.shift();
        //         if (task) {
        //             batch.push(task);
        //         }
        //     }
        //
        //     // Execute tasks from the batch concurrently
        //     await Promise.all(
        //         batch.map(async ({handle, message, metadata}) => {
        //             await handle(message, metadata);
        //         })
        //     )
        // }

        const interval = setInterval(
            async () => {
                // console.log(`processAsyncTasks: stop = ${this.stop}; asyncTasksQueue.length = ${this.asyncTasksQueue.length === 0}`)
                if (this.stop && this.asyncTasksQueue.length === 0) {
                    clearInterval(interval); // Exit the loop if stop is true and queue is empty
                }

                // Get tasks batch
                const batchSize = Math.min(this.maxConcurrentTasks, this.asyncTasksQueue.length);
                const batch: AsyncTask[] = [];

                for (let i = 0; i < batchSize; i++) {
                    const task = this.asyncTasksQueue.shift();
                    if (task) {
                        batch.push(task);
                    }
                }

                // Execute tasks from the batch concurrently
                await Promise.all(
                    batch.map(async ({handle, message, metadata}) => {
                        await handle(message, metadata);
                    })
                )

            },
            1);
    }

    private async processPipelines() {
        // while (true) {
        //     if (this.stop && this.pipelinesQueue.length === 0) {
        //         break; // Exit the loop if stop is true and queue is empty
        //     }
        //
        //     // Get pipelines batch
        //     const batchSize = Math.min(this.maxConcurrentPipelines, this.pipelinesQueue.length);
        //     const batch: PipelineTask[] = [];
        //
        //     for (let i = 0; i < batchSize; i++) {
        //         const task = this.pipelinesQueue.shift();
        //         if (task) {
        //             batch.push(task);
        //         }
        //     }
        //
        //     // Execute pipelines from the batch concurrently
        //     await Promise.all(
        //         batch.map(async ({pipeline, message, metadata}) => {
        //             await pipeline.execute(
        //                 message,
        //                 this.emit,
        //                 this.config.messageKeyAsEvent, metadata
        //             );
        //         })
        //     )
        // }

        const interval = setInterval(
            async () => {
                // console.log(`processPipelines: stop = ${this.stop}; pipelinesQueue.length = ${this.pipelinesQueue.length === 0}`)
                if (this.stop && this.pipelinesQueue.length === 0) {
                    clearInterval(interval); // Exit the loop if stop is true and queue is empty
                }

                // Get pipelines batch
                const batchSize = Math.min(this.maxConcurrentPipelines, this.pipelinesQueue.length);
                const batch: PipelineTask[] = [];

                for (let i = 0; i < batchSize; i++) {
                    const task = this.pipelinesQueue.shift();
                    if (task) {
                        batch.push(task);
                    }
                }

                // Execute pipelines from the batch concurrently
                await Promise.all(
                    batch.map(async ({pipeline, message, metadata}) => {
                        await pipeline.execute(
                            message,
                            this.emit.bind(this),
                            this.config.messageKeyAsEvent, metadata
                        );
                    })
                )

            },
            1);
    }

    private async processCaching() {
        // while (true) {
        //     if (this.stop && this.cachingQueue.length === 0) {
        //         break; // Exit the loop if stop is true and queue is empty
        //     }
        //
        //     // Get pipelines batch
        //     const batchSize = Math.min(this.maxConcurrentPipelines, this.cachingQueue.length);
        //     const batch: PipelineTask[] = [];
        //
        //     for (let i = 0; i < batchSize; i++) {
        //         const task = this.cachingQueue.shift();
        //         if (task) {
        //             batch.push(task);
        //         }
        //     }
        //
        //     // Execute pipelines from the batch concurrently
        //     await Promise.all(
        //         batch.map(async ({pipeline, message, metadata}) => {
        //             await pipeline.execute(
        //                 message,
        //                 this.emit,
        //                 this.config.messageKeyAsEvent, metadata
        //             );
        //         })
        //     )
        // }

        const interval = setInterval(
            async () => {
                // console.log(`processCaching: stop = ${this.stop}; cachingQueue.length = ${this.cachingQueue.length === 0}`)
                if (this.stop && this.cachingQueue.length === 0) {
                    clearInterval(interval); // Exit the loop if stop is true and queue is empty
                }

                // Get pipelines batch
                const batchSize = Math.min(this.maxConcurrentPipelines, this.cachingQueue.length);
                const batch: PipelineTask[] = [];

                for (let i = 0; i < batchSize; i++) {
                    const task = this.cachingQueue.shift();
                    if (task) {
                        batch.push(task);
                    }
                }

                // Execute pipelines from the batch concurrently
                await Promise.all(
                    batch.map(async ({pipeline, message, metadata}) => {
                        await pipeline.execute(
                            message,
                            this.emit.bind(this),
                            this.config.messageKeyAsEvent,
                            metadata
                        );
                    })
                )
            },
            1);
    }

    public async close() {
        this.logger.info(`${this.appName} is closing...`)
        this.stop = true;

        this.kafkaProducer.close().then(() => {
            this.kafkaListener.close().then(() => {
                this.logger.info(`${this.appName} closed.`);
            });
        });
    }
}


import * as kafka from "kafkajs";
import {randomUUID} from "crypto";


export interface ILogger {
    debug(message: string, extra?: Object): void

    info(message: string, extra?: Object): void

    warn(message: string, extra?: Object): void

    error(message: string, extra?: Object): void
}

export interface ConsumerCallbacks {
    "consumer.heartbeat"?: (listener: kafka.ConsumerHeartbeatEvent) => void
    "consumer.commit_offsets"?: (listener: kafka.ConsumerCommitOffsetsEvent) => void
    "consumer.group_join"?: (listener: kafka.ConsumerGroupJoinEvent) => void
    "consumer.fetch_start"?: (listener: kafka.InstrumentationEvent<{}>) => void
    "consumer.fetch"?: (listener: kafka.ConsumerFetchEvent) => void
    "consumer.start_batch_process"?: (listener: kafka.ConsumerStartBatchProcessEvent) => void
    "consumer.end_batch_process"?: (listener: kafka.ConsumerEndBatchProcessEvent) => void
    "consumer.connect"?: (listener: kafka.ConnectEvent) => void
    "consumer.disconnect"?: (listener: kafka.DisconnectEvent) => void
    "consumer.stop"?: (listener: kafka.InstrumentationEvent<null>) => void
    "consumer.crash"?: (listener: kafka.ConsumerCrashEvent) => void
    "consumer.rebalancing"?: (listener: kafka.ConsumerRebalancingEvent) => void
    "consumer.received_unsubscribed_topics"?: (listener: kafka.ConsumerReceivedUnsubcribedTopicsEvent) => void
    "consumer.network.request"?: (listener: kafka.RequestEvent) => void
    "consumer.network.request_timeout"?: (listener: kafka.RequestTimeoutEvent) => void
    "consumer.network.request_queue_size"?: (listener: kafka.RequestQueueSizeEvent) => void
}

export interface ProducerCallbacks {
    "consumer.connect"?: (listener: kafka.ConnectEvent) => void
    "consumer.disconnect"?: (listener: kafka.DisconnectEvent) => void
    "consumer.network.request"?: (listener: kafka.RequestEvent) => void
    "consumer.network.request_timeout"?: (listener: kafka.RequestTimeoutEvent) => void
    "consumer.network.request_queue_size"?: (listener: kafka.RequestQueueSizeEvent) => void
}

export interface ListenerConfig extends kafka.ConsumerConfig, kafka.ConsumerRunConfig {
    topics: kafka.ConsumerSubscribeTopic[]
    // consumerConfig?: kafka.ConsumerConfig;
    // consumerRunConfig?: kafka.ConsumerRunConfig;
    consumerCallbacks?: ConsumerCallbacks
}

export interface ProducerConfig extends kafka.ProducerConfig {
    producerCallbacks?: ProducerCallbacks
}

export interface KafkaConnectorConfig {
    clientConfig: kafka.KafkaConfig
    listenerConfig?: ListenerConfig
    producerConfig?: ProducerConfig
    logger?: ILogger
}

export class KafkaConnector {
    public config: KafkaConnectorConfig;
    private readonly _kafkaClient: kafka.Kafka;

    // public readonly logger: kafka.Logger;

    constructor(config: KafkaConnectorConfig) {
        this.config = config;

        function getLogCreator() {
            if (config.logger) {
                // console.log('getLogCreator')
                const logger = config.logger;
                return function (logLevel: kafka.logLevel): (entry: kafka.LogEntry) => void {
                    return (entry: kafka.LogEntry) => {
                        switch (entry.level) {
                            case kafka.logLevel.NOTHING:
                                break;
                            case kafka.logLevel.ERROR:
                                logger.error(entry.log.message);
                                break;
                            case kafka.logLevel.WARN:
                                logger.warn(entry.log.message);
                                break;
                            case kafka.logLevel.INFO:
                                logger.info(entry.log.message);
                                break;
                            case kafka.logLevel.DEBUG:
                                logger.debug(entry.log.message);
                                break;
                        }
                    };
                }
            } else {
                return undefined
            }
        }

        this.config.clientConfig.logCreator = getLogCreator();
        this._kafkaClient = new kafka.Kafka(this.config.clientConfig);
        // this.logger = this._kafkaClient.logger();
    }

    public getListener() {
        // if (!this.config.consumerConfig) {
        //     throw Error("Consumer config is not defined.");
        // }
        // if (!this.config.consumerRunConfig) {
        //     throw Error("Consumer run config is not defined.");
        // }
        if (!this.config.listenerConfig?.topics) {
            throw Error("Topics are not defined.");
        }

        // const conf: ListenerConfig = {
        //     consumerConfig: this.config.consumerConfig,
        //     consumerRunConfig: this.config.consumerRunConfig,
        //     consumerCallbacks: this.config.consumerCallbacks,
        //     topics: this.config.topics,
        // };
        return KafkaListener.create(this._kafkaClient, this.config.listenerConfig);
    }

    public getProducer() {
        // if (!this.config.producerConfig) {
        //     throw Error("Producer config is not defined.");
        // }
        return KafkaProducer.create(this._kafkaClient, this.config.producerConfig);
    }
}

export class KafkaListener {
    public config: ListenerConfig;
    public readonly logger;
    public readonly kafkaConsumer: kafka.Consumer;

    constructor(kafkaClient: kafka.Kafka, config: ListenerConfig) {
        this.config = config;
        this.logger = kafkaClient.logger();
        // const consumerConf = ((conf): kafka.ConsumerConfig => {
        //     if (conf) {
        //         return conf
        //     } else {
        //         return {groupId: randomUUID()}
        //     }
        // })(this.config.consumerConfig)
        const consumerConf: kafka.ConsumerConfig = {
            groupId: ((groupId) => {
                if (groupId) {
                    return groupId;
                } else {
                    return randomUUID()
                }
            })(this.config.groupId),
            partitionAssigners: this.config.partitionAssigners,
            metadataMaxAge: this.config.metadataMaxAge,
            sessionTimeout: this.config.sessionTimeout,
            rebalanceTimeout: this.config.rebalanceTimeout,
            heartbeatInterval: this.config.heartbeatInterval,
            maxBytesPerPartition: this.config.maxBytesPerPartition,
            minBytes: this.config.minBytes,
            maxBytes: this.config.maxBytes,
            maxWaitTimeInMs: this.config.maxWaitTimeInMs,
            retry: this.config.retry,
            allowAutoTopicCreation: this.config.allowAutoTopicCreation,
            maxInFlightRequests: this.config.maxInFlightRequests,
            readUncommitted: this.config.readUncommitted,
            rackId: this.config.rackId
        }
        this.kafkaConsumer = kafkaClient.consumer(consumerConf);
        this.initCallbacks();
    }

    private initCallbacks() {
        if (this.config.consumerCallbacks?.["consumer.heartbeat"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.HEARTBEAT, this.config.consumerCallbacks?.["consumer.heartbeat"])
        }
        if (this.config.consumerCallbacks?.["consumer.commit_offsets"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.COMMIT_OFFSETS, this.config.consumerCallbacks?.["consumer.commit_offsets"])
        }
        if (this.config.consumerCallbacks?.["consumer.fetch_start"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.FETCH_START, this.config.consumerCallbacks?.["consumer.fetch_start"])
        }
        if (this.config.consumerCallbacks?.["consumer.fetch"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.FETCH, this.config.consumerCallbacks?.["consumer.fetch"])
        }
        if (this.config.consumerCallbacks?.["consumer.start_batch_process"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.START_BATCH_PROCESS, this.config.consumerCallbacks?.["consumer.start_batch_process"])
        }
        if (this.config.consumerCallbacks?.["consumer.end_batch_process"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.END_BATCH_PROCESS, this.config.consumerCallbacks?.["consumer.end_batch_process"])
        }
        if (this.config.consumerCallbacks?.["consumer.connect"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.CONNECT, this.config.consumerCallbacks?.["consumer.connect"])
        }
        if (this.config.consumerCallbacks?.["consumer.disconnect"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.DISCONNECT, this.config.consumerCallbacks?.["consumer.disconnect"])
        }
        if (this.config.consumerCallbacks?.["consumer.stop"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.STOP, this.config.consumerCallbacks?.["consumer.stop"])
        }
        if (this.config.consumerCallbacks?.["consumer.crash"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.CRASH, this.config.consumerCallbacks?.["consumer.crash"])
        }
        if (this.config.consumerCallbacks?.["consumer.rebalancing"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.REBALANCING, this.config.consumerCallbacks?.["consumer.rebalancing"])
        }
        if (this.config.consumerCallbacks?.["consumer.received_unsubscribed_topics"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.RECEIVED_UNSUBSCRIBED_TOPICS, this.config.consumerCallbacks?.["consumer.received_unsubscribed_topics"])
        }
        if (this.config.consumerCallbacks?.["consumer.network.request"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.REQUEST, this.config.consumerCallbacks?.["consumer.network.request"])
        }
        if (this.config.consumerCallbacks?.["consumer.network.request_timeout"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.REQUEST_TIMEOUT, this.config.consumerCallbacks?.["consumer.network.request_timeout"])
        }
        if (this.config.consumerCallbacks?.["consumer.network.request_queue_size"]) {
            this.kafkaConsumer.on(this.kafkaConsumer.events.REQUEST_QUEUE_SIZE, this.config.consumerCallbacks?.["consumer.network.request_queue_size"])
        }
    }

    private async init() {
        // this.kafkaConsumer.on(this.kafkaConsumer.events.CONNECT, () => {
        //     this.logger.warn("Kafka consumer connected.");
        // });

        // this.initCallbacks();

        await this.kafkaConsumer.connect();

        await Promise.all(
            this.config.topics.map(async (conf) => {
                await this.kafkaConsumer.subscribe(conf).then(() => {
                    this.logger.debug(`Kafka consumer subscribed to: ${conf.topic}`);
                });
            })
        );
        this.logger.debug("Kafka listener started.");
    }

    public static async create(kafkaClient: kafka.Kafka, config: ListenerConfig) {
        const listener = new KafkaListener(kafkaClient, config);
        await listener.init();
        return listener;
    }

    public async listen() {
        const consumerRunConf: kafka.ConsumerRunConfig = {
            autoCommit: this.config.autoCommit,
            autoCommitInterval: this.config.autoCommitInterval,
            autoCommitThreshold: this.config.autoCommitThreshold,
            eachBatchAutoResolve: this.config.eachBatchAutoResolve,
            partitionsConsumedConcurrently: this.config.partitionsConsumedConcurrently,
            eachBatch: this.config.eachBatch,
            eachMessage: this.config.eachMessage,
        }
        try {
            await this.kafkaConsumer.run(consumerRunConf);
        } catch (e: any) {
            this.logger.error(e.toString());
        }
    }

    public async close() {
        this.logger.debug("Closing consumer...");
        await this.kafkaConsumer.disconnect().then((_) => {
            this.logger.debug("Consumer disconnected.");
        });
    }
}

export class KafkaProducer {
    public readonly config: kafka.ProducerConfig | undefined;
    public readonly logger;
    private readonly _kafkaProducer: kafka.Producer;

    constructor(kafkaClient: kafka.Kafka, config?: kafka.ProducerConfig) {
        this.config = config;
        this.logger = kafkaClient.logger();
        this._kafkaProducer = kafkaClient.producer(this.config);
    }

    private async init() {
        this._kafkaProducer.on(this._kafkaProducer.events.CONNECT, () => {
            this.logger.debug("Kafka producer connected.");
        });
        await this._kafkaProducer.connect();
    }

    public static async create(kafkaClient: kafka.Kafka, config?: kafka.ProducerConfig) {
        const producer = new KafkaProducer(kafkaClient, config);
        await producer.init();
        return producer;
    }

    public async send(record: kafka.ProducerRecord) {
        await this._kafkaProducer.send(record);
    }

    public async close() {
        this.logger.debug("Closing producer...");
        await this._kafkaProducer.disconnect().then((_) => {
            this.logger.debug("Producer disconnected.");
        });
    }
}

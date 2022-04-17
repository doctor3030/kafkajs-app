import * as kafka from "kafkajs";
import {randomUUID} from "crypto";


export interface ILogger {
    debug(message: string, extra?: object): void
    info(message: string, extra?: object): void
    warn(message: string, extra?: object): void
    error(message: string, extra?: object): void
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
    "producer.connect"?: (listener: kafka.ConnectEvent) => void
    "producer.disconnect"?: (listener: kafka.DisconnectEvent) => void
    "producer.network.request"?: (listener: kafka.RequestEvent) => void
    "producer.network.request_timeout"?: (listener: kafka.RequestTimeoutEvent) => void
    "producer.network.request_queue_size"?: (listener: kafka.RequestQueueSizeEvent) => void
}

export interface ListenerConfig extends kafka.ConsumerConfig, kafka.ConsumerRunConfig {
    topics: kafka.ConsumerSubscribeTopic[]
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
    public readonly kafkaClient: kafka.Kafka;

    constructor(config: KafkaConnectorConfig) {
        this.config = config;

        function getLogCreator() {
            if (config.logger) {
                const logger = config.logger;
                return (logLevel: kafka.logLevel) => (entry: kafka.LogEntry) => {
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
        this.kafkaClient = new kafka.Kafka(this.config.clientConfig);
    }

    public getListener() {
        if (!this.config.listenerConfig?.topics) {
            throw Error("Topics are not defined.");
        }
        return KafkaListener.create(this.kafkaClient, this.config.listenerConfig);
    }

    public getProducer() {
        return KafkaProducer.create(this.kafkaClient, this.config.producerConfig);
    }
}

export class KafkaListener {
    public config: ListenerConfig;
    public readonly logger;
    public readonly consumer: kafka.Consumer;

    constructor(kafkaClient: kafka.Kafka, config: ListenerConfig) {
        this.config = config;
        this.logger = kafkaClient.logger();

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
        this.consumer = kafkaClient.consumer(consumerConf);
        this.initCallbacks();
    }

    private initCallbacks() {
        for (const event of Object.values(this.consumer.events)) {
            const cb = this.config.consumerCallbacks?.[event]
            if (cb) {
                this.consumer.on(event, cb)
            }
        }
    }

    private async init() {
        await this.consumer.connect();
        await Promise.all(
            this.config.topics.map(async (conf) => {
                await this.consumer.subscribe(conf).then(() => {
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
            await this.consumer.run(consumerRunConf);
        } catch (e: any) {
            this.logger.error(e.toString());
        }
    }

    public async close() {
        this.logger.debug("Closing consumer...");
        await this.consumer.disconnect();
    }
}

export class KafkaProducer {
    public readonly config: ProducerConfig | undefined;
    public readonly logger;
    public readonly producer: kafka.Producer;

    constructor(kafkaClient: kafka.Kafka, config?: kafka.ProducerConfig) {
        this.config = config;
        this.logger = kafkaClient.logger();
        this.producer = kafkaClient.producer(this.config);
        this.initCallbacks();
    }

    private initCallbacks() {
        for (const event of Object.values(this.producer.events)) {
            const cb = this.config?.producerCallbacks?.[event]
            if (cb) {
                this.producer.on(event, cb)
            }
        }
    }

    private async init() {
        await this.producer.connect();
    }

    public static async create(kafkaClient: kafka.Kafka, config?: kafka.ProducerConfig) {
        const producer = new KafkaProducer(kafkaClient, config);
        await producer.init();
        return producer;
    }

    public async send(record: kafka.ProducerRecord) {
        return await this.producer.send(record);
    }

    public async close() {
        this.logger.debug("Closing producer...");
        await this.producer.disconnect();
    }
}

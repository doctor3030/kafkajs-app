import { Kafka, Consumer, LogEntry } from "kafkajs";
import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import { ILogger } from "winston-logger-kafka";

export interface ListenerConfig {
    consumerConfig: kafka.ConsumerConfig;
    consumerRunConfig: kafka.ConsumerRunConfig;
    topics: kafka.ConsumerSubscribeTopic[];
}

export interface KafkaConnectorConfig {
    clientConfig: kafka.KafkaConfig;
    producerConfig?: kafka.ProducerConfig;
    consumerConfig?: kafka.ConsumerConfig;
    consumerRunConfig?: kafka.ConsumerRunConfig;
    topics?: kafka.ConsumerSubscribeTopic[];
    loggerConfig: {
        loggerConfig: Logger.LoggerConfig;
        sinks?: Logger.Sink[];
    };
}

export class KafkaConnector {
    public config: KafkaConnectorConfig;
    private readonly _kafkaClient: kafka.Kafka;
    public readonly logger: kafka.Logger;

    constructor(config: KafkaConnectorConfig) {
        this.config = config;
        function logCreator(logLevel: kafka.logLevel): (entry: LogEntry) => void {
            let logger: ILogger;
            if (config.loggerConfig.sinks) {
                logger = Logger.getLogger(config.loggerConfig.loggerConfig, config.loggerConfig.sinks);
            } else {
                logger = Logger.getDefaultLogger(config.loggerConfig.loggerConfig);
            }
            // const logger = this._logger;
            return (entry: LogEntry) => {
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

        this.config.clientConfig.logCreator = logCreator;
        this._kafkaClient = new Kafka(this.config.clientConfig);
        this.logger = this._kafkaClient.logger();
    }

    public getListener() {
        if (!this.config.consumerConfig) {
            throw Error("Consumer config is not defined.");
        }
        if (!this.config.consumerRunConfig) {
            throw Error("Consumer run config is not defined.");
        }
        if (!this.config.topics) {
            throw Error("Topics are not defined.");
        }

        const conf = {
            consumerConfig: this.config.consumerConfig,
            consumerRunConfig: this.config.consumerRunConfig,
            topics: this.config.topics,
        };
        return KafkaListener.create(conf, this._kafkaClient, this.logger);
    }

    public getProducer() {
        if (!this.config.producerConfig) {
            throw Error("Producer config is not defined.");
        }
        return KafkaProducer.create(this.config.producerConfig, this._kafkaClient, this.logger);
    }
}

export class KafkaListener {
    public config: ListenerConfig;
    public readonly logger;
    public readonly kafkaConsumer: Consumer;

    constructor(config: ListenerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        this.config = config;
        this.logger = logger;
        this.kafkaConsumer = kafkaClient.consumer(this.config.consumerConfig);
    }

    private async init() {
        this.kafkaConsumer.on(this.kafkaConsumer.events.CONNECT, () => {
            this.logger.info("Kafka consumer connected.");
        });
        await this.kafkaConsumer.connect();

        await Promise.all(
            this.config.topics.map(async (conf) => {
                await this.kafkaConsumer.subscribe(conf).then(() => {
                    this.logger.info(`Kafka consumer subscribed to: ${conf.topic}`);
                });
            })
        );
        this.logger.info("Kafka listener started.");
    }

    public static async create(config: ListenerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        const newObject = new KafkaListener(config, kafkaClient, logger);
        await newObject.init();
        return newObject;
    }

    public async listen() {
        try {
            await this.kafkaConsumer.run(this.config.consumerRunConfig);
        } catch (e: any) {
            this.logger.error(e.toString());
        }
    }

    public async close() {
        this.logger.info("Closing consumer...");
        await this.kafkaConsumer.disconnect().then((_) => {
            this.logger.info("Consumer disconnected.");
        });
    }
}

export class KafkaProducer {
    public readonly config: kafka.ProducerConfig;
    public readonly logger;
    private readonly _kafkaProducer: kafka.Producer;

    constructor(config: kafka.ProducerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        this.config = config;
        this.logger = logger;
        // this._kafkaConsumer = kafkaClient.consumer(this._config.consumerConfig);
        this._kafkaProducer = kafkaClient.producer(this.config);
    }

    private async init() {
        this._kafkaProducer.on(this._kafkaProducer.events.CONNECT, () => {
            this.logger.info("Kafka producer connected.");
        });
        await this._kafkaProducer.connect();
    }

    public static async create(config: kafka.ProducerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        const newObject = new KafkaProducer(config, kafkaClient, logger);
        await newObject.init();
        return newObject;
    }

    public async send(record: kafka.ProducerRecord) {
        await this._kafkaProducer.send(record);
    }

    public async close() {
        this.logger.info("Closing producer...");
        await this._kafkaProducer.disconnect().then((_) => {
            this.logger.info("Producer disconnected.");
        });
    }
}

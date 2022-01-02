import {Kafka, Consumer, LogEntry} from 'kafkajs';
import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import path from "path";
import {ILogger} from "winston-logger-kafka";

export interface ListenerConfig {
    consumerConfig: kafka.ConsumerConfig,
    consumerRunConfig: kafka.ConsumerRunConfig,
    topics: kafka.ConsumerSubscribeTopic[]
}

export interface KafkaConnectorConfig {
    clientConfig: kafka.KafkaConfig;
    producerConfig?: kafka.ProducerConfig,
    consumerConfig?: kafka.ConsumerConfig,
    consumerRunConfig?: kafka.ConsumerRunConfig,
    topics?: kafka.ConsumerSubscribeTopic[],
    loggerConfig: {
        loggerConfig: Logger.LoggerConfig,
        sinks?: Logger.Sink[]
    }
}

export class KafkaConnector {
    private _config: KafkaConnectorConfig
    private readonly _kafkaClient: kafka.Kafka;
    private readonly _logger: kafka.Logger;

    constructor(config: KafkaConnectorConfig) {
        this._config = config;
        this._config.clientConfig.logCreator = this.logCreator;
        this._kafkaClient = new Kafka(this._config.clientConfig);
        this._logger = this._kafkaClient.logger();
    }

    private logCreator(logLevel: kafka.logLevel): (entry: LogEntry) => void {
        let logger: ILogger;
        if (this._config.loggerConfig.sinks) {
            logger = Logger.getLogger(this._config.loggerConfig.loggerConfig, this._config.loggerConfig.sinks);
        }
        else {
            logger = Logger.getDefaultLogger(this._config.loggerConfig.loggerConfig)
        }
        // const logger = this._logger;
        return (entry: LogEntry) => {
            switch(entry.level) {
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
        }
    }

    public getListener() {
        if (!this._config.consumerConfig) { throw Error('Consumer config is not defined.') }
        if (!this._config.consumerRunConfig) { throw Error('Consumer run config is not defined.') }
        if (!this._config.topics) { throw Error('Topics are not defined.') }

        let conf = {
            consumerConfig: this._config.consumerConfig,
            consumerRunConfig: this._config.consumerRunConfig,
            topics: this._config.topics
        }
        return KafkaListener.create(conf, this._kafkaClient, this._logger)
    }

    public getProducer() {
        if (!this._config.producerConfig) { throw Error('Producer config is not defined.') }
        return KafkaProducer.create(this._config.producerConfig, this._kafkaClient, this._logger)
    }
}

export class KafkaListener {
    private _config: ListenerConfig;
    private readonly _logger;
    private readonly _kafkaConsumer: Consumer;

    constructor(config: ListenerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        this._config = config;
        this._logger = logger;
        this._kafkaConsumer = kafkaClient.consumer(this._config.consumerConfig);
    }

    public async init() {
        this._kafkaConsumer.on(this._kafkaConsumer.events.CONNECT, () => {
            this._logger.info('Kafka consumer connected.');
        })
        await this._kafkaConsumer.connect();

        await Promise.all(this._config.topics.map(async (conf) => {
            await this._kafkaConsumer.subscribe(conf).then(() => {
                this._logger.info(`Kafka consumer subscribed to: ${conf.topic}`);
            })
        }))
        this._logger.info('Kafka listener started.');
    }

    public static async create(config: ListenerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        const newObject = new KafkaListener(config, kafkaClient, logger);
        await newObject.init();
        return newObject
    }

    public async listen() {
        try {
            await this._kafkaConsumer.run(this._config.consumerRunConfig);
        } catch (e: any) {
            this._logger.error(e.toString());
        }
    }

    public async close() {
        this._logger.info('Closing consumer...');
        await this._kafkaConsumer.disconnect().then((_) => {
            this._logger.info('Consumer disconnected.')
        });
    }
}

export class KafkaProducer {
    private readonly _config: kafka.ProducerConfig;
    private readonly _logger;
    private readonly _kafkaProducer: kafka.Producer;

    constructor(config: kafka.ProducerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        this._config = config;
        this._logger = logger;
        // this._kafkaConsumer = kafkaClient.consumer(this._config.consumerConfig);
        this._kafkaProducer = kafkaClient.producer(this._config);
    }

    public async init() {
        this._kafkaProducer.on(this._kafkaProducer.events.CONNECT, () => {
            this._logger.info('Kafka producer connected.');
        })
        await this._kafkaProducer.connect();
    }

    public static async create(config: kafka.ProducerConfig, kafkaClient: kafka.Kafka, logger: kafka.Logger) {
        const newObject = new KafkaProducer(config, kafkaClient, logger);
        await newObject.init();
        return newObject
    }

    public async send(record: kafka.ProducerRecord) {
        await this._kafkaProducer.send(record);
    }

    public async close() {
        this._logger.info('Closing producer...');
        await this._kafkaProducer.disconnect().then((_) => {
            this._logger.info('Producer disconnected.')
        });
    }
}

import {Kafka, Consumer, LogEntry} from 'kafkajs';
import * as Logger from "winston-logger-kafka";
import * as kafka from "kafkajs";
import path from "path";

export interface ListenerConfig {
    clientConfig: kafka.KafkaConfig;
    consumerConfig: kafka.ConsumerConfig,
    consumerRunConfig: kafka.ConsumerRunConfig,
    topics: kafka.ConsumerSubscribeTopic[]
}

export class KafkaListener {
    private _config: ListenerConfig;
    private readonly _kafkaConsumer: Consumer;
    private readonly _logger: Logger.ILogger;

    constructor(config: ListenerConfig, logger: Logger.ILogger) {
        this._config = config;
        this._logger = logger;
        this._config.clientConfig.logCreator = this.logCreator;
        this._kafkaConsumer = new Kafka(this._config.clientConfig).consumer(this._config.consumerConfig);
    }

    private logCreator(logLevel: kafka.logLevel): (entry: LogEntry) => void {
        const logger = Logger.getDefaultLogger({
            module: path.basename(__filename),
            component: 'kafka',
            serviceID: '123'
        })
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

    public static async create(config: ListenerConfig, logger: Logger.ILogger) {
        const newObject = new KafkaListener(config, logger);
        await newObject.init();
        return newObject
    }

    public async listen() {
        try {
            await this._kafkaConsumer.run(this._config.consumerRunConfig);
        } catch (e) {
            this._logger.error(e);
        }
    }

    public async close() {
        this._logger.info('Closing consumer...');
        await this._kafkaConsumer.disconnect().then((_) => {
            this._logger.info('Consumer disconnected.')
        });
    }
}

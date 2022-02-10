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

export interface Message {
    event: string;
    payload: any[];
    metadata?: TItem
}

const EVENT_MAP: TItem = {};

export class KafkaApp {
    public readonly config: KafkaAppConfig;
    private readonly _kafkaConnector: KafkaConnector;
    public kafkaListener!: KafkaListener;
    public kafkaProducer!: KafkaProducer;
    // public event_map: TItem;
    public readonly logger: kafka.Logger;

    constructor(config: KafkaAppConfig) {
        // this.event_map = {};
        this.config = config;
        const consumerRunConfig: kafka.ConsumerRunConfig = {
            autoCommit: true,
            eachMessage: this.processMessage
        }
        // this.config.connectorConfig.consumerRunConfig.eachMessage = this.processMessage;
        this._kafkaConnector = new KafkaConnector({
            clientConfig: this.config.connectorConfig.clientConfig,
            producerConfig: this.config.connectorConfig.producerConfig,
            consumerConfig: this.config.connectorConfig.consumerConfig,
            consumerRunConfig: consumerRunConfig,
            topics: this.config.connectorConfig.topics,
            loggerConfig: this.config.connectorConfig.loggerConfig
        });
        this.logger = this._kafkaConnector.logger;
        // this.event_map = {};
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
    // private aaaa() {
    //     return 'bbb'
    // }
    private async processMessage(payload: kafka.EachMessagePayload) {
        try {
            // If callback function exists, execute
            // console.log(payload)
            // if (this.config.processMessageCb) {
            //     this.config.processMessageCb(payload);
            // }
            // console.log(EVENT_MAP)
            const _value = payload.message.value
            if (_value) {

                const receivedMessage: Message = JSON.parse(_value.toString());
                // console.log(receivedMessage)
                if (receivedMessage.event) {
                    this.logger.info(`Received message: event: ${receivedMessage.event}`);
                    // console.log(EVENT_MAP)
                    // console.log(this.config)
                    EVENT_MAP[receivedMessage.event](receivedMessage);
                    // this.event_map[receivedMessage.event](receivedMessage);
                } else {
                    this.logger.error('Received message is missing property "event".');
                }
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

    public on(eventName: string, cb: (message: Message, kwargs?: TItem) => void) {
        EVENT_MAP[eventName] = cb;
        // this.event_map[eventName] = cb;
        // console.log(this.event_map)
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


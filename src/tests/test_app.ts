import {KafkaApp, KafkaAppConfig} from "../kafka_app"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
import * as chai from "chai";
import "mocha";
import {v4 as uuid} from "uuid";

const path = require("path");

interface Message {
    event: string;
    request_id: string
    payload?: any[];
    error?: Error
    metadata?: Record<any, any>
}

async function delay(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

describe("Kafka app tests", () => {
    it("Test", () => {
        (async () => {
            const KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092';
            // const KAFKA_BOOTSTRAP_SERVERS = "192.168.2.190:9092";
            const TEST_TOPIC = "test_topic";

            const TEST_MESSAGE_1: Message = {
                event: 'hello',
                request_id: 'test_req1',
                payload: ['Hello!']
            };

            const TEST_MESSAGE_2: Message = {
                event: 'goodbye',
                request_id: 'test_req2',
                payload: ['Goodbye!']
            };

            const TEST_MESSAGE_3: Message = {
                event: 'goodbye_async',
                request_id: 'test_req3',
                payload: ['Goodbye!']
            };

            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
            process.on("SIGBREAK", shutdown);

            const logger = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_application",
                level: Levels.INFO
            })
            const kafkaLogger = Logger.getChildLogger(logger, {
                module: path.basename(__filename),
                component: "Test_kafka_application-Kafka_client",
                level: Levels.INFO
            })

            function eachBatchMiddleware(payload: kafkajs.EachBatchPayload) {
                logger.info(`MIDDLEWARE FUNCTION: batch received: 
                topic: ${payload.batch.topic}: 
                partition: ${payload.batch.partition}
                N messages: ${payload.batch.messages.length}`);
            }

            const appConfig: KafkaAppConfig = {
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: `test_connector_${uuid()}`,
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: TEST_TOPIC,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "kafkajs-app_test_group",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                    consumerCallbacks: {
                        "consumer.network.request": (listener: kafkajs.RequestEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            apiKey: ${listener.payload.apiKey},
                            apiVersion: ${listener.payload.apiVersion},
                            broker: ${listener.payload.broker},
                            clientId: ${listener.payload.clientId},
                            correlationId: ${listener.payload.correlationId},
                            createdAt: ${listener.payload.createdAt},
                            duration: ${listener.payload.duration},
                            pendingDuration: ${listener.payload.pendingDuration},
                            sentAt: ${listener.payload.sentAt},
                            size: ${listener.payload.size}`);
                        },
                    }
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                    producerCallbacks: {
                        "producer.connect": () => (listener: kafkajs.ConnectEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            payload: ${listener.payload}`);
                        },
                        "producer.disconnect": (listener: kafkajs.DisconnectEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            payload: ${listener.payload}`);
                        }
                    }
                },
                logger: logger,
                kafkaLogger: kafkaLogger,
                middlewareBatchCb: eachBatchMiddleware
            };
            const kafkaApp = await KafkaApp.create(appConfig);

            function shutdown() {
                kafkaApp.close().then(() => {
                    // console.log("Closed.");
                });
            }

            function logMessage(message: any) {
                logger.info(`Received message: 
                event: ${message.event}, 
                request_id: ${message.request_id},
                payload: ${message.payload}`);
            }

            kafkaApp.on('hello', (message) => {
                logMessage(message);

                const name = "John"
                if (message.payload) {
                    chai.assert.equal([TEST_MESSAGE_1.payload, name].join(' '), [message.payload[0], name].join(' '))
                }
            })

            kafkaApp.on('goodbye', (message) => {
                logMessage(message);

                const name = "John"
                if (message.payload) {
                    chai.assert.equal([TEST_MESSAGE_2.payload, name].join(' '), [message.payload[0], name].join(' '))
                }
            })

            kafkaApp.on('goodbye_async', async (message) => {
                logMessage(message);

                const name = "John Async"
                if (message.payload) {
                    chai.assert.equal([TEST_MESSAGE_2.payload, name].join(' '), [message.payload[0], name].join(' '))
                }
            })

            await kafkaApp.run();

            await delay(2000);
            await kafkaApp.emit({
                topic: TEST_TOPIC,
                messages: [{value: JSON.stringify(TEST_MESSAGE_1)}],
                compression: kafkajs.CompressionTypes.GZIP,
            });

            await delay(2000);
            await kafkaApp.emit({
                topic: TEST_TOPIC,
                messages: [{value: JSON.stringify(TEST_MESSAGE_2)}],
                compression: kafkajs.CompressionTypes.GZIP,
            });

            await delay(2000);
            await kafkaApp.emit({
                topic: TEST_TOPIC,
                messages: [{value: JSON.stringify(TEST_MESSAGE_3)}],
                compression: kafkajs.CompressionTypes.GZIP,
            });

            await delay(2000);
            await kafkaApp.close();
        })();
    });
});

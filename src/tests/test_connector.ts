import * as kafka from "../kafka_connector";
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
import * as chai from "chai";
import "mocha";
import {v4 as uuid} from "uuid";
import {ConsumerEvents} from "kafkajs";

const path = require("path");

async function delay(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

describe("Kafka connector tests", () => {
    it("Test", () => {
        (async () => {
            const KAFKA_BOOTSTRAP_SERVERS = '10.0.0.74:9092';
            // const KAFKA_BOOTSTRAP_SERVERS = "192.168.2.190:9092";
            const TEST_TOPIC = "test_topic";
            const TEST_MESSAGE: Record<string, string> = {msg: "Hello!"};

            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
            process.on("SIGBREAK", shutdown);

            const logger = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_connector",
                level: Levels.INFO
            })

            async function processMessage(payload: kafkajs.EachMessagePayload) {
                if (payload.message.value) {
                    const receivedMessage: Record<string, string> = JSON.parse(payload.message.value.toString());
                    logger.info(`Test message: type: ${typeof TEST_MESSAGE}, message: ${TEST_MESSAGE.msg}`);
                    logger.info(`Received message: type: ${typeof receivedMessage}, message: ${receivedMessage.msg}`);
                    chai.assert.deepEqual(TEST_MESSAGE, receivedMessage);
                }
            }

            const configKafka: kafka.KafkaConnectorConfig = {
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: `test_connector_${uuid()}`,
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    groupId: "crawler_group",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    topics: [
                        {
                            topic: TEST_TOPIC,
                            fromBeginning: false,
                        },
                    ],
                    autoCommit: true,
                    eachMessage: processMessage,
                    consumerCallbacks: {
                        "consumer.connect": (listener: kafkajs.ConnectEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            payload: ${listener.payload}`);
                        },
                        "consumer.disconnect": (listener: kafkajs.DisconnectEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            payload: ${listener.payload}`);
                        },
                        "consumer.fetch": (listener: kafkajs.ConsumerFetchEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            numberOfBatches: ${listener.payload.numberOfBatches},
                            duration: ${listener.payload.duration}`);
                        },
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
                // consumerRunConfig: {
                //     autoCommit: true,
                //     eachMessage: processMessage,
                // },
                // topics: [
                //     {
                //         topic: TEST_TOPIC,
                //         fromBeginning: false,
                //     },
                // ],
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger
            };
            const kafkaConnector = new kafka.KafkaConnector(configKafka);
            const listener = await kafkaConnector.getListener();
            const producer = await kafkaConnector.getProducer();

            function shutdown() {
                producer.close().then(() => {
                    listener.close().then(() => {
                        console.log("Closed.");
                    });
                });
            }

            await listener.listen();
            await delay(2000);
            await producer.send({
                topic: TEST_TOPIC,
                messages: [{value: JSON.stringify(TEST_MESSAGE)}],
                compression: kafkajs.CompressionTypes.GZIP,
            });

            await delay(2000);
            await producer.close();
            await listener.close();
        })();
    });
});

// import * as Logger from '../logger';
// import { Logger as WinstonLogger } from 'winston';
import * as kafka from "../kafkajsconnector";
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import * as chai from "chai";
import "mocha";
import { v4 as uuid } from "uuid";

const path = require("path");

async function delay(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

describe("Kafka connector tests", () => {
    it("Test", () => {
        (async () => {
            // const KAFKA_BOOTSTRAP_SERVERS = '10.0.0.74:9092';
            const KAFKA_BOOTSTRAP_SERVERS = "192.168.2.190:9092";
            const TEST_TOPIC = "test_topic";
            const TEST_MESSAGE: Record<string, string> = { msg: "Hello!" };

            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
            process.on("SIGBREAK", shutdown);

            async function processMessage(payload: kafkajs.EachMessagePayload) {
                if (payload.message.value) {
                    const receivedMessage: Record<string, string> = JSON.parse(payload.message.value.toString());
                    console.log(`Test message: type: ${typeof TEST_MESSAGE}, message: ${TEST_MESSAGE.msg}`);
                    console.log(`Received message: type: ${typeof receivedMessage}, message: ${receivedMessage.msg}`);
                    chai.assert.deepEqual(TEST_MESSAGE, receivedMessage);
                    // console.log(TEST_MESSAGE.msg === receivedMessage.msg)
                }
            }

            const configKafka: kafka.KafkaConnectorConfig = {
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: `test_connector_${uuid()}`,
                    logLevel: kafkajs.logLevel.INFO,
                },
                consumerConfig: {
                    groupId: "crawler_group",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                },
                consumerRunConfig: {
                    autoCommit: true,
                    eachMessage: processMessage,
                },
                topics: [
                    {
                        topic: TEST_TOPIC,
                        fromBeginning: false,
                    },
                ],
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                loggerConfig: {
                    loggerConfig: {
                        module: path.basename(__filename),
                        component: "Test",
                        serviceID: uuid(),
                    },
                    sinks: [new Logger.ConsoleSink()],
                },
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
                messages: [{ value: JSON.stringify(TEST_MESSAGE) }],
                compression: kafkajs.CompressionTypes.GZIP,
            });

            await delay(2000);
            await producer.close();
            await listener.close();
        })();
    });
});

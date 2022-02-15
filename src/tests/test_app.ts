// import * as Logger from '../logger';
// import { Logger as WinstonLogger } from 'winston';
// import * as kafka from "../kafka_connector";
import {ConnectorConfig, KafkaAppConfig, Message, KafkaApp} from "../kafka_app"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import * as chai from "chai";
import "mocha";
import {v4 as uuid} from "uuid";

const path = require("path");

async function delay(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

describe("Kafka app tests", () => {
    it("Test", () => {
        (async () => {
            const KAFKA_BOOTSTRAP_SERVERS = '10.0.0.74:9092';
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

            function eachMessageMiddleware(payload: kafkajs.EachMessagePayload) {
                console.log(`MIDDLEWARE FUNCTION: message received: topic: ${payload.topic}: partition: ${payload.partition}`);
            }

            const appConfig: KafkaAppConfig = {
                connectorConfig: {
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
                },
                processMessageCb: eachMessageMiddleware
            };
            const kafkaApp = await KafkaApp.create(appConfig);

            function shutdown() {
                kafkaApp.close().then(() => {
                    console.log("Closed.");
                });
            }

            kafkaApp.on('hello', (message) => {
                const name = "John"
                if (message.payload) {
                    chai.assert.equal([TEST_MESSAGE_1.payload, name].join(' '), [message.payload[0], name].join(' '))
                }
            })

            kafkaApp.on('goodbye', (message) => {
                const name = "John"
                if (message.payload) {
                chai.assert.equal([TEST_MESSAGE_2.payload, name].join(' '), [message.payload[0], name].join(' '))
                }
            })

            kafkaApp.on('goodbye_async', async (message) => {
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

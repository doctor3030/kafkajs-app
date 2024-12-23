import {KafkaApp, KafkaAppConfig} from "../kafka_app"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
import "mocha";
import {expect} from "chai";

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
            const TEST_TOPIC = "test_topic";

            const messages: Message[] = [
                {
                    event: 'hello',
                    request_id: 'test_req1',
                    payload: ['Hello!']
                },
                {
                    event: 'goodbye',
                    request_id: 'test_req2',
                    payload: ['Goodbye!']
                },
                {
                    event: 'goodbye_async',
                    request_id: 'test_req3',
                    payload: ['Goodbye!']
                },

            ]

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
                    // clientId: `test_connector_${uuid()}`,
                    clientId: 'test_connector',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: TEST_TOPIC,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                    consumerCallbacks: {
                        // "consumer.network.request": (listener: kafkajs.RequestEvent) => {
                        //     logger.info(`Custom callback "${listener.type}".
                        //     id: ${listener.id},
                        //     timestamp: ${listener.timestamp},
                        //     apiKey: ${listener.payload.apiKey},
                        //     apiVersion: ${listener.payload.apiVersion},
                        //     broker: ${listener.payload.broker},
                        //     clientId: ${listener.payload.clientId},
                        //     correlationId: ${listener.payload.correlationId},
                        //     createdAt: ${listener.payload.createdAt},
                        //     duration: ${listener.payload.duration},
                        //     pendingDuration: ${listener.payload.pendingDuration},
                        //     sentAt: ${listener.payload.sentAt},
                        //     size: ${listener.payload.size}`);
                        // },
                        "consumer.commit_offsets": (listener: kafkajs.ConsumerCommitOffsetsEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            groupId: ${listener.payload.groupId},
                            memberId: ${listener.payload.memberId},
                            groupGenerationId: ${listener.payload.groupGenerationId},
                            topics: ${JSON.stringify(listener.payload.topics)}`);
                        },
                        "consumer.heartbeat": (listener: kafkajs.ConsumerHeartbeatEvent) => {
                            logger.info(`Custom callback "${listener.type}".
                            id: ${listener.id},
                            timestamp: ${listener.timestamp},
                            groupId: ${listener.payload.groupId},
                            memberId: ${listener.payload.memberId},
                            groupGenerationId: ${listener.payload.groupGenerationId},`);
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

            let msgCounter = 0;

            function logMessage(message: any) {
                logger.info(`Received message: 
                event: ${message.event}, 
                request_id: ${message.request_id},
                payload: ${message.payload}`);
                msgCounter++;
            }

            kafkaApp.on('hello', (message) => {
                logMessage(message);

                const name = "John"
                expect([message.payload[0], name].join(' ')).equal([messages[0].payload, name].join(' '))
            })

            kafkaApp.on('goodbye', (message) => {
                logMessage(message);

                const name = "John"
                expect([message.payload[0], name].join(' ')).equal([messages[1].payload, name].join(' '))
            })

            kafkaApp.on('goodbye_async', async (message) => {
                logMessage(message);

                const name = "John Async"
                expect([message.payload[0], name].join(' ')).equal([messages[2].payload, name].join(' '))
            })

            await kafkaApp.run();

            await kafkaApp.emit({
                topic: TEST_TOPIC,
                messages: messages.map((msg) => {
                    return {value: JSON.stringify(msg)}
                }),
                compression: kafkajs.CompressionTypes.GZIP,
            });

            const watcherInterval = setInterval(
                async () => {
                    if (msgCounter === messages.length) {
                        clearInterval(watcherInterval); // Exit the loop if stop is true and queue is empty
                        logger.info(`Received all messages (${msgCounter}) and now closing...`)
                        shutdown()
                    }
                },
                1);
        })();
    });
});

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

            const MESSAGES = (() => {
                const msgs: Message[] = [];
                for (let i = 1; i < 200; i++) {
                    msgs.push({
                        event: 'test_event',
                        request_id: uuid(),
                        payload: [`Test message #${i}`]
                    })
                }
                return msgs
            })()

            let handledTotal = 0;

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

            function logMessage(message: any) {
                logger.info(`Received message: 
                event: ${message.event}, 
                request_id: ${message.request_id},
                payload: ${message.payload}`);

                handledTotal++;
                logger.info(`Total handeled: ${handledTotal}`)
            }

            kafkaApp.on('test_event', async (message) => {
                logMessage(message);
                await delay(500);
            })

            await kafkaApp.run();

            await delay(2000);
            for (const msg of MESSAGES) {
                await kafkaApp.emit({
                    topic: TEST_TOPIC,
                    messages: [{value: JSON.stringify(msg)}],
                    compression: kafkajs.CompressionTypes.GZIP,
                });
            }



            await delay(120000);
            await kafkaApp.close();
        })();
    });
});

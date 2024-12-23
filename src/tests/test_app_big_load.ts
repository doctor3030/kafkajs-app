import {KafkaApp, KafkaAppConfig} from "../kafka_app"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
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
            const TEST_TOPIC = "test_topic";

            const N_MESSAGES = 2000

            const messages = (() => {
                const msgs: Message[] = [];
                for (let i = 1; i < N_MESSAGES + 1; i++) {
                    msgs.push({
                        event: 'test_event',
                        request_id: uuid(),
                        payload: [`Test message #${i}`]
                    })
                }
                return msgs
            })()

            let msgCounter = 0;

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
                    // consumerCallbacks: {
                    //     "consumer.commit_offsets": (listener: kafkajs.ConsumerCommitOffsetsEvent) => {
                    //         logger.info(`Custom callback "${listener.type}".
                    //         id: ${listener.id},
                    //         timestamp: ${listener.timestamp},
                    //         groupId: ${listener.payload.groupId},
                    //         memberId: ${listener.payload.memberId},
                    //         groupGenerationId: ${listener.payload.groupGenerationId},
                    //         topics: ${JSON.stringify(listener.payload.topics)}`);
                    //     },
                    //     "consumer.heartbeat": (listener: kafkajs.ConsumerHeartbeatEvent) => {
                    //         logger.info(`Custom callback "${listener.type}".
                    //         id: ${listener.id},
                    //         timestamp: ${listener.timestamp},
                    //         groupId: ${listener.payload.groupId},
                    //         memberId: ${listener.payload.memberId},
                    //         groupGenerationId: ${listener.payload.groupGenerationId},`);
                    //     },
                    // }
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                    // producerCallbacks: {
                    //     "producer.connect": () => (listener: kafkajs.ConnectEvent) => {
                    //         logger.info(`Custom callback "${listener.type}".
                    //         id: ${listener.id},
                    //         timestamp: ${listener.timestamp},
                    //         payload: ${listener.payload}`);
                    //     },
                    //     "producer.disconnect": (listener: kafkajs.DisconnectEvent) => {
                    //         logger.info(`Custom callback "${listener.type}".
                    //         id: ${listener.id},
                    //         timestamp: ${listener.timestamp},
                    //         payload: ${listener.payload}`);
                    //     }
                    // }
                },
                logger: logger,
                kafkaLogger: kafkaLogger,
                middlewareBatchCb: eachBatchMiddleware
            };
            const kafkaApp = await KafkaApp.create(appConfig);

            function shutdown() {
                kafkaApp.close().then(() => {
                    // console.log("Closing...");
                });
            }

            function logMessage(message: any) {
                logger.info(`Received message: 
                event: ${message.event}, 
                request_id: ${message.request_id},
                payload: ${message.payload}`);

                msgCounter++;
                logger.info(`Handled message ${msgCounter}/${messages.length}`)
            }

            kafkaApp.on('test_event', async (message) => {
                logMessage(message);
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

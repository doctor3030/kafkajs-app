import {KafkaApp, KafkaAppConfig, KafkaMessageMetadata} from "../kafka_app"
import {
    MessagePipeline,
    PipeEventFunction,
    PipeEventWithResponseFunction
} from "../kafka_app_pipeline"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
import * as chai from "chai";
import "mocha";
import {v4 as uuid} from "uuid";
import {createClient, RedisClientType} from 'redis';
import * as kafka from "kafkajs";

const path = require("path");

interface Message {
    event: string;
    payload?: any;
}

interface PersonPayload {
    firstName: string
    lastName: string
    age: number
}

interface CompanyPayload {
    name: string
    stockValue: number
}

async function delay(time: number) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

async function personAddMiddleName(message: Message, logger: any, kwargs: Record<string, any>) {
    const person = message.payload as PersonPayload;
    const divider = kwargs.divider;
    const midName = kwargs.midName
    person.firstName = [...person.firstName.split(divider), midName].join(divider);
    message.payload = person;
    logger.info(`Executing transaction: "personAddMiddleName" ==> result: ${JSON.stringify(message)}`);
    return message;
}

async function personMultiplyAge(message: Message, logger: any, kwargs: Record<string, any>) {
    const person = message.payload as PersonPayload;
    const multiplier = kwargs.multiplier;
    person.age = person.age * multiplier;
    message.payload = person;
    logger.info(`Executing transaction: "personMultiplyAge" ==> result: ${JSON.stringify(message)}`);
    return message;
}

async function companyAddInc(message: Message, logger: any, kwargs: Record<string, any>) {
    const company = message.payload as CompanyPayload;
    company.name = company.name + ' INC.'
    message.payload = company;
    logger.info(`Executing transaction: "companyAddInc" ==> result: ${JSON.stringify(message)}`);
    return message;
}

async function companyDoubleStockPrice(message: Message, logger: any, kwargs: Record<string, any>) {
    const company = message.payload as CompanyPayload;
    company.stockValue = company.stockValue * 2
    message.payload = company;
    logger.info(`Executing transaction: "companyDoubleStockPrice" ==> result: ${JSON.stringify(message)}`);
    return message;
}

let customMsg = '';

async function personAgePipeResult(
    appId: string | null,
    pipelineName: string | null,
    message: any,
    emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
    messageKeyAsEvent: boolean,
    fncPipeEvent: PipeEventFunction,
    fncPipeEventWithResponse: PipeEventWithResponseFunction,
    logger: any,
    metadata: KafkaMessageMetadata
) {
    const person = message.payload;
    if (Math.floor(person.age) === 0) {
        customMsg = "Person's age is EVEN!";
        logger.info(customMsg)
    } else {
        customMsg = "Person's age is ODD!";
        logger.info(customMsg)
    }

    await fncPipeEvent(
        appId,
        message,
        emitter,
        messageKeyAsEvent,
        {
            pipeEventName: Events.PERSON_MULTIPLY_AGE,
            pipeToTopic: Topics.APP_1
        },
        logger,
        metadata
    )
}

enum Events {
    PERSON_ADD_MIDDLE_NAME = 'person_add_middle_name',
    PERSON_MULTIPLY_AGE = 'person_multiply_age',

    COMPANY_ADD_INC = 'company_add_inc',
    COMPANY_DOUBLE_STOCK_PRICE = 'company_double_stock_price',
}

enum Topics {
    APP_1 = 'test_topic_1',
    APP_2 = 'test_topic_2',
    APP_3 = 'test_topic_3',
}

const messages: Message[] = [
    {
        'event': Events.PERSON_ADD_MIDDLE_NAME,
        'payload': {
            'firstName': 'John',
            'lastName': 'Doe',
            'age': 35
        }
    },
    {
        'event': Events.PERSON_MULTIPLY_AGE,
        'payload': {
            'firstName': 'John',
            'lastName': 'Doe',
            'age': 35
        }
    },
    {
        'event': Events.COMPANY_ADD_INC,
        'payload': {
            'name': 'SomeCompany',
            'stockValue': 1224.55
        }
    },
    {
        'event': Events.COMPANY_DOUBLE_STOCK_PRICE,
        'payload': {
            'name': 'SomeCompany',
            'stockValue': 1224.55
        }
    },
]

describe("Kafka app tests", () => {
    it("Test", () => {
        (async () => {
            const KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092';

            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
            process.on("SIGBREAK", shutdown);

            const logger1 = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_application_1",
                level: Levels.INFO
            })
            const kafkaLogger1 = Logger.getChildLogger(logger1, {
                module: path.basename(__filename),
                component: "Test_kafka_application-Kafka_client_1",
                level: Levels.INFO
            })

            const logger2 = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_application_2",
                level: Levels.INFO
            })
            const kafkaLogger2 = Logger.getChildLogger(logger2, {
                module: path.basename(__filename),
                component: "Test_kafka_application-Kafka_client_2",
                level: Levels.INFO
            })

            const logger3 = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_application_3",
                level: Levels.INFO
            })
            const kafkaLogger3 = Logger.getChildLogger(logger3, {
                module: path.basename(__filename),
                component: "Test_kafka_application-Kafka_client_3",
                level: Levels.INFO
            })

            const loggerRedis = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Redis",
                level: Levels.INFO
            })

            const cacheClient = createClient({
                url: `redis://127.0.0.1:6379`,
                password: 'pass',
                database: 0
            });
            cacheClient.on('error', (err: any) => loggerRedis.error(`Cache client: ${err}`));
            cacheClient.on('ready', () => loggerRedis.info('Cache client connected.'));
            cacheClient.on('end', () => loggerRedis.info('Cache client disconnected.'));
            await cacheClient.connect();

            const app2PersonMidNamePipeline = new MessagePipeline({
                name: 'app2_personMidNamePipeline',
                transactions: [
                    {
                        fnc: personAddMiddleName,
                        args: {
                            divider: '-',
                            midName: 'Joe'
                        },
                        pipeResultOptions: {
                            pipeEventName: Events.PERSON_ADD_MIDDLE_NAME,
                            pipeToTopic: Topics.APP_1
                        }
                    }
                ],
                logger: logger2
            })

            const app2PersonAgePipeline = new MessagePipeline({
                name: 'app2_personAgePipeline',
                transactions: [
                    {
                        fnc: personMultiplyAge,
                        args: {
                            multiplier: 2,
                        },
                        pipeResultOptionsCustom: {
                            fnc: personAgePipeResult
                        }
                    }
                ],
                logger: logger2
            })

            const app3CompanyAddIncPipeline = new MessagePipeline({
                name: 'app3_companyAddIncPipeline',
                transactions: [
                    {
                        fnc: companyAddInc,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_ADD_INC,
                            pipeToTopic: Topics.APP_1
                        }
                    }
                ],
                logger: logger3
            })

            const app3CompanyStockPricePipeline = new MessagePipeline({
                name: 'app3_stockPricePipeline',
                transactions: [
                    {
                        fnc: companyDoubleStockPrice,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_DOUBLE_STOCK_PRICE,
                            pipeToTopic: Topics.APP_1
                        }
                    }
                ],
                logger: logger3
            })


            const pipelinesMapApp2 = {
                person_add_middle_name: app2PersonMidNamePipeline,
                person_multiply_age: app2PersonAgePipeline
            }

            const pipelinesMapApp3 = {
                company_add_inc: app3CompanyAddIncPipeline,
                company_double_stock_price: app3CompanyStockPricePipeline
            }

            const appConfig1: KafkaAppConfig = {
                appId: uuid(),
                appName: 'Test APP #1',
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: 'test_connector_1',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: Topics.APP_1,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group_1",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger1,
                kafkaLogger: kafkaLogger1,
                emitWithResponseOptions: {
                    topicEventList: [
                        [Topics.APP_1, Events.PERSON_ADD_MIDDLE_NAME],
                        [Topics.APP_1, Events.PERSON_MULTIPLY_AGE],
                        [Topics.APP_1, Events.COMPANY_ADD_INC],
                        [Topics.APP_1, Events.COMPANY_DOUBLE_STOCK_PRICE],
                    ],
                    cacheClient: cacheClient,
                    returnEventTimeout: 30000
                }
            };

            const appConfig2: KafkaAppConfig = {
                appId: uuid(),
                appName: 'Test APP #2',
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: 'test_connector_2',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: Topics.APP_2,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group_2",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger2,
                kafkaLogger: kafkaLogger2,
                pipelinesMap: pipelinesMapApp2
            };

            const appConfig3: KafkaAppConfig = {
                appId: uuid(),
                appName: 'Test APP #3',
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: 'test_connector_3',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: Topics.APP_3,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group_3",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger3,
                kafkaLogger: kafkaLogger3,
                pipelinesMap: pipelinesMapApp3
            };

            const kafkaApp1 = await KafkaApp.create(appConfig1);
            const kafkaApp2 = await KafkaApp.create(appConfig2);
            const kafkaApp3 = await KafkaApp.create(appConfig3);

            function shutdown() {
                kafkaApp1.close().then(() => {
                    console.log("App #1 Closed.");
                });
                kafkaApp2.close().then(() => {
                    console.log("App #2 Closed.");
                });
                kafkaApp3.close().then(() => {
                    console.log("App #3 Closed.");
                });
                cacheClient.disconnect().then(() => {
                    console.log("Cache client disconnected.");
                })
            }

            async function doCalls() {
                const resp1 = await kafkaApp1.emitWithResponse({
                    topic: Topics.APP_2,
                    messages: [{value: JSON.stringify(messages[0])}],
                    compression: kafkajs.CompressionTypes.GZIP,
                });

                const resp2 = await kafkaApp1.emitWithResponse({
                    topic: Topics.APP_2,
                    messages: [{value: JSON.stringify(messages[1])}],
                    compression: kafkajs.CompressionTypes.GZIP,
                });

                const resp3 = await kafkaApp1.emitWithResponse({
                    topic: Topics.APP_3,
                    messages: [{value: JSON.stringify(messages[2])}],
                    compression: kafkajs.CompressionTypes.GZIP,
                });

                const resp4 = await kafkaApp1.emitWithResponse({
                    topic: Topics.APP_3,
                    messages: [{value: JSON.stringify(messages[3])}],
                    compression: kafkajs.CompressionTypes.GZIP,
                });

                console.log(`\nResponse #1: ${JSON.stringify(resp1)}`);
                console.log(`Response #2: ${JSON.stringify(resp2)}`);
                console.log(`Response #3: ${JSON.stringify(resp3)}`);
                console.log(`Response #4: ${JSON.stringify(resp4)}\n`);

                chai.assert.equal(resp1.payload.payload.firstName, 'John-Joe');
                chai.assert.equal(resp2.payload.payload.age, 70);
                chai.assert.equal(resp3.payload.payload.name, 'SomeCompany INC.');
                chai.assert.equal(resp4.payload.payload.stockValue, 2449.1);

                chai.assert.equal(customMsg, "Person's age is ODD!");

                await delay(2000);
                await kafkaApp3.close();
                await kafkaApp2.close();
                await kafkaApp1.close();
                await cacheClient.disconnect();
            }

            await kafkaApp1.run();
            await kafkaApp2.run();
            await kafkaApp3.run();

            await delay(100);

            await doCalls();
        })();
    });
});

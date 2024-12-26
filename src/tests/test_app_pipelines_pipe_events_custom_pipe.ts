import {KafkaApp, KafkaAppConfig, KafkaMessageMetadata} from "../kafka_app"
import {
    MessagePipeline,
    MessageTransaction,
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
import {expect} from "chai";
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

async function pipeEvent(message: Message, logger: any, kwargs: Record<string, any>) {
    return message;
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

async function printResultCompany(message: Message, logger: any, kwargs: Record<string, any>) {
    logger.info(`Company processing completed: result: ${message.payload}`)
    msgCountCompany++;
    return message;
}


enum Events {
    PROCESS_PERSON = 'process_person',
    PERSON_ADD_MIDDLE_NAME = 'person_add_middle_name',
    PERSON_MULTIPLY_AGE = 'person_multiply_age',
    PERSON_MULTIPLY_AGE_RETURN = 'person_multiply_age_return',
    PERSON_PROCESSED = 'person_processed',

    PROCESS_COMPANY = 'process_company',
    COMPANY_ADD_INC = 'company_add_inc',
    COMPANY_DOUBLE_STOCK_PRICE = 'company_double_stock_price',
    COMPANY_PROCESSED = 'company_processed',
}

enum Topics {
    APP_1 = 'test_topic_1',
    APP_2 = 'test_topic_2',
    APP_3 = 'test_topic_3',
}

const nMessages = 1000;
let msgCountPerson = 0;
let msgCountCompany = 0;

const firstNames = [
    'John',
    'Frank',
    'Alice',
    'Bob',
    'Julie'
]
const lastNames = [
    'Doe',
    'Anderson',
    'Ale',
    'Random',
    'First'
]
const companyNames = [
    'SomeCompany X',
    'SomeCompany Y',
    'SomeCompany Z',
    'SomeCompany J',
    'SomeCompany K'
]

const messagesPerson = new Array(nMessages).fill('').map((val) => {
    return {
        event: Events.PROCESS_PERSON,
        payload: {
            firstName: firstNames[Math.floor(Math.random() * 4)],
            lastName: lastNames[Math.floor(Math.random() * 4)],
            age: Math.floor(Math.random() * 50) + 15
        } as PersonPayload
    } as Message
})

const messagesCompany = new Array(nMessages).fill('').map((val) => {
    return {
        event: Events.PROCESS_COMPANY,
        payload: {
            name: companyNames[Math.floor(Math.random() * 4)],
            stockValue: Math.floor(Math.random() * 1800) + 20
        } as CompanyPayload
    } as Message
})

describe("Kafka app tests", () => {
    it("Test", () => {
        (async () => {
            const KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092';
            const PIPED_EVENT_RETURN_TIMEOUT = 30000;

            process.on("SIGINT", shutdown);
            process.on("SIGTERM", shutdown);
            process.on("SIGBREAK", shutdown);

            const logger0 = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: "Test_kafka_application_0",
                level: Levels.INFO
            })
            const kafkaLogger0 = Logger.getChildLogger(logger0, {
                module: path.basename(__filename),
                component: "Test_kafka_application-Kafka_client_0",
                level: Levels.INFO
            })

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
                database: 0,
                name: 'RedisClient'
            });
            cacheClient.on('error', (err: any) => loggerRedis.error(`Cache client: ${err}`));
            cacheClient.on('ready', () => loggerRedis.info('Cache client connected.'));
            cacheClient.on('end', () => loggerRedis.info('Cache client disconnected.'));
            await cacheClient.connect();

            async function app2PersonAgePipelinePipeResult(
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
                const responseMsg = await fncPipeEventWithResponse(
                    appId,
                    pipelineName,
                    message,
                    emitter,
                    messageKeyAsEvent,
                    {
                        pipeEventName: Events.PERSON_MULTIPLY_AGE_RETURN,
                        pipeToTopic: Topics.APP_3,
                        withResponseOptions: {
                            responseEventName: Events.PERSON_MULTIPLY_AGE_RETURN,
                            responseFromTopic: Topics.APP_2,
                            cacheClient: cacheClient,
                            returnEventTimeout: PIPED_EVENT_RETURN_TIMEOUT
                        }
                    },
                    logger,
                    metadata
                );

                const person = responseMsg.payload;
                logger.info(`Person's age will be ${person.age}`);

                await fncPipeEvent(
                    pipelineName,
                    message,
                    emitter,
                    messageKeyAsEvent,
                    {
                        pipeEventName: Events.PERSON_MULTIPLY_AGE,
                        pipeToTopic: Topics.APP_3
                    },
                    logger,
                    metadata
                )
            }

            async function app3PersonAgePipelinePipeResult(
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
                logger.info(`Person's age is ${Math.floor(person.age / 2) === 0 ? 'EVEN' : 'ODD'}!`)

                await fncPipeEvent(
                    pipelineName,
                    message,
                    emitter,
                    messageKeyAsEvent,
                    {
                        pipeEventName: Events.PERSON_PROCESSED,
                        pipeToTopic: Topics.APP_1
                    },
                    logger,
                    metadata
                )
            }

            const app1ProcessPersonPipeline = new MessagePipeline({
                name: 'app1ProcessPersonPipeline',
                transactions: [
                    {
                        fnc: pipeEvent,
                        pipeResultOptions: {
                            pipeEventName: Events.PERSON_ADD_MIDDLE_NAME,
                            pipeToTopic: Topics.APP_2
                        }
                    }
                ],
                logger: logger1
            })
            const app2ProcessPersonPipeline = new MessagePipeline({
                name: 'app2ProcessPersonPipeline',
                transactions: [
                    {
                        fnc: personAddMiddleName,
                        args: {
                            divider: '-',
                            midName: 'Joe'
                        },
                        pipeResultOptionsCustom: {
                            fnc: app2PersonAgePipelinePipeResult,
                            withResponseOptions: {
                                responseEventTopicKeys: [[Topics.APP_2, Events.PERSON_MULTIPLY_AGE_RETURN]],
                                cacheClient: cacheClient,
                                returnEventTimeout: PIPED_EVENT_RETURN_TIMEOUT
                            }
                        }
                    }
                ],
                logger: logger2
            })
            const app3ProcessPersonPipeline = new MessagePipeline({
                name: 'app3ProcessPersonPipeline',
                transactions: [
                    {
                        fnc: personMultiplyAge,
                        args: {
                            multiplier: 2,
                        },
                        pipeResultOptionsCustom: {
                            fnc: app3PersonAgePipelinePipeResult
                        }
                    }
                ],
                logger: logger3
            })
            const app3ProcessPersonPipelineReturn = new MessagePipeline({
                name: 'app3ProcessPersonPipelineReturn',
                transactions: [
                    {
                        fnc: personMultiplyAge,
                        args: {
                            multiplier: 2,
                        },
                        pipeResultOptions: {
                            pipeEventName: Events.PERSON_MULTIPLY_AGE_RETURN,
                            pipeToTopic: Topics.APP_2
                        }
                    }
                ],
                logger: logger3
            })

            const app1ProcessCompanyPipeline = new MessagePipeline({
                name: 'app1ProcessCompanyPipeline',
                transactions: [
                    {
                        fnc: pipeEvent,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_ADD_INC,
                            pipeToTopic: Topics.APP_2,
                            withResponseOptions: {
                                responseEventName: Events.COMPANY_ADD_INC,
                                responseFromTopic: Topics.APP_1,
                                cacheClient: cacheClient,
                                returnEventTimeout: PIPED_EVENT_RETURN_TIMEOUT
                            }
                        }
                    },
                    {
                        fnc: pipeEvent,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_DOUBLE_STOCK_PRICE,
                            pipeToTopic: Topics.APP_3,
                            withResponseOptions: {
                                responseEventName: Events.COMPANY_PROCESSED,
                                responseFromTopic: Topics.APP_1,
                                cacheClient: cacheClient,
                                returnEventTimeout: PIPED_EVENT_RETURN_TIMEOUT
                            }
                        }
                    },
                    {
                        fnc: printResultCompany
                    }
                ],
                logger: logger1
            })
            const app2ProcessCompanyPipeline = new MessagePipeline({
                name: 'app2ProcessCompanyPipeline',
                transactions: [
                    {
                        fnc: companyAddInc,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_ADD_INC,
                            pipeToTopic: Topics.APP_1
                        }
                    }
                ],
                logger: logger2
            })
            const app3ProcessCompanyPipeline = new MessagePipeline({
                name: 'app3ProcessCompanyPipeline',
                transactions: [
                    {
                        fnc: companyDoubleStockPrice,
                        pipeResultOptions: {
                            pipeEventName: Events.COMPANY_PROCESSED,
                            pipeToTopic: Topics.APP_1
                        }
                    }
                ],
                logger: logger3
            })

            const pipelinesMapApp1 = {
                process_person: app1ProcessPersonPipeline,
                process_company: app1ProcessCompanyPipeline
            }
            const pipelinesMapApp2 = {
                person_add_middle_name: app2ProcessPersonPipeline,
                company_add_inc: app2ProcessCompanyPipeline
            }
            const pipelinesMapApp3 = {
                person_multiply_age: app3ProcessPersonPipeline,
                person_multiply_age_return: app3ProcessPersonPipelineReturn,
                company_double_stock_price: app3ProcessCompanyPipeline
            }

            const appConfig0: KafkaAppConfig = {
                appId: uuid(),
                appName: 'Test APP #0 (event generator)',
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: 'test_connector_0',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: Topics.APP_1,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group_0",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger0,
                kafkaLogger: kafkaLogger0,
            };

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
                pipelinesMap: pipelinesMapApp1,
                maxConcurrentTasks: 128,
                maxConcurrentPipelines: 128,
                taskBufferMaxSize: 256
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
                pipelinesMap: pipelinesMapApp2,
                maxConcurrentTasks: 128,
                maxConcurrentPipelines: 128,
                taskBufferMaxSize: 256
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
                pipelinesMap: pipelinesMapApp3,
                maxConcurrentTasks: 128,
                maxConcurrentPipelines: 128,
                taskBufferMaxSize: 256
            };

            const kafkaApp0 = await KafkaApp.create(appConfig0);
            const kafkaApp1 = await KafkaApp.create(appConfig1);
            const kafkaApp2 = await KafkaApp.create(appConfig2);
            const kafkaApp3 = await KafkaApp.create(appConfig3);

            function shutdown() {
                kafkaApp0.close().then(() => {
                    console.log("App #0 (event generator) Closed.");

                    kafkaApp1.close().then(() => {
                        console.log("App #1 Closed.");

                        kafkaApp2.close().then(() => {
                            console.log("App #2 Closed.");

                            kafkaApp3.close().then(() => {
                                console.log("App #3 Closed.");

                                cacheClient.disconnect().then(() => {
                                    console.log("Cache client disconnected.");
                                })
                            });
                        });
                    });
                });


            }

            kafkaApp1.on(Events.PERSON_PROCESSED, (message) => {
                logger1.info(`Person processing completed: result: ${JSON.stringify(message.payload)}`)
                msgCountPerson++;
            })

            await kafkaApp0.run();
            await kafkaApp1.run();
            await kafkaApp2.run();
            await kafkaApp3.run();

            const messages = [...messagesPerson, ...messagesCompany];
            const batchSize: number = 50;
            while (messages.length > 0) {
                const batch = [];
                if (messages.length > batchSize) {
                    for (let i = 0; i < batchSize; i++) {
                        batch.push({value: JSON.stringify(messages.pop())});
                    }
                } else {
                    // tslint:disable-next-line:prefer-for-of
                    for (let i = 0; i < messages.length; i++) {
                        batch.push({value: JSON.stringify(messages.pop())});
                    }
                }

                await delay(10);
                await kafkaApp0.emit({
                    topic: Topics.APP_1,
                    messages: batch,
                    compression: kafkajs.CompressionTypes.GZIP,
                });
            }

            const watcherInterval = setInterval(
                async () => {
                    if (msgCountPerson === messagesPerson.length && msgCountCompany === messagesCompany.length) {
                        clearInterval(watcherInterval); // Exit the loop if stop is true and queue is empty
                        logger0.info(`Received all messages (${msgCountPerson + msgCountCompany}) and now closing...`)
                        shutdown()
                    }
                },
                1);
        })();
    });
});

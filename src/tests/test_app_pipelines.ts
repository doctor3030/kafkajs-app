import {KafkaApp, KafkaAppConfig, KafkaMessageMetadata, PipelineTask} from "../kafka_app"
import {MessagePipeline, MessageTransaction} from "../kafka_app_pipeline"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";
import "mocha";
import {v4 as uuid} from "uuid";
import {createClient, RedisClientType} from 'redis';

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

enum Events {
    PROCESS_PERSON = 'process_person',
    PROCESS_COMPANY = 'process_company',
    PROCESS_PERSON_ASYNC = 'process_person_async',
    PROCESS_COMPANY_ASYNC = 'process_company_async',
}

enum Topics {
    TEST_TOPIC = 'test_topic',
}

const messages: Message[] = [
    {
        'event': Events.PROCESS_PERSON,
        'payload': {
            'firstName': 'John',
            'lastName': 'Doe',
            'age': 35
        }
    },
    {
        'event': Events.PROCESS_COMPANY,
        'payload': {
            'firstName': 'John',
            'lastName': 'Doe',
            'age': 35
        }
    },
    {
        'event': Events.PROCESS_PERSON_ASYNC,
        'payload': {
            'first_name': 'John Async',
            'last_name': 'Doe',
            'age': 15
        }
    },
    {
        'event': Events.PROCESS_COMPANY_ASYNC,
        'payload': {
            'name': 'SomeCompany Async',
            'stock_value': 12424.55
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

            const personPipeline = new MessagePipeline({
                name: 'personPipeline',
                transactions: [
                    {
                        fnc: personAddMiddleName,
                        args: {
                            divider: '-',
                            midName: 'Joe'
                        }
                    },
                    {
                        fnc: personMultiplyAge,
                        args: {
                            multiplier: 2,
                        }
                    }
                ],
                logger: logger
            })

            const companyPipeline = new MessagePipeline({
                name: 'app3_companyAddIncPipeline',
                transactions: [
                    {
                        fnc: companyAddInc
                    },
                    {
                        fnc: companyDoubleStockPrice
                    }
                ],
                logger: logger
            })


            const pipelinesMap = {
                process_person: personPipeline,
                process_company: companyPipeline
            }

            const appConfig: KafkaAppConfig = {
                appId: uuid(),
                appName: 'Test APP',
                clientConfig: {
                    brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
                    clientId: 'test_connector',
                    logLevel: kafkajs.logLevel.INFO,
                },
                listenerConfig: {
                    topics: [
                        {
                            topic: Topics.TEST_TOPIC,
                            fromBeginning: false,
                        },
                    ],
                    groupId: "test_kafka_connectorjs_group",
                    sessionTimeout: 25000,
                    allowAutoTopicCreation: false,
                    autoCommit: false,
                    eachBatchAutoResolve: false,
                    partitionsConsumedConcurrently: 4,
                },
                producerConfig: {
                    allowAutoTopicCreation: false,
                },
                logger: logger,
                kafkaLogger: kafkaLogger,
                pipelinesMap: pipelinesMap
            };

            const kafkaApp = await KafkaApp.create(appConfig);

            function shutdown() {
                kafkaApp.close().then(() => {
                    console.log("App Closed.");
                });
                cacheClient.disconnect().then(() => {
                    console.log("Cache client disconnected.");
                })
            }

            let msgCounter = 0;

            kafkaApp.on(Events.PROCESS_PERSON, (message) => {
                logger.info('Handling "process_person" event..');
                logger.info(`Received: ${JSON.stringify(message)}\n`);
                msgCounter++;
            })

            kafkaApp.on(Events.PROCESS_COMPANY, (message) => {
                logger.info('Handling "process_company" event..');
                logger.info(`Received: ${JSON.stringify(message)}\n`);
                msgCounter++;
            })

            kafkaApp.on(Events.PROCESS_PERSON_ASYNC, async (message) => {
                logger.info('Handling "process_person_async" event..');
                logger.info(`Received: ${JSON.stringify(message)}\n`);
                msgCounter++;
            })

            kafkaApp.on(Events.PROCESS_COMPANY_ASYNC, async (message) => {
                logger.info('Handling "process_company_async" event..');
                logger.info(`Received: ${JSON.stringify(message)}\n`);
                msgCounter++;
            })

            await kafkaApp.run();

            await kafkaApp.emit({
                topic: Topics.TEST_TOPIC,
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

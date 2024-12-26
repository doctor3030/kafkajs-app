import {v4 as uuidv4} from 'uuid';
import * as kafka from "kafkajs";
import * as Logger from "winston-logger-kafka";
import path from "path";
import {CompressionTypes} from "kafkajs";
import {createHash} from "crypto";
import {KafkaMessageMetadata} from './kafka_app'

export interface TransactionPipeWithReturnOptions {
    responseEventName: string;
    responseFromTopic: string;
    cacheClient: any;
    returnEventTimeout: number;
}

interface TransactionPipeWithReturnOptionsMultikey {
    responseEventTopicKeys: [string, string][];
    cacheClient: any;
    returnEventTimeout: number;
}

export interface TransactionPipeResultOptions {
    pipeEventName: string;
    pipeToTopic: string;
    compression?: CompressionTypes;
    acks?: number
    timeout?: number;
    withResponseOptions?: TransactionPipeWithReturnOptions;
}

export interface TransactionPipeResultOptionsCustom {
    fnc: (
        appId: string | null,
        pipelineName: string | null,
        message: any,
        emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
        messageKeyAsEvent: boolean,
        fncPipeEvent: PipeEventFunction,
        fncPipeEventWithResponse: PipeEventWithResponseFunction,
        logger: any,
        metadata: KafkaMessageMetadata
    ) => any;
    withResponseOptions?: TransactionPipeWithReturnOptionsMultikey;
}

export interface MessageTransaction {
    fnc: (message: any, logger: any, kwargs: Record<string, any>) => any | Promise<any>;
    args?: { [key: string]: any };
    pipeResultOptions?: TransactionPipeResultOptions;
    pipeResultOptionsCustom?: TransactionPipeResultOptionsCustom;
}

export type PipeEventFunction = (
    pipelineName: string | null,
    message: any,
    emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
    messageKeyAsEvent: boolean,
    options: TransactionPipeResultOptions,
    logger: any,
    metadata: KafkaMessageMetadata
) => Promise<string | null>

export type PipeEventWithResponseFunction = (
    appId: string | null,
    pipelineName: string | null,
    message: any,
    emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
    messageKeyAsEvent: boolean,
    options: TransactionPipeResultOptions,
    logger: any,
    metadata: KafkaMessageMetadata
) => Promise<any>

export class MessagePipeline {
    public name: string | null = null;
    public transactions: MessageTransaction[];
    public appId: string | null = null;
    public logger: any = console;

    constructor(options: {
        name?: string;
        transactions: MessageTransaction[];
        appId?: string;
        logger?: any;
    }) {
        this.appId = options.appId || uuidv4();
        this.name = options.name || uuidv4();
        this.transactions = options.transactions;

        if (options.logger) {
            this.logger = options.logger;
        } else {
            this.logger = Logger.getDefaultLogger({
                module: path.basename(__filename),
                component: this.name,
                level: Logger.Levels.INFO
            })
        }
    }

    private static getEventIdHash(eventId: string): string {
        return createHash('sha256')
            .update(eventId)
            .digest('hex')
    }

    async execute(
        message: any,
        emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
        messageKeyAsEvent: any,
        metadata: KafkaMessageMetadata
    ): Promise<void> {
        const timeUp = Date.now();
        this.logger.info(`message pipeline (${this.name}) => STARTED`);

        try {
            for (const tx of this.transactions) {
                this.logger.debug(
                    `Executing transaction: name: ${tx.fnc.name || tx.fnc.toString()}`
                );

                if (tx.fnc.constructor.name === "AsyncFunction") {
                    if (tx.args) {
                        message = await tx.fnc(message, this.logger, {...tx.args, ...metadata});
                    } else {
                        message = await tx.fnc(message, this.logger, metadata);
                    }
                } else {
                    if (tx.args) {
                        message = tx.fnc(message, this.logger, {...tx.args, ...metadata});
                    } else {
                        message = tx.fnc(message, this.logger, metadata);
                    }
                }

                if (tx.pipeResultOptions) {
                    if (tx.pipeResultOptions.withResponseOptions) {
                        await MessagePipeline.pipeEventWithResponse(
                            this.appId,
                            this.name,
                            message,
                            emitter,
                            messageKeyAsEvent,
                            tx.pipeResultOptions,
                            this.logger,
                            metadata
                        );
                    } else {
                        await MessagePipeline.pipeEvent(
                            this.name,
                            message,
                            emitter,
                            messageKeyAsEvent,
                            tx.pipeResultOptions,
                            this.logger,
                            metadata
                        );
                    }

                } else if (tx.pipeResultOptionsCustom) {
                    if (tx.pipeResultOptionsCustom.fnc.constructor.name === "AsyncFunction") {
                        await tx.pipeResultOptionsCustom.fnc(
                            this.appId,
                            this.name,
                            message,
                            emitter,
                            messageKeyAsEvent,
                            MessagePipeline.pipeEvent,
                            MessagePipeline.pipeEventWithResponse,
                            this.logger,
                            metadata
                        )
                    } else {
                        tx.pipeResultOptionsCustom.fnc(
                            this.appId,
                            this.name,
                            message,
                            emitter,
                            messageKeyAsEvent,
                            MessagePipeline.pipeEvent,
                            MessagePipeline.pipeEventWithResponse,
                            this.logger,
                            metadata
                        )
                    }
                }
            }

        } catch (e: any) {
            const msg = `message pipeline (${this.name}) execute => Exception: 
            message: ${e.message}; 
            stack: ${e.stack}`
            this.logger.error(msg);

        } finally {
            const timeDown = Date.now();
            this.logger.info(
                `message pipeline (${this.name}) => FINISHED | latency(s): ${(timeDown - timeUp) / 1000}`
            );
        }
    }

    public static async pipeEvent(
        pipelineName: string | null,
        message: any,
        emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
        messageKeyAsEvent: boolean,
        options: TransactionPipeResultOptions,
        logger: any,
        metadata: KafkaMessageMetadata
    ): Promise<string | null> {
        let eventId: string | undefined;
        let headers = metadata.headers;

        if (headers) {
            eventId = Buffer.isBuffer(headers.event_id) ? new TextDecoder('utf-8').decode(headers.event_id) : headers.event_id;
        }

        if (!eventId) {
            eventId = uuidv4();
        }

        if (headers) {
            headers.event_id = eventId;
        } else {
            headers = {event_id: eventId}
        }

        try {
            logger.info(
                `<-------- PIPING EVENT (message pipeline ${pipelineName}): 
                name: ${options.pipeEventName}; 
                to: ${options.pipeToTopic}; 
                event_id: ${eventId}`
            );

            if (!messageKeyAsEvent) {
                message.event = options.pipeEventName;
                await emitter({
                    topic: options.pipeToTopic,
                    messages: [{
                        value: JSON.stringify(message),
                        headers: headers
                    }]
                })

            } else {
                await emitter({
                    topic: options.pipeToTopic,
                    messages: [{
                        value: JSON.stringify(message),
                        headers: headers
                    }]
                })
            }

            return eventId;

        } catch (e: any) {
            const msg = `message pipeline (${pipelineName}) pipe_event => Exception: 
            message: ${e.message}; 
            stack: ${e.stack};`;
            logger.error(msg);

            return null;
        }
    }

    public static async pipeEventWithResponse(
        appId: string | null,
        pipelineName: string | null,
        message: any,
        emitter: (record: kafka.ProducerRecord) => Promise<kafka.RecordMetadata[]>,
        messageKeyAsEvent: boolean,
        options: TransactionPipeResultOptions,
        logger: any,
        metadata: KafkaMessageMetadata
    ): Promise<any> {
        const cacheClient = options.withResponseOptions?.cacheClient;
        const eventId = await MessagePipeline.pipeEvent(
            pipelineName!,
            message,
            emitter,
            messageKeyAsEvent,
            options,
            logger,
            metadata
        );

        if (!eventId) {
            throw new Error('Pipe event failed');
        }

        const timeUp = Date.now();

        return new Promise((resolve, reject) => {
            const interval = setInterval(
                async () => {
                    const response = await cacheClient.get(MessagePipeline.getEventIdHash(eventId));

                    if (response) {
                        clearInterval(interval);
                        resolve(JSON.parse(response));

                        logger.info(
                            `--------> PIPE RESPONSE RECEIVED (message pipeline ${pipelineName}): 
                            name: ${options.withResponseOptions?.responseEventName} 
                            event_id: ${eventId}`
                        );
                    }

                    if (Date.now() - timeUp > options.withResponseOptions!.returnEventTimeout) {
                        clearInterval(interval);
                        reject(new Error(
                            `message pipeline (${pipelineName}) pipe event with response timeout: 
                            piped event: ${options.pipeEventName}; 
                            to: ${options.pipeToTopic}; 
                            response event: ${options.withResponseOptions!.responseEventName}; 
                            from: ${options.withResponseOptions!.responseFromTopic}; 
                            event_id: ${eventId}`
                        ));
                    }

                },
                1);
        });
    }
}

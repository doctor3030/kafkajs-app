# A lightweight application for consistent handling of kafka events

This application was developed to be a universal api endpoint for microservices that process kafka events.

## Installation

```
npm install kafkajs-app
```

## Usage

### Configure and create application instance.

Configuration parameters:

```typescript
export interface KafkaAppConfig {
    appName?: string
    appId?: string
    clientConfig: kafka.KafkaConfig
    producerConfig?: ProducerConfig
    listenerConfig: ListenerConfig
    messageKeyAsEvent?: boolean
    middlewareBatchCb?: (payload: kafka.EachBatchPayload) => void
    emitWithResponseOptions?: EmitWithResponseOptions
    pipelinesMap?: Record<string, MessagePipeline>
    maxConcurrentTasks?: number
    maxConcurrentPipelines?: number
    taskBufferMaxSize?: number
    taskBufferMaxSizeBytes?: number
    logger?: ILogger
    kafkaLogger?: ILogger
}
```

- **appName [OPTIONAL]**: application name.
- **appId [OPTIONAL]**: application ID.
- **clientConfig**: kafka client config (see [kafkajs client configuration](https://kafka.js.org/docs/configuration)).
- **producerConfig [OPTIONAL]**: kafka producer configuration
  (see [kafka connector](#connector) for details).
- **listenerConfig [OPTIONAL]**: kafka listener configuration
  (see [kafka connector](#connector) for details).

> **_NOTE_** The **ListenerConfig** interface
> extends ListenerConfig interface of [KafkaConnector](#connector).
>
> Extended options include two additional options called
> **keyDeserializer** and **valueDeserializer**.
>
> The **keyDeserializer** is a function that takes kafka message key (bytes)
> as an argument and returns a string. Default key deserializer
> is a standard **toString()** method.
> ```typescript
> // Default key deserializer:
> key_deserialized = key.toString();
> ```
>
> The **valueDeserializer** takes
> kafka message value as an argument and should return a json object.
> By default **valueDeserializer** is a standard json deserializer.
> ```typescript
> // Default value deserializer:
> value_deserialized = JSON.parse(value.toString());
> ```

- **messageKeyAsEvent [OPTIONAL]**: set to True if using kafka message key as event name.

> **_NOTE_** When setting ***messageKeyAsEvent*** to true,
> make sure to specify valid **keyDeserializer** in application config.

> **_NOTE_** When ***messageKeyAsEvent*** is false,
> the application will be looking for **event** property in
> deserialized message json.
> If kafka message value does not contain **event** property,
> the message will be ignored.

- **middlewareBatchCb [OPTIONAL]**: if provided, this callback will be executed with
  raw [EachBatchPayload](https://kafka.js.org/docs/consuming#eachbatch) as an argument before calling any handler.

- **emitWithResponseOptions [OPTIONAL]**: see [emitWithResponse method](#use-appemitwithresponse-method-to-send-messages-to-kafka-and-wait-for-response-event)

- **pipelinesMap [OPTIONAL]**: see [pipelines](#use-message-pipelines) usage.

- **maxConcurrentTasks [OPTIONAL]**: number - number of individual event handlers executed concurrently
- **maxConcurrentPipelines [OPTIONAL]**: number - number of pipelines executed concurrently
- **taskBufferMaxSize [OPTIONAL]**: number - maximum number of buffered tasks
- **taskBufferMaxSizeBytes [OPTIONAL]**: number - maximum size of task buffer in bytes

> **_NOTE_** The messages obtained from kafka consumer are assigned a corresponding task 
> that is either a standalone handler or a pipeline. 
> The tasks are then buffered before being 
> executed in batches (see **maxConcurrentTasks** and **maxConcurrentPipelines**) by corresponding worker.
> The **taskBufferMaxSize** and **taskBufferMaxSizeBytes** options are needed to prevent memory 
> overload when messages are consumed faster than being processed and task buffer gets overloaded.
> When task buffer size reaches **taskBufferMaxSize** or **taskBufferMaxSizeBytes** 
> the kafka listener is paused until some messages are processed 
> and task buffer size reduces.
> 
> Default: taskBufferMaxSize: 128; taskBufferMaxSizeBytes: 134217728 (128 Mb)

- **logger [OPTIONAL]**: any logger with standard log methods. If not provided,
  the [winston-logger-kafka](https://www.npmjs.com/package/winston-logger-kafka) is used with console transport and
  DEBUG level.

- **kafkaLogger [OPTIONAL]**: kafka client logger. Can be any logger with standard log methods. If not provided,
  the [winston-logger-kafka](https://www.npmjs.com/package/winston-logger-kafka) is used with console transport and
  DEBUG level.

```typescript
import {KafkaAppConfig, KafkaApp} from "kafkajs-app"
import * as kafkajs from "kafkajs";
import * as Logger from "winston-logger-kafka";
import {Levels} from "winston-logger-kafka";


// Setup kafka message payload object:
interface MyMessage {
    event: string
    prop1: string
    prop2: number
}

// This message type is specific for "test_topic3" only. 
interface MyMessageSpecific {
    event: string
    prop3: bool
    prop4: float
}

// Setup logger
const logger = Logger.getDefaultLogger({
    module: path.basename(__filename),
    component: "Test_kafka_application",
    level: Levels.DEBUG
});

const kafkaLogger = Logger.getChildLogger(logger, {
    module: path.basename(__filename),
    component: "Test_kafka_application-Kafka_client",
    level: Levels.DEBUG
})

// Create application config
const appConfig: KafkaAppConfig = {
    appName: 'My Application',
    clientConfig: {
        brokers: ['localhost:9092'],
        logLevel: kafkajs.logLevel.DEBUG,
    },
    consumerConfig: {
        groupId: "kafkajs-app_test_group",
        sessionTimeout: 25000,
        allowAutoTopicCreation: false,
        topics: [
            {
                topic: "test_topic",
                fromBeginning: false,
            },
        ],
    },

    producerConfig: {
        allowAutoTopicCreation: false,
    },
    logger: logger,
    kafkaLogger: kafkaLogger,
};

// Create application
const app = await KafkaApp.create(appConfig);
```

### Create event handlers using **app.on()** method:

app.on() method takes three arguments:

- **eventName**: event name
- **cb**: a callback function that that will be handeling event
- **topic [OPTIONAL]**: topic name to handle events from

If **topic** argument is provided to **app.on()** method, the callback is mapped to particular topic.event key that
means an event that comes from a topic other than the specified will not be processed. Otherwise, event will be
processed no matter which topic it comes from.

The callback function take two arguments:

- message: deserialized value of the kafka message (your payload object)
- metadata: kafka message metadata object (see **KafkaMessageMetadata** interface)

```typescript
// This handler will handle 'some_event' no matter which topic it comes from
app.on(
    'some_event', // event name
    (message, metadata) => {
        console.log(`Handling event: ${message.event}`);
        console.log(`prop1: ${message.prop1}`);
        console.log(`prop2: ${message.prop2}`);
    } // event callback
);

// This handler will handle 'another_event' from 'test_topic2' only.
// Events with this name but coming from another topic will be ignored.
app.on(
    'another_event', // event name
    (message, metadata) => {
        console.log(`Handling event: ${message.event}`);
        console.log(`prop1: ${message.prop1}`);
        console.log(`prop2: ${message.prop2}`);
    }, // event callback
    'test_topic2' // topic name
);


// This handler will handle 'another_event' from 'test_topic3' only.
app.on(
    'another_event', // event name
    (message, metadata) => {
        console.log(`Handling event: ${message.event}`);
        console.log(`prop3: ${message.prop3}`);
        console.log(`prop4: ${message.prop4}`);
    }, // event callback
    'test_topic3' // topic name
);
```

### Use message pipelines

The **app.on** method is used to register a single handler for a message. 
However, now it is possible to pass a message through a sequence of handlers
by providing a **pipelines_map** option into the application config.
The **pipelines_map** is an object of type **Record<string, MessagePipeline>**.

**MessagePipeline** class has the following properties:
- transactions: MessageTransaction[]
- logger (optional): any object that has standard logging methods (debug, info, error, etc.)

**MessageTransaction** properties:

```typescript
export interface MessageTransaction {
    fnc: (message: any, logger: any, kwargs: Record<string, any>) => any | Promise<any>;
    args?: { [key: string]: any };
    pipeResultOptions?: TransactionPipeResultOptions;
    pipeResultOptionsCustom?: TransactionPipeResultOptionsCustom;
}
```

- fnc: (message: any, logger: any, kwargs: Record<string, any>) => any | Promise\<any\> - transaction function that takes message, somehow transforms it and returns it to next 
transaction in the pipeline.
> **_NOTE:_** Transaction function can be sync or async.
- args (optional): Record<string, any> - collection of keyword arguments for transaction function.
- pipeResultOptions (optional): TransactionPipeWithReturnOptions - configuration to pipe message to 
another service.

**TransactionPipeResultOptions** class properties:

```typescript
export interface TransactionPipeResultOptions {
    pipeEventName: string;
    pipeToTopic: string;
    compression?: CompressionTypes;
    acks?: number
    timeout?: number;
    withResponseOptions?: TransactionPipeWithReturnOptions;
}
```

- pipeEventName: string - event name that will be sent
- pipeToTopic: string - destination topic
- compression (optional): CompressionTypes - type of compression for kafka message (see [kafkajs Producer](https://kafka.js.org/docs/producing) documentation).
- acks (optional): number - see [kafkajs Producer](https://kafka.js.org/docs/producing) documentation.
- timeout (optional): number - see [kafkajs Producer](https://kafka.js.org/docs/producing) documentation.
- withResponseOptions (optional): TransactionPipeWithReturnOptions - this config is used when pipeline 
needs to wait response from another service before proceeding to next transaction.

**TransactionPipeWithReturnOptions** class properties:

```typescript
export interface TransactionPipeWithReturnOptions {
    responseEventName: string;
    responseFromTopic: string;
    cacheClient: any;
    returnEventTimeout: number;
}
```
- responseEventName: string - event name of the response
- responseFromTopic: string - topic name that the response should come from
- cacheClient: any - client of any caching service that has methods **get** and **set**
> **get** method signature: get(key: str) -> bytes
> 
> **set** method signature: set(name: str, value: bytes, ex: int (record expiration in sec))
- returnEventTimeout: number - timeout in **milliseconds** for returned event

**TransactionPipeResultOptionsCustom** class properties:

> **_NOTE:_** Use this if you need to implement custom logic after transaction function is executed.

```typescript
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

interface TransactionPipeWithReturnOptionsMultikey {
    responseEventTopicKeys: [string, string][];
    cacheClient: any;
    returnEventTimeout: number;
}
```

- fnc: Callable - function that will be executed after transaction function is finished.
- withResponseOptions (optional): TransactionPipeWithReturnOptionsMultikey - these option will be 
used by **kafka app** to register caching pipelines. This is the same as **TransactionPipeWithReturnOptions** 
but instead of separate options **responseEventName** and **responseFromTopic** there is a 
list of tuples **(responseFromTopic, responseEventName)** called **responseEventTopicKeys**.

#### Example:
Suppose we receive a string with each message and that string needs to be processed.
Let's say we need replace all commas with a custom symbol and then 
add a number to the end of the string that correspond to the number of some
substring occurrences.

```typescript
import * as kafkajs from "kafkajs";
import * as kafka from "kafkajs-app";
import {v4 as uuid} from "uuid";

// Define transaction functions
function txnReplace(message: string, logger: any, kwargs: Record<string, any>) {
    return message.replace(',', kwargs.symbol);
}

function txnAddCount(message: string, logger: any, kwargs: Record<string, any>) {
    return `${message} ${message.split(kwargs.substr).length - 1}`;
}

// Define a pipeline
const pipeline = new MessagePipeline({
    transactions: [
        {
            fnc: txnReplace,
            args: {'symbol': '|'}
        },
        {
            fnc: txnAddCount,
            args: {'substr': 'foo'}
        }
    ]
});

// Define a pipelines map
const pipelinesMap = {'some_event': pipeline};

const appConfig = {
    appId: uuid(),
    appName: 'Test APP',
    clientConfig: {
        brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
        clientId: 'test_connector_1',
        logLevel: kafkajs.logLevel.INFO,
    },
    listenerConfig: {
        topics: [
            {
                topic: 'test_topic1',
                fromBeginning: false,
            },
        ],
        groupId: "test_app_group",
        sessionTimeout: 25000,
        allowAutoTopicCreation: false,
        autoCommit: false,
        eachBatchAutoResolve: false,
        partitionsConsumedConcurrently: 4,
    },
    producerConfig: {
        allowAutoTopicCreation: false,
    },
    pipelinesMap: pipelinesMap,
    maxConcurrentTasks: 128,
    maxConcurrentPipelines: 128,
    taskBufferMaxSize: 256
};


// Create application
const app = await KafkaApp.create(appConfig);
```

> **_NOTE:_** Define pipelines_map keys in a format 'topic.event' in order
> to restrict pipeline execution to events that come from a certain topic.

### Use **app.emit()** method to send messages to kafka:

The app.emit() method just wraps up send() method of the kafkajs Producer. It takes kafkajs ProducerRecord as an
argument and returns kafkajs RecordMetadata.

```typescript
import {ProducerRecord, RecordMetadata, CompressionTypes} from "kafkajs";


// Define a payload you want to send:
const msgValue = {
    event: 'some_event',
    prop1: 'prop1 value',
    prop2: 34
};

// Define kafka message:
const msg: ProducerRecord = {
    key: 'some key',
    value: msgValue
};

// Send message
await app.emit({
    topic: 'test_topic1',
    messages: [msg],
    compression: CompressionTypes.GZIP
}).then((metaData) => {
    console.log(metaData);
});
```

### Use **app.emitWithResponse()** method to send messages to kafka and wait for response event:

Specify **EmitWithResponseOptions** in the application config.

**EmitWithResponseOptions** class properties:

```typescript
interface EmitWithResponseOptions {
    topicEventList: [string, string][];
    cacheClient: any;
    returnEventTimeout: number; // milliseconds
}
```

- topicEventList: list of tuples (topicName: string, eventName: string) for returned events. 
This defines what events and from what topics should be cached as "response" events.
- cacheClient: Any - client of any caching service that has methods **get** and **set**
> get method signature: get(key: str) -> bytes
> 
> set method signature: set(name: str, value: bytes, ex: int (record expiration in sec))
- returnEventTimeout: number - timeout in **milliseconds** for returned event

```typescript
import * as kafkajs from "kafkajs";
import * as kafka from "kafkajs-app";
import {v4 as uuid} from "uuid";

// Create application config
// config = AppConfig(
//     app_name='Test application',
//     bootstrap_servers=['localhost:9092'],
//     consumer_config={
//         'group_id': 'test_app_group'
//     },
//     listen_topics=['test_topic1'],
//     emit_with_response_options=EmitWithReturnOptions(
//         event_topic_list=[
//             ("test_topic_2", "response_event_name")
//         ],
//         cache_client=redis_client,
//         return_event_timeout=30
//     ),
// )

const appConfig1: KafkaAppConfig = {
    appId: uuid(),
    appName: 'Test APP',
    clientConfig: {
        brokers: KAFKA_BOOTSTRAP_SERVERS.split(","),
        clientId: 'test_connector_1',
        logLevel: kafkajs.logLevel.INFO,
    },
    listenerConfig: {
        topics: [
            {
                topic: test_topic1,
                fromBeginning: false,
            },
        ],
        groupId: "test_app_group",
        sessionTimeout: 25000,
        allowAutoTopicCreation: false,
        autoCommit: false,
        eachBatchAutoResolve: false,
        partitionsConsumedConcurrently: 4,
    },
    producerConfig: {
        allowAutoTopicCreation: false,
    },
    emitWithResponseOptions: {
        topicEventList: [
            ["test_topic_2", "responseEventName"],
        ],
        cacheClient: redisClient,
        returnEventTimeout: 30000
    }
};

// Create application
const app = await KafkaApp.create(appConfig);

// Message to send
const msg = {
    event: "test_event_name",
    payload: "Hello?"
}

const response = await app.emitWithResponse({
    topic: "some_topic",
    messages: [{value: JSON.stringify(msg)}],
    compression: kafkajs.CompressionTypes.GZIP,
});
```

### Start application:

```typescript
await app.run();
```

## <a name="connector"></a> Standalone kafka connector

The **KafkaConnector** class can be used to simplify producer and consumer initialization.

When  **KafkaConnector** is initialized, you can use its methods to get producer and listener:

- **getProducer()** method returns an instance of the **KafkaProducer** class that wraps up initialization and
  connection of the
  [kafkajs producer](https://kafka.js.org/docs/producing).
- **getListener()** method returns an instance of the **KafkaListener** class that wraps
  up [kafkajs consumer](https://kafka.js.org/docs/consuming), its initialization, connection and subscription to topics.
  When listener created, use **
  listen()** method to start consume messages.

### Configuration

Kafka connector configuration has four parameters:

- **clientConfig**: kafkajs client configuration (
  see [kafkajs client configuration](https://kafka.js.org/docs/configuration)).
- **producerConfig [OPTIONAL]**: contains all standard kafkajs producer configuration options plus additional
  parameter **producerCallbacks**
  that can be used to pass custom callbacks for producer events.
- **listenerConfig [OPTIONAL]**: this configuration brings together all parameter of kafkajs consumer including
  ConsumerConfig, ConsumerRunConfig and topics. It also brings in **consumerCallbacks** parameter where you can specify
  callbacks for consumer events.
- **logger [OPTIONAL]**: this can be any logger having a standard log methods.

### KafkaProducer & KafkaListener classes

These classes play a role of a wrappers of kafkajs producer and consumer taking care of their initialization and
connection. When intantiated, producer expose methods **send()** and **close()**
and listener expose methods **listen()** and **close()**. The kafkajs producer and consumer can still be accessed
directly using **.producer** and **.consumer**
properties of the KafkaProducer and KafkaListener correspondingly.

### Usage

Import libraries:

```typescript
import * as kafkaConnector from "kafkajs-app";
import * as kafkajs from "kafkajs";
```

Define callback function for process messages:

```typescript
async function processMessage(payload: kafkajs.EachMessagePayload) {
    if (payload.message.value) {
        const receivedMessage: Record<string, string> = JSON.parse(payload.message.value.toString());
        logger.info(`Received message: type: ${typeof receivedMessage}, message: ${receivedMessage.msg}`);
    }
}
```

Create connector configuration:

```typescript
const configKafka: kafka.KafkaConnectorConfig = {
    clientConfig: {
        brokers: ['localhost:9092'],
        clientId: 'test_connector_1',
        logLevel: kafkajs.logLevel.INFO,
    },
    listenerConfig: {
        groupId: "test_kafka_connectorjs_group",
        sessionTimeout: 25000,
        allowAutoTopicCreation: false,
        topics: [
            {
                topic: 'test_topic',
                fromBeginning: false,
            },
        ],
        autoCommit: true,
        eachMessage: processMessage,
        consumerCallbacks: {
            "consumer.connect": (listener: kafkajs.ConnectEvent) => {
                logger.info(`Consumer connected at ${listener.timestamp}`);
            },
            "consumer.disconnect": (listener: kafkajs.DisconnectEvent) => {
                logger.info(`Consumer disconnected at ${listener.timestamp}`);
            },
        }
    },
    producerConfig: {
        allowAutoTopicCreation: false,
        producerCallbacks: {
            "producer.connect": () => (listener: kafkajs.ConnectEvent) => {
                logger.info(`Producer connected at ${listener.timestamp}`);
            },
            "producer.disconnect": (listener: kafkajs.DisconnectEvent) => {
                logger.info(`Producer connected at ${listener.timestamp}`);
            }
        }
    }
};
```

Create connector instance, listener and producer:

```typescript
const kafkaConnector = new kafka.KafkaConnector(configKafka);
const listener = await kafkaConnector.getListener();
const producer = await kafkaConnector.getProducer();
```

Start consuming messages:

```typescript
await listener.listen();
```

Send messages:

```typescript
const test_message: Record<string, string> = {msg: "Hello!"};
await producer.send({
    topic: 'test_topic',
    messages: [{value: JSON.stringify(test_message)}],
    compression: kafkajs.CompressionTypes.GZIP,
});
```

## LICENSE
MIT

##### AUTHOR: [Dmitry Amanov](https://github.com/doctor3030)
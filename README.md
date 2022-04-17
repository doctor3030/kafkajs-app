# A lightweight application for consistent handling of kafka events

This application was developed to be a universal api endpoint for microservices that process kafka events.

## Installation

```
npm install kafkajs-app
```

## Usage

### Configure and create application instance.

Configuration parameters:

- **app_name [OPTIONAL]**: application name.
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
                topic: TEST_TOPIC,
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
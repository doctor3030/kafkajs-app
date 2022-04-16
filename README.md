# A lightweight application for consistent handling of kafka events

This application was developed to be a universal api endpoint for microservices that process kafka events.

## Installation

```
npm install kafkajs-app
```

## Usage

#### Configure and create application instance.

[test](#test)

Configuration parameters:

- **app_name [OPTIONAL]**: application name.
- **clientConfig**: kafka client config (see [kafkajs client configuration](https://kafka.js.org/docs/configuration)).
- **producerConfig [OPTIONAL]**: kafka producer configuration (
  see [kafkajs producer configuration](https://kafka.js.org/docs/producing)).

> **_NOTE_** The **ListenerConfig** interface
> extends ConsumerConfig interface from kafkajs.
>

- **listenerConfig [OPTIONAL]**: kafka listener configuration.

> **_NOTE_** The **AppListenerConfig** interface
> extends ConsumerConfig interface from kafkajs.
>
> Extended options include all **commit strategy options**
> from kafkajs **ConsumerRunConfig** type and  
> two additional options called **keyDeserializer** and **valueDeserializer**.
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

- **topics**: list of topics to subscribe to.
- **messageKeyAsEvent [OPTIONAL]**: set to True if using kafka message key as event name.

> **_NOTE_** When setting ***messageKeyAsEvent*** to true,
> make sure to specify valid **keyDeserializer** in application config.

> **_NOTE_** When ***messageKeyAsEvent*** is false,
> the application will be looking for **event** property in
> deserialized message json.
> If kafka message value does not contain **event** property,
> the message will be ignored.

- **middlewareMessageCb [OPTIONAL]**: if provided, this callback will be executed with raw kafka message as an argument
  before calling any handler.

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

// Create application config
const appConfig: KafkaAppConfig = {
    appName: 'My Application',
    clientConfig: {
        brokers: ['localhost:9092'],
        logLevel: kafkajs.logLevel.DEBUG,
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
    logger: logger,
    middlewareMessageCb: eachMessageMiddleware
};

// Create application
const app = await KafkaApp.create(appConfig);
```

#### Create event handlers using **app.on()** method:

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

> **_NOTE:_** If **topic** argument is provided to **app.on()** method,
> the callback is mapped to particular topic.event key that means
> an event that comes from a topic other than the specified
> will not be processed. Otherwise, event will be processed
> no matter which topic it comes from.

Use **app.emit()** method to send messages to kafka:

```typescript
import {ProducerRecord, CompressionTypes} from "kafkajs";


// Define a payload you want to send:
msgValue = {
    event: 'some_event',
    prop1: 'prop1 value',
    prop2: 34
};

// Define kafka message:
msg: ProducerRecord = {
    key: 'some key',
    value: msgValue
};

// Send message
app.emit({
    topic: 'test_topic1',
    messages: [msg],
    compression: CompressionTypes.GZIP
});
```

#### Start application:

```typescript
await app.run();
```

## <a name="test"></a> Use standalone kafka connector

The **KafkaConnector** class can be used to simplify producer and consumer initialization.

When  **KafkaConnector** is initialized, you can use its methods to get producer and listener:

- **getProducer()** method returns an instance of the **KafkaProducer** class that wraps up initialization and
  connection of the [kafkajs producer](https://kafka.js.org/docs/producing).
- **getListener()** method returns an instance of the **KafkaListener** class that wraps
  up [kafkajs consumer](https://kafka.js.org/docs/consuming), its initialization, connection and subscription to topics.
  When listener created, use **
  listen()** method to start consume messages.

Use **ListenerConfig** class to create listener configuration:

```python
from kafka_python_app.connector import ListenerConfig

kafka_listener_config = ListenerConfig(
    bootstrap_servers=['ip1:port1', 'ip2:port2', ...],
    process_message_cb=my_process_message_func,
    consumer_config={'group_id': 'test_group'},
    topics=['topic1', 'topic2', ...],
    logger=my_logger
)
```

- **bootstrap_servers**: list of kafka bootstrap servers addresses 'host:port'.
- **process_message_cb**: a function that will be called on each message.

```python
from kafka.consumer.fetcher import ConsumerRecord


def my_process_message_func(message: ConsumerRecord):
    print('Received kafka message: key: {}, value: {}'.format(
        message.key,
        message.value
    ))
```

- **consumer_config [OPTIONAL]**: kafka consumer configuration (
  see [kafka-python documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html)).
- **topics**: list of topics to listen from.
- **logger [OPTIONAL]**: any logger with standard log methods. If not provided, the standard python logger is used.
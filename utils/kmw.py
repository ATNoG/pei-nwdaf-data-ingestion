# KAFKA PYTHON BRIDGE
# An abstraction layer to ease communication of whichever component with Kafka
#
# Author: Miguel Neto

import asyncio

from typing import List, Iterable, Optional, TypedDict
import logging
from collections.abc import Callable

import re

from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                    datefmt="%m-%d %H:%M:%S",
                    handlers=[
                        # logging.FileHandler(f"./logs/student_{datetime.datetime.now().strftime("%d_%m_%y_at_%H_%M_%S")}.log"),
                        logging.StreamHandler()
                    ])

# Noisy logs begone!
kafka_loggers = ['kafka', 'kafka.producer', 'kafka.consumer', 'kafka.conn',
                 'kafka.protocol', 'kafka.client', 'kafka.cluster', 'kafka.coordinator',
                 'kafka.metrics', 'kafka.record', 'kafka.serializer', 'kafka.server',
                 'kafka.oauth', 'kafka.sasl']

for logger_name in kafka_loggers:
    kafka_logger = logging.getLogger(logger_name)
    kafka_logger.setLevel(logging.CRITICAL)  # Only show CRITICAL errors (practically nothing)
    kafka_logger.propagate = False  # Disable propagation to root logger

logger = logging.getLogger(__name__)

# TODO: Analyze whether multiprocessing would be a better choice
class PyKafBridge():
    def __init__(self, *topics, hostname: str = 'localhost', port: str = '9092', debug_label: str = 'PKB'):
        self._hostname = hostname
        self._port = port
        self._topics = set(topics)
        self.topic_binds = dict()
        self._consumer_data = {topic: list() for topic in self._topics}

        self._last_consumed = dict()

        # TODO: See if we use JSON or not (changes implementation slightly)
        bootstrap_server = f'{self._hostname}:{self._port}'

        # Configure producer
        producer_config = {
            'bootstrap.servers': bootstrap_server,
            'message.timeout.ms': 30000
        }
        self.producer = Producer(producer_config)

        # Configure consumer
        consumer_config = {
            'bootstrap.servers': bootstrap_server,
            'group.id': f'pykafbridge_group_{id(self)}',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True
        }
        self.consumer: Optional[Consumer] = None
        self._consumer_config = consumer_config

        # Admin client for metadata
        admin_config = {'bootstrap.servers': bootstrap_server}
        self._admin_client = AdminClient(admin_config)

        self._consumer_task: Optional[asyncio.Task] = None
        self._running = False

        self._debug_label = debug_label

    def last_consumed(self, topic: str):
        if self._last_consumed.get(topic):
            return self._last_consumed[topic]
        return -1

    def _real_topic(topic_action):
        def checker(*args, **kwargs):
            _self = args[0]
            topic = args[1]

            cluster_metadata = _self._admin_client.list_topics()

            if isinstance(topic, str):
                topic = [topic]

            print(type(topic))
            for t in topic:
                if t not in cluster_metadata.topics:
                    logging.error(f"Topic {t} does not exist in the target Kafka instance!")
                    return None

            return topic_action(*args, **kwargs)
        return checker

    def _update_topics(adder):
        def checker(*args, **kwargs):
            topic = args[1]  # the first one is self

            if isinstance(topic, str):
                topic = [topic]

            print(type(topic))
            for t in topic:
                if topic == '':
                    logging.error("Empty topics are not allowed!")
                    return None
                if topic[0] == '^':
                    logging.error("The circumflex accent is not allowed in the beginning of the topic name!")
                    return None
            return adder(*args, **kwargs)
        return checker

    @_real_topic
    @_update_topics
    def add_topic_and_subtopics(self, parent: str, bind: Callable = None):
        pat = f"\\.?{parent}(\\..*)?"
        if self.consumer:
            cluster_metadata = self.consumer.list_topics()
            matching_topics = [topic for topic in cluster_metadata.topics if re.search(pat, topic)]
            if matching_topics:
                # Confluent Kafka indeed supports regex but i blocked it for now, due topic-related abstractions
                self._topics.update(matching_topics)
                self.consumer.subscribe(list(self._topics))
                for topic in matching_topics:
                    if topic not in self._consumer_data:
                        self._consumer_data[topic] = list()
                    if bind:
                        self.bind_topic(topic, bind)
        else:
            cluster_metadata = self._admin_client.list_topics()
            matching_topics = [topic for topic in cluster_metadata.topics if re.search(pat, topic)]
            self._topics.update(matching_topics)
            for topic in matching_topics:
                if topic not in self._consumer_data:
                    self._consumer_data[topic] = list()
                if bind:
                    self.bind_topic(topic, bind)

    @_real_topic
    @_update_topics
    def bind_topic(self, topic: str, func: Callable):
        if self.topic_binds.get(topic):
            self.topic_binds[topic].append(func)
        else:
            self.topic_binds[topic] = [func]

    @_real_topic
    @_update_topics
    def add_topics_regex(self, pat: str, bind: Callable = None) -> None:
        cluster_metadata = self._admin_client.list_topics()
        matching_topics = [topic for topic in cluster_metadata.topics if re.search(pat, topic)]
        if matching_topics:
            self._topics.update(matching_topics)

        if self.consumer:
            self.consumer.subscribe(list(self._topics))
            for topic in matching_topics:
                if topic not in self._consumer_data:
                    self._consumer_data[topic] = list()
                if bind:
                    self.bind_topic(topic, bind)
        else:
            for topic in matching_topics:
                if topic not in self._consumer_data:
                    self._consumer_data[topic] = list()
                if bind:
                    self.bind_topic(topic, bind)

    @_real_topic
    @_update_topics
    def add_topic(self, topic: str, bind: Callable = None) -> None:
        if topic != '' and topic[0] == '^':
            logging.error("Regex is not allowed in this method!")
            return
        if self.consumer:
            current_subscription = self.consumer.subscription()
            if current_subscription is not None:
                self._topics.add(topic)
                self.consumer.subscribe(list(self._topics))
            else:
                self.consumer.subscribe([topic])

            if bind:
                self.bind_topic(topic, bind)

        self._topics.add(topic)
        if topic not in self._consumer_data:
            self._consumer_data[topic] = list()

        if bind:
            self.bind_topic(topic, bind)

    @_real_topic
    @_update_topics
    def add_n_topics(self, topics: Iterable, bind: Callable = None) -> None:
        if self.consumer:
            if topics is not None:
                self._topics.update(topics)
                self.consumer.subscribe(list(self._topics))

            if bind:
                for topic in topics:
                    self.bind_topic(topic, bind)

        self._topics.update(topics)
        for topic in topics:
            if topic not in self._consumer_data:
                self._consumer_data[topic] = list()

            if bind:
                for topic in topics:
                    self.bind_topic(topic, bind)

    def metrics(self, topic=None) -> dict:
        """ Get consumer metrics """
        if not self.consumer:
            logger.warning("Consumer not initialized")
            return {}

        return self.consumer.metrics()

    async def _run(self):
        """ Deprecated """
        consumer_task = asyncio.create_task(self.consume())
        try:
            await consumer_task
        except KeyboardInterrupt:
            logger.info("Shutdown signal received: keyboard interrupt")
            self._running = False
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                logger.error("There was an error cancelling: asyncio.CancelledError")
            except Exception as e:
                logger.error(f"Fatal error: {e}")
        except Exception as e:
            logger.error(f"Fatal error: {e}")

    async def start_consumer(self) -> None:
        """ Initialize consumer and start the event loop. Blocks until consumer is ready. """

        if not self._topics:
            logger.warning("No topics to subscribe to")
            return

        cluster_metadata = self._admin_client.list_topics()
        diff = set(self._topics) - set(cluster_metadata.topics)

        if diff:
            for topic in diff:
                logging.error(f"Topic {topic} does not exist in the target Kafka instance!")

            self._topics -= diff

            if not self._topics:
                logging.error("NO TOPICS EXIST IN THE KAFKA INSTANCE! ABORTING...")
                return None

        self.consumer = Consumer(self._consumer_config)
        self.consumer.subscribe(list(self._topics))
        self._running = True

        self._consumer_task = asyncio.create_task(self.consume())

        logger.debug("Waiting for partition assignment...")
        max_wait = 10  # seconds
        start_time = asyncio.get_event_loop().time()

        while True:
            assignment = self.consumer.assignment()
            if assignment:
                logger.info(f"Consumer ready. Assigned partitions: {assignment}")
                break

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > max_wait:
                logger.warning(f"Consumer did not receive partition assignment within {max_wait}s")
                break

            await asyncio.sleep(0.1)

        # Give a small additional buffer for the consumer to seek to the correct position
        await asyncio.sleep(0.5)
        logger.info("Consumer is ready to receive messages")
        logger.info(f"Started Kafka consumer, subscribed to: {self._topics}")

    async def close(self) -> None:
        self._running = False

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            self.consumer.close()

        # Flush producer to ensure all messages are delivered
        self.producer.flush()
        logger.info("Stopping...")

    def get_topic(self, topic: str):
        data = self._consumer_data.get(topic)
        return data if data else []

    # TODO:
    # EXTRA: offset behaviour must be tracked
    async def consume(self) -> None:
        """ Consume events from subscribed topics. (WIP) """
        if not self.consumer:
            logger.error("Consumer not initialized")
            return

        loop = asyncio.get_event_loop()

        try:
            while self._running:
                # Run blocking consumer poll in executor to avoid blocking event loop. #asyncioflex
                msg = await loop.run_in_executor(None, self.consumer.poll, 1.0)

                if msg is None:
                    await asyncio.sleep(0)
                    continue

                logging.debug(msg)

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    await asyncio.sleep(0)
                    continue

                topic = msg.topic()
                data = {
                    'offset': msg.offset(),
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'content': msg.value().decode() if msg.value() else '',
                    'timestamp': msg.timestamp()[1] if msg.timestamp()[0] != 0 else None,
                }

                # Transform data with functions bound to that topic
                if self.topic_binds.get(topic):
                    for func in self.topic_binds[topic]:
                        data = func(data)

                if topic not in self._consumer_data:
                    self._consumer_data[topic] = []

                self._consumer_data[topic].append(data)
                self._last_consumed[topic] = data['offset']

                await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.info("Consumer task cancelled")
        except Exception as e:
            logger.error(f"Error in consumer task: {e}")

    # TODO: May use transactions in specific cases
    @_real_topic
    def produce(self, topic: str, message: str) -> bool:
        """ Send an event. Using str for now for simplicity. Returns true on success """
        try:
            # We inject a callback to be sure to know how produce might have failed, in case it does :)
            def delivery_callback(err, msg):
                if err:
                    logger.error(f"Message delivery failed: {err}")
                else:
                    logger.debug(f"Message delivered in topic={msg.topic()}; partition={msg.partition()}; offset={msg.offset()}")

            self.producer.produce(topic, message.encode(), callback=delivery_callback)
            # Poll to trigger delivery callbacks
            self.producer.poll(0)
            # logger.debug(f"Message sent to topic {topic}")
            return True
        except KafkaException as e:
            logger.error(f"Kafka error sending to topic {topic}: {e}")
            return False
        except BufferError as e:
            logger.error(f"Buffer error sending to topic {topic}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending to topic {topic}: {e}")
            return False


if __name__ == '__main__':
    async def main():
        pk1 = PyKafBridge('test-event')
        pk2 = PyKafBridge()

        await pk1.start_consumer()

        pk2.produce('test-event', 'MESSAGE1')
        pk2.produce('test-event', 'MESSAGE2')

        def transform_message(data: dict):
            data['content'] = data['content'].lower()
            return data

        pk2.produce('test-event', 'MESSAGE3')

        await asyncio.sleep(1)

        pk1.bind_topic('test-event', transform_message)

        pk2.produce('test-event', 'MESSAGE4')
        pk2.produce('test-event', 'MESSAGE5')

        await asyncio.sleep(1)

        data = pk1.get_topic('test-event')

        logger.info(f"Consumed {len(data)} messages")
        for d in data:
            logger.debug(f'\t - {d}')

        logger.debug(f"Last consumed offset: {pk1.last_consumed('test-event')}")

        await pk2.close()
        await pk1.close()

    asyncio.run(main())

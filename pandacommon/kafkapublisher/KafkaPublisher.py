#
# Module for sending messages to Kafka that will be used
# by the panda server and JEDI to publish task/job state transitions.
# pandamon will consume those messages for realtime logging.
#

import json
from confluent_kafka import Producer
from pandacommon.pandalogger.logger_utils import logger_utils

class KafkaPublisher:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        self.logger = logger_utils.setup_logger()

    def publish_message(self, topic, payload):
        # Convert payload to JSON string
        message = json.dumps(payload)

        # Produce message asynchronously
        self.producer.produce(topic, value=message, callback=self._delivery_report)

        # Wait for the message to be sent
        self.producer.flush()

    def _delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f'Failed to deliver message: {err}')
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def close(self):
        self.producer.flush()
        self.producer.close()
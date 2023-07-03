#
# Module for sending messages to Kafka that will be used
# by the panda server and JEDI to publish task/job state transitions.
# pandamon will consume those messages for realtime logging.
#

import json
import hashlib
import socket
import sys
from confluent_kafka import Producer
from pandacommon.pandalogger import logger_utils
from ..commonconfig import common_config

class KafkaPublisher:
    def __init__(self):
        config = common_config.get('kafka')

        self.producer = Producer({
                'bootstrap.servers': get_bootstrap_servers(),
                'group.id': config.GROUP_ID,
                'ssl.ca.location': config.CACERTS,
                'security.protocol': 'SASL_SSL',
                'sasl.kerberos.keytab': config.KEYTAB,
                'auto.offset.reset': 'latest',
                'enable.auto.offset.store': True,
                'sasl.kerberos.principal': config.PRINCIPAL,
                'log_level': 0
        })
        self.logger = logger_utils.setup_logger()

    def get_bootstrap_servers():
        KAFKA_CLUSTER = "kafka-gp"
        return ",".join(
            map(lambda x: x + ":9093",
                sorted([(socket.gethostbyaddr(i))[0] for i in (socket.gethostbyname_ex(KAFKA_CLUSTER + ".cern.ch"))[2]])
                )
        )
    def publish_message(self, topic, payload):
        # Convert payload to JSON string
        message = json.dumps(payload)

        # Hash the message payload using SHA-1
        hash_string = message.encode()
        hashed_payload = hashlib.sha1(hash_string).hexdigest()

        # Add hashed payload as 'message_id'
        payload['message_id'] = hashed_payload

        # Produce message asynchronously
        self.producer.produce(topic, value=json.dumps(payload), callback=self._delivery_report)

        # Wait for the message to be sent
        self.producer.flush()

    def _delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f'Failed to deliver message: {err}')
            #print(f'Failed to deliver message: {err}')
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            #print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def close(self):
        self.producer.flush()

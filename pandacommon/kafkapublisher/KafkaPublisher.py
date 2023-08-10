#
# Module for sending messages to Kafka that will be used
# by the panda server and JEDI to publish task/job state transitions.
# pandamon will consume those messages for realtime logging.
#

import json
import hashlib
import socket
from confluent_kafka import Producer
from pandacommon.pandalogger import logger_utils
from pandacommon.commonconfig import common_config

class KafkaPublisher:
    def __init__(self):
        kafka_config = common_config.get('kafka')
        self.producer = Producer({
                'bootstrap.servers': self.get_bootstrap_servers(kafka_config['kafka_cluster'], kafka_config['kafka_cluster_domain']),
                'ssl.ca.location': kafka_config['cacerts'],
                'security.protocol': 'SASL_SSL',
                'sasl.kerberos.keytab': kafka_config['keytab'],
                'sasl.kerberos.principal': kafka_config['principal'],
                'log_level': 0
        })
        self.topic = kafka_config['topic']
        self.logger = logger_utils.setup_logger()

    def get_bootstrap_servers(self, cluster, domain):
        return ",".join(
            map(lambda x: x + ":9093",
                sorted([(socket.gethostbyaddr(i))[0] for i in (socket.gethostbyname_ex(cluster + domain))[2]])
                )
        )

    def publish_message(self, payload, topic=None):
        # Convert payload to JSON string
        message = json.dumps(payload)

        # Hash the message payload using SHA-1
        hash_string = message.encode()
        hashed_payload = hashlib.sha1(hash_string).hexdigest()

        # Add hashed payload as 'message_id'
        payload['message_id'] = hashed_payload

        # If no topic was passed, then use the one specified in panda_common.cfg
        if topic is None:
            topic = self.topic

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

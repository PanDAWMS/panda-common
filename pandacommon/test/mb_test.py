import sys
import time
import datetime

from pandacommon.pandalogger import PandaLogger
from pandacommon.pandamsgbkr import MsgBkrUtils, MsgConsumer

#-----------------------------------------------------------------------------

CONFIG_CONSUMER_PLUGIN_MAP = {
                                'C1': 'TestMsgConsumerPlugin_1',
                                'C2': 'TestMsgConsumerPlugin_2',
                            }

CONFIG_CONSUMER_PLUGIN_INST_MAP = {
                                'C1': TestMsgConsumerPlugin_1(),
                                'C2': TestMsgConsumerPlugin_2(),
                            }

CONFIG_CONSUMER_QUEUE_MAP = {
                                'queue1': { 'host_port_list': ['127.0.0.1:61613'],
                                            'destination': '/queue/test_1',
                                            'use_ssl': False,
                                            'cert_file': None,
                                            'key_file': None},
                                'queue2': { 'host_port_list': ['127.0.0.1:61613'],
                                            'destination': '/queue/test_2',
                                            'use_ssl': False,
                                            'cert_file': None,
                                            'key_file': None},
                            }


#-----------------------------------------------------------------------------

# loggers
logger = PandaLogger().getLogger('mb_test', log_level='DEBUG')
logger.prefix = datetime.datetime.utcnow().isoformat('/')

sender_logger = PandaLogger().getLogger('mb_test', log_level='DEBUG')
sender_logger.prefix = datetime.datetime.utcnow().isoformat('/') + ' [sender]'


class TestMsgConsumerPlugin_1(MsgConsumer.MsgConsumerPluginBase):

    def initialize(self):
        logger.debug('TestMsgConsumerPlugin_1.initialize called')

    def consume(self, msg_obj):
        logger.debug('TestMsgConsumerPlugin_1.consume called')
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))


class TestMsgConsumerPlugin_2(MsgConsumer.MsgConsumerPluginBase):

    def initialize(self):
        logger.debug('TestMsgConsumerPlugin_2.initialize called')

    def consume(self, msg_obj):
        logger.debug('TestMsgConsumerPlugin_2.consume called')
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))

class TestMsgConsumerAgent(MsgConsumer.MsgConsumerAgentBase):

    def initialize(self):
        logger.debug('TestMsgConsumerAgent.initialize called')
        self.init_consumer_list = list(CONFIG_CONSUMER_PLUGIN_MAP.keys())
        self.consumer_info_map = dict(CONFIG_CONSUMER_PLUGIN_INST_MAP)

        init_queue_set = set(CONFIG_CONSUMER_QUEUE_MAP.values())

        self.init_mb_proxy_list = [ MsgBkrUtils.MBProxy(name=queue, logger=logger,
                                                        **CONFIG_CONSUMER_QUEUE_MAP[queue])
                                        for queue in init_queue_set ]


def main():
    # start
    sys.stderr.write('Start test \n')

    # senders
    sender_1 = MsgBkrUtils.MBProxy(name=queue1, logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP[queue1])
    sender_2 = MsgBkrUtils.MBProxy(name=queue2, logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP[queue2])

    # consumer agent
    sys.stderr.write('Run consumer agent \n')
    consumer_agent = TestMsgConsumerAgent()
    consumer_agent.start()

    # send something
    sys.stderr.write('Sending \n')
    for i in range(10):
        sender_1.send('A{0}'.format(i))
        sender_2.send('B{0}'.format(i))
    time.sleep(10)

    # kill threads
    sys.stderr.write('Killing threads \n')
    consumer_agent._kill_consumers(self.init_consumer_list)
    consumer_agent._kill_listeners(self.init_mb_proxy_list)

    # wait
    while consumer_agent.is_alive():
        sys.stderr.write('Waiting agent to stop... \n')
        time.sleep(2)

    # end
    sys.stderr.write('End test \n')

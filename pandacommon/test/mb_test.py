import sys
import time
import datetime

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandamsgbkr import MsgBkrUtils, MsgConsumer

#-----------------------------------------------------------------------------

CONFIG_CONSUMER_PLUGIN_MAP = {
                                'C1': ('TestMsgConsumerPlugin_1', 'queue1'),
                                'C2': ('TestMsgConsumerPlugin_2', 'queue2'),
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
logger = MsgConsumer.some_logger
sender_logger = PandaLogger().getLogger('mb_test_sender', log_level='DEBUG')


# verification set
test_set_1 = set()
test_set_2 = set()
answer_set_1 = { 'A{0}'.format(i) for i in range(20) }
answer_set_2 = { 'B{0}'.format(i) for i in range(20) }


# class

class TestMsgConsumerPlugin_1(MsgConsumer.MsgConsumerPluginBase):

    def initialize(self):
        logger.debug('TestMsgConsumerPlugin_1.initialize called')

    def consume(self, msg_obj):
        logger.debug('TestMsgConsumerPlugin_1.consume called')
        test_set_1.add(msg_obj.data)
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))


class TestMsgConsumerPlugin_2(MsgConsumer.MsgConsumerPluginBase):

    def initialize(self):
        logger.debug('TestMsgConsumerPlugin_2.initialize called')

    def consume(self, msg_obj):
        logger.debug('TestMsgConsumerPlugin_2.consume called')
        test_set_2.add(msg_obj.data)
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))

class TestMsgConsumerAgent(MsgConsumer.MsgConsumerAgentBase):

    def initialize(self):
        logger.debug('TestMsgConsumerAgent.initialize called')
        self.init_consumer_list = list(CONFIG_CONSUMER_PLUGIN_MAP.keys())
        self.consumer_info_map = dict(CONFIG_CONSUMER_PLUGIN_INST_MAP)

        init_queue_set = set(CONFIG_CONSUMER_QUEUE_MAP.keys())

        self.init_mb_proxy_list = [ MsgBkrUtils.MBProxy(name=queue, logger=logger,
                                                        **CONFIG_CONSUMER_QUEUE_MAP[queue])
                                        for queue in init_queue_set ]


CONFIG_CONSUMER_PLUGIN_INST_MAP = {
                                'C1': (TestMsgConsumerPlugin_1(), 'queue1'),
                                'C2': (TestMsgConsumerPlugin_2(), 'queue2'),
                            }

def main():
    # start
    sys.stderr.write('Start test \n')
    sys.stderr.flush()

    # senders
    sys.stderr.write('Start senders ...')
    sys.stderr.flush()
    sender_1 = MsgBkrUtils.MsgSender(name='queue1', logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP['queue1'])
    sender_2 = MsgBkrUtils.MsgSender(name='queue2', logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP['queue2'])
    sender_1.go()
    sender_2.go()
    sys.stderr.write('\t OK! \n')

    # clean test MQs
    sys.stderr.write('Clean up test MQs ...')
    sys.stderr.flush()
    sender_1.waste(3)
    sender_2.waste(3)
    sys.stderr.write('\t OK! \n')

    # send something
    sys.stderr.write('Sending ...')
    sys.stderr.flush()
    for i in range(10):
        sender_1.send('A{0}'.format(i))
        sender_2.send('B{0}'.format(i))
    sys.stderr.write('\t\t OK! \n')

    # consumer agent
    sys.stderr.write('Run consumer agent ...')
    sys.stderr.flush()
    consumer_agent = TestMsgConsumerAgent()
    consumer_agent.start()
    time.sleep(5)
    sys.stderr.write('\t OK! \n')

    # send something again
    sys.stderr.write('Sending again ...')
    sys.stderr.flush()
    for i in range(10, 20):
        sender_1.send('A{0}'.format(i))
        sender_2.send('B{0}'.format(i))
    sys.stderr.write('\t OK! \n')

    # stop sender
    sys.stderr.write('Stop senders ...')
    sys.stderr.flush()
    sender_1.stop()
    sender_2.stop()
    sys.stderr.write('\t OK! \n')

    # kill threads
    time.sleep(2)
    sys.stderr.write('Killing threads ...')
    sys.stderr.flush()
    consumer_agent._kill_consumers(consumer_agent.init_consumer_list)
    consumer_agent._kill_listeners(consumer_agent.init_mb_proxy_list)
    sys.stderr.write('\t OK! \n')

    # wait
    sys.stderr.write('Waiting agent to stop...')
    sys.stderr.flush()
    while consumer_agent.is_alive():
        time.sleep(2)
    sys.stderr.write(' OK! \n')

    # verify
    sys.stderr.write('Verify result...')
    sys.stderr.flush()
    while consumer_agent.is_alive():
        time.sleep(2)
    if answer_set_1 == test_set_1 and answer_set_2 == test_set_2:
        sys.stderr.write('\t OK! \n')
    else:
        print(answer_set_1, test_set_1)
        print(answer_set_2, test_set_2)
        sys.stderr.write('\t Failed! Check logs for details \n')

    # end
    sys.stderr.write('End test \n')


# run
if __name__ == '__main__':
    main()

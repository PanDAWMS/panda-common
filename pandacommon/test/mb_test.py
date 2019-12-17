import sys
import time
import datetime
import tempfile

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandamsgbkr import msg_bkr_utils, msg_processor

#-----------------------------------------------------------------------------

CONFIG_JSON = """
{
    "mb_servers": {
        "local": {
            "host_port_list": ["127.0.0.1:61613"],
            "use_ssl": 0,
            "cert_file": null,
            "key_file": null,
            "username": "admin",
            "passcode": "pw"
        }
    },
    "queues": {
        "Q1": {
            "server": "local",
            "destination": "/queue/test_1"
        },
        "Q2": {
            "server": "local",
            "destination": "/queue/test_2"
        },
        "Q3": {
            "server": "local",
            "destination": "/queue/test_3"
        }
    },
    "processors": {
        "P1": {
            "module": "pandacommon.test.mb_test",
            "name": "TestMsg_processorPlugin_1",
            "in_queue": "Q1",
            "out_queue": "Q3"
        },
        "P2": {
            "module": "pandacommon.test.mb_test",
            "name": "TestMsg_processorPlugin_2",
            "in_queue": "Q2",
            "out_queue": "Q3"
        }
    }
}
"""

EXTRA_PROXY_INFO = {
    'Q1': {
        'host_port_list': ['127.0.0.1:61613'],
        'destination': '/queue/test_1',
        'use_ssl': False,
    },
    'Q2': {
        'host_port_list': ['127.0.0.1:61613'],
        'destination': '/queue/test_2',
        'use_ssl': False,
    },
    'Q3': {
        'host_port_list': ['127.0.0.1:61613'],
        'destination': '/queue/test_3',
        'use_ssl': False,
    },
}

#-----------------------------------------------------------------------------

# loggers
logger = PandaLogger().getLogger('mb_test_thread', log_level='DEBUG')


# verification set
answer_set_1 = { 'A{0}'.format(i) for i in range(20) }
answer_set_2 = { 'B{0}'.format(i) for i in range(20) }
answer_set_3 = answer_set_1 | answer_set_2


# class

class TestMsg_processorPlugin_1(msg_processor.SimpleMsgProcPluginBase):

    def initialize(self):
        logger.debug('TestMsg_processorPlugin_1.initialize called')

    def process(self, msg_obj):
        logger.debug('TestMsg_processorPlugin_1.process called')
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsg_processorPlugin_2(msg_processor.SimpleMsgProcPluginBase):

    def initialize(self):
        logger.debug('TestMsg_processorPlugin_2.initialize called')

    def process(self, msg_obj):
        logger.debug('TestMsg_processorPlugin_2.process called')
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsg_processorAgent(msg_processor.MsgProcAgentBase):

    def initialize(self):
        logger.debug('TestMsg_processorAgent.initialize called')
        pass


def main():
    # start
    sys.stderr.write('Start test \n')
    sys.stderr.flush()

    # extra senders
    sys.stderr.write('Start extra senders ...')
    sys.stderr.flush()
    sender_1 = msg_bkr_utils.MBSenderProxy(name='Q1', **EXTRA_PROXY_INFO['Q1'])
    sender_2 = msg_bkr_utils.MBSenderProxy(name='Q2', **EXTRA_PROXY_INFO['Q2'])
    sender_1.go()
    sender_2.go()
    sys.stderr.write('\t OK! \n')

    # extra receiver
    sys.stderr.write('Start extra receiver ...')
    sys.stderr.flush()
    receiver_3 = msg_bkr_utils.MBProxy(name='Q3', **EXTRA_PROXY_INFO['Q3'], skip_buffer=True)
    receiver_3.go()
    sys.stderr.write(' OK! \n')

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

    # processor agent
    sys.stderr.write('Run processor agent ...')
    sys.stderr.flush()
    temp_conifg = tempfile.NamedTemporaryFile(mode='w+t')
    temp_conifg.write(CONFIG_JSON)
    temp_conifg.flush()
    processor_agent = TestMsg_processorAgent(config_file=temp_conifg.name)
    processor_agent.start()
    time.sleep(5)
    sys.stderr.write('\t OK! \n')

    # send something again
    sys.stderr.write('Sending again ...')
    sys.stderr.flush()
    for i in range(10, 20):
        sender_1.send('A{0}'.format(i))
        sender_2.send('B{0}'.format(i))
    sys.stderr.write('\t OK! \n')

    # stop extra senders
    sys.stderr.write('Stop extra senders ...')
    sys.stderr.flush()
    sender_1.stop()
    sender_2.stop()
    sys.stderr.write('\t OK! \n')

    # stop extra receiver
    sys.stderr.write('Stop extra receiver ...')
    sys.stderr.flush()
    receiver_3.stop()
    sys.stderr.write('\t OK! \n')

    # stop threads and agent
    time.sleep(2)
    processor_agent.stop()
    sys.stderr.write('Stop agent ...')
    sys.stderr.flush()
    while processor_agent.is_alive():
        time.sleep(2)
    temp_conifg.close()
    sys.stderr.write('\t\t OK! \n')

    # verify
    sys.stderr.write('Verify result...')
    sys.stderr.flush()
    test_set = set(receiver_3.dump_msgs)
    if answer_set_3 == test_set and len(receiver_3.dump_msgs) == len(answer_set_3):
        sys.stderr.write('\t OK! \n')
    else:
        print(answer_set_3, receiver_3.dump_msgs)
        sys.stderr.write('\t Failed! Check logs for details \n')

    # end
    sys.stderr.write('End test \n')


# run
if __name__ == '__main__':
    main()

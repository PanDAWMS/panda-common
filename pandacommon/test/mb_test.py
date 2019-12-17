import sys
import time
import datetime
import tempfile

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandamsgbkr import MsgBkrUtils, MsgProcessor

#-----------------------------------------------------------------------------

CONFIG_JSON = """
{
    "mb_servers": {
        "local": {
            "host_port_list": ['127.0.0.1:61613'],
            "destination": '/queue/test_1',
            "use_ssl": False,
            "cert_file": None,
            "key_file": None,
            "username": "admin",
            "passcode": "pw"}
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
            "module": "pandacommon.pandamsgbkr.msg_processor",
            "name": "TestMsgProcessorPlugin_1",
            "in_queue": "Q1",
            "out_queue": "Q3"
        },
        "P2": {
            "module": "pandacommon.pandamsgbkr.msg_processor",
            "name": "TestMsgProcessorPlugin_2",
            "in_queue": "Q2",
            "out_queue": "Q3"
        }
    }
}
"""


#-----------------------------------------------------------------------------

# loggers
logger = MsgProcessor.some_logger
sender_logger = PandaLogger().getLogger('mb_test_sender', log_level='DEBUG')


# verification set
test_set_1 = set()
test_set_2 = set()
test_set_3 = set()
answer_set_1 = { 'A{0}'.format(i) for i in range(20) }
answer_set_2 = { 'B{0}'.format(i) for i in range(20) }
answer_set_3 = answer_set_1 + answer_set_2


# class

class TestMsgProcessorPlugin_1(MsgProcessor.MsgProcessorPluginBase):

    def initialize(self):
        logger.debug('TestMsgProcessorPlugin_1.initialize called')

    def process(self, msg_obj):
        logger.debug('TestMsgProcessorPlugin_1.process called')
        test_set_1.add(msg_obj.data)
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsgProcessorPlugin_2(MsgProcessor.MsgProcessorPluginBase):

    def initialize(self):
        logger.debug('TestMsgProcessorPlugin_2.initialize called')

    def process(self, msg_obj):
        logger.debug('TestMsgProcessorPlugin_2.process called')
        test_set_2.add(msg_obj.data)
        logger.info('got message obj: sub_id={s}, msg_id={m}, data={d}'.format(
                        s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsgProcessorAgent(MsgProcessor.MsgProcessorAgentBase):

    def initialize(self):
        logger.debug('TestMsgProcessorAgent.initialize called')
        pass


def main():
    # start
    sys.stderr.write('Start test \n')
    sys.stderr.flush()

    # senders
    sys.stderr.write('Start extra senders ...')
    sys.stderr.flush()
    sender_1 = MsgBkrUtils.MBSenderProxy(name='queue1', logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP['queue1'])
    sender_2 = MsgBkrUtils.MBSenderProxy(name='queue2', logger=sender_logger, **CONFIG_CONSUMER_QUEUE_MAP['queue2'])
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

    # processor agent
    sys.stderr.write('Run processor agent ...')
    sys.stderr.flush()
    temp_conifg = tempfile.NamedTemporaryFile(mode='w+t')
    temp_conifg.write(CONFIG_JSON)
    temp_conifg.flush()
    processor_agent = TestMsgProcessorAgent(config_file=temp_conifg.name)
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

    # stop sender
    sys.stderr.write('Stop senders ...')
    sys.stderr.flush()
    sender_1.stop()
    sender_2.stop()
    sys.stderr.write('\t OK! \n')

    # stop threads and agent
    time.sleep(2)
    processor_agent.stop()
    sys.stderr.write('Stop agent ...')
    sys.stderr.flush()
    while processor_agent.is_alive():
        time.sleep(2)
    temp_conifg.close()
    sys.stderr.write('\t OK! \n')

    # verify
    sys.stderr.write('Verify result...')
    sys.stderr.flush()
    while processor_agent.is_alive():
        time.sleep(2)
    if answer_set_1 == test_set_1 and answer_set_2 == test_set_2 and answer_set_3 == test_set_3:
        sys.stderr.write('\t OK! \n')
    else:
        print(answer_set_1, test_set_1)
        print(answer_set_2, test_set_2)
        print(answer_set_3, test_set_3)
        sys.stderr.write('\t Failed! Check logs for details \n')

    # end
    sys.stderr.write('End test \n')


# run
if __name__ == '__main__':
    main()

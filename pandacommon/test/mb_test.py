import sys
import tempfile
import time
from typing import TypedDict

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandamsgbkr import msg_bkr_utils, msg_processor

# -----------------------------------------------------------------------------

CONFIG_JSON = """
{
    "mb_servers": {
        "local": {
            "host_port_list": ["127.0.0.1:61613"],
            "use_ssl": 0,
            "cert_file": null,
            "key_file": null,
            "username": "admin",
            "passcode": "pw",
            "vhost": null
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
        },
        "Q4": {
            "server": "local",
            "destination": "/queue/test_4"
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
            "name": "TestMsgProcessorPlugin2",
            "in_queue": "Q2",
            "out_queue": "Q4"
        }
    }
}
"""


class _ProxyInfo(TypedDict):
    """The proxy arguments one test queue is built with.

    Stated as a TypedDict because these are splatted into MBSenderProxy and
    MBListenerProxy: as a plain dict literal the values widen to object, and nothing
    then matches the parameters they are passed to.
    """

    host_port_list: list[str]
    destination: str
    use_ssl: bool
    vhost: str | None


EXTRA_PROXY_INFO: dict[str, _ProxyInfo] = {
    "Q1": {
        "host_port_list": ["127.0.0.1:61613"],
        "destination": "/queue/test_1",
        "use_ssl": False,
        "vhost": None,
    },
    "Q2": {
        "host_port_list": ["127.0.0.1:61613"],
        "destination": "/queue/test_2",
        "use_ssl": False,
        "vhost": None,
    },
    "Q3": {
        "host_port_list": ["127.0.0.1:61613"],
        "destination": "/queue/test_3",
        "use_ssl": False,
        "vhost": None,
    },
    "Q4": {
        "host_port_list": ["127.0.0.1:61613"],
        "destination": "/queue/test_4",
        "use_ssl": False,
        "vhost": None,
    },
}

# -----------------------------------------------------------------------------

# loggers
logger = PandaLogger().getLogger("mb_test_thread", log_level="DEBUG")


# verification set
answer_set_a = {"A{0}".format(i) for i in range(20)}
answer_set_b = {"B{0}".format(i) for i in range(20)}


# class


class TestMsg_processorPlugin_1(msg_processor.SimpleMsgProcPluginBase):
    def initialize(self) -> None:
        logger.debug("TestMsg_processorPlugin_1.initialize called")

    # the base declares msg_obj as Any because a plugin with no input queue is handed
    # None; these tests always run with one, so they state what they actually get
    def process(self, msg_obj: msg_bkr_utils.MsgObj) -> str:
        logger.debug("TestMsg_processorPlugin_1.process called")
        logger.info("got message obj: sub_id={s}, msg_id={m}, data={d}".format(s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsgProcessorPlugin2(msg_processor.SimpleMsgProcPluginBase):
    def initialize(self) -> None:
        logger.debug("TestMsgProcessorPlugin2.initialize called")

    def process(self, msg_obj: msg_bkr_utils.MsgObj) -> str:
        logger.debug("TestMsgProcessorPlugin2.process called")
        logger.info("got message obj: sub_id={s}, msg_id={m}, data={d}".format(s=msg_obj.sub_id, m=msg_obj.msg_id, d=msg_obj.data))
        return msg_obj.data


class TestMsgProcessorAgent(msg_processor.MsgProcAgentBase):
    def initialize(self) -> None:
        logger.debug("TestMsgProcessorAgent.initialize called")
        pass


def main() -> None:
    # start
    sys.stderr.write("Start test \n")
    sys.stderr.flush()

    # extra senders
    sys.stderr.write("Start extra senders ...")
    sys.stderr.flush()
    sender_1 = msg_bkr_utils.MBSenderProxy(name="Q1", **EXTRA_PROXY_INFO["Q1"])
    sender_2 = msg_bkr_utils.MBSenderProxy(name="Q2", **EXTRA_PROXY_INFO["Q2"])
    sender_3 = msg_bkr_utils.MBSenderProxy(name="Q3", **EXTRA_PROXY_INFO["Q3"])
    sender_4 = msg_bkr_utils.MBSenderProxy(name="Q4", **EXTRA_PROXY_INFO["Q4"])
    sender_1.go()
    sender_2.go()
    sender_3.go()
    sender_4.go()
    sys.stderr.write("\t OK! \n")

    # clean test MQs
    sys.stderr.write("Clean up test MQs ...")
    sys.stderr.flush()
    sender_3.waste(2)
    sender_4.waste(2)
    sender_1.waste(2)
    sender_2.waste(2)
    sender_3.stop()
    sender_4.stop()
    sys.stderr.write("\t OK! \n")

    # extra receiver
    sys.stderr.write("Start extra receiver ...")
    sys.stderr.flush()
    receiver_3 = msg_bkr_utils.MBListenerProxy(name="Q3", skip_buffer=True, **EXTRA_PROXY_INFO["Q3"])
    receiver_4 = msg_bkr_utils.MBListenerProxy(name="Q4", skip_buffer=True, **EXTRA_PROXY_INFO["Q4"])
    receiver_3.go()
    receiver_4.go()
    sys.stderr.write(" OK! \n")

    # send something
    sys.stderr.write("Sending ...")
    sys.stderr.flush()
    for i in range(10):
        sender_1.send("A{0}".format(i))
        sender_2.send("B{0}".format(i))
    sys.stderr.write("\t\t OK! \n")

    # processor agent
    sys.stderr.write("Run processor agent ...")
    sys.stderr.flush()
    tmp_config = tempfile.NamedTemporaryFile(mode="w+t")
    tmp_config.write(CONFIG_JSON)
    tmp_config.flush()
    processor_agent = TestMsgProcessorAgent(config_file=tmp_config.name)
    processor_agent.start()
    time.sleep(5)
    sys.stderr.write("\t OK! \n")

    # send something again
    sys.stderr.write("Sending again ...")
    sys.stderr.flush()
    for i in range(10, 20):
        sender_1.send("A{0}".format(i))
        sender_2.send("B{0}".format(i))
    sys.stderr.write("\t OK! \n")

    # stop extra senders
    sys.stderr.write("Stop extra senders ...")
    sys.stderr.flush()
    time.sleep(5)
    sender_1.stop()
    sender_2.stop()
    sys.stderr.write("\t OK! \n")

    # stop extra receiver
    sys.stderr.write("Stop extra receiver ...")
    sys.stderr.flush()
    receiver_3.stop()
    receiver_4.stop()
    time.sleep(2)
    sys.stderr.write("\t OK! \n")

    # stop threads and agent
    sys.stderr.write("Stop agent ...")
    sys.stderr.flush()
    processor_agent.stop()
    while processor_agent.is_alive():
        time.sleep(2)
    tmp_config.close()
    sys.stderr.write("\t\t OK! \n")

    # verify
    sys.stderr.write("Verify result...")
    sys.stderr.flush()
    test_set_a = set(receiver_3.dump_msgs)
    test_len_a = len(receiver_3.dump_msgs)
    test_set_b = set(receiver_4.dump_msgs)
    test_len_b = len(receiver_4.dump_msgs)
    if answer_set_a == test_set_a and test_len_a == len(answer_set_a) and answer_set_b == test_set_b and test_len_b == len(answer_set_b):
        sys.stderr.write("\t OK! \n")
    else:
        print(answer_set_a, receiver_3.dump_msgs)
        print(answer_set_b, receiver_4.dump_msgs)
        sys.stderr.write("\t Failed! Check logs for details \n")

    # end
    sys.stderr.write("End test \n")


# run
if __name__ == "__main__":
    main()

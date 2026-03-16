import gc
import json
import os
import re
import time
import traceback
from typing import Any

from pandacommon.pandalogger import logger_utils
from pandacommon.pandautils.PandaUtils import try_malloc_trim
from pandacommon.pandautils.plugin_factory import PluginFactory
from pandacommon.pandautils.thread_utils import GenericThread

from .msg_bkr_utils import MBListenerProxy, MBSenderProxy, MsgBuffer

# logger
base_logger = logger_utils.setup_logger("msg_processor")


# get mb proxy instance
def get_mb_proxy(name: str, sconf: dict[str, Any], qconf: dict[str, Any], mode: str = "listener", **kwargs) -> MBListenerProxy | MBSenderProxy:
    """
    Get MBListenerProxy or MBSenderProxy instance according to config dict.

    Args:
        name: Name identifier for the proxy.
        sconf: Server configuration dictionary with connection parameters.
        qconf: Queue configuration dictionary with queue parameters.
        mode: Mode of operation - 'listener' or 'sender'.
        **kwargs: Additional keyword arguments to pass to the proxy.

    Returns:
        MBListenerProxy or MBSenderProxy instance based on mode.
    """
    # class of mb proxy
    the_class = MBListenerProxy
    if mode == "sender":
        the_class = MBSenderProxy
    # resolve env variables if any
    host_port_list = sconf["host_port_list"]
    if host_port_list:
        new_list = []
        for host_port in host_port_list:
            match = re.search(r"^\${(\w+)\}$", host_port)
            if match and match.group(1) in os.environ:
                host_port = os.environ[match.group(1)]
            new_list += host_port.split(",")
        host_port_list = new_list
    username = sconf.get("username")

    if username:
        match = re.search(r"^\${(\w+)\}$", username)
        if match and match.group(1) in os.environ:
            username = os.environ[match.group(1)]
    passcode = sconf.get("passcode")

    if passcode:
        match = re.search(r"^\${(\w+)\}$", passcode)
        if match and match.group(1) in os.environ:
            passcode = os.environ[match.group(1)]

    # instantiate
    mb_proxy = the_class(
        name=name,
        host_port_list=host_port_list,
        destination=qconf["destination"],
        use_ssl=sconf.get("use_ssl", False),
        cert_file=sconf.get("cert_file"),
        key_file=sconf.get("key_file"),
        username=username,
        passcode=passcode,
        vhost=sconf.get("vhost"),
        send_heartbeat_ms=sconf.get("send_heartbeat_ms", 60000),
        recv_heartbeat_ms=sconf.get("recv_heartbeat_ms", 0),
        wait=True,
        ack_mode=qconf.get("ack_mode", "client-individual"),
        prefetch_size=qconf.get("prefetch_size"),
        max_buffer_len=qconf.get("max_buffer_len", 999),
        buffer_block_sec=qconf.get("buffer_block_sec", 10),
        use_transaction=qconf.get("use_transaction", True),
        verbose=sconf.get("verbose", False) or qconf.get("verbose", False),
        **kwargs,
    )
    return mb_proxy


# simple message processor plugin Base
class SimpleMsgProcPluginBase:
    """
    Base class of simple message processor plugin.

    Simple message processor suits following cases:
        - one-out: to create messages and send them to one queue
        - one-in: to receive messages from one queue and process them
        - one-in-one-out: to receive messages from one queue, process them, and send new messages to another queue
    """

    def __init__(self, **params: Any):
        """
        Low level initialization called by plugin factory.

        Args:
            **params: Dictionary of parameters configured for this plugin.
        """
        self.params = params

    def initialize(self):
        """
        Initialize plugin instance, run once before loop in thread.
        """

    def terminate(self):
        """
        Terminate plugin instance, run before stopping the thread.
        """

    def process(self, msg_obj: Any) -> Any:
        """
        Process the message.

        Get msg_obj from the incoming MQ (if any; otherwise msg_obj is None).
        Returned value will be sent to the outgoing MQ (if any).

        Args:
            msg_obj: Message object from incoming queue, or None if no input queue.

        Returns:
            Processed message object to send to outgoing queue.
        """
        raise NotImplementedError

    def get_pid(self) -> str:
        """
        Get generic pid, including hostname, OS process ID, and thread ID.

        Returns:
            String representation of generic PID.
        """
        return GenericThread().get_pid(current=True)


# multi-message processor plugin Base
# class MultiMsgProcPluginBase(SimpleMsgProcPluginBase):
#     """
#     Base class of multi-message processor plugin
#     For multi-in-multi-out message processor thread
#     """

#     pass


# simple message processor thread
class SimpleMsgProcThread(GenericThread):
    """
    Thread of simple message processor with certain plugin.
    """

    def __init__(self, plugin: SimpleMsgProcPluginBase, attr_dict: dict[str, Any], sleep_time_min: float, sleep_time_max: float, thread_j: int):
        """
        Initialize SimpleMsgProcThread.

        Args:
            plugin: SimpleMsgProcPluginBase instance to process messages.
            attr_dict: Dictionary containing thread attributes (in_queue, mb_sender_proxy, verbose, etc).
            sleep_time_min: Minimum sleep time in seconds when message is processed.
            sleep_time_max: Maximum sleep time in seconds when no message is processed.
            thread_j: Thread index number.
        """
        GenericThread.__init__(self)
        self.logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="SimpleMsgProcThread.__init__")
        self.__to_run = True
        self.plugin = plugin
        self.in_queue = attr_dict.get("in_queue")
        self.mb_sender_proxy = attr_dict.get("mb_sender_proxy")
        self.sleep_time_min = sleep_time_min
        self.sleep_time_max = sleep_time_max
        self.thread_j = thread_j
        self.verbose = attr_dict.get("verbose", False)

    def run(self):
        """
        Main thread execution loop for simple message processing.
        """
        # update logger thread id
        self.logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="SimpleMsgProcThread")
        # start
        self.logger.info("start run")
        # initialization step of plugin
        self.logger.info("plugin initialize")
        self.plugin.initialize()
        # message buffer
        self.logger.info(f"message buffer is {self.in_queue}")
        msg_buffer = MsgBuffer(queue_name=self.in_queue)
        # main loop
        self.logger.info("start loop")
        while self.__to_run:
            is_processed = False
            proc_ret = None
            # as consumer
            if self.in_queue:
                # get from buffer
                msg_obj = msg_buffer.get()
                if msg_obj is not None:
                    if self.verbose:
                        self.logger.debug("received a new message")
                        self.logger.debug("plugin process start")
                    try:
                        with msg_obj as _msg_obj:
                            proc_ret = self.plugin.process(_msg_obj)
                        is_processed = True
                        if self.verbose:
                            self.logger.debug("successfully processed")
                    except Exception as e:
                        tb_str = traceback.format_exc()
                        self.logger.error(f"error when process message msg_id={msg_obj.msg_id} with {e.__class__.__name__}: {e} \n{tb_str}")
                    finally:
                        del msg_obj
                    if self.verbose:
                        self.logger.debug("plugin process end")
            else:
                if self.verbose:
                    self.logger.debug("plugin process start")
                try:
                    proc_ret = self.plugin.process(None)
                    is_processed = True
                    if self.verbose:
                        self.logger.debug("successfully processed")
                except Exception as e:
                    tb_str = traceback.format_exc()
                    self.logger.error(f"error when process with {e.__class__.__name__}: {e} \n{tb_str}")
                if self.verbose:
                    self.logger.debug("plugin process end")
            # as producer
            if self.mb_sender_proxy and is_processed:
                self.mb_sender_proxy.send(proc_ret)
                if self.verbose:
                    self.logger.debug("sent a processed message")
            # sleep
            if is_processed:
                time.sleep(self.sleep_time_min)
            else:
                time.sleep(self.sleep_time_max)
        # stop loop
        self.logger.info("stopped loop")
        # tear down
        # terminate plugin
        self.logger.info("plugin terminate")
        self.plugin.terminate()
        self.logger.info("stopped run")

    def stop(self):
        """
        Send stop signal to this thread; will stop after current loop done
        """
        self.logger.debug("stop method called")
        self.__to_run = False


# simple message processor thread
class MultiMsgProcThread(GenericThread):
    """
    Thread of multi-message processor of certain plugin.
    """

    def __init__(self, plugin: SimpleMsgProcPluginBase, attr_dict: dict[str, Any], sleep_time_min: float, sleep_time_max: float, thread_j: int):
        """
        Initialize MultiMsgProcThread.

        Args:
            plugin: SimpleMsgProcPluginBase instance to process messages.
            attr_dict: Dictionary containing thread attributes.
            sleep_time_min: Minimum sleep time in seconds.
            sleep_time_max: Maximum sleep time in seconds.
            thread_j: Thread index number.
        """
        GenericThread.__init__(self)
        self.logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="MultiMsgProcThread.__init__")
        self.__to_run = True
        self.plugin = plugin
        self.in_queue_list = attr_dict.get("in_queue_list", [])
        self.mb_sender_proxy_list = attr_dict.get("mb_sender_proxy_list", [])
        self.sleep_time_min = sleep_time_min
        self.sleep_time_max = sleep_time_max
        self.thread_j = thread_j
        self.verbose = attr_dict.get("verbose", False)

    def run(self):
        """
        Main thread execution loop for multi-message processing.
        """
        # update logger thread id
        self.logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="MultiMsgProcThread")
        # start
        self.logger.info("start run")
        # initialization step of plugin
        self.logger.info("plugin initialize")
        self.plugin.initialize()
        # message buffers
        self.logger.info(f"message buffers are {self.in_queue_list}")
        self.msg_buffer_map = {}
        for in_queue in self.in_queue_list:
            msg_buffer = MsgBuffer(queue_name=in_queue)
            self.msg_buffer_map[in_queue] = msg_buffer
        # main loop
        self.logger.info("start loop")
        while self.__to_run:
            is_processed = False
            proc_ret = None
            # as consumer
            if self.msg_buffer_map:
                for in_queue, msg_buffer in self.msg_buffer_map.items():
                    # get from buffer
                    msg_obj = msg_buffer.get()
                    if msg_obj is not None:
                        if self.verbose:
                            self.logger.debug(f"received a new message from {in_queue}")
                            self.logger.debug("plugin process start")
                        try:
                            with msg_obj as _msg_obj:
                                proc_ret = self.plugin.process(_msg_obj)
                            is_processed = True
                            if self.verbose:
                                self.logger.debug("successfully processed")
                        except Exception as e:
                            tb_str = traceback.format_exc()
                            self.logger.error(f"error when process message msg_id={msg_obj.msg_id} with {e.__class__.__name__}: {e} \n{tb_str}")
                        finally:
                            del msg_obj
                        if self.verbose:
                            self.logger.debug("plugin process end")
            else:
                if self.verbose:
                    self.logger.debug("plugin process start")
                try:
                    proc_ret = self.plugin.process(None)
                    is_processed = True
                    if self.verbose:
                        self.logger.debug("successfully processed")
                except Exception as e:
                    tb_str = traceback.format_exc()
                    self.logger.error(f"error when process with {e.__class__.__name__}: {e} \n{tb_str}")
                if self.verbose:
                    self.logger.debug("plugin process end")
            # as producer
            if self.mb_sender_proxy_list and is_processed:
                for mb_sender_proxy in self.mb_sender_proxy_list:
                    mb_sender_proxy.send(proc_ret)
                    if self.verbose:
                        self.logger.debug(f"sent a processed message to {mb_sender_proxy.name}")
            # sleep
            if is_processed:
                time.sleep(self.sleep_time_min)
            else:
                time.sleep(self.sleep_time_max)
        # stop loop
        self.logger.info("stopped loop")
        # tear down
        # terminate plugin
        self.logger.info("plugin terminate")
        self.plugin.terminate()
        self.logger.info("stopped run")

    def stop(self):
        """
        Send stop signal to this thread; will stop after current loop done
        """
        self.logger.debug("stop method called")
        self.__to_run = False


# message processing agent base
class MsgProcAgentBase(GenericThread):
    """
    Base class of message processing agent (main thread)
    """

    def __init__(self, config_file: str, process_sleep_time_min: float = 0.0001, process_sleep_time_max: float = 0.005, **kwargs: Any):
        """
        Initialize MsgProcAgentBase.

        Args:
            config_file: Path to the configuration JSON file.
            process_sleep_time_min: Minimum sleep time for message processing.
            process_sleep_time_max: Maximum sleep time for message processing.
            **kwargs: Additional keyword arguments.
        """
        GenericThread.__init__(self)
        self.__to_run = True
        self.config_file = config_file
        self.process_sleep_time_min = process_sleep_time_min
        self.process_sleep_time_max = process_sleep_time_max
        self.init_mb_listener_proxy_list = []
        self.init_mb_sender_proxy_list = []
        self.init_processor_list = []
        self.processor_attr_map = {}
        self.processor_instance_map = {}
        self.processor_thread_map = {}
        self.passive_mb_listener_proxy_dict = {}
        self.passive_mb_sender_proxy_dict = {}
        self.guard_period = 300
        self._last_guard_timestamp = 0
        # log
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="__init__")
        # parse config
        self._parse_config()
        # done
        tmp_logger.info("done")

    def _parse_config(self):
        """
        Parse message processor configuration JSON file.

        The configuration file is a JSON document with (at least) the following
        top-level keys:

        - ``mb_servers`` (dict): Message broker server definitions.
        - ``queues`` (dict): Queue and listener/sender definitions.
        - ``processors`` (dict, optional): Processor plugin definitions.
        - ``guard_period`` (number, optional): Guard period in seconds.

        A typical configuration file looks like:

        .. code-block:: json

            {
              "guard_period": 300,
              "mb_servers": {
                "main_broker": {
                  "host": "broker.example.com",
                  "port": 5672,
                  "vhost": "/",
                  "user": "panda",
                  "password": "secret",
                  "impl": "pandacommon.pandamsgbkr.msg_bkr_impl.RabbitMQImpl"
                }
              },
              "queues": {
                "task_queue": {
                  "mb_server": "main_broker",
                  "queue": "tasks",
                  "exchange": "tasks-exchange",
                  "routing_key": "tasks.key",
                  "prefetch_count": 10,
                  "listener": {
                    "class": "pandacommon.pandamsgbkr.msg_listeners.TaskListener",
                    "n_threads": 4
                  },
                  "sender": {
                    "class": "pandacommon.pandamsgbkr.msg_senders.TaskSender"
                  }
                }
              },
              "processors": {
                "task_processor": {
                  "class": "pandacommon.pandamsgbkr.msg_processors.TaskProcessor",
                  "queues": ["task_queue"],
                  "config": {
                    "max_retries": 3,
                    "retry_delay": 60
                  }
                }
              }
            }

        Only the subset of fields accessed in this method is mandatory:
        ``mb_servers`` and ``queues`` are required, ``processors`` and
        ``guard_period`` are optional.
        """
        # logger
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_parse_config")
        tmp_logger.debug("start")
        # parse config json
        with open(self.config_file, "r") as _f:
            raw_dict = json.load(_f)
        self._mb_servers_dict = raw_dict["mb_servers"]
        self._queues_dict = raw_dict["queues"]
        self._processors_dict = raw_dict.get("processors", {})
        # set self optional attributes
        if raw_dict.get("guard_period") is not None:
            self.guard_period = raw_dict["guard_period"]
        tmp_logger.debug("done")

    def _setup_instances(self):
        """
        Set up attributes and MBListenerProxy/plugin instances accordingly.
        """
        # logger
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_setup_instances")
        tmp_logger.debug("start")
        # processor thread attribute dict
        processor_attr_map = {}
        # inward/outward queues and plugin instances
        in_q_set = set()
        out_q_set = set()
        for proc, pconf in self._processors_dict.items():
            # skip if not enabled
            if not pconf.get("enable", True):
                continue
            # queues
            in_queue = pconf.get("in_queue")
            out_queue = pconf.get("out_queue")
            if in_queue:
                in_q_set.add(in_queue)
            if out_queue:
                out_q_set.add(out_queue)
            # n_threads of processors
            n_threads = pconf.get("n_threads", 1)
            # plugin: one instance for each thread
            plugin_factory = PluginFactory()
            plugin_class_name = None
            for thread_j in range(n_threads):
                processor_id = (proc, thread_j)
                plugin = plugin_factory.get_plugin(pconf)
                self.processor_instance_map[processor_id] = plugin
                if thread_j == 0:
                    plugin_class_name = plugin.__class__.__name__
            # fill in thread attribute dict
            processor_attr_map[proc] = {}
            processor_attr_map[proc]["n_threads"] = n_threads
            processor_attr_map[proc]["in_queue"] = in_queue
            processor_attr_map[proc]["out_queue"] = out_queue
            processor_attr_map[proc]["plugin_class_name"] = plugin_class_name
        # mb_listener_proxy instances
        mb_listener_proxy_dict = {}
        for in_queue in in_q_set:
            qconf = self._queues_dict[in_queue]
            if not qconf.get("enable", True):
                continue
            sconf = self._mb_servers_dict[qconf["server"]]
            mb_listener_proxy = get_mb_proxy(name=in_queue, sconf=sconf, qconf=qconf, mode="listener")
            mb_listener_proxy_dict[in_queue] = mb_listener_proxy
        # mb_sender_proxy instances
        mb_sender_proxy_dict = {}
        for out_queue in out_q_set:
            qconf = self._queues_dict[out_queue]
            if not qconf.get("enable", True):
                continue
            sconf = self._mb_servers_dict[qconf["server"]]
            mb_sender_proxy = get_mb_proxy(name=out_queue, sconf=sconf, qconf=qconf, mode="sender")
            mb_sender_proxy_dict[out_queue] = mb_sender_proxy
        # keep filling in thread attribute dict
        for proc in processor_attr_map.keys():
            in_queue = processor_attr_map[proc]["in_queue"]
            if in_queue:
                if in_queue in mb_listener_proxy_dict:
                    processor_attr_map[proc]["mb_listener_proxy"] = mb_listener_proxy_dict[in_queue]
                else:
                    tmp_logger.warning(f"processor {proc} input queue {in_queue} is missing or disabled. Skip attaching listener")
            out_queue = processor_attr_map[proc]["out_queue"]
            if out_queue:
                if out_queue in mb_sender_proxy_dict:
                    processor_attr_map[proc]["mb_sender_proxy"] = mb_sender_proxy_dict[out_queue]
                else:
                    tmp_logger.warning(f"processor {proc} output queue {out_queue} is missing or disabled. Skip attaching sender")
        # fill processor list
        self.init_processor_list = []
        for processor_name, attr_dict in processor_attr_map.items():
            n_threads = attr_dict["n_threads"]
            for thread_j in range(n_threads):
                processor_id = (processor_name, thread_j)
                self.init_processor_list.append(processor_id)
        # set self attributes
        self.init_mb_listener_proxy_list = list(mb_listener_proxy_dict.values())
        self.init_mb_sender_proxy_list = list(mb_sender_proxy_dict.values())
        self.processor_attr_map = dict(processor_attr_map)
        # tear down
        del in_q_set, out_q_set, mb_listener_proxy_dict, mb_sender_proxy_dict, processor_attr_map
        tmp_logger.debug("done")

    def _spawn_listeners(self, mb_listener_proxy_list: list):
        """
        spawn connection/listener threads of certain message broker listener proxy

        Args:
            mb_listener_proxy_list: List of MBListenerProxy instances to spawn.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_spawn_listeners")
        tmp_logger.debug("start")
        for mb_proxy in mb_listener_proxy_list:
            mb_proxy.go()
            tmp_logger.info(f"spawned listener {mb_proxy.name}")
        tmp_logger.debug("done")

    def _guard_listeners(self, mb_listener_proxy_list: list):
        """
        guard connection/listener threads of certain message broker listener proxy, reconnect when disconnected

        Args:
            mb_listener_proxy_list: List of MBListenerProxy instances to guard.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_guard_listeners")
        tmp_logger.debug("start")
        for mb_proxy in mb_listener_proxy_list:
            if mb_proxy.got_disconnected and not mb_proxy.to_disconnect:
                tmp_logger.debug(f"found listener {mb_proxy.name} disconnected unexpectedly; trigger restart...")
                mb_proxy.restart()
                if mb_proxy.n_restart > 10:
                    tmp_logger.warning(f"found listener {mb_proxy.name} keep getting disconnected; already restarted {mb_proxy.n_restart} times")
                tmp_logger.info(f"restarted listener {mb_proxy.name}")
        tmp_logger.debug("done")

    def _kill_listeners(self, mb_listener_proxy_list: list):
        """
        kill connection/listener threads of certain message broker listener proxy

        Args:
            mb_listener_proxy_list: List of MBListenerProxy instances to kill.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_kill_listeners")
        tmp_logger.debug("start")
        for mb_proxy in mb_listener_proxy_list:
            mb_proxy.stop()
            tmp_logger.info(f"stopped listener {mb_proxy.name}")
        tmp_logger.debug("done")

    def _spawn_senders(self, mb_sender_proxy_list: list):
        """
        spawn connection/sender threads of certain message broker sender proxy

        Args:
            mb_sender_proxy_list: List of MBSenderProxy instances to spawn.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_spawn_senders")
        tmp_logger.debug("start")
        for mb_proxy in mb_sender_proxy_list:
            mb_proxy.go()
            tmp_logger.info(f"spawned sender {mb_proxy.name}")
        tmp_logger.debug("done")

    def _guard_senders(self, mb_sender_proxy_list: list):
        """
        guard connection/sender threads of certain message broker sender proxy, reconnect when disconnected

        Args:
            mb_sender_proxy_list: List of MBSenderProxy instances to guard.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_guard_senders")
        tmp_logger.debug("start")
        for mb_proxy in mb_sender_proxy_list:
            if mb_proxy.got_disconnected and not mb_proxy.to_disconnect:
                tmp_logger.debug(f"found sender {mb_proxy.name} disconnected unexpectedly; trigger restart...")
                mb_proxy.restart()
                if mb_proxy.n_restart > 10:
                    tmp_logger.warning(f"found sender {mb_proxy.name} keep getting disconnected; already restarted {mb_proxy.n_restart} times")
                tmp_logger.info(f"restarted sender {mb_proxy.name}")
        tmp_logger.debug("done")

    def _kill_senders(self, mb_sender_proxy_list: list):
        """
        kill connection/sender threads of certain message broker sender proxy

        Args:
            mb_sender_proxy_list: List of MBSenderProxy instances to kill.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_kill_senders")
        tmp_logger.debug("start")
        for mb_proxy in mb_sender_proxy_list:
            mb_proxy.stop()
            tmp_logger.info(f"stopped sender {mb_proxy.name}")
        tmp_logger.debug("done")

    def _spawn_processors(self, processor_list: list):
        """
        spawn processors threads

        Args:
            processor_list: List of processor IDs to spawn.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_spawn_processors")
        tmp_logger.debug("start")
        for processor_id in processor_list:
            attr_dict = {}
            try:
                processor_name, thread_j = processor_id
                attr_dict = self.processor_attr_map[processor_name]
                plugin = self.processor_instance_map[processor_id]
                self.processor_thread_map[processor_id] = SimpleMsgProcThread(
                    plugin, attr_dict, sleep_time_min=self.process_sleep_time_min, sleep_time_max=self.process_sleep_time_max, thread_j=thread_j
                )
                mc_thread = self.processor_thread_map[processor_id]
                mc_thread.start()
                tmp_logger.info(
                    f"spawned processor thread {processor_id} ({mc_thread.__class__.__name__}) with plugin={attr_dict['plugin_class_name']} , in_q={attr_dict.get('in_queue')}, out_q={attr_dict.get('out_queue')}"
                )
            except Exception as e:
                tmp_logger.error(
                    f"failed to spawn processor thread {processor_id} with plugin={attr_dict.get('plugin_class_name')} , in_q={attr_dict.get('in_queue')}, out_q={attr_dict.get('out_queue')} ; {e.__class__.__name__}: {e} "
                )
        tmp_logger.debug("done")

    def _kill_processors(self, processor_list: list, block: bool = True):
        """
        kill processor threads

        Args:
            processor_list: List of processor IDs to kill.
            block: Whether to block until threads are fully stopped.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="_kill_processors")
        tmp_logger.debug("start")
        for processor_id in processor_list:
            try:
                processor_name, thread_j = processor_id
                mc_thread = self.processor_thread_map.get(processor_id)
                if mc_thread is None:
                    tmp_logger.debug(f"processor thread {processor_id} does not exist. Skipped...")
                elif not mc_thread.is_alive():
                    tmp_logger.debug(f"processor thread {processor_id} already stopped. Skipped...")
                else:
                    mc_thread.stop()
                    tmp_logger.info(f"signaled stop to processor thread {processor_id}, block={block}")
                    if block:
                        while mc_thread.is_alive():
                            time.sleep(0.125)
                        tmp_logger.info(f"processor thread {processor_id} stopped")
            except Exception as e:
                tmp_logger.error(f"failed to stop processor thread {processor_id} ; {e.__class__.__name__}: {e} ")
        tmp_logger.debug("done")

    def initialize(self):
        """
        customized initialize method
        this method can override attributes set from config file
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="initialize")
        tmp_logger.debug("start")

        tmp_logger.debug("done")

    def stop(self, block: bool = True):
        """
        send stop signal to this thread

        Args:
            block: Whether to block until this thread is fully stopped.
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="stop")
        tmp_logger.debug("start")
        self.__to_run = False
        tmp_logger.info("signaled stop")
        if block:
            while self.is_alive():
                time.sleep(0.01)
        tmp_logger.debug("done")

    def run(self):
        """
        Main thread
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="run")
        tmp_logger.debug("start")
        # set up instances from config
        self._setup_instances()
        # initialize
        self.initialize()
        # spawn all message broker listener proxy threads
        self._spawn_listeners(self.init_mb_listener_proxy_list)
        # spawn all message broker sender proxy threads
        self._spawn_senders(self.init_mb_sender_proxy_list)
        # spawn all processor threads according to config
        self._spawn_processors(self.init_processor_list)
        # main loop
        tmp_logger.debug("looping")
        while self.__to_run:
            # guard listeners and senders, and trim memory
            if time.time() >= self._last_guard_timestamp + self.guard_period:
                self._guard_listeners(self.init_mb_listener_proxy_list)
                self._guard_senders(self.init_mb_sender_proxy_list)
                gc.collect()
                try_malloc_trim(tmp_logger)
                self._last_guard_timestamp = time.time()
            # sleep
            time.sleep(0.01)
        # tear down
        tmp_logger.debug("tearing down")
        # kill all message broker listener proxy threads
        self._kill_listeners(self.init_mb_listener_proxy_list)
        # kill all message broker sender proxy threads
        self._kill_senders(self.init_mb_sender_proxy_list)
        # kill all processor threads according to config
        self._kill_processors(self.init_processor_list)
        tmp_logger.debug("done")

    def start_passive_mode(self, in_q_list: list[str] | None = None, out_q_list: list[str] | None = None) -> dict:
        """
        start passive mode: only spawn mb proxies (without spawning agent and plugin threads)

        Args:
            in_q_list: List of inward queue names
            out_q_list: List of outward queue names

        Returns:
            Dict with keys "in" and "out", mapping to dicts of queue name to mb proxy instance for inward and outward queues respectively
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="start_passive_mode")
        tmp_logger.debug("start")
        # initialize
        # self.initialize()
        # all_queue_names = list(self._queues_dict.keys())
        if in_q_list is None:
            in_q_list = []
        if out_q_list is None:
            out_q_list = []
        # mb_listener_proxy instances
        for in_queue in in_q_list:
            if in_queue not in self._queues_dict:
                continue
            qconf = self._queues_dict[in_queue]
            if not qconf.get("enable", True):
                continue
            if qconf.get("prefetch_size") is None:
                qconf["prefetch_size"] = 5
            sconf = self._mb_servers_dict[qconf["server"]]
            mb_listener_proxy = get_mb_proxy(name=in_queue, sconf=sconf, qconf=qconf, mode="listener")
            self.passive_mb_listener_proxy_dict[in_queue] = mb_listener_proxy
        # mb_sender_proxy instances
        for out_queue in out_q_list:
            if out_queue not in self._queues_dict:
                continue
            qconf = self._queues_dict[out_queue]
            if not qconf.get("enable", True):
                continue
            sconf = self._mb_servers_dict[qconf["server"]]
            mb_sender_proxy = get_mb_proxy(name=out_queue, sconf=sconf, qconf=qconf, mode="sender")
            self.passive_mb_sender_proxy_dict[out_queue] = mb_sender_proxy
        # spawn message broker listener proxy connections
        for queue_name, mb_proxy in self.passive_mb_listener_proxy_dict.items():
            mb_proxy.go()
            tmp_logger.debug(f"spawned listener for {queue_name}")
        # spawn message broker sender proxy connections
        for queue_name, mb_proxy in self.passive_mb_sender_proxy_dict.items():
            mb_proxy.go()
            tmp_logger.debug(f"spawned sender for {queue_name}")
        tmp_logger.debug("done")
        # return
        return {
            "in": self.passive_mb_listener_proxy_dict,
            "out": self.passive_mb_sender_proxy_dict,
        }

    def stop_passive_mode(self):
        """
        stop mb proxies which were spawned in passive mode
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="stop_passive_mode")
        tmp_logger.debug("start")
        # kill message broker listener proxy connections
        for queue_name, mb_proxy in self.passive_mb_listener_proxy_dict.items():
            mb_proxy.stop()
            tmp_logger.debug(f"stopped listener for {queue_name}")
        # kill message broker sender proxy connections
        for queue_name, mb_proxy in self.passive_mb_sender_proxy_dict.items():
            mb_proxy.stop()
            tmp_logger.debug(f"stopped sender for {queue_name}")
        # clean up
        self.passive_mb_listener_proxy_dict = {}
        self.passive_mb_sender_proxy_dict = {}
        tmp_logger.debug("done")

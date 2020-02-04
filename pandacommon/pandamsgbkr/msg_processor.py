import os
import time
import socket
import json
import logging

from .msg_bkr_utils import MsgBuffer, MBProxy, MBSenderProxy
from pandacommon.pandautils.thread_utils import GenericThread
from pandacommon.pandautils.plugin_factory import PluginFactory
from pandacommon.pandalogger import logger_utils

# logger
base_logger = logger_utils.setup_logger('msg_processor')


# simple message processor plugin Base
class SimpleMsgProcPluginBase(object):
    """
    Base class of simple message processor plugin
    Simple message processor suits following cases:
        - one-out: to create a messages and send them to one queue
        - one-in: to receive messages from one queue and proceess the messages
        - one-in-one-out: to receive messages from one queue, proceess the messages to create new messages, and then and send new messages to another queue
    """

    def __init__(self):
        # Do NOT change here
        pass

    def initialize(self):
        """
        initialize plugin instance, run once before loop in thread
        """
        pass

    def process(self, msg_obj):
        """
        process the message
        Get msg_obj from the incoming MQ (if any; otherwise msg_obj is None)
        Returned value will be sent to the outgoing MQ (if any)
        """
        raise NotImplementedError


# muti-message processor plugin Base
class MultiMsgProcPluginBase(object):
    """
    Base class of multi-message processor plugin
    For multi-in-multi-out message processor thread
    """
    # TODO
    pass


# simple message processor thread
class SimpleMsgProcThread(GenericThread):
    """
    Thread of simple message processor of certain plugin
    """

    def __init__(self, attr_dict, sleep_time=0.03125):
        GenericThread.__init__(self)
        self.logger = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='SimpleMsgProcThread')
        self.__to_run = True
        self.plugin = attr_dict['plugin']
        self.in_queue = attr_dict.get('in_queue')
        self.mb_sender_proxy = attr_dict.get('mb_sender_proxy')
        self.sleep_time = sleep_time

    def run(self):
        # start
        self.logger.debug('start run')
        # initialization step of plugin
        self.logger.debug('plugin initialize')
        self.plugin.initialize()
        # message buffer
        self.logger.debug('message buffer is {0}'.format(self.in_queue))
        msg_buffer = MsgBuffer(queue_name=self.in_queue)
        # main loop
        self.logger.debug('start loop')
        while self.__to_run:
            is_processed = False
            proc_ret = None
            # as consumer
            if self.in_queue:
                # get from buffer
                msg_obj = msg_buffer.get()
                if msg_obj is not None:
                    self.logger.debug('received a new message')
                    self.logger.debug('plugin process start')
                    try:
                        with msg_obj as _msg_obj:
                            proc_ret = self.plugin.process(_msg_obj)
                        is_processed = True
                        self.logger.debug('successfully processed')
                    except Exception as e:
                        self.logger.error('error when process message msg_id={0} with {1}: {2} '.format(
                                                                msg_obj.msg_id , e.__class__.__name__, e))
                    self.logger.debug('plugin process end')
            else:
                self.logger.debug('plugin process start')
                try:
                    proc_ret = self.plugin.process(None)
                    is_processed = True
                    self.logger.debug('successfully processed')
                except Exception as e:
                    self.logger.error('error when process with {0}: {1} '.format(
                                                            msg_obj.msg_id , e.__class__.__name__, e))
                self.logger.debug('plugin process end')
            # as producer
            if self.mb_sender_proxy and is_processed:
                self.mb_sender_proxy.send(proc_ret)
                self.logger.debug('sent a processed message')
            # sleep
            time.sleep(self.sleep_time)
        # stop loop
        self.logger.debug('stopped loop')
        # tear down
        self.logger.debug('stopped run')

    def stop(self):
        """
        send stop signal to this thread; will stop after current loop done
        """
        self.logger.debug('stop method called')
        self.__to_run = False


# simple message processor thread
class MultiMsgProcThread(GenericThread):
    """
    Thread of multi-message processor of certain plugin
    """
    # TODO
    pass


# message processing agent base
class MsgProcAgentBase(GenericThread):
    """
    Base class of message processing agent (main thread)
    """

    def __init__(self, config_file, **kwargs):
        GenericThread.__init__(self)
        self.__to_run = True
        self.config_file = config_file
        self.init_mb_proxy_list = []
        self.init_mb_sender_proxy_list = []
        self.init_processor_list = []
        self.processor_attr_map = dict()
        self.processor_thread_map = dict()
        # log
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='__init__')
        # set from config
        # done
        tmp_logger.info('done, pid='.format(self.get_pid()))

    def _set_from_config(self):
        """
        parse message processor configuration json file and set attributes accordingly
        Typical example dict from config json:
        mb_servers_dict = {
                'Server_1': {
                    'host_port_list': ['192.168.0.1:777', '192.168.0.2:777'],
                    'use_ssl': True,
                    'cert_file': 'aaa.cert.pem',
                    'key_file': 'bbb.key.pem',
                    'username': 'someuser',
                    'passcode': 'xxxxyyyyzzzz',
                },
                ...
            }
        queues_dict = {
                'Queue_1': {
                    'server': 'Server_1',
                    'destination': '/queue/some_queue',
                },
                ...
            }
        processors_dict = {
                'Processor_1': {
                    'module': 'plugin.module',
                    'name': 'PluginClassName',
                    'in_queue': 'Queue_1',
                    'out_queue': 'Queue_2',
                },
                ...
            }
        """
        # logger
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_set_from_config')
        tmp_logger.debug('start')
        # parse config json
        with open(self.config_file, 'r') as _f:
            raw_dict = json.load(_f)
        mb_servers_dict = raw_dict['mb_servers']
        queues_dict = raw_dict['queues']
        processors_dict = raw_dict['processors']
        # processor thread attribute dict
        processor_attr_map = dict()
        # inward/outward queues and plugin instances
        in_q_set = set()
        out_q_set = set()
        for proc, pconf in processors_dict.items():
            # queues
            in_queue = pconf.get('in_queue')
            out_queue = pconf.get('out_queue')
            if in_queue:
                in_q_set.add(in_queue)
            if out_queue:
                out_q_set.add(out_queue)
            # plugin
            plugin_factory = PluginFactory()
            plugin = plugin_factory.get_plugin(pconf)
            # fill in thread attribute dict
            processor_attr_map[proc] = dict()
            processor_attr_map[proc]['in_queue'] = in_queue
            processor_attr_map[proc]['out_queue'] = out_queue
            processor_attr_map[proc]['plugin'] = plugin

        # mb_proxy instances
        mb_proxy_dict = dict()
        for in_queue in in_q_set:
            qconf = queues_dict[in_queue]
            sconf = mb_servers_dict[qconf['server']]
            mb_proxy = MBProxy(name=in_queue,
                                host_port_list=sconf['host_port_list'],
                                destination=qconf['destination'],
                                use_ssl=sconf['use_ssl'],
                                cert_file=sconf['cert_file'],
                                key_file=sconf['key_file'],
                                username=sconf['username'],
                                passcode=sconf['passcode'],
                                wait=True)
            mb_proxy_dict[in_queue] = mb_proxy
        # mb_sender_proxy instances
        mb_sender_proxy_dict = dict()
        for out_queue in out_q_set:
            qconf = queues_dict[out_queue]
            sconf = mb_servers_dict[qconf['server']]
            mb_sender_proxy = MBSenderProxy(name=out_queue,
                                            host_port_list=sconf['host_port_list'],
                                            destination=qconf['destination'],
                                            use_ssl=sconf['use_ssl'],
                                            cert_file=sconf['cert_file'],
                                            key_file=sconf['key_file'],
                                            username=sconf['username'],
                                            passcode=sconf['passcode'],
                                            wait=True)
            mb_sender_proxy_dict[out_queue] = mb_sender_proxy
        # keep filling in thread attribute dict
        for proc in processors_dict.keys():
            in_queue = processor_attr_map[proc]['in_queue']
            if in_queue:
                processor_attr_map[proc]['mb_proxy'] = mb_proxy_dict[in_queue]
            out_queue = processor_attr_map[proc]['out_queue']
            if out_queue:
                processor_attr_map[proc]['mb_sender_proxy'] = mb_sender_proxy_dict[out_queue]
        # set self attributes
        self.init_processor_list = list(processors_dict.keys())
        self.init_mb_proxy_list = list(mb_proxy_dict.values())
        self.init_mb_sender_proxy_list = list(mb_sender_proxy_dict.values())
        self.processor_attr_map = dict(processor_attr_map)
        # tear down
        del in_q_set, out_q_set, mb_proxy_dict, mb_sender_proxy_dict, processor_attr_map
        tmp_logger.debug('done')

    def _spawn_listeners(self, mb_proxy_list):
        """
        spawn connection/listener threads of certain message broker proxy
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_spawn_listeners')
        tmp_logger.debug('start')
        for mb_proxy in mb_proxy_list:
            mb_proxy.go()
            tmp_logger.info('spawned listener {0}'.format(mb_proxy.name))
        tmp_logger.debug('done')

    def _kill_listeners(self, mb_proxy_list):
        """
        kill connection/listener threads of certain message broker proxy
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_kill_listeners')
        tmp_logger.debug('start')
        for mb_proxy in mb_proxy_list:
            mb_proxy.stop()
            tmp_logger.info('signaled stop to listener {0}'.format(mb_proxy.name))
        tmp_logger.debug('done')

    def _spawn_senders(self, mb_sender_proxy_list):
        """
        spawn connection/listener threads of certain message broker proxy
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_spawn_senders')
        tmp_logger.debug('start')
        for mb_proxy in mb_sender_proxy_list:
            mb_proxy.go()
            tmp_logger.info('spawned listener {0}'.format(mb_proxy.name))
        tmp_logger.debug('done')

    def _kill_senders(self, mb_sender_proxy_list):
        """
        kill connection/listener threads of certain message broker proxy
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_kill_senders')
        tmp_logger.debug('start')
        for mb_proxy in mb_sender_proxy_list:
            mb_proxy.stop()
            tmp_logger.info('signaled stop to listener {0}'.format(mb_proxy.name))
        tmp_logger.debug('done')

    def _spawn_processors(self, processor_list):
        """
        spawn processors threads
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_spawn_processors')
        tmp_logger.debug('start')
        for processor_name in processor_list:
            try:
                attr_dict = self.processor_attr_map[processor_name]
                self.processor_thread_map[processor_name] = SimpleMsgProcThread(attr_dict)
                mc_thread = self.processor_thread_map[processor_name]
                mc_thread.start()
                tmp_logger.info('spawned processors thread {0} with plugin={1} , in_q={2}, out_q={3}'.format(
                                                processor_name, attr_dict['plugin'].__class__.__name__,
                                                attr_dict['in_queue'], attr_dict['out_queue']))
            except Exception as e:
                tmp_logger.error('falied to spawn processor thread {0} with plugin={1} , in_q={2}, out_q={3} ; {4}: {5} '.format(
                                                processor_name, attr_dict['plugin'].__class__.__name__,
                                                attr_dict['in_queue'], attr_dict['out_queue'], e.__class__.__name__, e))
        tmp_logger.debug('done')

    def _kill_processors(self, processor_list, block=True):
        """
        kill processor threads
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='_kill_processors')
        tmp_logger.debug('start')
        for processor_name in processor_list:
            try:
                mc_thread = self.processor_thread_map.get(processor_name)
                if mc_thread is None:
                    tmp_logger.debug('processor thread {0} does not exist. Skipped...'.format(processor_name))
                elif not mc_thread.is_alive():
                    tmp_logger.debug('processor thread {0} already stopped. Skipped...'.format(processor_name))
                else:
                    mc_thread.stop()
                    tmp_logger.info('signaled stop to processor thread {0}, block={1}'.format(processor_name, block))
                    if block:
                        while mc_thread.is_alive():
                            time.sleep(0.125)
                        tmp_logger.info('processor thread {0} stopped'.format(processor_name))
            except Exception as e:
                tmp_logger.error('falied to stop processor thread {0} ; {1}: {2} '.format(
                                                processor_name, e.__class__.__name__, e))
        tmp_logger.debug('done')

    def initialize(self):
        """
        customized initialize method
        this method can override attributes set from config file
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='initialize')
        tmp_logger.debug('start')
        pass
        tmp_logger.debug('done')

    def stop(self):
        """
        send stop signal to this thread
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='initialize')
        tmp_logger.debug('start')
        self.__to_run = False
        tmp_logger.info('signaled stop to self')
        tmp_logger.debug('done')

    def run(self):
        """
        main thread
        """
        tmp_logger = logger_utils.make_logger(base_logger, token=self.__class__.__name__, method_name='run')
        tmp_logger.debug('start')
        # set attributes from config
        self._set_from_config()
        # initialize
        self.initialize()
        # spawn all message broker proxy threads
        self._spawn_listeners(self.init_mb_proxy_list)
        # spawn all message broker sender proxy threads
        self._spawn_senders(self.init_mb_sender_proxy_list)
        # spawn all processor threads according to config
        self._spawn_processors(self.init_processor_list)
        # main loop
        tmp_logger.debug('looping')
        while self.__to_run:
            # TODO: monitor ?!
            time.sleep(1)
        # tear down
        tmp_logger.debug('tearing down')
        # kill all message broker proxy threads
        self._kill_listeners(self.init_mb_proxy_list)
        # kill all message broker sender proxy threads
        self._kill_senders(self.init_mb_sender_proxy_list)
        # kill all processor threads according to config
        self._kill_processors(self.init_processor_list)
        tmp_logger.debug('done')

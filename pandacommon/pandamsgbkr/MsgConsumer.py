import os
import time
import threading
import socket
import logging

from .MsgBkrUtils import MsgBuffer
from pandacommon.pandalogger.PandaLogger import PandaLogger


# logger # FIXME
some_logger = PandaLogger().getLogger('MsgConsumer', log_level='DEBUG')


# plugin Base
class MsgConsumerPluginBase(object):
    """
    Base class of message consumer plugin
    """

    def __init__(self):
        pass

    def initialize(self):
        """
        initialize plugin instance, run once before loop in thread
        """
        raise NotImplementedError

    def consume(self, msg_obj):
        """
        consume the message. Note msg_obj will be None if getting nothing from the queue
        """
        raise NotImplementedError


# message consumer thread
class MsgConsumerThread(threading.Thread):
    """
    Thread of message consumer of certain plugin
    """

    def __init__(self, plugin, queue_name):
        threading.Thread.__init__(self)
        self.__to_run = True
        self.plugin = plugin
        self.logger = some_logger # FIXME
        self.queue_name = queue_name

    def run(self):
        # start
        self.logger.debug('start consumer')
        # initialization step of plugin
        self.logger.debug('plugin initialize')
        self.plugin.initialize()
        # message buffer
        self.logger.debug('use message buffer {0}'.format(self.queue_name))
        msg_buffer = MsgBuffer(name=self.queue_name)
        # main loop
        self.logger.debug('start loop')
        while self.__to_run:
            # get from buffer
            self.logger.debug('get message')
            msg_obj = msg_buffer.get()
            # consume
            self.logger.debug('plugin consume start')
            if msg_obj is not None:
                with msg_obj as _msg_obj:
                    self.plugin.consume(_msg_obj)
            self.logger.debug('plugin consume end')
            # sleep
            time.sleep(0.03125)
        # stop loop
        self.logger.debug('stopped loop')
        # tear down
        self.logger.debug('stopped consumer')

    def stop(self):
        """
        send stop signal to this thread; will stop after current loop done
        """
        self.__to_run = False


# agent base
class MsgConsumerAgentBase(threading.Thread):
    """
    Base class of message consumer agent (main thread)
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.__to_run = True
        self.stopEvent = None
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()
        self.logger = some_logger # FIXME
        self.init_mb_proxy_list = [] # FIXME, from config and instances
        self.init_consumer_list = [] # FIXME, from config
        self.consumer_info_map = dict() # FIXME, from config and plugin instances
        self.consumer_thread_map = dict()

    def _spawn_listeners(self, mb_proxy_list):
        """
        spawn connection/listener threads of certain message broker proxy
        """
        for mb_proxy in mb_proxy_list:
            mb_proxy.go()

    def _kill_listeners(self, mb_proxy_list):
        """
        kill connection/listener threads of certain message broker proxy
        """
        for mb_proxy in mb_proxy_list:
            mb_proxy.stop()

    def _spawn_consumers(self, consumer_list):
        """
        spawn consumer threads
        """
        for consumer_name in consumer_list:
            try:
                plugin, queue_name = self.consumer_info_map[consumer_name]
                self.consumer_thread_map[consumer_name] = MsgConsumerThread(plugin, queue_name)
                mc_thread = self.consumer_thread_map[consumer_name]
                mc_thread.start()
                self.logger.info('spawned consumer thread {0} with plugin={1} , mq={2}'.format(
                                                consumer_name, plugin.__class__.__name__, queue_name))
            except Exception as e:
                self.logger.error('falied to spawn consumer thread {0} with plugin={1} , mq={2} ; {3}: {4} '.format(
                                                consumer_name, plugin.__class__.__name__, queue_name, e.__class__.__name__, e))

    def _kill_consumers(self, consumer_list, block=True):
        """
        kill consumer threads
        """
        for consumer_name in consumer_list:
            try:
                mc_thread = self.consumer_thread_map.get(consumer_name)
                if mc_thread is None:
                    self.logger.debug('consumer thread {0} does not exist. Skipped...'.format(consumer_name))
                elif not mc_thread.is_alive():
                    self.logger.debug('consumer thread {0} already stopped. Skipped...'.format(consumer_name))
                else:
                    mc_thread.stop()
                    self.logger.info('signaled stop to consumer thread {0}, block={1}'.format(consumer_name, block))
                    if block:
                        while mc_thread.is_alive():
                            time.sleep(0.125)
                        self.logger.info('consumer thread {0} stopped'.format(consumer_name))
            except Exception as e:
                self.logger.error('falied to stop consumer thread {0} ; {1}: {2} '.format(
                                                consumer_name, e.__class__.__name__, e))

    def get_pid(self):
        """
        get process/thread identifier
        """
        thread_id = self.ident if self.ident else 0
        return '{0}_{1}-{2}'.format(self.hostname, self.os_pid, format(thread_id, 'x'))

    def initialize(self):
        # fill in self.consumer_info_map accroding to config, plugins must be instantiated
        # instanciate mb_proxy instances accorgind to config
        pass

    def stop(self):
        """
        send stop signal to this thread
        """
        self.__to_run = False

    def run(self):
        """
        main thread
        """
        # initialize
        self.initialize()
        # spawn all message broker proxy threads according to config
        self._spawn_listeners(self.init_mb_proxy_list)
        # spawn all consumer threads according to config
        self._spawn_consumers(self.init_consumer_list)

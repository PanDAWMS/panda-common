import threading
import socket
import ssl
import random
import collections
import time
import copy

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

import stomp

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger('msg_bkr_utils')

# global lock
_GLOBAL_LOCK = threading.Lock()

# global map of message buffers
_BUFFER_MAP = dict()


# get connection list
def _get_connection_list(host_port_list, use_ssl=False, cert_file=None, key_file=None, force=False):
    """
    get list of (conn_id, connection)
    """
    tmp_logger = logger_utils.make_logger(base_logger, method_name='_get_connection_list')
    ssl_opts = {'use_ssl' : use_ssl,
                'ssl_version' : ssl.PROTOCOL_TLSv1,
                'ssl_cert_file' : cert_file,
                'ssl_key_file'  : key_file}
    conn_dict = dict()
    for host_port in host_port_list:
        host, port = host_port.split(':')
        # ip_list = socket.gethostbyname_ex(host)[-1]
        # for ip in ip_list:
        #     conn_id = '{0}:{1}'.format(ip, port)
        #     if conn_id not in conn_dict:
        #         conn = stomp.Connection(host_and_ports = [(ip, port)], **ssl_opts)
        #         conn_dict[conn_id] = conn
        conn_id = host_port
        if conn_id not in conn_dict:
            conn = stomp.Connection12(host_and_ports = [(host, int(port))], **ssl_opts)
            conn_dict[conn_id] = conn
    ret_list = list(conn_dict.items())
    tmp_logger.debug('got {0} connections to {1}'.format(len(ret_list), ' , '.join(conn_dict.keys())))
    return ret_list


# message buffer
class MsgBuffer(object):
    """
    Global message buffer. Singleton for each queue name
    """

    @staticmethod
    def _initialize(self, queue_name):
        """
        Write init here becuase of singleton
        """
        # name of the message queue
        self.queue_name = queue_name
        # interal fifo
        self.__fifo = collections.deque()

    def __new__(cls, queue_name):
        key = queue_name
        with _GLOBAL_LOCK:
            if key not in _BUFFER_MAP:
                inst = object.__new__(cls)
                _BUFFER_MAP[key] = inst
                cls._initialize(inst, queue_name)
            return _BUFFER_MAP[key]

    def __init__(self, *args, **kwargs):
        # Do NOT write anything here becuase of singleton
        pass

    def size(self):
        return len(self.__fifo)

    def get(self):
        try:
            ret = self.__fifo.popleft()
        except IndexError:
            ret = None
        return ret

    def put(self, obj):
        self.__fifo.append(obj)


# message object
class MsgObj(object):
    """
    Message object, stored in local buffer and consumed by consumer threads
    Support with-statement
    """

    __slots__ = ('__mb_proxy', 'sub_id', 'msg_id', 'ack_id', 'data')

    def __init__(self, mb_proxy, msg_id, ack_id, data):
        # associated proxy object
        self.__mb_proxy = mb_proxy
        # subscription ID
        self.sub_id = self.__mb_proxy.sub_id
        # message ID
        self.msg_id = msg_id
        # ack ID
        self.ack_id = ack_id
        # real message data
        self.data = data

    def __enter__(self):
        self.__mb_proxy.logger.debug('msg_id={m} MsgObj.__enter__ called'.format(m=self.msg_id))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__mb_proxy.logger.debug('msg_id={m} MsgObj.__exit__ called'.format(m=self.msg_id))
        if exc_type or exc_value:
            # exception occurs, send nack
            self.__mb_proxy._nack(self.msg_id, self.ack_id)
        else:
            # done, send ack
            self.__mb_proxy._ack(self.msg_id, self.ack_id)


# message listener
class MsgListener(stomp.ConnectionListener):
    """
    Message listener of STOMP
    """

    def __init__(self, mb_proxy, *args, **kwargs):
        # logger
        _token = '{0}-{1}'.format(mb_proxy.__class__.__name__, mb_proxy.name)
        self.logger = logger_utils.make_logger(base_logger, token=_token, method_name='MsgListener')
        # associated messgage broker proxy
        self.mb_proxy = mb_proxy

    def on_error(self, headers, message):
        self.logger.error('on_error start: {h} "{m}"'.format(h=headers, m=message))
        self.logger.error('on_error done: {h}'.format(h=headers))

    def on_disconnected(self):
        self.logger.info('on_disconnected start')
        self.logger.info('on_disconnected done')

    def on_send(self, frame):
        obscured_headers = frame.headers
        if 'passcode' in frame.headers:
            obscured_headers = copy.deepcopy(frame.headers)
            obscured_headers['passcode'] = '********'
        self.logger.debug('on_send frame: {0} {1} "{2}"'.format(frame.cmd, obscured_headers, frame.body))

    def on_message(self, headers, message):
        self.logger.debug('on_message start: {h} "{m}"'.format(h=headers, m=message))
        self.mb_proxy._on_message(headers, message)
        self.logger.debug('on_message done: {h}'.format(h=headers))


# message broker proxy for receiver
class MBProxy(object):

    def __init__(self, name, host_port_list, destination, use_ssl=False, cert_file=None, key_file=None,
                    username=None, passcode=None, wait=True, ack_mode='client-individual', skip_buffer=False):
        # logger
        self.logger = logger_utils.make_logger(base_logger, token=name, method_name='MBProxy')
        # name of message queue
        self.name = name
        # connection; FIXME: how to choose a connection? Round-robin?
        conn_list = _get_connection_list(host_port_list, use_ssl, cert_file, key_file)
        self.conn_id, self.conn = random.choice(conn_list)
        # destination queue to subscribe
        self.destination = destination
        # subscription ID
        self.sub_id = 'panda-MBProxy_{0}_r{1:06}'.format(socket.getfqdn(), random.randrange(10**6))
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))
        # connect parameters
        self.connect_params = {'username': username, 'passcode': passcode, 'wait': wait,
                                'headers': {'client-id': self.client_id}}
        # acknoledge mode
        self.ack_mode = ack_mode
        # associate message buffer
        self.msg_buffer = MsgBuffer(queue_name=self.name)
        # message listener
        self.listener = MsgListener(mb_proxy=self)
        # whether to skip buffer and dump to self.dump_msgs; True only in testing
        self.skip_buffer = skip_buffer
        # dump messages
        self.dump_msgs = []

    def _ack(self, msg_id, ack_id):
        if self.ack_mode in ['client', 'client-individual']:
            self.conn.ack(ack_id)
            self.logger.debug('{mid} {ackid} ACKed'.format(mid=msg_id, ackid=ack_id))

    def _nack(self, msg_id, ack_id):
        if self.ack_mode in ['client', 'client-individual']:
            self.conn.nack(ack_id)
            self.logger.debug('{mid} {ackid} NACKed'.format(mid=msg_id, ackid=ack_id))

    def _on_message(self, headers, message):
        msg_obj = MsgObj(mb_proxy=self, msg_id=headers['message-id'], ack_id=headers['ack'], data=message)
        self.logger.debug('_on_message made message object: {h}'.format(h=headers))
        if self.skip_buffer:
            self.logger.debug('_on_message (buffer_skipped) dump the message: {h}'.format(h=headers))
            self.dump_msgs.append(msg_obj.data)
            self._ack(msg_obj.msg_id, msg_obj.ack_id)
        else:
            self.msg_buffer.put(msg_obj)
            self.logger.debug('_on_message put into buffer: {h}'.format(h=headers))

    def go(self):
        self.logger.debug('go called')
        try:
            if not self.conn.is_connected():
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                self.conn.connect(**self.connect_params)
                self.conn.subscribe(destination=self.destination, id=self.sub_id, ack='client-individual')
                self.logger.info('connected to {0} {1}'.format(self.conn_id, self.destination))
            else:
                self.logger.info('connection to {0} {1} already exists. Skipped...'.format(
                                                                        self.conn_id, self.destination))
        except Exception as e:
            self.logger.error('falied to start connection to {0} {1} ; {2}: {3} '.format(
                                            self.conn_id, self.destination, e.__class__.__name__, e))

    def stop(self):
        self.logger.debug('stop called')
        self.conn.disconnect()
        self.logger.info('disconnect from {0} {1}'.format(self.conn_id, self.destination))


# message broker proxy for sender, waster...
class MBSenderProxy(object):

    def __init__(self, name, host_port_list, destination, use_ssl=False, cert_file=None, key_file=None,
                    username=None, passcode=None, wait=True):
        # logger
        self.logger = logger_utils.make_logger(base_logger, token=name, method_name='MBSenderProxy')
        # name of message queue
        self.name = name
        # connection; FIXME: how to choose a connection? Round-robin?
        conn_list = _get_connection_list(host_port_list, use_ssl, cert_file, key_file)
        self.conn_id, self.conn = random.choice(conn_list)
        # destination queue to subscribe
        self.destination = destination
        # subscription ID
        self.sub_id = 'panda-MBSenderProxy_{0}_r{1:06}'.format(socket.getfqdn(), random.randrange(10**6))
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))
        # connect parameters
        self.connect_params = {'username': username, 'passcode': passcode, 'wait': wait,
                                'headers': {'client-id': self.client_id}}
        # message listener
        self.listener = MsgListener(mb_proxy=self)

    def _on_message(self, headers, message):
        self.logger.debug('_on_message drop message: {h} "{m}"'.format(h=headers, m=message))

    def send(self, data):
        """
        send a message to queue
        """
        self.conn.send(destination=self.destination, body=data)
        self.logger.debug('send to {dest} "{data}"'.format(dest=self.destination, data=data))

    def waste(self, duration=3):
        """
        drop all messages gotten during duration time
        """
        self.conn.subscribe(destination=self.destination, id=self.sub_id, ack='auto')
        time.sleep(duration)
        self.conn.unsubscribe(id=self.sub_id)
        self.logger.debug('waste dropped messages for {t} sec'.format(t=duration))

    def go(self):
        self.logger.debug('go called')
        try:
            if not self.conn.is_connected():
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                self.conn.connect(**self.connect_params)
                self.logger.info('connected to {0} {1}'.format(self.conn_id, self.destination))
            else:
                self.logger.info('connection to {0} {1} already exists. Skipped...'.format(
                                                                        self.conn_id, self.destination))
        except Exception as e:
            self.logger.error('falied to start connection to {0} {1} ; {2}: {3} '.format(
                                            self.conn_id, self.destination, e.__class__.__name__, e))

    def stop(self):
        self.logger.debug('stop called')
        self.conn.disconnect()
        self.logger.info('disconnect from {0} {1}'.format(self.conn_id, self.destination))

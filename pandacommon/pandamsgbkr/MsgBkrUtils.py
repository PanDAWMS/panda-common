import threading
import socket
import ssl
import random
import collections
import time

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

import stomp


# global lock
_GLOBAL_LOCK = threading.Lock()

# global map of message buffers
_BUFFER_MAP = dict()


# get connection list
def _get_connection_list(host_port_list, use_ssl=False, cert_file=None, key_file=None, force=False):
    """
    get list of (conn_id, connection)
    """
    ssl_opts = {'use_ssl' : use_ssl,
                'ssl_version' : ssl.PROTOCOL_TLSv1,
                'ssl_cert_file' : cert_file,
                'ssl_key_file'  : key_file}
    conn_dict = dict()
    for host_port in host_port_list:
        host, port = host_port.split(':')
        ip_list = socket.gethostbyname_ex(host)[-1]
        for ip in ip_list:
            conn_id = '{0}:{1}'.format(ip, port)
            if conn_id not in conn_dict:
                conn = stomp.Connection(host_and_ports = [(ip, port)], **ssl_opts)
                conn_dict[conn_id] = conn
    ret_list = list(conn_dict.items())
    return ret_list


# message buffer
class MsgBuffer_x(object):
    """
    Global message buffer. Singleton for each queue name
    """

    def __new__(cls, name):
        with _GLOBAL_LOCK:
            if name not in _BUFFER_MAP:
                _BUFFER_MAP[name] = object.__new__(cls)
            return _BUFFER_MAP[name]

    def __init__(self, name):
        # name of the message queue
        self.name = name
        # interal fifo
        self.__fifo = collections.deque()

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


# message buffer
class MsgBuffer(object):
    """
    Global message buffer. Singleton for each queue name
    """

    @staticmethod
    def _initialize(self, name):
        """
        Write init here becuase of singleton
        """
        # name of the message queue
        self.name = name
        # interal fifo
        self.__fifo = Queue()

    def __new__(cls, name):
        with _GLOBAL_LOCK:
            if name not in _BUFFER_MAP:
                inst = object.__new__(cls)
                _BUFFER_MAP[name] = inst
                cls._initialize(inst, name)
            return _BUFFER_MAP[name]

    def __init__(self, name):
        # Do NOT write anything here becuase of singleton
        pass

    def size(self):
        return self.__fifo.qsize()

    def get(self):
        try:
            ret = self.__fifo.get(False)
        except Empty:
            ret = None
        return ret

    def put(self, obj):
        self.__fifo.put(obj)


# message object
class MsgObj(object):
    """
    Message object, stored in local buffer and consumed by consumer threads
    Support with-statement
    """

    __slots__ = ('__mb_proxy', 'sub_id', 'msg_id', 'data')

    def __init__(self, mb_proxy, msg_id, data):
        # associated proxy object
        self.__mb_proxy = mb_proxy
        # subscription ID
        self.sub_id = self.__mb_proxy.sub_id
        # message ID
        self.msg_id = msg_id
        # real message data
        self.data = data

    def __enter__(self):
        self.__mb_proxy.logger.debug('sub_id={s} msg_id={m} MsgObj.__enter__ called'.format(s=self.sub_id, m=self.msg_id))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__mb_proxy.logger.debug('sub_id={s} msg_id={m} MsgObj.__exit__ called'.format(s=self.sub_id, m=self.msg_id))
        if exc_type or exc_value:
            # exception occurs, send nack
            self.__mb_proxy._nack(self.msg_id)
        else:
            # done, send ack
            self.__mb_proxy._ack(self.msg_id)


# message listener
class MsgListener(stomp.ConnectionListener):
    """
    Message listener of STOMP
    """

    def __init__(self, logger, sub_id, mb_proxy, *args, **kwargs):
        # logger
        self.logger = logger
        # subscription ID
        self.sub_id = sub_id
        # associated messgage broker proxy
        self.mb_proxy = mb_proxy

    def on_error(self, headers, message):
        self.logger.error('{id} on_error start: {h} ; {m}'.format(id=self.sub_id, h=headers, m=message))
        self.logger.error('{id} on_error done: {h}'.format(id=self.sub_id, h=headers))

    def on_disconnected(self):
        self.logger.info('{id} on_disconnected start'.format(id=self.sub_id))
        self.logger.info('{id} on_disconnected done'.format(id=self.sub_id))

    def on_send(self, frame):
        self.logger.info('on_send frame: {0} {1} "{2}"'.format(frame.cmd, frame.headers, frame.body))

    def on_message(self, headers, message):
        self.logger.info('{id} on_message start: {h} ; {m}'.format(id=self.sub_id, h=headers, m=message))
        self.mb_proxy._on_message(headers, message)
        self.logger.info('{id} on_message done: {h}'.format(id=self.sub_id, h=headers))


# message broker proxy
class MBProxy(object):

    def __init__(self, name, logger, host_port_list, destination,
                    use_ssl=False, cert_file=None, key_file=None, ack_mode='client-individual'):
        # logger
        self.logger = logger
        # name of message queue
        self.name = name
        # connection; FIXME: how to choose a connection? Round-robin?
        conn_list = _get_connection_list(host_port_list, use_ssl, cert_file, key_file)
        self.conn_id, self.conn = random.choice(conn_list)
        # destination queue to subscribe
        self.destination = destination
        # subscription ID
        self.sub_id = 'panda-MBProxy_{0}_{1}'.format(socket.getfqdn(), 0)
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))
        # acknoledge mode
        self.ack_mode = ack_mode
        # associate message buffer
        self.msg_buffer = MsgBuffer(name=self.name)
        # message listener
        self.listener = MsgListener(logger=logger, sub_id=self.sub_id, mb_proxy=self)

    def _ack(self, msg_id):
        if self.ack_mode in ['client', 'client-individual']:
            self.conn.ack(msg_id, self.sub_id)
            self.logger.debug('{mid} {id} ACKed'.format(mid=msg_id, id=self.sub_id))

    def _nack(self, msg_id):
        if self.ack_mode in ['client', 'client-individual']:
            self.conn.nack(msg_id, self.sub_id)
            self.logger.debug('{mid} {id} NACKed'.format(mid=msg_id, id=self.sub_id))

    def _on_message(self, headers, message):
        msg_obj = MsgObj(mb_proxy=self, msg_id=headers['message-id'], data=message)
        self.logger.debug('_on_message made message object: {h}'.format(h=headers))
        self.msg_buffer.put(msg_obj)
        self.logger.debug('_on_message put into buffer: {h}'.format(h=headers))

    def go(self):
        self.logger.debug('go called')
        try:
            if not self.conn.is_connected():
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                self.conn.start()
                self.conn.connect(headers = {'client-id': self.client_id})
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


# message sender
class MsgSender(MBProxy):

    def __init__(self, *args, **kwargs):
        MBProxy.__init__(self, *args, **kwargs)
        # subscription ID
        self.sub_id = 'panda-MsgSender_{0}_{1}'.format(socket.getfqdn(), 0)
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))

    def _on_message(self, headers, message):
        self.logger.debug('_on_message drop message: {h} "{m}"'.format(h=headers, m=message))

    def send(self, data):
        """
        send a message to queue
        """
        self.conn.send(destination=self.destination, body=data)
        self.logger.debug('SEND to {dest}: {data}'.format(dest=self.destination, data=data))

    def waste(self, duration=3):
        """
        drop all messages gotten during duration time
        """
        self.conn.subscribe(destination=self.destination, id=self.sub_id, ack='auto')
        time.sleep(duration)
        self.conn.unsubscribe(id=self.sub_id)

    def go(self):
        self.logger.debug('go called')
        try:
            if not self.conn.is_connected():
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                self.conn.start()
                self.conn.connect(headers = {'client-id': self.client_id})
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

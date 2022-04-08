import threading
import socket
import ssl
import random
import collections
import time
import copy
import traceback

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


# get connection dict
def _get_connection_dict(host_port_list, use_ssl=False, cert_file=None, key_file=None, vhost=None, force=False):
    """
    get dict {conn_id: connection}
    """
    tmp_logger = logger_utils.make_logger(base_logger, method_name='_get_connection_dict')
    conn_dict = dict()
    # resolve all distinct hosts behind hostname
    resolved_host_port_set = set()
    for host_port in host_port_list:
        host, port = host_port.split(':')
        port = int(port)
        addrinfos = socket.getaddrinfo(host, port)
        for addrinfo in addrinfos:
            resolved_host = socket.getfqdn(addrinfo[4][0])
            resolved_host_port_set.add((resolved_host, port))
    # make connections
    for host, port in resolved_host_port_set:
        host_port = '{0}:{1}'.format(host, port)
        conn_id = host_port
        if conn_id not in conn_dict:
            try:
                conn = stomp.Connection12(host_and_ports=[(host, port)], vhost=vhost)
                if use_ssl:
                    ssl_opts = {
                                'ssl_version' : ssl.PROTOCOL_TLSv1,
                                'cert_file' : cert_file,
                                'key_file'  : key_file
                                }
                    conn.set_ssl(for_hosts=[(host, port)], **ssl_opts)
            except AttributeError:
                # Older version of stomp.py
                ssl_opts = {
                            'use_ssl' : use_ssl,
                            'ssl_version' : ssl.PROTOCOL_TLSv1,
                            'ssl_cert_file' : cert_file,
                            'ssl_key_file'  : key_file
                            }
                conn = stomp.Connection12(host_and_ports=[(host, port)], vhost=vhost, **ssl_opts)
            conn_dict[conn_id] = conn
    tmp_logger.debug('got {0} connections to {1}'.format(len(conn_dict), ' , '.join(conn_dict.keys())))
    return conn_dict


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

    __slots__ = ('__mb_proxy', 'conn_id', 'sub_id', 'msg_id', 'ack_id', 'data', 'is_transacted', 'txs_id')

    def __init__(self, mb_proxy, conn_id, msg_id, ack_id, data, is_transacted=True):
        # associated proxy object
        self.__mb_proxy = mb_proxy
        # connection ID
        self.conn_id = conn_id
        # subscription ID
        self.sub_id = self.__mb_proxy.sub_id
        # message ID
        self.msg_id = msg_id
        # acknowledgement ID
        self.ack_id = ack_id
        # real message data
        self.data = data
        # whether use transaction
        self.is_transacted = is_transacted

    def __enter__(self):
        # self.__mb_proxy.logger.debug('msg_id={m} MsgObj.__enter__ called'.format(m=self.msg_id))
        if self.is_transacted:
            # transcation ID
            self.txs_id = self.__mb_proxy._begin(self.conn_id)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # self.__mb_proxy.logger.debug('msg_id={m} MsgObj.__exit__ called'.format(m=self.msg_id))
        if self.is_transacted:
            if exc_type or exc_value:
                # exception occurs, send abort
                self.__mb_proxy._abort(self.conn_id, self.txs_id)
            else:
                # done, send ack and commit
                self.__mb_proxy._ack(self.conn_id, self.msg_id, self.ack_id)
                self.__mb_proxy._commit(self.conn_id, self.txs_id)
        else:
            if exc_type or exc_value:
                # exception occurs, send nack
                self.__mb_proxy._nack(self.conn_id, self.msg_id, self.ack_id)
            else:
                # done, send ack
                self.__mb_proxy._ack(self.conn_id, self.msg_id, self.ack_id)


# message listener
class MsgListener(stomp.ConnectionListener):
    """
    Message listener of STOMP
    """

    def __init__(self, mb_proxy, conn_id, *args, **kwargs):
        # logger
        _token = '{0}-{1}'.format(mb_proxy.__class__.__name__, mb_proxy.name)
        self.logger = logger_utils.make_logger(base_logger, token=_token, method_name='MsgListener')
        # associated messgage broker proxy
        self.mb_proxy = mb_proxy
        # connection id
        self.conn_id = conn_id
        # whether log verbosely
        self.verbose = kwargs.get('verbose', False)

    def _parse_args(self, args):
        """
        Parse the args for different versions of stomp.py
        return (cmd, headers, body)
        """
        if len(args) == 1:
            # [frame] : in newer version
            frame = args[0]
            return (frame.cmd, frame.headers, frame.body)
        elif len(args) == 2:
            # [headers, message] : in older version
            headers, message = args
            return (None, headers, message)

    def on_error(self, *args):
        cmd, headers, body = self._parse_args(args)
        self.logger.error('on_error : {h} | {b}'.format(h=headers, b=body))

    def on_disconnected(self):
        self.logger.info('on_disconnected start')
        self.mb_proxy._on_disconnected(conn_id=self.conn_id)
        self.logger.info('on_disconnected done')

    def on_send(self, *args):
        cmd, headers, body = self._parse_args(args)
        obscured_headers = headers
        if 'passcode' in headers:
            obscured_headers = copy.deepcopy(headers)
            obscured_headers['passcode'] = '********'
        if self.verbose:
            self.logger.debug('on_send frame: {0} {1} | {2}'.format(cmd, obscured_headers, body))

    def on_message(self, *args):
        cmd, headers, body = self._parse_args(args)
        if self.verbose:
            self.logger.debug('on_message start: {h} | {b}'.format(h=headers, b=body))
        self.mb_proxy._on_message(headers, body, conn_id=self.conn_id)
        if self.verbose:
            self.logger.debug('on_message done: {h}'.format(h=headers))


# message broker proxy for receiver
class MBListenerProxy(object):

    def __init__(self, name, host_port_list, destination, use_ssl=False, cert_file=None, key_file=None, vhost=None,
                    username=None, passcode=None, wait=True, ack_mode='client-individual', skip_buffer=False, conn_mode='all',
                    prefetch_size=None, verbose=False, **kwargs):
        # logger
        self.logger = logger_utils.make_logger(base_logger, token=name, method_name='MBListenerProxy')
        # name of message queue
        self.name = name
        # connection parameters
        self.host_port_list = host_port_list
        self.use_ssl = use_ssl
        self.cert_file = cert_file
        self.key_file = key_file
        self.vhost = vhost
        # destination queue to subscribe
        self.destination = destination
        # subscription ID
        self.sub_id = 'panda-MBListenerProxy_{0}_r{1:06}'.format(socket.getfqdn(), random.randrange(10**6))
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))
        # connect parameters
        self.connect_params = {'username': username, 'passcode': passcode, 'wait': wait,
                                'headers': {'client-id': self.client_id}}
        # acknowledge mode
        self.ack_mode = ack_mode
        # associate message buffer
        self.msg_buffer = MsgBuffer(queue_name=self.name)
        # connection mode; "all" or "any"
        self.conn_mode = conn_mode
        # connection dict
        self.connection_dict = {}
        # message listener dict
        self.listener_dict = {}
        # whether to skip buffer and dump to self.dump_msgs; True only in testing
        self.skip_buffer = skip_buffer
        # dump messages
        self.dump_msgs = []
        # number of attempts to restart
        self.n_restart = 0
        # whether got disconnected from on_disconnected
        self.got_disconnected = False
        # whether to disconnect intentionally
        self.to_disconnect = False
        # whether to log verbosely
        self.verbose = verbose
        # prefetch count of the MB (max number of un-acknowledge messages allowed)
        self.prefetch_size = prefetch_size
        # evaluate subscription headers
        self._evaluate_subscription_headers()
        # get connections
        self._get_connections()

    def _get_connections(self):
        """
        get connections and generate listener objects
        """
        self.connection_dict = _get_connection_dict(self.host_port_list, self.use_ssl, self.cert_file, self.key_file, self.vhost)
        self.logger.debug('start, conn_mode={0}'.format(self.conn_mode))
        if self.conn_mode == 'all':
            # for receiver, subscribe all hosts behind the same hostname
            for conn_id, conn in self.connection_dict.items():
                listener = MsgListener(mb_proxy=self, conn_id=conn_id, verbose=self.verbose)
                self.listener_dict[conn_id] = listener
                self.logger.debug('got connection about {0}'.format(conn_id))
        elif self.conn_mode == 'any':
            # for receiver, subscribe any single host behind the same hostname
            conn_id, conn = random.choice([self.connection_dict.items()])
            listener = MsgListener(mb_proxy=self, conn_id=conn_id, verbose=self.verbose)
            self.listener_dict[conn_id] = listener
            self.logger.debug('got connection about {0}'.format(conn_id))
        self.logger.debug('done')

    def _evaluate_subscription_headers(self):
        self.subscription_headers = {}
        if self.prefetch_size is not None:
            self.subscription_headers.update({
                    'activemq.prefetchSize': self.prefetch_size, # for ActiveMQ
                    'prefetch-count': self.prefetch_size, # for RabbitMQ
                })

    def _begin(self, conn_id):
        conn = self.connection_dict[conn_id]
        txs_id = conn.begin()
        if self.verbose:
            self.logger.debug('{conid} txid={txid} BEGIN'.format(conid=conn_id, txid=txs_id))
        return txs_id

    def _commit(self, conn_id, txs_id):
        conn = self.connection_dict[conn_id]
        conn.commit(txs_id)
        if self.verbose:
            self.logger.debug('{conid} txid={txid} COMMIT'.format(conid=conn_id, txid=txs_id))

    def _abort(self, conn_id, txs_id):
        conn = self.connection_dict[conn_id]
        conn.abort(txs_id)
        self.logger.warning('{conid} txid={txid} ABORT'.format(conid=conn_id, txid=txs_id))

    def _ack(self, conn_id, msg_id, ack_id):
        if self.ack_mode in ['client', 'client-individual']:
            conn = self.connection_dict[conn_id]
            conn.ack(ack_id)
            if self.verbose:
                self.logger.debug('{conid} {mid} {ackid} ACK'.format(conid=conn_id, mid=msg_id, ackid=ack_id))

    def _nack(self, conn_id, msg_id, ack_id):
        if self.ack_mode in ['client', 'client-individual']:
            conn = self.connection_dict[conn_id]
            conn.nack(ack_id)
            self.logger.warning('{conid} {mid} {ackid} NACK'.format(conid=conn_id, mid=msg_id, ackid=ack_id))

    def _on_message(self, headers, body, conn_id):
        msg_obj = MsgObj(mb_proxy=self, conn_id=conn_id, msg_id=headers['message-id'], ack_id=headers['ack'], data=body)
        if self.verbose:
            self.logger.debug('_on_message from {c} made message object: {h}'.format(c=conn_id, h=headers))
        if self.skip_buffer:
            if self.verbose:
                self.logger.debug('_on_message (buffer_skipped) dump the message: {h}'.format(h=headers))
            self.dump_msgs.append(msg_obj.data)
            self._ack(msg_obj.conn_id, msg_obj.msg_id, msg_obj.ack_id)
        else:
            self.msg_buffer.put(msg_obj)
            if self.verbose:
                self.logger.debug('_on_message put into buffer: {h}'.format(h=headers))

    def _on_disconnected(self, conn_id):
        self.logger.debug('_on_disconnected from {c} called'.format(c=conn_id))
        self.got_disconnected = True

    def go(self):
        self.logger.debug('go called')
        self.to_disconnect = False
        for conn_id, conn in self.connection_dict.items():
            try:
                if not conn.is_connected():
                    listener = self.listener_dict[conn_id]
                    self.got_disconnected = False
                    conn.set_listener(listener.__class__.__name__, listener)
                    conn.connect(**self.connect_params)
                    conn.subscribe(destination=self.destination, id=self.sub_id, ack='client-individual',
                                    headers=self.subscription_headers)
                    self.logger.info('connected to {0} {1}'.format(conn_id, self.destination))
                else:
                    self.logger.info('connection to {0} {1} already exists. Skipped...'.format(
                                                                        conn_id, self.destination))
            except Exception as e:
                tb_str = traceback.format_exc()
                self.logger.error('failed to start connection to {0} {1} ; {2} \n{3}'.format(
                                    conn_id, self.destination, e.__class__.__name__, tb_str))
                self.got_disconnected = True
                break

    def stop(self):
        self.logger.debug('stop called')
        self.to_disconnect = True
        for conn_id, conn in self.connection_dict.items():
            conn.disconnect()
            self.logger.info('disconnect from {0} {1}'.format(conn_id, self.destination))
        self.logger.info('done')

    def restart(self):
        self.logger.debug('restart called')
        self.n_restart += 1
        self.logger.debug('the {0}th attempt to restart...'.format(self.n_restart))
        self.stop()
        self._get_connections()
        self.go()
        self.logger.info('the {0}th restart ended'.format(self.n_restart))

    def get_messages(self, limit=100):
        """
        get some messages capped by limit from local buffer
        return list of message objects
        """
        if self.verbose:
            self.logger.debug('get_messages called')
        # get messages from local buffer
        msg_list = []
        for j in range(limit):
            msg_obj = self.msg_buffer.get()
            if msg_obj is None:
                break
            msg_list.append(msg_obj)
        if self.verbose:
            self.logger.debug('got {n} messages'.format(n=len(msg_list)))
        return msg_list


# message broker proxy for sender, waster...
class MBSenderProxy(object):

    def __init__(self, name, host_port_list, destination, use_ssl=False, cert_file=None, key_file=None, vhost=None,
                    username=None, passcode=None, wait=True, verbose=False, **kwargs):
        # logger
        self.logger = logger_utils.make_logger(base_logger, token=name, method_name='MBSenderProxy')
        # name of message queue
        self.name = name
        # connection parameters
        self.host_port_list = host_port_list
        self.use_ssl = use_ssl
        self.cert_file = cert_file
        self.key_file = key_file
        self.vhost = vhost
        # destination queue to subscribe
        self.destination = destination
        # subscription ID
        self.sub_id = 'panda-MBSenderProxy_{0}_r{1:06}'.format(socket.getfqdn(), random.randrange(10**6))
        # client ID
        self.client_id = 'client_{0}_{1}'.format(self.sub_id, hex(id(self)))
        # connect parameters
        self.connect_params = {'username': username, 'passcode': passcode, 'wait': wait,
                                'headers': {'client-id': self.client_id}}
        # number of attempts to restart
        self.n_restart = 0
        # whether got disconnected from on_disconnected
        self.got_disconnected = False
        # whether to disconnect intentionally
        self.to_disconnect = False
        # whether to log verbosely
        self.verbose = verbose
        # get connection
        self._get_connection()

    def _get_connection(self):
        """
        get a connection and a listener
        """
        conn_dict = _get_connection_dict(self.host_port_list, self.use_ssl, self.cert_file, self.key_file, self.vhost)
        self.conn_id, self.conn = random.choice(list(conn_dict.items()))
        self.listener = MsgListener(mb_proxy=self, conn_id=self.conn_id, verbose=self.verbose)
        self.logger.debug('got connection about {0}'.format(self.conn_id))

    def _on_message(self, headers, body, conn_id):
        if self.verbose:
            self.logger.debug('_on_message from {c} drop message: {h} | {b}'.format(c=conn_id, h=headers, b=body))

    def _on_disconnected(self, conn_id):
        self.logger.debug('_on_disconnected from {c} called'.format(c=conn_id))
        self.got_disconnected = True

    def send(self, data):
        """
        send a message to queue
        """
        self.conn.send(destination=self.destination, body=data)
        if self.verbose:
            self.logger.debug('send to {dest} | {data}'.format(dest=self.destination, data=data))

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
        self.to_disconnect = False
        try:
            if not self.conn.is_connected():
                self.got_disconnected = False
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                self.conn.connect(**self.connect_params)
                self.logger.info('connected to {0} {1}'.format(self.conn_id, self.destination))
            else:
                self.logger.info('connection to {0} {1} already exists. Skipped...'.format(
                                                                        self.conn_id, self.destination))
        except Exception as e:
            tb_str = traceback.format_exc()
            self.logger.error('failed to start connection to {0} {1} ; {2} \n{3}'.format(
                                self.conn_id, self.destination, e.__class__.__name__, tb_str))
            self.got_disconnected = True

    def stop(self):
        self.logger.debug('stop called')
        self.to_disconnect = True
        self.conn.disconnect()
        self.logger.info('disconnect from {0} {1}'.format(self.conn_id, self.destination))

    def restart(self):
        self.logger.debug('restart called')
        self.n_restart += 1
        self.logger.debug('the {0}th attempt to restart...'.format(self.n_restart))
        self.stop()
        self._get_connection()
        self.go()
        self.logger.info('the {0}th restart done'.format(self.n_restart))

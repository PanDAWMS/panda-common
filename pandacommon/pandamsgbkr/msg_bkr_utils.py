import collections
import copy
import datetime
import logging
import os
import random
import re
import socket
import ssl
import threading
import time
import traceback
import uuid

import stomp

from pandacommon.pandalogger import logger_utils

# logger
base_logger = logger_utils.setup_logger("msg_bkr_utils")

# adjust stomp logger
stomp_log_level = "INFO"
panda_stomp_logger = logger_utils.setup_logger("stomp.py")
stomp_logger = logging.getLogger("stomp")
for handler in stomp_logger.handlers.copy():
    handler.close()
    stomp_logger.removeHandler(handler)
for handler in panda_stomp_logger.handlers.copy():
    handler.setLevel(stomp_log_level)
    stomp_logger.addHandler(handler)
stomp_logger.setLevel(stomp_log_level)
stomp_logger.propagate = False

# global lock
_GLOBAL_LOCK = threading.Lock()

# global map of message buffers
_BUFFER_MAP = {}


# get connection dict
def _get_connection_dict(
    host_port_list: list[str],
    use_ssl: bool = False,
    cert_file: str | None = None,
    key_file: str | None = None,
    vhost: str | None = None,
    keepalive: bool = True,
    send_heartbeat_ms: int = 60000,
    recv_heartbeat_ms: int = 0,
) -> dict[str, stomp.Connection12]:
    """
    Get dictionary mapping connection IDs to STOMP connections.

    Args:
        host_port_list: List of host:port strings to connect to.
        use_ssl: Whether to use SSL/TLS for connections.
        cert_file: Path to SSL certificate file.
        key_file: Path to SSL key file.
        vhost: Virtual host for STOMP connection.
        keepalive: Whether to enable TCP keepalive.
        send_heartbeat_ms: Client heartbeat interval in milliseconds.
        recv_heartbeat_ms: Server heartbeat interval in milliseconds.

    Returns:
        Dictionary mapping connection IDs (host:port strings) to stomp.Connection12 objects.
    """
    tmp_logger = logger_utils.make_logger(base_logger, method_name="_get_connection_dict")
    conn_dict = dict()
    # resolve all distinct hosts behind hostname
    resolved_host_port_set = set()
    for host_port in host_port_list:
        host, port_str = host_port.split(":")
        port = int(port_str)
        addrinfos = socket.getaddrinfo(host, port)
        for addrinfo in addrinfos:
            resolved_host = socket.getfqdn(addrinfo[4][0])
            resolved_host_port_set.add((resolved_host, port))
    # make connections
    for host, port in resolved_host_port_set:
        host_port = f"{host}:{port}"
        conn_id = host_port
        if conn_id not in conn_dict:
            try:
                conn = stomp.Connection12(host_and_ports=[(host, port)], vhost=vhost, keepalive=keepalive, heartbeats=(send_heartbeat_ms, recv_heartbeat_ms))
                if use_ssl:
                    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = ssl.CERT_NONE
                    if cert_file and key_file:
                        ssl_ctx.load_cert_chain(cert_file, key_file)
                    try:
                        conn.set_ssl(for_hosts=[(host, port)], ssl_context=ssl_ctx)
                    except TypeError:
                        # Older stomp.py without ssl_context parameter
                        conn.set_ssl(for_hosts=[(host, port)], cert_file=cert_file, key_file=key_file)
            except AttributeError:
                # Older version of stomp.py without set_ssl method
                ssl_opts = {"use_ssl": use_ssl, "ssl_cert_file": cert_file, "ssl_key_file": key_file} if use_ssl else {}
                conn = stomp.Connection12(
                    host_and_ports=[(host, port)], vhost=vhost, keepalive=keepalive, heartbeats=(send_heartbeat_ms, recv_heartbeat_ms), **ssl_opts
                )
            conn_dict[conn_id] = conn
    tmp_logger.debug(f"got {len(conn_dict)} connections to {', '.join(conn_dict.keys())}")
    return conn_dict


# get fqdn pid
def get_fqdn_pid() -> str:
    """
    Get string containing fully qualified domain name and process ID.

    Returns:
        String in format 'fqdn_pid'.
    """
    fqdn = socket.getfqdn()
    os_pid = os.getpid()
    return f"{fqdn}_{os_pid}"


# message buffer
class MsgBuffer:
    """
    Global message buffer. Singleton for each queue name
    """

    # installed by _initialize, which __new__ runs once per queue name
    queue_name: str
    __fifo: "collections.deque[MsgObj]"

    @staticmethod
    def _initialize(self: "MsgBuffer", queue_name: str):
        """
        Initialize MsgBuffer singleton instance.

        Args:
            self: Message buffer instance.
            queue_name: Name of the message queue.
        """
        # name of the message queue
        self.queue_name = queue_name
        # interval fifo
        self.__fifo = collections.deque()

    def __new__(cls, queue_name: str) -> "MsgBuffer":
        """
        Create or retrieve singleton MsgBuffer instance.

        Args:
            queue_name: Name of the message queue.

        Returns:
            MsgBuffer instance for the given queue name.
        """
        key = queue_name
        with _GLOBAL_LOCK:
            if key not in _BUFFER_MAP:
                inst = object.__new__(cls)
                _BUFFER_MAP[key] = inst
                cls._initialize(inst, queue_name)
            return _BUFFER_MAP[key]

    def __init__(self, *args, **kwargs):
        # Do NOT write anything here because of singleton
        pass

    def size(self) -> int:
        """
        Get current number of messages in buffer.

        Returns:
            Number of messages in the FIFO queue.
        """
        return len(self.__fifo)

    def get(self) -> "MsgObj | None":
        """
        Get message from buffer (FIFO).

        Returns:
            MsgObj instance if available, None if buffer is empty.
        """
        try:
            ret = self.__fifo.popleft()
        except IndexError:
            ret = None
        return ret

    def put(self, obj: "MsgObj"):
        """
        Put message into buffer (FIFO).

        Args:
            obj: MsgObj instance to add to the buffer.
        """
        self.__fifo.append(obj)


# message object
class MsgObj(object):
    """
    Message object, stored in local buffer and consumed by consumer threads
    Support with-statement
    """

    __slots__ = ("__mb_proxy", "conn_id", "sub_id", "msg_id", "ack_id", "data", "is_transacted", "txs_id")

    def __init__(self, mb_proxy: "MBListenerProxy", conn_id: str, msg_id: str, ack_id: str | None, data: str, is_transacted: bool = True):
        """
        Initialize MsgObj instance.

        Args:
            mb_proxy: Associated message broker proxy object.
            conn_id: Connection ID.
            msg_id: Message ID.
            ack_id: Acknowledgement ID.
            data: Message data.
            is_transacted: Whether to use transaction for this message.
        """
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

    def __enter__(self) -> "MsgObj":
        """
        Enter context manager.

        Returns:
            Self.
        """
        # self.__mb_proxy.logger.debug('msg_id={m} MsgObj.__enter__ called'.format(m=self.msg_id))
        if self.is_transacted:
            # transaction ID
            self.txs_id = self.__mb_proxy._begin(self.conn_id)
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: object):
        """
        Exit context manager.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_value: Exception instance if an exception occurred.
            traceback: Traceback object if an exception occurred.
        """
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

    def __init__(self, mb_proxy: "MBProxyBase", conn_id: str, *args, **kwargs):
        """
        Initialize MsgListener instance.

        Args:
            mb_proxy: Associated message broker proxy object.
            conn_id: Connection ID.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        # logger
        _token = f"{mb_proxy.__class__.__name__}-{mb_proxy.name}"
        self.logger = logger_utils.make_logger(base_logger, token=_token, method_name="MsgListener")
        # associated message broker proxy
        self.mb_proxy = mb_proxy
        # connection id
        self.conn_id = conn_id
        # whether log verbosely
        self.verbose = kwargs.get("verbose", False)

    def _parse_args(self, args: tuple) -> tuple[str | None, dict, str]:
        """
        Parse arguments for different versions of stomp.py.

        Args:
            args: Arguments tuple from stomp callback.

        Returns:
            Tuple of (cmd, headers, body).
        """
        if len(args) == 1:
            # [frame] : in newer version
            frame = args[0]
            return frame.cmd, frame.headers, frame.body

        if len(args) == 2:
            # [headers, message] : in older version
            headers, message = args
            return None, headers, message

        raise ValueError(f"cannot parse {len(args)} arguments from the stomp callback")

    def on_error(self, *args):
        """
        Handle error message from message broker.

        Args:
            *args: Variable arguments from stomp callback.
        """
        self.logger.debug("on_error start")
        cmd, headers, body = self._parse_args(args)
        self.logger.error(f"on_error from {self.conn_id}: {headers} | {body}")
        self.mb_proxy._on_error(headers)
        self.logger.debug("on_error done")

    def on_connected(self, *args):
        """
        Handle connection established message from message broker.

        Args:
            *args: Variable arguments from stomp callback.
        """
        self.logger.debug("on_connected start")
        cmd, headers, body = self._parse_args(args)
        self.logger.debug(f"on_connected from {self.conn_id}: {headers} | {body}")
        self.mb_proxy._on_connected(headers=headers)
        self.logger.debug("on_connected done")

    def on_disconnected(self):
        """
        Handle disconnection message from message broker.
        """
        self.logger.debug("on_disconnected start")
        self.mb_proxy._on_disconnected(conn_id=self.conn_id)
        self.logger.debug("on_disconnected done")

    def on_send(self, *args):
        """
        Handle send frame message from message broker.

        Args:
            *args: Variable arguments from stomp callback.
        """
        cmd, headers, body = self._parse_args(args)
        obscured_headers = headers
        if "passcode" in headers:
            obscured_headers = copy.deepcopy(headers)
            obscured_headers["passcode"] = "********"
        if self.verbose:
            self.logger.debug(f"on_send frame: {cmd} {obscured_headers} | {body}")

    def on_message(self, *args):
        """
        Handle incoming message from message broker.

        Args:
            *args: Variable arguments from stomp callback.
        """
        cmd, headers, body = self._parse_args(args)
        if self.verbose:
            self.logger.debug(f"on_message start: {headers} | {body}")
        self.mb_proxy._on_message(headers, body, conn_id=self.conn_id)
        if self.verbose:
            self.logger.debug(f"on_message done: {headers}")


# message broker proxy base
class MBProxyBase:
    """
    Base MBProxy class
    """

    # Defined by every subclass and called from here: MsgListener.on_message reaches
    # _on_message through this class, and _on_error restarts through restart()

    def _on_message(self, headers: dict, body: str, conn_id: str) -> None:
        raise NotImplementedError

    def restart(self) -> None:
        raise NotImplementedError

    def __init__(
        self,
        name: str,
        host_port_list: list[str],
        destination: str,
        use_ssl: bool = False,
        cert_file: str | None = None,
        key_file: str | None = None,
        vhost: str | None = None,
        username: str | None = None,
        passcode: str | None = None,
        wait: bool = True,
        verbose: bool = False,
        keepalive: bool = True,
        send_heartbeat_ms: int = 60000,
        recv_heartbeat_ms: int = 0,
        proxy_class_name: str = "MBProxyBase",
    ):
        """
        Initialize MBProxyBase with common connection parameters.

        Args:
            name: Name of message queue.
            host_port_list: List of host:port pairs for message broker.
            destination: Message destination/queue name.
            use_ssl: Whether to use SSL for connections.
            cert_file: Path to SSL certificate file.
            key_file: Path to SSL key file.
            vhost: Virtual host for message broker.
            username: Username for authentication.
            passcode: Password for authentication.
            wait: Wait for connection.
            verbose: Enable verbose logging.
            keepalive: Enable keepalive for connections.
            send_heartbeat_ms: Send heartbeat interval in milliseconds.
            recv_heartbeat_ms: Receive heartbeat interval in milliseconds.
            proxy_class_name: Class name of the proxy (for subscription ID generation).
        """
        # logger
        self.logger = logger_utils.make_logger(base_logger, token=name, method_name=proxy_class_name)
        # name of message queue
        self.name = name
        # connection parameters
        self.host_port_list = host_port_list
        self.use_ssl = use_ssl
        self.cert_file = cert_file
        self.key_file = key_file
        self.vhost = vhost
        # original destination
        self.orig_destination = destination
        # lock for destination change
        self.dest_lock = threading.Lock()
        # destination to subscribe
        self.destination = self.orig_destination
        # destination used in retry
        self.new_destination = self.orig_destination
        # whether to freeze destination change (used when RabbitMQ not_found error occurs)
        self._to_freeze_dest = False
        # randomness
        fqdn_pid = get_fqdn_pid()
        tmp_timestamp_str = str(time.time())
        random.seed(f"{fqdn_pid}:{tmp_timestamp_str}")
        n_rand = random.randrange(10**6)
        # subscription ID
        self.sub_id = f"panda-{proxy_class_name}_{fqdn_pid}_r{n_rand:06}"
        # client ID
        self.client_id = f"client_{self.sub_id}_{hex(id(self))}"
        # connect parameters
        self.connect_params = {"username": username, "passcode": passcode, "wait": wait, "headers": {"client-id": self.client_id}}
        # number of attempts to restart
        self.n_restart = 0
        # whether got connected from on_connected (thread-safe event)
        self._got_connected_event = threading.Event()
        # whether got disconnected from on_disconnected (thread-safe event)
        self._got_disconnected_event = threading.Event()
        # whether to disconnect intentionally
        self.to_disconnect = False
        # whether to log verbosely
        self.verbose = verbose
        # whether to enable keepalive
        self.keepalive = keepalive
        # sending and wanting-to-receive heartbeat period in microseconds
        self.send_heartbeat_ms = send_heartbeat_ms
        self.recv_heartbeat_ms = recv_heartbeat_ms

    def is_connected_to_rabbitmq(self) -> bool:
        """
        Check if connected to RabbitMQ message broker.

        Returns:
            True if connected to RabbitMQ, False otherwise.
        """
        # mq_server is only set once a CONNECTED frame names the server
        mq_server: str | None = getattr(self, "mq_server", None)
        return mq_server is not None and mq_server.startswith("RabbitMQ/")

    @property
    def got_connected(self) -> bool:
        """
        Check if connected to message broker.

        Returns:
            True if connected, False otherwise.
        """
        return self._got_connected_event.is_set()

    @got_connected.setter
    def got_connected(self, value: bool):
        """
        Set connection status.

        Args:
            value: Connection status to set.
        """
        if value:
            self._got_connected_event.set()
        else:
            self._got_connected_event.clear()

    @property
    def got_disconnected(self) -> bool:
        """
        Check if disconnected from message broker.

        Returns:
            True if disconnected, False otherwise.
        """
        return self._got_disconnected_event.is_set()

    @got_disconnected.setter
    def got_disconnected(self, value: bool):
        """
        Set disconnection status.

        Args:
            value: Disconnection status to set.
        """
        if value:
            self._got_disconnected_event.set()
        else:
            self._got_disconnected_event.clear()

    def _on_connected(self, headers: dict):
        """
        Internal handler for connection established event.

        Args:
            headers: Connection headers from message broker.
        """
        # fill mq_server
        self.mq_server = headers.get("server")
        # rabbitmq
        if self.is_connected_to_rabbitmq():
            if not getattr(self, "_to_freeze_dest", False) and self.destination.startswith("/queue"):
                # destination change for rabbitmq queue /queue vs /amq/queue
                self.destination = re.sub(r"^/queue/", "/amq/queue/", self.orig_destination)
                self.new_destination = self.destination
                self.logger.debug(f"_on_connected : connected RabbitMQ; modified destination into {self.destination}")
        # done
        self.got_connected = True

    def _on_disconnected(self, conn_id: str):
        """
        Internal handler for disconnection event.

        Args:
            conn_id: Connection ID that was disconnected.
        """
        self.logger.debug(f"_on_disconnected from {conn_id} called")
        self.got_disconnected = True

    def _on_error(self, headers: dict):
        """
        Internal handler for error event.

        Args:
            headers: Error headers from message broker.
        """
        # reset new_destination and restart if getting rabbitmq not_found for queue
        if self.is_connected_to_rabbitmq() and headers.get("message") == "not_found":
            if self.destination.startswith("/amq/queue"):
                # new_destination change for rabbitmq queue /queue vs /amq/queue
                self.new_destination = re.sub(r"^/amq/queue/", "/queue/", self.orig_destination)
                self._to_freeze_dest = True
            self.logger.debug(f"_on_error : got not_found from RabbitMQ; modified new destination into {self.new_destination} ; restarting")
            self.restart()
            self.logger.debug(f"_on_error : restarted")


# message broker proxy for receiver
class MBListenerProxy(MBProxyBase):
    def __init__(
        self,
        name,
        host_port_list,
        destination,
        use_ssl=False,
        cert_file=None,
        key_file=None,
        vhost=None,
        username=None,
        passcode=None,
        wait=True,
        ack_mode="client-individual",
        skip_buffer=False,
        conn_mode="all",
        prefetch_size=None,
        max_buffer_len=999,
        buffer_block_sec=10,
        use_transaction=True,
        verbose=False,
        keepalive=True,
        send_heartbeat_ms=60000,
        recv_heartbeat_ms=0,
        **kwargs,
    ):
        # initialize base class
        super().__init__(
            name=name,
            host_port_list=host_port_list,
            destination=destination,
            use_ssl=use_ssl,
            cert_file=cert_file,
            key_file=key_file,
            vhost=vhost,
            username=username,
            passcode=passcode,
            wait=wait,
            verbose=verbose,
            keepalive=keepalive,
            send_heartbeat_ms=send_heartbeat_ms,
            recv_heartbeat_ms=recv_heartbeat_ms,
            proxy_class_name="MBListenerProxy",
        )
        # acknowledge mode
        self.ack_mode = ack_mode
        # associate message buffer
        self.msg_buffer = MsgBuffer(queue_name=self.name)
        # max length before blocking put to buffer
        self.max_buffer_len = max_buffer_len
        # put retry period in seconds to wait for blocking
        self.buffer_block_sec = buffer_block_sec
        # whether to enable transaction of message broker to wrap the message processing
        self.use_transaction = use_transaction
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
        # prefetch count of the MB (max number of un-acknowledge messages allowed)
        self.prefetch_size = prefetch_size
        # evaluate subscription headers
        self._evaluate_subscription_headers()
        # get connections
        self._get_connections()

    def _get_connections(self):
        """
        Get connections and generate listener objects.
        """
        self.connection_dict = _get_connection_dict(
            self.host_port_list,
            self.use_ssl,
            self.cert_file,
            self.key_file,
            self.vhost,
            keepalive=self.keepalive,
            send_heartbeat_ms=self.send_heartbeat_ms,
            recv_heartbeat_ms=self.recv_heartbeat_ms,
        )
        self.logger.debug(f"start, conn_mode={self.conn_mode}")
        if self.conn_mode == "all":
            # for receiver, subscribe all hosts behind the same hostname
            for conn_id, conn in self.connection_dict.items():
                listener = MsgListener(mb_proxy=self, conn_id=conn_id, verbose=self.verbose)
                self.listener_dict[conn_id] = listener
                self.logger.debug(f"got connection about {conn_id}")
        elif self.conn_mode == "any":
            # for receiver, subscribe any single host behind the same hostname
            conn_id, conn = random.choice(list(self.connection_dict.items()))
            listener = MsgListener(mb_proxy=self, conn_id=conn_id, verbose=self.verbose)
            self.listener_dict[conn_id] = listener
            self.logger.debug(f"got connection about {conn_id}")
        self.logger.debug("done")

    def _evaluate_subscription_headers(self):
        """
        Evaluate and set subscription headers based on configuration.
        """
        self.subscription_headers = {}
        if self.prefetch_size is not None:
            self.subscription_headers.update(
                {
                    "activemq.prefetchSize": self.prefetch_size,  # for ActiveMQ
                    "prefetch-count": self.prefetch_size,  # for RabbitMQ
                }
            )

    def _begin(self, conn_id: str) -> str:
        """
        Begin a transaction on the connection.

        Args:
            conn_id: Connection ID.

        Returns:
            Transaction ID.
        """
        conn = self.connection_dict[conn_id]
        txs_id = conn.begin()
        if self.verbose:
            self.logger.debug(f"{conn_id} txid={txs_id} BEGIN")
        return txs_id

    def _commit(self, conn_id: str, txs_id: str):
        """
        Commit a transaction.

        Args:
            conn_id: Connection ID.
            txs_id: Transaction ID.
        """
        conn = self.connection_dict[conn_id]
        conn.commit(txs_id)
        if self.verbose:
            self.logger.debug(f"{conn_id} txid={txs_id} COMMIT")

    def _abort(self, conn_id: str, txs_id: str):
        """
        Abort a transaction.

        Args:
            conn_id: Connection ID.
            txs_id: Transaction ID.
        """
        conn = self.connection_dict[conn_id]
        conn.abort(txs_id)
        self.logger.warning(f"{conn_id} txid={txs_id} ABORT")

    def _ack(self, conn_id: str, msg_id: str, ack_id: str | None):
        """
        Acknowledge a message.

        Args:
            conn_id: Connection ID.
            msg_id: Message ID.
            ack_id: Acknowledgement ID.
        """
        if self.ack_mode in ["client", "client-individual"]:
            conn = self.connection_dict[conn_id]
            conn.ack(ack_id)
            if self.verbose:
                self.logger.debug(f"{conn_id} {msg_id} {ack_id} ACK")

    def _nack(self, conn_id: str, msg_id: str, ack_id: str | None):
        """
        Negatively acknowledge a message.

        Args:
            conn_id: Connection ID.
            msg_id: Message ID.
            ack_id: Acknowledgement ID.
        """
        if self.ack_mode in ["client", "client-individual"]:
            conn = self.connection_dict[conn_id]
            conn.nack(ack_id)
            self.logger.warning(f"{conn_id} {msg_id} {ack_id} NACK")

    def _on_message(self, headers: dict, body: str, conn_id: str):
        """
        Internal handler for incoming message.

        Args:
            headers: Message headers from broker.
            body: Message body content.
            conn_id: Connection ID message came from.
        """
        msg_obj = MsgObj(mb_proxy=self, conn_id=conn_id, msg_id=headers["message-id"], ack_id=headers.get("ack"), data=body, is_transacted=self.use_transaction)
        if self.verbose:
            self.logger.debug(f"_on_message from {conn_id} made message object: {headers}")
        if self.skip_buffer:
            if self.verbose:
                self.logger.debug(f"_on_message (buffer_skipped) dump the message: {headers}")
            self.dump_msgs.append(msg_obj.data)
            self._ack(msg_obj.conn_id, msg_obj.msg_id, msg_obj.ack_id)
        else:
            to_block = True
            while to_block:
                n_buffered_msg = self.msg_buffer.size()
                if n_buffered_msg >= self.max_buffer_len:
                    if self.verbose:
                        self.logger.debug(f"_on_message too many buffered messages ({n_buffered_msg}); waiting...")
                    time.sleep(self.buffer_block_sec)
                else:
                    to_block = False
            self.msg_buffer.put(msg_obj)
            if self.verbose:
                n_buffered_msg = self.msg_buffer.size()
                self.logger.debug(f"_on_message put into buffer ({n_buffered_msg}): {headers}")

    def go(self):
        """
        Start listening to message queue.
        """
        self.logger.debug("go called")
        self.to_disconnect = False
        self.logger.debug(f"last destination is {self.destination}, new destination is {self.new_destination}")
        self.destination = self.new_destination
        for conn_id, conn in self.connection_dict.items():
            try:
                if not conn.is_connected():
                    listener = self.listener_dict[conn_id]
                    self.got_disconnected = False
                    conn.set_listener(listener.__class__.__name__, listener)
                    with self.dest_lock:
                        conn.connect(**self.connect_params)
                        # wait for on_connected done for a while before subscribe
                        for wait_i in range(100):
                            if self.got_connected:
                                break
                            time.sleep(0.003)
                        self.logger.debug(f"connected to {conn_id}, subscribing...")
                        conn.subscribe(destination=self.destination, id=self.sub_id, ack=self.ack_mode, headers=self.subscription_headers)
                        self.logger.info(f"connected to {conn_id} and subscribed {self.destination}")
                else:
                    self.logger.info(f"connection to {conn_id} {self.destination} already exists. Skipped...")
            except Exception as e:
                tb_str = traceback.format_exc()
                self.logger.error(f"failed to start connection to {conn_id} {self.destination} ; {e.__class__.__name__} \n{tb_str}")
                self.got_disconnected = True
                break

    def stop(self):
        """
        Stop listening to message queue.
        """
        self.logger.debug("stop called")
        self.to_disconnect = True
        for conn_id, conn in self.connection_dict.items():
            conn.disconnect()
            self.logger.info(f"disconnect from {conn_id} {self.destination}")
        self.got_connected = False
        self.logger.info("done")

    def restart(self):
        """
        Restart connection to message queue.
        """
        self.logger.debug("restart called")
        self.n_restart += 1
        self.logger.debug(f"the {self.n_restart}th attempt to restart...")
        self.stop()
        self._get_connections()
        self.go()
        self.logger.info(f"the {self.n_restart}th restart ended")

    def get_messages(self, limit: int = 100) -> list["MsgObj"]:
        """
        Get some messages from local buffer.

        Args:
            limit: Maximum number of messages to retrieve.

        Returns:
            List of MsgObj instances from the buffer.
        """
        if self.verbose:
            self.logger.debug("get_messages called")
        # get messages from local buffer
        msg_list = []
        for j in range(limit):
            msg_obj = self.msg_buffer.get()
            if msg_obj is None:
                break
            msg_list.append(msg_obj)
        if self.verbose:
            self.logger.debug(f"got {len(msg_list)} messages")
        return msg_list


# message broker proxy for sender, waster...
class MBSenderProxy(MBProxyBase):
    def __init__(
        self,
        name,
        host_port_list,
        destination,
        use_ssl=False,
        cert_file=None,
        key_file=None,
        vhost=None,
        username=None,
        passcode=None,
        wait=True,
        verbose=False,
        keepalive=True,
        send_heartbeat_ms=60000,
        recv_heartbeat_ms=0,
        **kwargs,
    ):
        # initialize base class
        super().__init__(
            name=name,
            host_port_list=host_port_list,
            destination=destination,
            use_ssl=use_ssl,
            cert_file=cert_file,
            key_file=key_file,
            vhost=vhost,
            username=username,
            passcode=passcode,
            wait=wait,
            verbose=verbose,
            keepalive=keepalive,
            send_heartbeat_ms=send_heartbeat_ms,
            recv_heartbeat_ms=recv_heartbeat_ms,
            proxy_class_name="MBSenderProxy",
        )
        # instance lock for removers
        self.remover_lock = threading.Lock()
        # removers
        self.removers = {}
        # get connection
        self._get_connection()

    def _get_connection(self):
        """
        Get a connection and a listener.
        """
        conn_dict = _get_connection_dict(
            self.host_port_list,
            self.use_ssl,
            self.cert_file,
            self.key_file,
            self.vhost,
            keepalive=self.keepalive,
            send_heartbeat_ms=self.send_heartbeat_ms,
            recv_heartbeat_ms=self.recv_heartbeat_ms,
        )
        self.conn_id, self.conn = random.choice(list(conn_dict.items()))
        self.listener = MsgListener(mb_proxy=self, conn_id=self.conn_id, verbose=self.verbose)
        self.logger.debug(f"got connection about {self.conn_id}")

    def _on_message(self, headers: dict, body: str, conn_id: str):
        """
        Internal handler for incoming message (drops messages in sender mode).

        Args:
            headers: Message headers from broker.
            body: Message body content.
            conn_id: Connection ID message came from.
        """
        if self.verbose:
            self.logger.debug(f"_on_message from {conn_id} drop message: {headers} | {body}")

    def send(self, data: str | None, headers: dict | None = None, **kwargs):
        """
        Send a message to queue.

        Args:
            data: Message data to send.
            headers: Optional headers dictionary for the message.
            **kwargs: Additional headers as keyword arguments.
        """
        if data is None:
            self.logger.debug("got None, not to send")
        else:
            headers_dict = {}
            if headers is not None:
                headers_dict.update(headers)
            headers_dict.update(kwargs)
            try:
                self.conn.send(destination=self.destination, body=data, headers=headers_dict)
            except Exception as e:
                tb_str = traceback.format_exc()
                self.logger.error(f"failed to send message to {self.destination} ; data={data} headers={headers_dict} ; {e.__class__.__name__} \n{tb_str}")
            else:
                if self.verbose:
                    self.logger.debug(f"send to {self.destination} | {data}")

    def waste(self, duration: int = 3):
        """
        Drop all messages received during a duration.

        Args:
            duration: Duration in seconds to wait for messages to drop.
        """
        self.conn.subscribe(destination=self.destination, id=self.sub_id, ack="auto")
        time.sleep(duration)
        self.conn.unsubscribe(id=self.sub_id)
        self.logger.debug(f"waste dropped messages for {duration} sec")

    def go(self):
        """
        Start sending messages to queue.
        """
        self.logger.debug("go called")
        self.to_disconnect = False
        self.logger.debug(f"last destination is {self.destination}, new destination is {self.new_destination}")
        self.destination = self.new_destination
        try:
            if not self.conn.is_connected():
                self.got_disconnected = False
                self.conn.set_listener(self.listener.__class__.__name__, self.listener)
                with self.dest_lock:
                    self.conn.connect(**self.connect_params)
                    # wait for on_connected done for a while before subscribe
                    for wait_i in range(100):
                        if self.got_connected:
                            break
                        time.sleep(0.003)
                    self.logger.debug(f"connected to {self.conn_id}")
                    # add removers
                    with self.remover_lock:
                        for r_id in self.removers:
                            headers = self.removers[r_id]["headers"]
                            self.conn.subscribe(destination=self.destination, headers=headers, id=r_id, ack="auto")
                    self.logger.info(f"connected to {self.conn_id} and ready to send to {self.destination}")
            else:
                self.logger.info(f"connection to {self.conn_id} {self.destination} already exists. Skipped...")
        except Exception as e:
            tb_str = traceback.format_exc()
            self.logger.error(f"failed to start connection to {self.conn_id} {self.destination} ; {e.__class__.__name__} \n{tb_str}")
            self.got_disconnected = True

    def stop(self):
        """
        Stop sending messages to queue.
        """
        self.logger.debug("stop called")
        self.to_disconnect = True
        self.conn.disconnect()
        self.got_connected = False
        self.logger.info(f"disconnect from {self.conn_id} {self.destination}")

    def restart(self):
        """
        Restart connection to message queue.
        """
        self.logger.debug("restart called")
        self.n_restart += 1
        self.logger.debug(f"the {self.n_restart}th attempt to restart...")
        self.stop()
        self._get_connection()
        self.go()
        self.logger.info(f"the {self.n_restart}th restart done")

    def add_remover(self, headers: dict, timeout: int):
        """
        Add a message remover to delete matching messages.

        Args:
            headers: Dictionary to specify the selector for message removal.
            timeout: Lifetime of the subscription in seconds.
        """
        self.logger.debug(f"adding remover with headers={headers}")
        # unique id for each remover
        r_id = self.sub_id + "." + str(uuid.uuid4())
        with self.remover_lock:
            self.removers[r_id] = {
                "timeout": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) + datetime.timedelta(seconds=timeout),
                "headers": copy.copy(headers),
            }
        # reconnect if necessary
        if self.got_disconnected:
            self.restart()
        # subscribe to remove the messages
        self.conn.subscribe(destination=self.destination, headers=headers, id=r_id, ack="auto")
        self.logger.debug(f"added remover id={r_id}")

    def purge_removers(self):
        """
        Purge old message removers that have expired.
        """
        self.logger.debug("purging old removers")
        with self.remover_lock:
            time_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            n_old = len(self.removers)
            for r_id in list(self.removers):
                timeout = self.removers[r_id]["timeout"]
                # unsubscribe if old
                if timeout < time_now:
                    self.conn.unsubscribe(id=r_id)
                    del self.removers[r_id]
                    self.logger.debug(f"purged remover id={r_id}")
            n_new = len(self.removers)
            self.logger.debug(f"purged {n_old - n_new} removers in total among {n_old} removers")

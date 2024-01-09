import json
import logging
import logging.handlers
import os
import threading
import time

from . import logger_config

try:
    import http.client as httplib
except ImportError:
    import httplib
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode


# encodings
JSON = "json"
URL = "url"

# set TZ for timestamp
os.environ["TZ"] = "UTC"

# log rotation
rotateLog = False

# logger map
loggerMap = {}
loggerMapLock = threading.Lock()


# wrapper to avoid duplication of loggers with the same name
def getLoggerWrapper(logger_name, checkNew=False):
    loggerMapLock.acquire()
    global loggerMap
    new_flag = False
    if logger_name not in loggerMap:
        loggerMap[logger_name] = logging.getLogger(logger_name)
        new_flag = True
    loggerMapLock.release()
    if checkNew:
        return loggerMap[logger_name], new_flag

    return loggerMap[logger_name]


# a thread to send a record to a web server
class _Emitter(threading.Thread):
    # constructor
    def __init__(self, host, port, url, method, data, semaphore):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.url = url
        self.method = method
        self.data = data
        self.semaphore = semaphore

    def getData(self, src, chunk_size=1024):
        """
        Use this function for debug purposes in order to print
        out the response from the server
        """
        data = src.read(chunk_size)
        while data:
            yield data
            data = src.read(chunk_size)

    # main
    def run(self):
        # send the record to the Web server as an URL-encoded dictionary
        try:
            connection = httplib.HTTPConnection(self.host, self.port, timeout=1)
            url = self.url
            if self.method == "GET":
                if url.find("?") >= 0:
                    sep = "&"
                else:
                    sep = "?"
                url = url + "%c%s" % (sep, self.data)
            connection.putrequest(self.method, url)
            if self.method == "POST":
                connection.putheader("Content-length", str(len(self.data)))
                connection.putheader("Content-type", "application/json; charset=UTF-8")
            connection.endheaders()
            if self.method == "POST":
                connection.send(self.data)
            connection.getresponse()

        except Exception:
            pass
        self.semaphore.release()


class _PandaHTTPLogHandler(logging.Handler):
    """
    Customized HTTP handler for Python logging module.
    A class which sends records to a Web server, using either GET or
    POST semantics.
    """

    def __init__(self, host, url, port=80, urlprefix="", method="POST", encoding=URL):
        """
        Initialize the instance with the host, the request URL, and the method
        ("GET" or "POST")
        """

        logging.Handler.__init__(self)
        method = method.upper()
        if method not in ["GET", "POST"]:
            raise ValueError("method must be GET or POST")
        self.host = host
        self.url = url
        self.port = port
        self.urlprefix = urlprefix
        self.method = method
        self.encoding = encoding
        # create lock for params, cannot use createLock()
        self.mylock = threading.Lock()
        # semaphore to limit the number of concurrent emitters
        if "nemitters" in logger_config.daemon:
            self.my_semaphore = threading.Semaphore(int(logger_config.daemon["nemitters"]))
        else:
            self.my_semaphore = threading.Semaphore(10)
        # parameters
        self.params = {"PandaID": -1, "User": "unknown", "Type": "unknown", "ID": "tester"}

    def mapLogRecord(self, record):
        """
        Default implementation of mapping the log record into a dict
        that is sent as the CGI data. Overwrite in your class.
        Contributed by Franz  Glasner.
        """
        newrec = record.__dict__
        for p in self.params:
            newrec[p] = self.params[p]
        maxParamLength = 4000
        # truncate and clean the message from non-UTF-8 characters
        try:
            newrec["msg"] = newrec["msg"][:maxParamLength].decode("utf-8", "ignore").encode("utf-8")
        except Exception:
            pass
        try:
            newrec["message"] = newrec["message"][:maxParamLength].decode("utf-8", "ignore").encode("utf-8")
        except Exception:
            pass
        return newrec

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as an URL-encoded dictionary
        """
        # encode data
        # Panda logger is going to be migrated. Until this is completed we need to support the old and new logger
        # The new logger needs to be json encoded and use POST method
        try:
            if self.encoding == JSON:
                arr = [
                    {
                        "headers": {"timestamp": int(time.time()) * 1000, "host": "%s:%s" % (self.url, self.port)},
                        "body": "{0}".format(json.dumps(self.mapLogRecord(record))),
                    }
                ]
                data = json.dumps(arr)
            else:
                data = urlencode(self.mapLogRecord(record))

            # try to lock Semaphore
            if self.my_semaphore.acquire(False):
                # start Emitter
                _Emitter(self.host, self.port, self.urlprefix, self.method, data, self.my_semaphore).start()
        except UnicodeDecodeError:
            # We lose the message
            pass

    def setParams(self, params):
        for pname in params.keys():
            self.params[pname] = params[pname]

    # acquire lock
    def lockHandler(self):
        self.mylock.acquire()

    # release lock
    def releaseHandler(self):
        try:
            self.mylock.release()
        except Exception:
            pass


# log level
logLevel = logging.DEBUG
if "log_level" in logger_config.daemon:
    if logger_config.daemon["log_level"] in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]:
        logLevel = getattr(logging, logger_config.daemon["log_level"])
# setup logger
_rootlog = getLoggerWrapper("")
_rootlog.setLevel(logLevel)
_pandalog = getLoggerWrapper("panda")
_pandalog.setLevel(logLevel)
_txtlog = getLoggerWrapper("panda.log")
_weblog = getLoggerWrapper("panda.mon")
_newweblog = getLoggerWrapper("panda.mon_new")
_formatter = logging.Formatter("%(asctime)s %(name)-12s: %(levelname)-8s %(message)s")

if len(_weblog.handlers) < 2:
    _allwebh = _PandaHTTPLogHandler(
        logger_config.daemon["loghost"],
        "http://%s" % logger_config.daemon["loghost"],
        logger_config.daemon["monport-apache"],
        logger_config.daemon["monurlprefix"],
        logger_config.daemon["method"],
        logger_config.daemon["encoding"],
    )
    _allwebh.setLevel(logging.DEBUG)
    _allwebh.setFormatter(_formatter)

    if "loghost_new" in logger_config.daemon:
        _newwebh = _PandaHTTPLogHandler(
            logger_config.daemon["loghost_new"],
            "http://%s" % logger_config.daemon["loghost_new"],
            logger_config.daemon["monport-apache_new"],
            logger_config.daemon["monurlprefix"],
            logger_config.daemon["method_new"],
            logger_config.daemon["encoding_new"],
        )
        _newwebh.setLevel(logging.DEBUG)
        _newwebh.setFormatter(_formatter)

    tmp_file_path = os.path.join(logger_config.daemon["logdir"], "panda.log")
    _txth = logging.FileHandler(tmp_file_path, encoding="utf-8")
    _txth.setLevel(logging.DEBUG)
    _txth.setFormatter(_formatter)

    _weblog.addHandler(_txth)  # if http log doesn't have a text handler it doesn't work
    _weblog.addHandler(_allwebh)
    if "loghost_new" in logger_config.daemon:
        _weblog.addHandler(_newwebh)
    try:
        # panda.log is owned by root if daemon is launched first
        os.chmod(tmp_file_path, 0o666)
    except Exception:
        pass

# no more HTTP handler
del _PandaHTTPLogHandler


class PandaLogger:
    """
    Logger and monitoring data collector for Panda.
    Custom fields added to the logging:

    user     Who is running the app
    PandaID  Panda job ID (if applicable)
    ID       General usage ID (eg. pilot ID, scheduler ID). A string.
    type     Message type
    """

    def __init__(self, pid=0, user="", id="", type=""):
        self.params = {"PandaID": pid, "ID": id, "User": user, "Type": type}

    def getLogger(self, log_name, log_level=None):
        log_h, new_log_flag = getLoggerWrapper("panda.log.%s" % log_name, True)
        log_h.propagate = False
        tmp_attr = "rotating_policy"

        if tmp_attr in logger_config.daemon and logger_config.daemon[tmp_attr] == "time":
            # interval
            tmp_attr = "rotating_interval"
            if tmp_attr in logger_config.daemon:
                rotating_interval = int(logger_config.daemon[tmp_attr])
            else:
                rotating_interval = 24
            # backup count
            tmp_attr = "rotating_backup_count"
            if tmp_attr in logger_config.daemon:
                backup_count = int(logger_config.daemon[tmp_attr])
            else:
                backup_count = 1
            # handler with timed rotating
            txt_handler = logging.handlers.TimedRotatingFileHandler(
                "%s/panda-%s.log" % (logger_config.daemon["logdir"], log_name), when="h", interval=rotating_interval, backupCount=backup_count, utc=True
            )
            if new_log_flag and rotateLog:
                txt_handler.doRollover()

        elif tmp_attr in logger_config.daemon and logger_config.daemon[tmp_attr] == "size":
            # max bytes
            tmp_attr = "rotating_max_size"
            if tmp_attr in logger_config.daemon:
                max_size = int(logger_config.daemon[tmp_attr])
            else:
                max_size = 1024
            max_size *= 1024 * 1024
            # backup count
            tmp_attr = "rotating_backup_count"
            if tmp_attr in logger_config.daemon:
                backup_count = int(logger_config.daemon[tmp_attr])
            else:
                backup_count = 1
            # handler with rotating based on size
            txt_handler = logging.handlers.RotatingFileHandler(
                "%s/panda-%s.log" % (logger_config.daemon["logdir"], log_name), maxBytes=max_size, backupCount=backup_count
            )
            if new_log_flag and rotateLog:
                txt_handler.doRollover()
        else:
            txt_handler = logging.FileHandler("%s/panda-%s.log" % (logger_config.daemon["logdir"], log_name), encoding="utf-8")
        if log_level in ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]:
            log_level = getattr(logging, log_level)
            if new_log_flag:
                log_h.setLevel(log_level)
        else:
            log_level = logging.DEBUG
        txt_handler.setLevel(log_level)
        txt_handler.setFormatter(_formatter)
        log_h.addHandler(txt_handler)
        return log_h

    def getHttpLogger(self, log_name):
        httph = getLoggerWrapper("panda.mon.%s" % log_name)
        return httph

    def setParams(self, params):
        for pname in params.keys():
            self.params[pname] = params[pname]
        _allwebh.setParams(self.params)
        if "loghost_new" in logger_config.daemon:
            _newwebh.setParams(self.params)

    def getParam(self, pname):
        return self.params[pname]

    # acquire lock for HTTP handler
    def lock(self):
        _allwebh.lockHandler()
        if "loghost_new" in logger_config.daemon:
            _newwebh.lockHandler()

    # release lock
    def release(self):
        _allwebh.releaseHandler()
        if "loghost_new" in logger_config.daemon:
            _newwebh.releaseHandler()

    # rollover
    @staticmethod
    def doRollOver():
        global rotateLog
        rotateLog = True

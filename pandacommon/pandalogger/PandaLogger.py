import logging, logging.handlers, string
import logger_config
import threading
import httplib
import urllib

# set TZ for timestamp
import os
os.environ['TZ'] = 'UTC'

# a thread to send a record to a web server
class _Emitter (threading.Thread):
    # constructor
    def __init__(self,host,port,url,method,data):
        threading.Thread.__init__(self)
        self.host   = host
        self.port   = port
        self.url    = url
        self.method = method
        self.data   = data

    # main
    def run(self):
        # send the record to the Web server as an URL-encoded dictionary
        try:
            h = httplib.HTTPConnection(self.host, self.port)
            url = self.url
            if self.method == "GET":
                if (string.find(url, '?') >= 0):
                    sep = '&'
                else:
                    sep = '?'
                url = url + "%c%s" % (sep, self.data)
            h.putrequest(self.method, url)
            if self.method == "POST":
                h.putheader("Content-length", str(len(self.data)))
            h.endheaders()
            if self.method == "POST":
                h.send(self.data)
            h.getresponse()    # can't do anything with the result
        except:
            pass

class _PandaHTTPLogHandler(logging.Handler):
    """
    Customized HTTP handler for Python logging module.
    A class which sends records to a Web server, using either GET or
    POST semantics.
    """

    def __init__(self, host, url, port=80, urlprefix='', method="GET"):
        """
        Initialize the instance with the host, the request URL, and the method
        ("GET" or "POST")
        """

        logging.Handler.__init__(self)
        method = string.upper(method)
        if method not in ["GET", "POST"]:
            raise ValueError, "method must be GET or POST"
        self.host = host
        self.url = url
        self.port = port
        self.urlprefix = urlprefix
        self.method = method
        # create lock for params, cannot use createLock()
        self.mylock = threading.Lock()
        # parameters
        self.params = {}
        self.params['PandaID'] = -1
        self.params['User'] = 'unknown'
        self.params['Type'] = 'unknown'
        self.params['ID'] = 'tester'

    def mapLogRecord(self, record):
        """
        Default implementation of mapping the log record into a dict
        that is sent as the CGI data. Overwrite in your class.
        Contributed by Franz  Glasner.
        """

        newrec = record.__dict__
        for p in self.params:
            newrec[p] = self.params[p]
        return newrec

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as an URL-encoded dictionary
        """
        # encode data
        data = urllib.urlencode(self.mapLogRecord(record))
        url = "%s:%s%s" % ( self.url, self.port, self.urlprefix )
        # start Emitter
        _Emitter(self.host,self.port,self.urlprefix,self.method,data).start()

    def setParams(self, params):
        for pname in params.keys():
            self.params[pname] =params[pname]

    # acquire lock
    def lockHandler(self):
        self.mylock.acquire()

    # release lock
    def releaseHandler(self):
        self.mylock.release()


# setup logger
_pandalog = logging.getLogger('panda')
_pandalog.setLevel(logging.DEBUG)
_txtlog = logging.getLogger('panda.log')
_weblog = logging.getLogger('panda.mon')
_allwebh = _PandaHTTPLogHandler(logger_config.daemon['loghost'],'http://%s'%logger_config.daemon['loghost'],
                                logger_config.daemon['monport-apache'],logger_config.daemon['monurlprefix'],'GET')
_allwebh.setLevel(logging.DEBUG)
_txth = logging.FileHandler('%s/panda.log'%logger_config.daemon['logdir'])
_txth.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s %(name)-12s: %(levelname)-8s %(message)s')
_txth.setFormatter(_formatter)
_allwebh.setFormatter(_formatter)
#_txtlog.addHandler(_txth)
_weblog.addHandler(_txth)   # if http log doesn't have a text handler it doesn't work
_weblog.addHandler(_allwebh)

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
    
    def __init__(self, pid=0, user='', id='', type=''):
        self.params = {}
        self.params['PandaID'] = pid
        self.params['ID'] = id
        self.params['User'] = user
        self.params['Type'] = type

    def getLogger(self, lognm):
        logh = logging.getLogger("panda.log.%s"%lognm)
        txth = logging.FileHandler('%s/panda-%s.log'%(logger_config.daemon['logdir'],lognm))
        txth.setLevel(logging.DEBUG)
        txth.setFormatter(_formatter)
        logh.addHandler(txth)
        return logh

    def getHttpLogger(self, lognm):
        httph = logging.getLogger('panda.mon.%s'%lognm)
        return httph

    def setParams(self, params):
        for pname in params.keys():
            self.params[pname] = params[pname]
        _allwebh.setParams(self.params)

    def getParam(self, pname):
        return self.params[pname]

    # acquire lock for HTTP handler
    def lock(self):
        _allwebh.lockHandler()

    # release lock
    def release(self):
        _allwebh.releaseHandler()
        

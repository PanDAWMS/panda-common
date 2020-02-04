import os
import threading
import socket
import datetime


class GenericThread(threading.Thread):

    def __init__(self, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()

    def get_pid(self):
        """
        get host/process/thread identifier
        """
        thread_id = self.ident if self.ident else 0
        return '{0}_{1}-{2}'.format(self.hostname, self.os_pid, format(thread_id, 'x'))


# map with lock
class MapWithLockAndTimeout(dict):

    def __init__(self, *args, **kwargs):
        # set timeout
        if 'timeout' in kwargs:
            self.timeout = kwargs['timeout']
            del kwargs['timeout']
        else:
            self.timeout = 10
        self.lock = threading.Lock()
        dict.__init__(self, *args, **kwargs)

    # get item regardless of freshness to avoid race-condition in check->get
    def __getitem__(self, item):
        with self.lock:
            ret = dict.__getitem__(self, item)
            return ret['data']

    def __setitem__(self, item, value):
        with self.lock:
            dict.__setitem__(self, item, {'time_stamp': datetime.datetime.utcnow(),
                                          'data': value})

    # check data by taking freshness into account
    def __contains__(self, item):
        with self.lock:
            try:
                ret = dict.__getitem__(self, item)
                if ret['time_stamp'] > datetime.datetime.utcnow() - datetime.timedelta(minutes=self.timeout):
                    return True
            except Exception:
                pass
        return False

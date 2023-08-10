import os
import threading
import socket
import datetime
import random
import multiprocessing


class GenericThread(threading.Thread):

    def __init__(self, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()

    def get_thread_id(self, current=False):
        """
        get thread identifier
        """
        if current:
            thread_id = threading.get_ident()
            if not thread_id:
                thread_id = 0
        else:
            thread_id = self.ident if self.ident else 0

        return thread_id

    def get_pid(self, current=False):
        """
        get host/process/thread identifier
        """
        thread_id = self.get_thread_id(current)
        return '{0}_{1}-{2}'.format(self.hostname, self.os_pid, format(thread_id, 'x'))

    def get_full_id(self, file_name, module_name):
        """
        combines the host/process/thread identifier with the module information
        """

        host_id = self.hostname
        process_id = self.os_pid
        thread_id = self.get_thread_id()

        file_basename = os.path.basename(file_name)  # remove the path for better readability
        full_id = "host={0} filename={1} module={2} process={3} thread={4}".format(host_id, file_basename, module_name,
                                                                                   process_id, thread_id)

        return full_id


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


# weighted lists
class WeightedLists(object):

    def __init__(self, lock):
        self.lock = multiprocessing.Lock()
        self.data = multiprocessing.Queue()
        self.data.put(dict())
        self.weights = multiprocessing.Queue()
        self.weights.put(dict())

    def __len__(self):
        with self.lock:
            l = 0
            data = self.data.get()
            for item in data:
                l += len(data[item])
            self.data.put(data)
            return l

    def add(self, weight, list_data):
        if not list_data or weight <= 0:
            return
        with self.lock:
            data = self.data.get()
            weights = self.weights.get()
            item = len(weights)
            weights[item] = weight
            data[item] = list_data
            self.weights.put(weights)
            self.data.put(data)

    def pop(self):
        with self.lock:
            weights = self.weights.get()
            if not weights:
                self.weights.put(weights)
                return None
            item = random.choices(list(weights.keys()), weights=list(weights.values()))[0]
            data = self.data.get()
            d = data[item].pop()
            # delete empty
            if not data[item]:
                del data[item]
                del weights[item]
            self.weights.put(weights)
            self.data.put(data)
            return d

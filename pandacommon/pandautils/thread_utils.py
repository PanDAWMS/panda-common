import datetime
import multiprocessing
import os
import random
import socket
import threading
from typing import Any, TypedDict


class GenericThread(threading.Thread):
    def __init__(self, **kwargs: Any) -> None:
        threading.Thread.__init__(self, **kwargs)
        self.hostname = socket.gethostname()
        self.os_pid = os.getpid()

    def get_thread_id(self, current: bool = False) -> int:
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

    def get_pid(self, current: bool = False) -> str:
        """
        get host/process/thread identifier
        """
        thread_id = self.get_thread_id(current)
        return "{0}_{1}-{2}".format(self.hostname, self.os_pid, format(thread_id, "x"))

    def get_full_id(self, module_name: str, file_name: str) -> str:
        """
        combines the host/process/thread identifier with the module information
        """

        host_id = self.hostname
        process_id = self.os_pid
        thread_id = self.get_thread_id()

        file_basename = os.path.basename(file_name)  # remove the path for better readability
        full_id = "host={0} filename={1} module={2} process={3} thread={4}".format(host_id, file_basename, module_name, process_id, thread_id)

        return full_id


class _TimedEntry(TypedDict):
    """What MapWithLockAndTimeout stores per key.

    The value the caller set, plus when it was set: the timestamp is what makes the
    freshness check in __contains__ possible, and it is why the stored shape is not the
    value itself.
    """

    time_stamp: datetime.datetime
    data: Any


# map with lock
# The key type is left open because nothing about the class constrains it; the value type
# is the informative half, and it is the wrapper above rather than what the caller sets.
class MapWithLockAndTimeout(dict[Any, _TimedEntry]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # set timeout
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        else:
            self.timeout = 10
        self.lock = threading.Lock()
        dict.__init__(self, *args, **kwargs)

    # get item regardless of freshness to avoid race-condition in check->get
    def __getitem__(self, item: Any) -> Any:
        with self.lock:
            ret = dict.__getitem__(self, item)
            return ret["data"]

    def __setitem__(self, item: Any, value: Any) -> None:
        with self.lock:
            # named so the literal has a declared type to be checked against; passed
            # straight to dict.__setitem__ it would have none
            entry: _TimedEntry = {"time_stamp": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None), "data": value}
            dict.__setitem__(self, item, entry)

    # check data by taking freshness into account
    def __contains__(self, item: object) -> bool:
        with self.lock:
            try:
                ret = dict.__getitem__(self, item)
                if ret["time_stamp"] > datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=self.timeout):
                    return True
            except Exception:
                pass
        return False


# weighted lists
class WeightedLists(object):
    # lock is accepted and ignored: this class makes its own. Kept in the signature
    # because callers pass one, e.g. add_main.py in panda-server
    def __init__(self, lock: Any) -> None:
        self.lock = multiprocessing.Lock()
        self.data: "multiprocessing.Queue[dict[int, list[Any]]]" = multiprocessing.Queue()
        self.data.put(dict())
        self.weights: "multiprocessing.Queue[dict[int, float]]" = multiprocessing.Queue()
        self.weights.put(dict())

    def __len__(self) -> int:
        with self.lock:
            len_data = 0
            data = self.data.get()
            for item in data:
                len_data += len(data[item])
            self.data.put(data)
            return len_data

    def add(self, weight: float, list_data: list[Any]) -> None:
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

    def pop(self) -> Any:
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


# lock pool
class LockPool(object):
    def __init__(self, pool_size: int = 100) -> None:
        self.pool_size = pool_size
        self.lock = multiprocessing.Lock()
        self.manager = multiprocessing.Manager()
        self.key_to_lock = self.manager.dict()
        self.lock_ref_count = self.manager.dict()
        self.lock_pool = {i: multiprocessing.Lock() for i in range(pool_size)}

    def get(self, key: Any) -> Any:
        with self.lock:
            if key not in self.key_to_lock:
                in_used = set(self.key_to_lock.values())
                free_locks = set(range(self.pool_size)).difference(in_used)
                if not free_locks:
                    return None
                index = free_locks.pop()
                self.key_to_lock[key] = index
                self.lock_ref_count[index] = 1
            else:
                index = self.key_to_lock[key]
                self.lock_ref_count[index] += 1
            return self.lock_pool[index]

    def release(self, key: Any) -> None:
        with self.lock:
            if key not in self.key_to_lock:
                return
            index = self.key_to_lock[key]
            count = self.lock_ref_count[index]
            count -= 1
            if count <= 0:
                count = 0
                del self.key_to_lock[key]
            self.lock_ref_count[index] = count

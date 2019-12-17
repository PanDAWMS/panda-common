import os
import threading
import socket


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

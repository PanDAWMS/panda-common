import datetime
import resource

from .PandaLogger import PandaLogger


# wrapper to set prefix to logging messages
class LogWrapper:
    def __init__(self, log, prefix="", lineLimit=100, monToken=None, seeMem=False, hook=None):
        # use timestamp as prefix
        if prefix is None:
            self.prefix = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat("/")
        else:
            self.prefix = prefix
        # logger instance
        self.logger = log
        # message buffer
        self.msg_buffer = []
        self.line_limit = lineLimit
        # token for monitor
        if monToken is not None:
            self.mon_token = monToken
        else:
            self.mon_token = self.prefix
        self.see_mem = seeMem
        self.hook = hook
        try:
            self.name = self.logger.name.split(".")[-1]
        except Exception:
            self.name = ""

    # get memory usage
    def getMemoryUsage(self):
        return " (mem usage {0} MB)".format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024)

    def keepMsg(self, msg):
        # keep max message depth
        if len(self.msg_buffer) > self.line_limit:
            self.msg_buffer.pop(0)
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        self.msg_buffer.append("{0} : {1}".format(timeNow.isoformat(" "), msg))

    def debug(self, msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, "DEBUG", self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != "":
            msg = self.prefix + " " + str(msg)
        if self.see_mem:
            msg += self.getMemoryUsage()
        self.logger.debug(msg)

    def info(self, msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, "INFO", self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != "":
            msg = self.prefix + " " + str(msg)
        if self.see_mem:
            msg += self.getMemoryUsage()
        self.logger.info(msg)

    def error(self, msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, "ERROR", self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != "":
            msg = self.prefix + " " + str(msg)
        if self.see_mem:
            msg += self.getMemoryUsage()
        self.logger.error(msg)

    def warning(self, msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, "WARNING", self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != "":
            msg = self.prefix + " " + str(msg)
        if self.see_mem:
            msg += self.getMemoryUsage()
        self.logger.warning(msg)

    def critical(self, msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, "CRITICAL", self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != "":
            msg = self.prefix + " " + str(msg)
        if self.see_mem:
            msg += self.getMemoryUsage()
        self.logger.critical(msg)

    def dumpToString(self):
        str_msg = ""
        for msg in self.msg_buffer:
            str_msg += msg
            str_msg += "\n"
        return str_msg

    # send message to logger
    def sendMsg(self, message, logger_name, msg_type, msgLevel="info"):
        try:
            # get logger
            tmp_panda_logger = PandaLogger()
            # lock HTTP handler
            tmp_panda_logger.lock()
            tmp_panda_logger.setParams({"Type": msg_type})
            # get logger
            tmp_logger = tmp_panda_logger.getHttpLogger(logger_name)
            # add message
            message = self.mon_token + " " + message
            if msgLevel == "error":
                tmp_logger.error(message)
            elif msgLevel == "warning":
                tmp_logger.warning(message)
            elif msgLevel == "info":
                tmp_logger.info(message)
            else:
                tmp_logger.debug(message)
        except Exception:
            pass
        finally:
            # release HTTP handler
            tmp_panda_logger.release()

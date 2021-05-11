import resource
import datetime
from .PandaLogger import PandaLogger


# wrapper to set prefix to logging messages
class LogWrapper:
    def __init__(self,log,prefix='',lineLimit=100,monToken=None,seeMem=False,hook=None):
        # use timestamp as prefix
        if prefix is None:
            self.prefix = datetime.datetime.utcnow().isoformat('/')
        else:
            self.prefix = prefix
        # logger instance
        self.logger = log
        # message buffer
        self.msgBuffer = []
        self.lineLimit = lineLimit
        # token for monitor
        if monToken is not None:
            self.monToken = monToken
        else:
            self.monToken = self.prefix
        self.seeMem = seeMem
        self.hook = hook
        try:
            self.name = self.logger.name.split('.')[-1]
        except Exception:
            self.name = ''


    # get memory usage
    def getMemoryUsage(self):
        return ' (mem usage {0} MB)'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024)


    def keepMsg(self,msg):
        # keep max message depth
        if len(self.msgBuffer) > self.lineLimit:
            self.msgBuffer.pop(0)
        timeNow = datetime.datetime.utcnow()
        self.msgBuffer.append('{0} : {1}'.format(timeNow.isoformat(' '),msg))


    def debug(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, 'DEBUG', self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.debug(msg)

    def info(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, 'INFO', self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.info(msg)

    def error(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, 'ERROR', self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.error(msg)

    def warning(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, 'WARNING', self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.warning(msg)

    def critical(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        try:
            if self.hook is not None:
                self.hook.add_dialog_message(msg, 'CRITICAL', self.name, self.prefix)
        except Exception:
            pass
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.critical(msg)

    def dumpToString(self):
        strMsg = ''
        for msg in self.msgBuffer:
            strMsg += msg
            strMsg += "\n"
        return strMsg


    # send message to logger
    def sendMsg(self,message,loggerName,msgType,msgLevel='info'):
        try:
            # get logger
            tmpPandaLogger = PandaLogger()
            # lock HTTP handler
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({'Type':msgType})
            # get logger
            tmpLogger = tmpPandaLogger.getHttpLogger(loggerName)
            # add message
            message = self.monToken + ' ' + message
            if msgLevel=='error':
                tmpLogger.error(message)
            elif msgLevel=='warning':
                tmpLogger.warning(message)
            elif msgLevel=='info':
                tmpLogger.info(message)
            else:
                tmpLogger.debug(message)
        except Exception:
            pass
        finally:
            # release HTTP handler
            tmpPandaLogger.release()

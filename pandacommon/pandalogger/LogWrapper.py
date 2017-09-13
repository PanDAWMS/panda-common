import resource
import datetime
from .PandaLogger import PandaLogger


# wrapper to set prefix to logging messages
class LogWrapper:
    def __init__(self,log,prefix='',lineLimit=100,monToken=None,seeMem=False):
        # use timestamp as prefix 
        if prefix == None:
            self.prefix = datetime.datetime.utcnow().isoformat('/')
        else:
            self.prefix = prefix
        # logger instance
        self.logger = log
        # message buffer
        self.msgBuffer = []
        self.lineLimit = lineLimit
        # token for monitor
        if monToken != None:
            self.monToken = monToken
        else:
            self.monToken = self.prefix
        self.seeMem = seeMem

    
    # get memory usage
    def getMemoryUsage(self):
        return ' (mem usage {0} MB)'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


    def keepMsg(self,msg):
        # keep max message depth
        if len(self.msgBuffer) > self.lineLimit:
            self.msgBuffer.pop(0)
        timeNow = datetime.datetime.utcnow()
        self.msgBuffer.append('{0} : {1}'.format(timeNow.isoformat(' '),msg))


    def debug(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.debug(msg)

    def info(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()        
        self.logger.info(msg)

    def error(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.error(msg)

    def warning(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        if self.seeMem:
            msg += self.getMemoryUsage()
        self.logger.warning(msg)


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
        except:
            pass
        finally:
            # release HTTP handler
            tmpPandaLogger.release()

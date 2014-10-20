import datetime

# wrapper to set prefix to logging messages
class LogWrapper:
    def __init__(self,log,prefix='',lineLimit=100):
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
        self.logger.debug(msg)

    def info(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.info(msg)

    def error(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.error(msg)

    def warning(self,msg):
        msg = str(msg)
        self.keepMsg(msg)
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.warning(msg)


    def dumpToString(self):
        strMsg = ''
        for msg in self.msgBuffer:
            strMsg += msg
            strMsg += "\n"
        return strMsg

            

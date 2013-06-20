import datetime

# wrapper to set prefix to logging messages
class LogWrapper:
    def __init__(self,log,prefix=''):
        # use timestamp as prefix 
        if prefix == None:
            self.prefix = datetime.datetime.utcnow().isoformat('/')
        else:
            self.prefix = prefix
        # logger instance
        self.logger = log

    def debug(self,msg):
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.debug(msg)

    def info(self,msg):
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.info(msg)

    def error(self,msg):
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.error(msg)

    def warning(self,msg):
        if self.prefix != '':
            msg = self.prefix + ' ' + str(msg)
        self.logger.warning(msg)

            

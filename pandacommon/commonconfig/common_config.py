from ..liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_common.cfg')


# get section
def get(section):
    return getattr(tmpConf, section)

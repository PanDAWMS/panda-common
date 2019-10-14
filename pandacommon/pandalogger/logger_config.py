import sys
from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_common.cfg')

# get logger section
daemon = tmpConf.logger


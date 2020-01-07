import sys
from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser


# expand config parameters to module attributes
def expandConfig(configFileName,sectionName,moduleName):
    try:
        # get ConfigParser
        tmpConf = LiveConfigParser()
        # read config file
        tmpConf.read(configFileName)
        # get section
        tmpDict = getattr(tmpConf,sectionName)
        tmpSelf = sys.modules[moduleName]
        for tmpKey,tmpVal in tmpDict.iteritems():
            # convert string to bool/int
            if tmpVal == 'True':
                tmpVal = True
            elif tmpVal == 'False':
                tmpVal = False
            elif re.match('^\d+$',tmpVal):
                tmpVal = int(tmpVal)
            # update dict
            tmpSelf.__dict__[tmpKey] = tmpVal
    except Exception:
        pass

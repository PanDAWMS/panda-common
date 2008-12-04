#
# Module to be/wrap a ConfigParser instance, providing simple access via standard Python member 
# and Dictionary interfaces. This is purely for convenience 
#
# [default]
#   key1=value1
#   key2=value2
#
# [section1]
#   key3=val3
#   key4=val4
#
# A LiveConfigParser object lets you access these via
# c.default['key1'] -> value1
# c.section1['key3'] -> val3
#

import os
from ConfigParser import ConfigParser, NoSectionError

class LiveConfigParser(ConfigParser):
    
    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs )
    
    # We want to retain case sensitivity
    def optionxform(self, optionstr):
        return optionstr  
    
    def __getattr__(self, a):
        try:
            i = self.items(a)
            d = {}
            for (key,val) in i:
                d[key] = val
            return d
        except NoSectionError:
            raise AttributeError("ConfigParser instance has no attribute '%s'" % a)

    # search for configs in standard places and read them
    def read(self,fileName):
        confFiles = [
            # system
            '/etc/panda/%s' % fileName,
            # home dir
            os.path.expanduser('~/etc/panda/%s' % fileName),
            ]
        # use PANDA_HOME if it is defined
        if os.environ.has_key('PANDA_HOME'):
            confFiles.append('%s/etc/panda/%s' % (os.environ['PANDA_HOME'],fileName))
        # read
        ConfigParser.read(self,confFiles)
        

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

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from configparser import ConfigParser, NoSectionError
except ImportError:
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
    def read(self, fileName, config_url=None):
        confFiles = [
            # system
            '/etc/panda/%s' % fileName,
            '/usr/etc/panda/%s' % fileName,
            # home dir
            os.path.expanduser('~/etc/panda/%s' % fileName),
            ]
        # use PANDA_HOME if it is defined
        if 'PANDA_HOME'in os.environ:
            confFiles.append('%s/etc/panda/%s' % (os.environ['PANDA_HOME'], fileName))
            confFiles.append('%s/usr/etc/panda/%s' % (os.environ['PANDA_HOME'], fileName))
        # read from URL
        if config_url is not None:
            try:
                res = urlopen(config_url)
            except Exception as e:
                raise Exception('failed to load cfg from URL={0} since {1}'.format(config_url, str(e)))
            ConfigParser.read_file(self, res)
        # read
        ConfigParser.read(self, confFiles)

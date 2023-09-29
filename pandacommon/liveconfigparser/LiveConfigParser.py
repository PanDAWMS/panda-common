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
import re

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
        ConfigParser.__init__(self, *args, **kwargs)

    # We want to retain case sensitivity
    def optionxform(self, optionstr):
        return optionstr

    def __getattr__(self, attribute):
        try:
            i = self.items(attribute)
            attr_dict = {}
            for key, val in i:
                attr_dict[key] = val
            return attr_dict
        except NoSectionError:
            raise AttributeError("ConfigParser instance has no attribute '%s'" % attribute)

    # search for configs in standard places and read them
    def read(self, file_name, config_url=None):
        config_files = [
            # system
            "/etc/panda/%s" % file_name,
            "/usr/etc/panda/%s" % file_name,
            # home dir
            os.path.expanduser("~/etc/panda/%s" % file_name),
        ]
        # use PANDA_HOME if it is defined
        if "PANDA_HOME" in os.environ:
            config_files.append("%s/etc/panda/%s" % (os.environ["PANDA_HOME"], file_name))
            config_files.append("%s/usr/etc/panda/%s" % (os.environ["PANDA_HOME"], file_name))
        # read from URL
        if config_url is not None:
            try:
                res = urlopen(config_url)
            except Exception as exc:
                raise Exception("failed to load cfg from URL={0} since {1}".format(config_url, str(exc)))
            ConfigParser.read_file(self, res)
        # read
        ConfigParser.read(self, config_files)


# expand values
def expand_values(target, values_dict):
    for tmp_key in values_dict:
        tmp_val = values_dict[tmp_key]

        # env variable like $VAR, ${VAR}, ${{VAR}}
        match_object = re.search(r"^\$\{*(\w+)\}*$", tmp_val)
        if match_object and match_object.group(1) in os.environ:
            tmp_val = os.environ[match_object.group(1)]

        # convert string to bool/int
        if tmp_val == "True":
            tmp_val = True
        elif tmp_val == "False":
            tmp_val = False
        elif tmp_val == "None":
            tmp_val = None

        # number like 1, 123, 99999
        # pylint: disable=W1401
        elif isinstance(tmp_val, str) and re.match("^\d+$", tmp_val):
            tmp_val = int(tmp_val)

        # update dict
        target.__dict__[tmp_key] = tmp_val

import re
import sys

from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser


# expand config parameters to module attributes
def expandConfig(config_file_name, section_name, module_name):
    try:
        # get ConfigParser
        tmp_conf = LiveConfigParser()

        # read config file
        tmp_conf.read(config_file_name)

        # get section
        tmp_dict = getattr(tmp_conf, section_name)
        tmp_self = sys.modules[module_name]

        for tmp_key, tmp_val in tmp_dict.iteritems():
            # convert string to bool/int
            if tmp_val == "True":
                tmp_val = True
            elif tmp_val == "False":
                tmp_val = False
            elif re.match("^\d+$", tmp_val):
                tmp_val = int(tmp_val)

            # update dict
            tmp_self.__dict__[tmp_key] = tmp_val
    except Exception:
        pass

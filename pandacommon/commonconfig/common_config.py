"""Common configuration file for all panda modules"""

from typing import cast

from ..liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read("panda_common.cfg")


# get section
def get(section: str) -> dict[str, str]:
    # LiveConfigParser.__getattr__ is annotated to return the section as dict[str, str],
    # but getattr() with a name computed at runtime is typed as returning Any, so the
    # declared type has to be restated here
    return cast(dict[str, str], getattr(tmpConf, section))

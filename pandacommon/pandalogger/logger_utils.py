import inspect
import sys
import traceback

from .LogWrapper import LogWrapper
from .PandaLogger import PandaLogger

# global memory profiling option
with_memory_profile = False


# enable memory profiling
def enable_memory_profiling():
    global with_memory_profile
    with_memory_profile = True


# setup logger
def setup_logger(name=None, log_level=None):
    if name is None:
        frm = inspect.stack()[1][0]
        mod = inspect.getmodule(frm)
        name = mod.__name__.split(".")[-1]

    if log_level:
        return PandaLogger().getLogger(name, log_level=log_level)

    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log, token=None, method_name=None, hook=None):
    # get method name of caller
    if method_name is None:
        tmp_str = inspect.stack()[1][3]
    else:
        tmp_str = method_name

    if token is not None:
        tmp_str += " <{0}>".format(token)
    else:
        tmp_str += " :"

    new_log = LogWrapper(tmp_log, tmp_str, seeMem=with_memory_profile, hook=hook)
    return new_log


# dump error message
def dump_error_message(tmp_log, err_str=None, no_message=False):
    if not isinstance(tmp_log, LogWrapper):
        method_name = "{0} : ".format(inspect.stack()[1][3])
    else:
        method_name = ""
    # error
    if err_str is None:
        err_type, err_value = sys.exc_info()[:2]
        err_str = "{0} {1} {2} ".format(method_name, err_type.__name__, err_value)
        err_str += traceback.format_exc()
    if not no_message:
        tmp_log.error(err_str)
    return err_str


# rollover for log files
def do_log_rollover():
    PandaLogger.doRollOver()

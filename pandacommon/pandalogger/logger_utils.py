import inspect

from .PandaLogger import PandaLogger
from .LogWrapper import LogWrapper


# global memory profiling option
with_memory_profile = False


# enable memory profiling
def enable_memory_profiling():
    global with_memory_profile
    with_memory_profile = True


# setup logger
def setup_logger(name=None):
    if name is None:
        frm = inspect.stack()[1][0]
        mod = inspect.getmodule(frm)
        name = mod.__name__.split('.')[-1]
    try:
        log_level = getattr(harvester_config.log_level, name)
        return PandaLogger().getLogger(name, log_level=log_level)
    except Exception:
        pass
    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log, token=None, method_name=None, hook=None):
    # get method name of caller
    if method_name is None:
        tmpStr = inspect.stack()[1][3]
    else:
        tmpStr = method_name
    if token is not None:
        tmpStr += ' <{0}>'.format(token)
    else:
        tmpStr += ' :'.format(token)
    newLog = LogWrapper(tmp_log, tmpStr, seeMem=with_memory_profile, hook=hook)
    return newLog


# dump error message
def dump_error_message(tmp_log, err_str=None, no_message=False):
    if not isinstance(tmp_log, LogWrapper):
        methodName = '{0} : '.format(inspect.stack()[1][3])
    else:
        methodName = ''
    # error
    if err_str is None:
        errtype, errvalue = sys.exc_info()[:2]
        err_str = "{0} {1} {2} ".format(methodName, errtype.__name__, errvalue)
        err_str += traceback.format_exc()
    if not no_message:
        tmp_log.error(err_str)
    return err_str


# rollover for log files
def do_log_rollover():
    PandaLogger.doRollOver()

import inspect
import logging
import sys
import traceback
from typing import Any

from .LogWrapper import LogWrapper
from .PandaLogger import PandaLogger

# global memory profiling option
with_memory_profile = False


# enable memory profiling
def enable_memory_profiling() -> None:
    global with_memory_profile
    with_memory_profile = True


# setup logger
def setup_logger(name: str | None = None, log_level: str | None = None) -> logging.Logger:
    if name is None:
        frm = inspect.stack()[1][0]
        mod = inspect.getmodule(frm)
        # getmodule does not always find one, e.g. for a frame from exec()
        name = mod.__name__.split(".")[-1] if mod is not None else "unknown"

    if log_level:
        return PandaLogger().getLogger(name, log_level=log_level)

    return PandaLogger().getLogger(name)


# make logger
def make_logger(tmp_log: logging.Logger, token: str | None = None, method_name: str | None = None, hook: Any = None) -> LogWrapper:
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
def dump_error_message(tmp_log: logging.Logger | LogWrapper, err_str: str | None = None, no_message: bool = False) -> str:
    if not isinstance(tmp_log, LogWrapper):
        method_name = "{0} : ".format(inspect.stack()[1][3])
    else:
        method_name = ""
    # error
    if err_str is None:
        err_type, err_value = sys.exc_info()[:2]
        # exc_info gives None outside an except block, which is what format_exc prints too
        err_type_name = err_type.__name__ if err_type is not None else "NoneType"
        err_str = "{0} {1} {2} ".format(method_name, err_type_name, err_value)
        err_str += traceback.format_exc()
    if not no_message:
        tmp_log.error(err_str)
    return err_str


# rollover for log files
def do_log_rollover() -> None:
    PandaLogger.doRollOver()

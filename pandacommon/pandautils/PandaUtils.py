import ctypes
import datetime
import itertools
import os
from ctypes.util import find_library
from functools import lru_cache

import pytz

from . import utils_config


# check if logrotating
def isLogRotating(before_limit, after_limit):
    # time zone
    if hasattr(utils_config, "rotate_tz"):
        tmp_tz = utils_config.rotate_tz
    else:
        tmp_tz = "Europe/Zurich"

    # hour
    if hasattr(utils_config, "rotate_h"):
        tmp_hour = utils_config.rotate_h
    else:
        tmp_hour = 4

    # minute
    if hasattr(utils_config, "rotate_m"):
        tmp_minute = utils_config.rotate_m
    else:
        tmp_minute = 0

    # current time in TZ
    time_now = datetime.datetime.now(pytz.timezone(tmp_tz))
    time_cron = time_now.replace(hour=tmp_hour, minute=tmp_minute, second=0, microsecond=0)
    if (time_now - time_cron) < datetime.timedelta(seconds=60 * after_limit) and (time_cron - time_now) < datetime.timedelta(seconds=60 * before_limit):
        return True
    return False


def aware_utcnow() -> datetime.datetime:
    """
    Return the current UTC date and time, with tzinfo timezone.utc

    Returns:
        datetime: current UTC date and time, with tzinfo timezone.utc
    """
    return datetime.datetime.now(datetime.timezone.utc)


def aware_utcfromtimestamp(timestamp: float) -> datetime.datetime:
    """
    Return the local date and time, with tzinfo timezone.utc, corresponding to the POSIX timestamp

    Args:
        timestamp (float): POSIX timestamp

    Returns:
        datetime: current UTC date and time, with tzinfo timezone.utc
    """
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def naive_utcnow() -> datetime.datetime:
    """
    Return the current UTC date and time, without tzinfo

    Returns:
        datetime: current UTC date and time, without tzinfo
    """
    return aware_utcnow().replace(tzinfo=None)


def naive_utcfromtimestamp(timestamp: float) -> datetime.datetime:
    """
    Return the local date and time, without tzinfo, corresponding to the POSIX timestamp

    Args:
        timestamp (float): POSIX timestamp

    Returns:
        datetime: current UTC date and time, without tzinfo
    """
    return aware_utcfromtimestamp(timestamp).replace(tzinfo=None)


def batched(iterable, n, *, strict=False):
    """
    Batch data from the iterable into tuples of length n. The last batch may be shorter than n
    If strict is true, will raise a ValueError if the final batch is shorter than n
    Note this function is for Python <= 3.11 as it mimics itertools.batched() in Python 3.13
    """
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError("batched(): incomplete batch")
        yield batch


def get_sql_IN_bind_variables(values, prefix: str, value_as_suffix=False) -> tuple[str, dict]:
    """
    Get the comma-separated string expression with bind variables to be used with SQL IN-condition and the corresponding variable map
    E.g. get_sql_IN_bind_variables(["done", "finished", "aborted"], prefix=":status_") will return the tuple:
         (':status_1,:status_2,:status_3', {':status_1': 'done', ':status_2': 'finished', ':status_3': 'aborted'})

    Args:
        values (iterable): list or other iterables of the values
        prefix (str): prefix of variable name
        value_as_suffix (bool): if True, use the string of value as suffix of variable name, otherwise use the number index

    Returns:
        str: comma-separated string of variable names of all bind variables, to be put inside the parentheses of SQL IN (...)
        dict: map of variable names and values, to be put as variable map of SQL execute
    """
    var_name_list = []
    ret_var_map = {}
    for j, value in enumerate(values):
        if value_as_suffix:
            var_name = f"{prefix}{str(value)}"
        else:
            var_name = f"{prefix}{j}"
        var_name_list.append(var_name)
        ret_var_map[var_name] = value
    ret_var_names_str = ",".join(var_name_list)
    return ret_var_names_str, ret_var_map


@lru_cache(maxsize=1)
def _get_malloc_trim():
    if os.name != "posix":
        return None
    libc_path = find_library("c")
    if not libc_path:
        return None
    libc = ctypes.CDLL(libc_path)
    malloc_trim = libc.malloc_trim
    malloc_trim.argtypes = [ctypes.c_size_t]
    malloc_trim.restype = ctypes.c_int
    return malloc_trim


def try_malloc_trim(logger=None) -> bool:
    """
    Best-effort release of free heap pages to the OS on supported platforms.

    Args:
        logger: optional logger instance with debug method

    Returns:
        bool: True if malloc_trim was successfully called, False otherwise
    """
    try:
        malloc_trim = _get_malloc_trim()
    except Exception as e:
        if logger is not None:
            logger.debug(f"malloc_trim unavailable: {e}")
        return False
    if malloc_trim is None:
        return False
    try:
        malloc_trim(0)
        if logger is not None:
            logger.debug("called malloc_trim to release free heap pages to OS")
        return True
    except Exception as e:
        if logger is not None:
            logger.debug(f"malloc_trim failed: {e}")
        return False

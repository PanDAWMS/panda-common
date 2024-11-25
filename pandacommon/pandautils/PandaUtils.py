import datetime
import itertools

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

def get_sql_IN_bind_variables(values, prefix: str) -> tuple[str, dict]:
    """
    Get the comma-separated string expression with bind variables to be used with SQL IN-condition and the corresponding variable map
    E.g. get_sql_IN_bind_variables(["done", "finished", "aborted"], prefix=":status_") will return the tuple:
         (':status_1,:status_2,:status_3', {':status_1': 'done', ':status_2': 'finished', ':status_3': 'aborted'})

    Args:
        values (iterable): list or other iterables of the values
        prefix (str): prefix of variable name 

    Returns:
        str: comma-separated string of variable names of all bind variables, to be put inside the parentheses of SQL IN (...)
        dict: map of variable names and values, to be put as variable map of SQL execute
    """
    var_name_list = []
    ret_var_map = {}
    for j, value in enumerate(values):
        var_name = f"{prefix}{j}"
        var_name_list.append(var_name)
        ret_var_map[var_name] = value
    ret_var_names_str = ",".join(var_name_list)
    return ret_var_names_str, ret_var_map

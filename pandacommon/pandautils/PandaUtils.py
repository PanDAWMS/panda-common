import datetime

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

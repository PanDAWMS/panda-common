import pytz
import datetime
from . import utils_config


# check if logrotating
def isLogRotating(beforeLimit,afterLimit):
    # time zone
    if hasattr(utils_config,'rotate_tz'):
        tmpTZ = utils_config.rotate_tz
    else:
        tmpTZ = 'Europe/Zurich' 
    # hour
    if hasattr(utils_config,'rotate_h'):
        tmpH = utils_config.rotate_h
    else:
        tmpH = 4
    # minute
    if hasattr(utils_config,'rotate_m'):
        tmpM = utils_config.rotate_m
    else:
        tmpM = 0
    # current time in TZ   
    timeNow  = datetime.datetime.now(pytz.timezone(tmpTZ))
    timeCron = timeNow.replace(hour=tmpH,minute=tmpM,second=0,microsecond=0)
    if (timeNow-timeCron) < datetime.timedelta(seconds=60*afterLimit) and \
            (timeCron-timeNow) < datetime.timedelta(seconds=60*beforeLimit):
        return True
    return False
    

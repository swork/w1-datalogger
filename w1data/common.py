import re, datetime
import logging
logger = logging.getLogger(__name__)

s3_re = re.compile(r'^s3:// (?P<bucket>[^/]+) /? (?P<key>.*?)$', re.X)

def location_is_s3(location):
    return s3_re.match(location)

def datetime_isoformat(dt):
    return dt.replace(tzinfo=datetime.timezone.utc).isoformat()


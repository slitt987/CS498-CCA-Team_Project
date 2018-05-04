import pytz
import datetime
import sys
from pprint import pprint

utc = pytz.UTC


def to_epoch(t):
    """
    Convert datetime to epoch seconds
    :param t: datetime
    :return: long: epoch seconds
    """
    return long(t.strftime('%s'))


def from_epoch(t):
    """
    Convert epoch seconds to datetime
    :param t: long: epoch seconds
    :return: datetime
    """
    return datetime.datetime.fromtimestamp(t)


def byteify(i):
    """
    Changes unicode to utf8 for any object (used for JSON loads)
    :param i: object
    :return: object (utf8)
    """
    if isinstance(i, dict):
        return {byteify(key): byteify(value)
                for key, value in i.iteritems()}
    elif isinstance(i, list):
        return [byteify(element) for element in i]
    elif isinstance(i, unicode):
        return i.encode('utf-8')
    else:
        return i


def eprint(s):
    """
    Print formatted debug to STDERR
    :param s: message
    :return: None
    """
    sys.stderr.write('{0}: {1}\n'.format(datetime.datetime.now().strftime('%Y-%m-%D %H:%M:%S'), s))

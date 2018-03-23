import pytz
import datetime
import sys

utc = pytz.UTC


def to_epoch(t):
    return long(t.strftime('%s'))


def from_epoch(t):
    return datetime.datetime.fromtimestamp(t)


def byteify(i):
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
    sys.stderr.write('{0}: {1}\n'.format(datetime.datetime.now().strftime('%Y-%m-%D %H:%M:%S'), s))

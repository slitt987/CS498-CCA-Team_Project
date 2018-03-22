import datetime
from ReadData import ReadData
from InstanceMap import InstanceMap
from common import *

instances = InstanceMap()
reader = ReadData(instances=instances)
start = utc.localize(datetime.datetime(2018, 03, 01, 00, 00, 00))
start_epoch = long(start.strftime('%s'))
reader.backfill_read_api(start)

from ReadData import ReadData
from InstanceMap import InstanceMap
import datetime
import common

instances = InstanceMap()
reader = ReadData(instances=instances)
start = common.utc.localize(datetime.datetime(2018, 03, 01, 00, 00, 00))
start_epoch = long(start.strftime('%s'))
reader.backfill_read_api(start)

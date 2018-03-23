from ReadData import ReadData
from InstanceMap import InstanceMap
from common import *

instances = InstanceMap()
with open('output.json', 'w') as out:
    reader = ReadData(instances=instances, period=24*60, writer=out)
    start = utc.localize(datetime.datetime(2018, 03, 01, 00, 00, 00))
    reader.backfill_read_api(start)

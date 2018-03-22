import datetime
import pytz
import sys
import json
import boto3
import re
from botocore.exceptions import ClientError
from InstanceMap import InstanceMap
from common import *


class ReadData:
    __regionSplit = re.compile('[a-z]*$')

    def __init__(self, **kwargs):
        self.__period = kwargs.get('period', 10) * 60
        end = kwargs.get('end', utc.localize(datetime.datetime.now()))
        self.__endEpoch = (long(end.strftime('%s')) / self.__period) * self.__period
        self.__instances = kwargs.get('instances')
        if self.__instances is None:
            self.__instances = InstanceMap()

        (self.start, self.end) = self.get_period()

    def get_period(self, **kwargs):
        end_epoch = kwargs.get('end_epoch', self.__endEpoch)
        start = utc.localize(datetime.datetime.fromtimestamp(end_epoch - 2 * self.__period))
        end = utc.localize(datetime.datetime.fromtimestamp(end_epoch - self.__period))
        return start, end

    def set_period(self, **kwargs):
        end_epoch = kwargs.get('end_epoch', self.__endEpoch)

        (self.start, self.end) = self.get_period(end_epoch=end_epoch)

    def backfill_read_api(self, start):
        end = self.end
        continue_flag = 1
        while self.start > start:
            self.read_api(continue_flag)
            if continue_flag == 1:
                continue_flag = 2

            self.set_period(end_epoch=long(self.start.strftime('%s')))
            self.start = max(self.start, start)

        self.read_api(3)

        self.end = end

    def read_api(self, continue_flag=0):
        if continue_flag == 0:
            print '['

        if continue_flag < 2:
            i = 0
        else:
            i = 1

        for region in self.__instances.get_regions():
            try:
                ec2 = boto3.client('ec2', region_name=region)
                history = ec2.describe_spot_price_history(
                    InstanceTypes=self.__instances.get_types(),
                    StartTime=self.start,
                    EndTime=self.end)
            except ClientError:
                sys.stderr.write('Insufficient Privileges in AWS for region {0} \n'.format(region))
                continue
    
            for row in history.get('SpotPriceHistory'):
                row = byteify(row)
                timestamp = row.get('Timestamp')
                if timestamp < self.start or timestamp >= self.end:
                    continue

                row['Timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')

                if i > 0:
                    print ','

                i = i + 1
                region = row.pop('AvailabilityZone')
                region = self.__regionSplit.sub('', region)
                row['AvailabilityZone'] = region
                instance = row.get('InstanceType')
                attributes = self.__instances.get(region, instance)
                if attributes is not None:
                    row['Attributes'] = attributes

                print json.dumps(row, indent=4, sort_keys=True)

        if continue_flag == 0 or continue_flag == 3:
            print ']'

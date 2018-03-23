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
        self.__period = kwargs.get('period', 60) * 60
        self.__instances = kwargs.get('instances')
        self.__writer = kwargs.get('writer', sys.stdout)
        self.pretty = kwargs.get('pretty', False)

        end = kwargs.get('end', utc.localize(datetime.datetime.now()))
        if end.tzinfo is None or end.tzinfo.utcoffset(end) is None:
            end = utc.localize(end)
        self.end = end
        (self.start, self.end) = self.get_period()

        # Check if this is a basic writer (file/stdout) by checking for method fileno
        self.__byte_writer = callable(getattr(self.__writer, "fileno", None))
        if self.__instances is None:
            self.__instances = InstanceMap()

    def get_period(self, **kwargs):
        end_epoch = (to_epoch(kwargs.get('end', self.end)) / self.__period) * self.__period
        start = utc.localize(from_epoch(end_epoch - self.__period))
        end = utc.localize(from_epoch(end_epoch))
        return start, end

    def set_period(self, **kwargs):
        end = kwargs.get('end', self.end)
        self.__period = kwargs.get('period', self.__period / 60) * 60

        (self.start, self.end) = self.get_period(end=end)

    def backfill_read_api(self, start):
        end = self.end
        continue_flag = 1
        while self.start > start:
            eprint('Reading data for period: {0} to {1}'.format(self.start.strftime('%Y-%m-%d %H:%M:%S'), self.end.strftime('%Y-%m-%d %H:%M:%S')))
            self.read_api(continue_flag)
            if continue_flag == 1:
                continue_flag = 2

            self.set_period(end=self.start)
            self.start = max(self.start, start)

        if continue_flag == 1 and self.__byte_writer:
            self.__writer.write('[\n')

        eprint('Reading data for period: {0} to {1}'.format(start.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')))
        self.read_api(3)
        self.end = end

    # continue_flag = 0 - single run, 1 - start output, 2 - continue output, 3 - close output
    def read_api(self, continue_flag=0):
        if continue_flag < 2:
            i = 0
            if self.__byte_writer:
                self.__writer.write('[\n')
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
                eprint('Insufficient Privileges in AWS for region {0}'.format(region))
                self.__instances.regions.remove(region)
                continue
    
            for row in history.get('SpotPriceHistory'):
                row = byteify(row)
                timestamp = row.get('Timestamp')
                if timestamp < self.start or timestamp >= self.end:
                    continue

                row['Timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')

                if i > 0 and self.__byte_writer:
                    self.__writer.write(',\n')

                i = i + 1
                region = row.get('AvailabilityZone')
                region = self.__regionSplit.sub('', region)
                row['Region'] = region
                instance = row.get('InstanceType')
                attributes = self.__instances.get(region, instance)
                if attributes is not None:
                    row['Attributes'] = attributes

                if self.pretty:
                    self.__writer.write(json.dumps(row, indent=4, sort_keys=True))
                else:
                    self.__writer.write(json.dumps(row, sort_keys=True))

        if (continue_flag == 0 or continue_flag == 3) and self.__byte_writer:
            self.__writer.write('\n]\n')

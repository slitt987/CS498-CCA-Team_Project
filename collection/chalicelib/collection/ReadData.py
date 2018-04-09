import sys
import json
import boto3
import re
from botocore.exceptions import ClientError
from .InstanceMap import InstanceMap
from .common import *
import csv
import dateutil

class ReadData:
    __regionSplit = re.compile('[a-z]*$')

    def __init__(self, **kwargs):
        """Create a new ReadData object for data load

        Keyword arguments:
        period -- how many minutes to load per read (default 60)
        instances -- an InstanceMap object to use for data enrichment (default InstanceMap())
        writer -- object to write JSON data to (e.g. file), must provide a write() method (default sys.stdout)
        pretty -- should the JSON be pretty printed (default False)
        end -- datetime to read based on (default datetime.datetime.now())

        NOTE: it is assumed that if the writer has a fileno method this must be a file and
              delimiters will be written, if this method is missing it assumed to be a complex writer that
              doesn't need a delimiter between messages
        """
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
        """get the period for the requested end datetime based on the period minutes

        Keyword arguments:
        end -- datetime to read based on (default self.end)
        """
        end_epoch = (to_epoch(kwargs.get('end', self.end)) / self.__period) * self.__period
        start = utc.localize(from_epoch(end_epoch - self.__period))
        end = utc.localize(from_epoch(end_epoch))
        return start, end

    def set_period(self, **kwargs):
        """set the period for the requested end datetime based

        Keyword arguments:
        end -- datetime to read based on (default self.end)
        period -- how many minutes to load per read (default self.__period)
        """
        end = kwargs.get('end', self.end)
        self.__period = kwargs.get('period', self.__period / 60) * 60

        (self.start, self.end) = self.get_period(end=end)

    def backfill_read_api(self, start):
        """get data incrementally from the start to self.end in period minute increments"""
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

        eprint('Reading data for period: {0} to {1}'.format(self.start.strftime('%Y-%m-%d %H:%M:%S'), self.end.strftime('%Y-%m-%d %H:%M:%S')))
        self.read_api(3)
        self.end = end

    def read_api(self, continue_flag=0):
        """Read spot price data via the boto3 API

        continue_flag: int (default 0)
            0 -- single run
            1 -- start output
            2 -- continue output
            3 -- close output
        """
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

            for rowdict in history.get('SpotPriceHistory'):
                i = self.write_row(rowdict, i)

        if (continue_flag == 0 or continue_flag == 3) and self.__byte_writer:
            self.__writer.write('\n]\n')

    def read_file(self, filename, **kwargs):
        reader = kwargs.get('reader', csv.reader)
        start = kwargs.get('start', utc.localize(from_epoch(0)))

        self.start = start
        if self.__byte_writer:
            self.__writer.write('[\n')

        i = 0
        with open(filename) as file:
            for row in reader(file):
                rowdict = {
                    "Timestamp": dateutil.parser.parse(row[0]),
                    "InstanceType": row[1],
                    "ProductDescription": row[2],
                    "AvailabilityZone": row[3],
                    "SpotPrice": row[4]
                }
                i = self.write_row(rowdict, i)

        if self.__byte_writer:
            self.__writer.write('\n]\n')


    def write_row(self, row, i=0):
        row = byteify(row)
        timestamp = row.get('Timestamp')
        # Validate the API only pulls the data we are interested in
        if timestamp < self.start or timestamp >= self.end:
            return i

        # for a byte_writer stringify the timestamps
        if self.__byte_writer:
            row['Timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        region = self.__regionSplit.sub('', row.get('AvailabilityZone'))
        row['Region'] = region
        instance = row.get('InstanceType')
        attributes = self.__instances.get(region, instance)
        if attributes is not None:
            row['Attributes'] = attributes

        i = i + 1
        if self.__byte_writer:
            if i > 1:
                self.__writer.write(',\n')

            if self.pretty:
                self.__writer.write(json.dumps(row, indent=4, sort_keys=True))
            else:
                self.__writer.write(json.dumps(row, sort_keys=True))
        else:
            self.__writer.write(row)

        return i
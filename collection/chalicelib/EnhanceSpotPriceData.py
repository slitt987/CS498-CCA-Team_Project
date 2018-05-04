import sys
import json
import boto3
import re
from botocore.exceptions import ClientError
from .InstanceMap import InstanceMap
from .common import *
import csv
import dateutil


class EnhanceSpotPriceData:
    __regionSplit = re.compile('[a-z]*$')

    def __init__(self, period=60, instances=None, writer=sys.stdout, pretty=False,
                 end=utc.localize(datetime.datetime.now())):
        """
        Constructor

        NOTE: it is assumed that if the writer has a fileno method this must be a file and
              delimiters will be written, if this method is missing it assumed to be a complex writer that
              doesn't need a delimiter between messages
        :param period: how many minutes to load per read (default 60)
        :param instances: an InstanceMap object to use for data enrichment (default InstanceMap())
        :param writer: object to write JSON data to (e.g. file), must provide a write() method (default sys.stdout)
        :param pretty: should the JSON be pretty printed (default False)
        :param end: datetime to read based on (default datetime.datetime.now())
        """
        self.__period = period * 60
        self.__instances = instances
        self.__writer = writer
        self.pretty = pretty

        if end.tzinfo is None or end.tzinfo.utcoffset(end) is None:
            end = utc.localize(end)
        self.end = end
        (self.start, self.end) = self.get_period()

        # Check if this is a basic writer (file/stdout) by checking for method fileno
        self.__byte_writer = callable(getattr(self.__writer, "fileno", None))
        if self.__instances is None:
            self.__instances = InstanceMap()

    def get_period(self, end=None):
        """
        get the period for the requested end datetime based on the period minutes
        :param end: datetime to read based on (default self.end)
        :return: pair: (start, end)
        """
        end = end if end is not None else self.end
        end_epoch = (to_epoch(end) / self.__period) * self.__period
        start = utc.localize(from_epoch(end_epoch - self.__period))
        end = utc.localize(from_epoch(end_epoch))
        return start, end

    def set_period(self, end=None, period=None):
        """
        set the period for the requested end datetime based
        :param end: datetime to read based on (default self.end)
        :param period: how many minutes to load per read (default self.__period)
        :return: None
        """
        end = end if end is not None else self.end
        period = period if period is not None else self.__period / 60
        self.__period = period * 60
        (self.start, self.end) = self.get_period(end=end)

    def backfill_read_api(self, start):
        """
        get data incrementally from the start to self.end in period minute increments
        :param start: time to read back to
        :return: None
        """
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
            self.__writer.fetch('[\n')

        eprint('Reading data for period: {0} to {1}'.format(self.start.strftime('%Y-%m-%d %H:%M:%S'), self.end.strftime('%Y-%m-%d %H:%M:%S')))
        self.read_api(3)
        self.end = end

    def read_api(self, continue_flag=0):
        """
        Read spot price data via the boto3 API
        :param continue_flag: int (default 0)
            0 -- single run
            1 -- start output
            2 -- continue output
            3 -- close output
        :return: None
        """
        if continue_flag < 2:
            i = 0
            if self.__byte_writer:
                self.__writer.fetch('[\n')
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
            self.__writer.fetch('\n]\n')

    def read_file(self, filename, reader=csv.reader, start=utc.localize(from_epoch(0))):
        """
        Reads spot price data from a file to enhance (constructs dict to match boto3 api)
        :param filename: filename to read
        :param reader: parser to use to read the file data (Default: csv.reader)
        :param start: start time to filter the file data based on (Default: epoch)
        :return: None
        """
        self.start = start
        if self.__byte_writer:
            self.__writer.fetch('[\n')

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
            self.__writer.fetch('\n]\n')


    def write_row(self, row, i=0):
        """
        Enhances a row of spot price data and writes the data to the target
        :param row: row of pricing data (dict)
        :param i: row number in file (used for file writer) (Default: 0)
        :return: the row number writen to the target (i + 1 on write, i on skip)
        """
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
            attributes.pop('Region', None)
            attributes.pop('InstanceType', None)
            row['Attributes'] = attributes

        i = i + 1
        if self.__byte_writer:
            if i > 1:
                self.__writer.fetch(',\n')

            if self.pretty:
                self.__writer.write(json.dumps(row, indent=4, sort_keys=True))
            else:
                self.__writer.write(json.dumps(row, sort_keys=True))
        else:
            self.__writer.write(row)

        return i

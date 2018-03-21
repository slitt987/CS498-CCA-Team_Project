import datetime, sys, json
import common
import boto3
from botocore.exceptions import ClientError
from instanceMap import instanceMap
import re

class readData:
    _regionSplit = re.compile('[a-z]*$')

    def __init__(self, **kwargs):
        self._period = kwargs.get('period', 10) * 60
        self._startEpoch = (long(datetime.datetime.now().strftime('%s')) / self._period) * self._period
        self._instances = kwargs.get('instances')
        if self._instances == None:
            self._instances = instanceMap()
       
        self.setPeriod()

    def setPeriod(self, **kwargs):
        startEpoch = kwargs.get('start', self._startEpoch)

        self.start = datetime.datetime.fromtimestamp(startEpoch - 2 * self._period)
        self.end = datetime.datetime.fromtimestamp(startEpoch - self._period)

    def readApi(self):
        print '['
        i = 0
        for region in self._instances.getRegions(): 
            try:
               ec2 = boto3.client('ec2', region_name = region)
               history = ec2.describe_spot_price_history(InstanceTypes = self._instances.getTypes(), StartTime = self.start, EndTime = self.end)
            except ClientError:
               sys.stderr.write('Insufficient Privileges in AWS for region {0} \n'.format(region))
               continue 
    
            for row in history.get('SpotPriceHistory'):
                if i > 0:
                    print ','
                i = i + 1
                row = common.byteify(row)
                region = row.pop('AvailabilityZone')
                region = self._regionSplit.sub('', region)
                row['AvailabilityZone'] = region
                instance = row.get('InstanceType')
                attributes = self._instances.get(region, instance)
                if attributes != None:
                    row['Attributes'] = attributes
                row['Timestamp'] = row.get('Timestamp').strftime('%Y-%m-%d %H:%M:%S')
                print json.dumps(row, indent=4, sort_keys=True)    
            
        print ']'

import requests
import json
import re
import time
import os
import stat
import sys
from bs4 import BeautifulSoup
from common import *


class InstanceMap:
    __delimiters = ",;"
    __splitter = re.compile('|'.join(map(re.escape, list(__delimiters))))
    __storageSplitter = re.compile(r'([0-9]+) x ([0-9.]+) *([^ ]*.*)$')
    __regionTableId = "w163aac15c27c15"
    __minRegions = 10
    __regionUrl = 'https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html'
    __pricingJson = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json'

    def __init__(self, **kwargs):
        self.__mapFile = kwargs.get('file', 'instanceMap.json')
        self.__ttl = int(kwargs.get('ttl', 86400))
        self.instances = self.load()
        self.instanceTypes = self.load_types()
        self.regions = self.load_regions()
    
    def get_region_map(self):
        html = requests.get(self.__regionUrl).content
    
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", attrs={"id": self.__regionTableId})
    
        # The first tr contains the field names.
        headings = [th.get_text().lower() for th in table.find("tr").find_all("th")]
    
        regions = {}
        for row in table.find_all("tr")[1:]:
            region = dict(zip(headings, (td.get_text() for td in row.find_all("td"))))
            regions[region.get('region name')] = byteify(region).get('region')

        # arbitrary test to make sure we didn't completely fail on parsing data
        if len(regions.keys()) < self.__minRegions:
            raise Exception('Not able to parse regions from website, please validate the table id is right')
    
        return regions

    @staticmethod
    def build_key(region, instance):
        return "{0}/{1}".format(region, instance)

    @staticmethod
    def split_key(key):
        return key.split('/')

    def get(self, region, instance):
        return self.instances.get(self.build_key(region, instance))

    def get_types(self):
        return self.instanceTypes
 
    def get_regions(self):
        return self.regions

    def keys(self):
        return [self.split_key(key) for key in self.instances.keys()]

    def __iter__(self):
        return iter(self.keys())

    def load(self):
        if os.path.isfile(self.__mapFile):
            file_age = time.time() - os.stat(self.__mapFile)[stat.ST_MTIME]
            if file_age < self.__ttl:
                with open(self.__mapFile) as json_data:
                    return byteify(json.load(json_data))

        sys.stderr.write('Local file not found or to old, re-pulling instance data\n')
        return self.write()
   
    def load_types(self):
        instance_types = set()
        for key in self:
            instance_type = key[1]
            instance_types.add(instance_type)

        return list(instance_types)

    def load_regions(self):
        regions = set()
        for key in self:
            region = key[0]
            regions.add(region)

        return list(regions)

    def write(self):
        regions = self.get_region_map()
        json_data = requests.get(self.__pricingJson).content
        products = json.loads(json_data).get('products')
        
        instances = {}
        for product in products:
            attributes = byteify(products.get(product).get('attributes'))
            location = attributes.pop('location', None)
            region = regions.get(location)
            if 'vcpu' not in attributes or region is None:
                continue
         
            # attributes we are going to process
            instance_type = attributes.pop('instanceType', None)
            features = attributes.pop('processorFeatures', None)
            storage = attributes.pop('storage', None)
            memory = attributes.pop('memory', None)
        
            # remove un-needed attributes
            attributes.pop('locationType', None)
            attributes.pop('operatingSystem', None)
            attributes.pop('operation', None)
            attributes.pop('preInstalledSw', None)
            attributes.pop('servicecode', None)
            attributes.pop('servicename', None)
            attributes.pop('usagetype', None)
        
            # change type to number
            ecu = attributes.pop('ecu', None)
            try:
                attributes['ecu'] = float(ecu)
            except ValueError:
                pass
            normalization_size_factor = attributes.pop('normalizationSizeFactor', None)
            try:
                attributes['normalizationSizeFactor'] = int(normalization_size_factor)
            except ValueError:
                pass
            attributes['vcpu'] = int(attributes.get('vcpu'))
        
            # generate the table key
            key = self.build_key(region, instance_type)
        
            if features is not None:
                attributes['processorFeatures'] = [feature.strip() for feature in self.__splitter.split(features)]
        
            # Parse storage info
            if storage is None:
                attributes['storageType'] = 'None'
            elif 'EBS' in storage:
                attributes['storageType'] = 'EBS'
            elif 'x' in storage:
                storage_features = [feature.strip() for feature in self.__storageSplitter.match(storage).groups()]
                attributes['driveQuantity'] = int(storage_features[0])
                attributes['driveSize'] = float(storage_features[1].replace(',', ''))
                if storage_features[2] == '':
                    attributes['storageType'] = 'Unknown'
                else:
                    attributes['storageType'] = storage_features[2]
            else:
                attributes['storageType'] = 'None'
        
            # Parse memory info
            memory_features = (memory + ' ').split(' ')
            try:
                attributes['memorySize'] = float(memory_features[0])
                attributes['memorySizeUnits'] = memory_features[1]
            except ValueError:
                pass
        
            # Parse clockspeed
            try:
                attributes['clockSpeed'] = float(attributes.get('clockSpeed', 'NA').split(' ')[0])
            except ValueError:
                pass
        
            instances[key] = attributes
        
        with open(self.__mapFile + '.tmp', 'w') as outfile:
            json.dump(instances, outfile, indent=4, sort_keys=True)

        os.rename(self.__mapFile + '.tmp', self.__mapFile)

        return instances

import requests, lxml
import json, re
import time, os, stat
import common
from bs4 import BeautifulSoup
from pprint import pprint

class instanceMap:
    _delimiters = ",;"
    _spliter = re.compile('|'.join(map(re.escape, list(_delimiters))))
    _storageSpliter = re.compile(r'([0-9]+) x ([0-9.]+) *([^ ]*.*)$')
    _mapFile = 'instanceMap.json'
    _ttl = 86400

    instances = {}

    def __init__(self, **kwargs):
        self._mapFile = kwargs.get('file', 'instanceMap.json')
        self._ttl = int(kwargs.get('ttl', 86400))
        self.load()
    
    def getRegionMap(self):
        html = requests.get('https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html').content
    
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", attrs={"id":"w163aac15c27c15"})
    
        # The first tr contains the field names.
        headings = [th.get_text().lower() for th in table.find("tr").find_all("th")]
    
        regions = {}
        for row in table.find_all("tr")[1:]:
            region = dict(zip(headings, (td.get_text() for td in row.find_all("td"))))
            regions[region.get('region name')] = common.byteify(region).get('region')
    
        return regions

    def get(self, key):
        return self.instances.get(key)

    def keys(self):
        return self.instances.keys()

    def load(self):    
        if os.path.isfile(self._mapFile) and time.time() - os.stat(self._mapFile)[stat.ST_MTIME] < self._ttl:
            with open(self._mapFile) as json_data:
                self.instances = common.byteify(json.load(json_data))
        else:
            self.instances = self.write()

    def write(self):
        regions = self.getRegionMap()
        json_data = requests.get('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json').content
        products = json.loads(json_data).get('products')
        
        setTemp = set()
        instances = {}
        for product in products:
            attributes = common.byteify(products.get(product).get('attributes'))
            location = attributes.pop('location', None)
            region = regions.get(location)
            if 'vcpu' not in attributes or region == None: 
                continue
         
            # attributes we are going to process
            instanceType = attributes.pop('instanceType', None)
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
            normalizationSizeFactor = attributes.pop('normalizationSizeFactor', None)
            try:
                attributes['normalizationSizeFactor'] = int(normalizationSizeFactor)
            except ValueError:
                pass
            attributes['vcpu'] = int(attributes.get('vcpu'))
        
            # generate the table key
            key = "{0}/{1}".format(region, instanceType)
        
            if features != None:
                attributes['processorFeatures'] = [ feature.strip() for feature in self._spliter.split(features) ]
        
            # Parse storage info
            if storage == None:
                attributes['storageType'] = 'None'
            elif 'EBS' in storage:
                attributes['storageType'] = 'EBS'
            elif 'x' in storage:
                storageFeatures = [ feature.strip() for feature in self._storageSpliter.match(storage).groups() ]
                attributes['driveQuantity'] = int(storageFeatures[0])
                attributes['driveSize'] = float(storageFeatures[1].replace(',', ''))
                if storageFeatures[2] == '':
                    attributes['storageType'] = 'Unknown'
                else:
                    attributes['storageType'] = storageFeatures[2]
            else:
                attributes['storageType'] = 'None'
        
            # Parse memory info
            memoryFeatures = (memory + ' ').split(' ')
            try:
                attributes['memorySize'] = float(memoryFeatures[0])
                attributes['memorySizeUnits'] = memoryFeatures[1]
            except ValueError:
                pass
        
            # Parse clockspeed
            try:
                attributes['clockSpeed'] = float(attributes.get('clockSpeed', 'NA').split(' ')[0])
            except ValueError:
                pass
        
            instances[key] = attributes
        
        with open(self._mapFile, 'w') as outfile:
            json.dump(instances, outfile, indent=4, sort_keys=True)

        return instances

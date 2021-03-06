from flask import Flask, request
from flask_restful import Resource, Api, reqparse, abort
from OpenSSL import SSL
from expiringdict import ExpiringDict
from chalicelib import *
import dateutil
from pprint import pformat
import json
import itertools
from math import ceil
from elasticsearch import NotFoundError

try:
    # Python 2.6-2.7
    from HTMLParser import HTMLParser
except ImportError:
    # Python 3
    from html.parser import HTMLParser

h = HTMLParser()
app = Flask(__name__)
config = ConfigStage('chalicelib/collection.ini')

context = SSL.Context(SSL.SSLv23_METHOD)
api = Api(app)
bid_cache_ttl = float(config.get("api", "ttl_seconds"))
bid_cache = ExpiringDict(max_len=int(config.get("api", "cache_length")),
                         max_age_seconds=bid_cache_ttl * 2)
search_cache = ExpiringDict(max_len=int(config.get("api", "search_cache_length")),
                            max_age_seconds=float(config.get("api", "search_ttl_seconds")))

elastic_dict = config.items("elastic", {})
elastic_url = elastic_dict.pop("url", "localhost").split(",")

index_dict = config.items("history_index", {})
index = index_dict.pop("name", "spot_price_history")
doc_type = index_dict.pop("doc_type", "price")
mappings = json.loads(index_dict.pop("mappings", "{}"))

instance_index_dict = config.items("instance_index", {})
instance_index = instance_index_dict.pop("name", "instance_map")
instance_doc_type = instance_index_dict.pop("doc_type", "instance")
instance_mappings = json.loads(instance_index_dict.pop("mappings", "{}"))

bid_index_dict = config.items("bid_index", {})
bid_index = bid_index_dict.pop("name", "spot_bids")
bid_doc_type = bid_index_dict.pop("doc_type", "bid")
bid_mappings = json.loads(bid_index_dict.pop("mappings", "{}"))

instances_index = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                            index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)

bid_index = IndexData(elastic_url, bid_index, doc_type=bid_doc_type, connection_options=elastic_dict,
                      index_settings=bid_index_dict, index_mappings=bid_mappings)

predictor = BidPredictor(None, bid_index)

# Get Bid endpoint
class GetBid(Resource):
    @staticmethod
    def get_bid_cache_key(instance, region, os, timestamp, duration):
        """
        pieces together our key format
        :param instance:
        :param region:
        :param os:
        :param timestamp: datetime
        :param duration: int: hours?
        :return: string: key
        """
        epoch_hour = long(to_epoch(timestamp) / bid_cache_ttl)
        return "{0}.{1}.{2}.{3}.{4}".format(instance, region, os, epoch_hour, duration)

    def get_bid_cache(self, instance, region, os, timestamp, duration):
        """
        looks for value in cache
        :param instance:
        :param region:
        :param os:
        :param timestamp: datetime
        :param duration: int: hours?
        :return: float: bid
        """
        bid = bid_cache.get(self.get_bid_cache_key(instance, region, os, timestamp, duration))
        return bid if bid is not None else [None, None]

    def put_bid_cache(self, instance, region, os, timestamp, duration, bid):
        """
        puts a value on the cache (miss)
        :param instance:
        :param region:
        :param os:
        :param timestamp: datetime
        :param duration: int: hours?
        :param bid:
        :return: None
        """
        bid = bid if bid is not None else -1.0
        bid_cache[self.get_bid_cache_key(instance, region, os, timestamp, duration)] = bid

    def get_bid(self, instance, region, os, timestamp, duration):
        """
        returns the bid value evaluated for the input parameters
        :param instance:
        :param region:
        :param os:
        :param timestamp: datetime
        :param duration: int: hours?
        :return: float: bid
        """

        eprint("Getting bid for params - {}".format([instance, region, os, timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration]))
        bid = predictor.get_bid(region, instance, os, duration)
        eprint("Modeled price: {}".format(bid))

        self.put_bid_cache(instance, region, os, timestamp, duration, bid)
        return bid

    @staticmethod
    def get_instances_cache_key(query, numeric_as_min):
        """
        Returns the cache key (if we should cache) for a given query
        :param query:
        :param numeric_as_min:
        :return: string: key
        """
        query = str(query)
        if len(query) < 500:
            return query if numeric_as_min else "E-{}".format(query)
        else:
            return None

    def get_instances(self, query, numeric_as_min):
        """
        gets a list of instances matching the search criteria
        :param query: query parsed from post (dict)
        :param numeric_as_min: should numeric parameters be treated as min values (instead of max)
        :return: list((region, instance))
        """
        #only try to cache simple queries
        cache_key = self.get_instances_cache_key(query, numeric_as_min)
        if cache_key is not None:
            instances = search_cache.get(cache_key, set())
        else:
            instances = set()

        if len(instances) == 0:
            search_results = instances_index.search_terms(query, numeric_as_min=numeric_as_min)
            for instance in search_results:
                instances.add((instance.get("InstanceType"), instance.get("Region")))
            if len(instances) > 0 and cache_key is not None:
                search_cache[cache_key] = list(instances)

        return list(instances)

    def post(self, duration):
        """
        Post API
        :param os:
        :param duration: int: hours?
        :return: bid response (dict)
        """

        query = byteify(request.get_json(force=True))
        timestamp = query.pop("timestamp", None)
        os = query.pop("os", "Linux/Unix").lower()

        if timestamp is None:
            timestamp = utc.localize(datetime.datetime.now())
        else:
            timestamp = utc.localize(dateutil.parser.parse(timestamp))

        numeric_as_min = query.pop("numeric_as_min", "true").lower()[0] == "t"

        instance_matches = self.get_instances(query, numeric_as_min)

        # loop through instance/region combos
        out_instance = None
        out_region = None
        out_az = None
        bid_price = -1.0
        for instance, region in instance_matches:
            instance = instance.lower()
            region = region.lower()
            # Try cache, fallback to model
            az, bid = self.get_bid_cache(instance, region, os, timestamp, duration)
            if bid is None:
                az, bid = self.get_bid(instance, region, os, timestamp, duration)
            elif bid < 0:
                bid = None

            if az is not None and (out_instance is None or bid < bid_price):
                out_instance = instance
                out_region = region
                out_az = az
                bid_price = bid

        if out_instance is None:
            abort(404, reason="ERROR: Not Found - no instances can be found matching criteria")
        else:
            return {
                'instance': out_instance,
                'region': out_region,
                'availability_zone': out_az,
                'os': os,
                'bid_price': bid_price,
                'matching_instances': len(instance_matches)
            }


context.use_privatekey_file('key.pem')
context.use_certificate_file('cert.pem')
api.add_resource(GetBid, '/get_bid/<int:duration>')

if __name__ == '__main__':
    context = ('cert.pem', 'key.pem')
    app.run(host='0.0.0.0', ssl_context=context)

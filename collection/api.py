from flask import Flask, request
from flask_restful import Resource, Api, reqparse, abort
from OpenSSL import SSL
from expiringdict import ExpiringDict
from chalicelib import *
import dateutil
from pprint import pformat
import json

app = Flask(__name__)
config = ConfigStage('chalicelib/collection.ini')

context = SSL.Context(SSL.SSLv23_METHOD)
api = Api(app)
bid_cache = ExpiringDict(max_len=int(config.get("api", "cache_length")), max_age_seconds=float(config.get("api", "ttl_seconds")))
search_cache = ExpiringDict(max_len=int(config.get("api", "search_cache_length")), max_age_seconds=float(config.get("api", "search_ttl_seconds")))

elastic_dict = config.items("elastic", {})
elastic_url = elastic_dict.pop("url", "localhost")

instance_index_dict = config.items("instance_index", {})
instance_index = instance_index_dict.pop("name", "instance_map")
instance_doc_type = instance_index_dict.pop("doc_type", "instance")
instance_mappings = json.loads(instance_index_dict.pop("mappings", "{}"))

instances_index = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                            index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)

# Get Bid endpoint
class GetBid(Resource):
    @staticmethod
    def get_bid_cache_key(instance, region, timestamp, duration):
        """ pieces together our key format """
        epoch_ten_minute = long(to_epoch(timestamp) / 600)
        return "{0}.{1}.{2}.{3}".format(instance, region, epoch_ten_minute, duration)

    def get_bid_cache(self, instance, region, timestamp, duration):
        """ looks for value in cache and compares against whitelist """
        return bid_cache.get(self.get_bid_cache_key(instance, region, timestamp, duration))

    def put_bid_cache(self, instance, region, timestamp, duration, bid):
        """ puts a value on the cache (miss) """
        bid_cache[self.get_bid_cache_key(instance, region, timestamp, duration)] = bid

    def get_bid(self, instance, region, timestamp, duration):
        """ returns the bid value evaluted for the input parameters """
        bid = 1.0
        #TODO

        self.put_bid_cache(instance, region, timestamp, duration, bid)
        return bid

    def get_instances(self, query):
        """ gets a list of instances matching the search criteria """
        #only try to cache simple queries
        query_length = len(str(query))
        if query_length < 500:
            instances = search_cache.get(str(query), set())
        else:
            instances = set()

        if len(instances) == 0:
            search_results = instances_index.search_terms(query)
            for instance in search_results:
                instances.add((instance.get("InstanceType"), instance.get("Region")))
            if len(instances) > 0 and query_length < 500:
                search_cache[str(query)] = list(instances)

        return (list(instances), pformat({"query": query, "matches": instances, "search": list(search_results)}))

    def post(self, duration):
        """ Implementation of API GET """
        query = byteify(request.get_json(force=True))
        timestamp = query.pop("timestamp", None)
        if timestamp is None:
            timestamp = utc.localize(datetime.datetime.now())
        else:
            timestamp = utc.localize(dateutil.parser.parse(timestamp))

        (instance_matches, debug) = self.get_instances(query)

        # loop through instance/region combos
        out_instance = None
        out_region = None
        bid_price = -1.0
        for instance, region in instance_matches:
            # Try cache, fallback to model
            bid = self.get_bid_cache(instance, region, timestamp, duration)
            if bid is None:
                bid = self.get_bid(instance, region, timestamp, duration)

            if out_instance is None or bid < bid_price:
                out_instance = instance
                out_region = region
                bid_price = bid

        if out_instance is None:
            abort(404, reason="ERROR: Not Found - no instances can be found matching criteria - Debug: {}".format(debug))
        else:
            return {
                'instance': out_instance,
                'region': out_region,
                'bid_price': bid
            }


context.use_privatekey_file('key.pem')
context.use_certificate_file('cert.pem')
api.add_resource(GetBid, '/get_bid/<int:duration>')

if __name__ == '__main__':
    context = ('cert.pem', 'key.pem')
    app.run(host='0.0.0.0', ssl_context=context)

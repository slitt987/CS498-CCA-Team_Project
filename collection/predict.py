from chalicelib import *
from optparse import OptionParser
import dateutil
import json

import multiprocessing


config = ConfigStage('chalicelib/collection.ini')

minutes = int(config.get("main", "minutes", 60))
input_type = config.get("main", "input", "api")
output_type = config.get("main", "output", "file")
pretty = bool(config.get("main", "pretty", False))

outfile = config.get("file", "outfile", "output.json")

elastic_dict = config.items("elastic", {})
elastic_url = elastic_dict.pop("url", "localhost")

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

cores = max(multiprocessing.cpu_count() / 2 - 1, 1)
opt_parser = OptionParser()
opt_parser.add_option("--pretty", "-p", action="store_true", dest="pretty", default=pretty,
                      help="Pretty format output")
opt_parser.add_option("--elasticurl", "-e", action="store", type="string", dest="elastic_url", default=elastic_url,
                      help="URL for the elasticsearch server (Default: {})".format(elastic_url))
opt_parser.add_option("--threads", "-t", action="store", type="int", dest="threads",
                      default=cores,
                      help="Number of threads to use (Default: {})".format(cores))

(options, args) = opt_parser.parse_args()
elastic_url = options.elastic_url.split(',')


history_index = IndexData(elastic_url, index, doc_type=doc_type, connection_options=elastic_dict, index_settings=index_dict, index_mappings=mappings)
instance_index = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                           index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)
bid_index = IndexData(elastic_url, bid_index, doc_type=bid_doc_type, connection_options=elastic_dict,
                      index_settings=bid_index_dict, index_mappings=bid_mappings)
instances = InstanceMap(elastic_index=instance_index, ttl=8640000).get_types()


eprint("Training")
instances.sort()
cores = int(options.threads)
predictor = BidPredictor(history_index, bid_index)
threads = []
for core in range(0, cores):
    instances_slice = [instance for instance in instances if hash(instance) % cores == core]
    thread = predictor.thread_process_instance(instances_slice)
    threads.append(thread)

[thread.join() for thread in threads]
eprint("Training Complete")





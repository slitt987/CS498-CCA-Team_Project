from optparse import OptionParser
from chalicelib import *
import dateutil
from pprint import pformat
import json
import itertools
from math import ceil


config = ConfigStage('chalicelib/collection.ini')

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

instances_index = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                            index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)

history_index = IndexData(elastic_url, index, doc_type=doc_type, connection_options=elastic_dict, index_settings=index_dict,
                    index_mappings=mappings)


opt_parser = OptionParser()
opt_parser.add_option("--region", "-r", action="store", type="string", dest="region", default="us-east-1", help="Region Name")
opt_parser.add_option("--instance", "-i", action="store", type="string", dest="instance", help="Instance Type")
opt_parser.add_option("--os", "-o", action="store", type="string", dest="os", default="Linux/UNIX", help="OS")
opt_parser.add_option("--rows", "-n", action="store", type="int", dest="rows", default=1000, help="Number of rows")

(options, args) = opt_parser.parse_args()
history = history_index.search_terms({"Region": options.region, "InstanceType": options.instance, "ProductDescription": options.os})
history_gen = (
    {
        "Region": r.get("Region").lower(),
        "InstanceType": r.get("InstanceType").lower(),
        "ProductDescription": r.get("ProductDescription").lower(),
        #"Timestamp": to_epoch(dateutil.parser.parse(r.get("Timestamp"))),
        "SpotPrice": float(r.get("SpotPrice"))
    } for r in history)

for row in itertools.islice(history_gen, int(options.rows)):
    print row

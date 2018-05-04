from chalicelib import *
from optparse import OptionParser

config = ConfigStage('chalicelib/collection.ini')

elastic_dict = config.items("elastic", {})
elastic_url = elastic_dict.pop("url", "localhost")

opt_parser = OptionParser()
opt_parser.add_option("--elasticurl", "-e", action="store", type="string", dest="elastic_url", default=elastic_url,
                      help="URL for the elasticsearch server (Default: {})".format(elastic_url))
opt_parser.add_option("--indexname", "-x", action="store", type="string", dest="index", default=None,
                      help="elasticsearch index name")
(options, args) = opt_parser.parse_args()
elastic_url = options.elastic_url.split(',')

out = IndexData(elastic_url, index=options.index, doc_type="price")
out.get_client().indices.delete(options.index)

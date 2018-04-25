from chalicelib import *
from optparse import OptionParser
import dateutil
import os
import json

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

usage_msg="""usage: %prog [-i <api|file>] [-f <filename>] [-m <N>] [-s <timestamp>] [-o <filename>]"""
opt_parser = OptionParser(usage_msg)
opt_parser.add_option("--input", "-i", action="store", type="string", dest="input", default=input_type,
                      help="Input for this run (Default: {})".format(input_type), metavar="api|file")
opt_parser.add_option("--filename", "-f", action="store", type="string", dest="filename", default=None,
                      help="Filename to use (only compatible with file input)", metavar="file")
opt_parser.add_option("--minutes", "-m", action="store", type="int", dest="minutes", default=minutes,
                      help="Number of minutes to process (only compatible with API input) (Default: {})".format(minutes), metavar="minutes")
opt_parser.add_option("--start", "-s", action="store", type="string", dest="start", default=None,
                      help="Starting point to read data from (Default: API - now - period, File - beginning of file)", metavar="timestamp")
opt_parser.add_option("--outtype", "-t", action="store", type="string", dest="output_type", default=output_type,
                      help="Output type (Default: {})".format(output_type))
opt_parser.add_option("--outfile", "-o", action="store", type="string", dest="outfile", default=outfile,
                      help="Output file name (Default: {})".format(outfile))
opt_parser.add_option("--pretty", "-p", action="store_true", dest="pretty", default=pretty,
                      help="Pretty format output")
opt_parser.add_option("--elasticurl", "-e", action="store", type="string", dest="elastic_url", default=elastic_url,
                      help="URL for the elasticsearch server (Default: {})".format(elastic_url))
opt_parser.add_option("--indexname", "-x", action="store", type="string", dest="index", default=index,
                      help="elasticsearch index name (Default: {})".format(index))
(options, args) = opt_parser.parse_args()
elastic_url = options.elastic_url.split(',')


# Open the writer
if options.output_type.lower().startswith("f"):
    tmpfile = ".{}.tmp".format(options.outfile)
    out = open(tmpfile, 'w')
    instances = InstanceMap(file="instanceMap.json")
elif options.output_type.lower().startswith("e"):
    out = IndexData(elastic_url, options.index, doc_type=doc_type, connection_options=elastic_dict, index_settings=index_dict, index_mappings=mappings)
    instance_out = IndexData(elastic_url, instance_index, doc_type=instance_doc_type, connection_options=elastic_dict,
                             index_settings=instance_index_dict, index_mappings=instance_mappings, alias=True)
    instances = InstanceMap(elastic_index=instance_out)
else:
    eprint("ERROR: Invalid output type provided: {}".format(options.output_type))
    exit(1)

# Initialize the reader class
reader = EnhanceSpotPriceData(instances=instances, period=options.minutes, writer=out, pretty=options.pretty)
if options.start is not None:
    start = utc.localize(dateutil.parser.parse(options.start))
else:
    start = utc.localize(from_epoch(0))

# Call the correct reader
if options.start is not None and options.input.lower().startswith("a"):
    reader.backfill_read_api(start)
elif options.input.lower().startswith("f"):
    if options.filename is None:
        eprint("ERROR: running in file mode, but no filename supplied. Aborting.")
        exit(1)

    reader.read_file(options.filename, start=start)
elif options.input.lower().startswith("a"):
    reader.read_api()
else:
    eprint("ERROR: Invalid input mode supplied: {}.  Aborting".format(options.input))

# Close the writer
if options.output_type.lower().startswith("f"):
    out.close()
    try:
        os.remove(options.outfile)
    except OSError:
        pass

    os.rename(tmpfile, options.outfile)

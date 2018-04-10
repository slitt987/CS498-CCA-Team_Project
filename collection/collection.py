from chalicelib import *
from optparse import OptionParser
import ConfigParser
import dateutil
import os
import json

Config = ConfigParser.ConfigParser()
Config.read('chalicelib/collection.ini')


def config_get_default(config, section, option, default=None):
    if config.has_section(section):
        if config.has_option(section, option):
            return config.get(section, option)

    return default


minutes = int(config_get_default(Config, "main", "minutes", 60))
input_type = config_get_default(Config, "main", "input", "api")
output_type = config_get_default(Config, "main", "output", "file")
pretty = bool(config_get_default(Config, "main", "pretty", False))

outfile = config_get_default(Config, "file", "outfile", "output.json")

# For elastic info we are going to go a bit lower level
if Config.has_section("elastic"):
    elastic_dict = dict(Config.items("elastic"))
else:
    elastic_dict = {}

elastic_url = elastic_dict.pop("url", "localhost")

if Config.has_section("elastic_index"):
    index_dict = dict(Config.items("elastic_index"))
else:
    index_dict = {}

index = index_dict.pop("name", "spot_price_history")
doc_type = index_dict.pop("doc_type", "price")
mappings = json.loads(index_dict.pop("mappings", "{}"))

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

instances = InstanceMap()

# Open the writer
if options.output_type.lower().startswith("f"):
    tmpfile = ".{}.tmp".format(options.outfile)
    out = open(tmpfile, 'w')
elif options.output_type.lower().startswith("e"):
    out = IndexData(elastic_url, options.index, doc_type=doc_type, connection_options=elastic_dict, index_settings=index_dict, index_mappings=mappings)
else:
    eprint("ERROR: Invalid output type provided: {}".format(options.output_type))
    exit(1)

# Initialize the reader class
reader = EnhanceSpotPriceData(instances=instances, period=options.minutes, writer=out, pretty=options.pretty)
if options.start is not None:
    start = utc.localize(dateutil.parser.parse(start))
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

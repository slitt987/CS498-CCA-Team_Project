from ReadData import ReadData
from InstanceMap import InstanceMap
from common import *
from optparse import OptionParser
import ConfigParser
import dateutil
import elasticsearch
import os

Config = ConfigParser.ConfigParser()
Config.read('collection.ini')


def config_get_default(config, section, option, default=None):
    if config.has_section(section):
        if config.has_option(section, option):
            return config.get(section, option)

    return default


minutes = config_get_default(Config, "main", "minutes", 60)
input_type = config_get_default(Config, "main", "input", "api")
output_type = config_get_default(Config, "main", "output", "file")
pretty = bool(config_get_default(Config, "main", "pretty", False))

outfile = config_get_default(Config, "file", "outfile", "output.json")

elastic_url = config_get_default(Config, "elastic", "url", "localhost").split(',')

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
(options, args) = opt_parser.parse_args()

instances = InstanceMap()
tmpfile = ".{}.tmp".format(options.outfile)
with open(tmpfile, 'w') as out:
    # Initialize the reader class
    reader = ReadData(instances=instances, period=options.minutes, writer=out, pretty=options.pretty)
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

try:
    os.remove(options.outfile)
except OSError:
    pass

os.rename(tmpfile, options.outfile)
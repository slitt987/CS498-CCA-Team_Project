from ReadData import ReadData
from InstanceMap import InstanceMap
from common import *
from optparse import OptionParser
import dateutil

period = 60
usage_msg="""usage: %prog [--input <api|file>] [--filename <filename>] [--minutes <N>] [--start <timestamp>] [--outfile <filename>]"""
opt_parser = OptionParser(usage_msg)
opt_parser.add_option("--input", "-i", action="store", type="string", dest="input", default="api",
                      help="Input for this run (API or File) Default: api", metavar="api|file")
opt_parser.add_option("--filename", "-f", action="store", type="string", dest="filename", default=None,
                      help="Filename to use (only compatible with file input)", metavar="file")
opt_parser.add_option("--minutes", "-m", action="store", type="int", dest="minutes", default=period,
                      help="Number of minutes to process (only compatible with API input) Default: {}".format(period), metavar="minutes")
opt_parser.add_option("--start", "-s", action="store", type="string", dest="start", default=None,
                      help="Starting point to read data from Default: API - now - period, File - beginning of file", metavar="timestamp")
opt_parser.add_option("--outfile", "-o", action="store", type="string", dest="outfile", default="output.json",
                      help="Output file name Defautl: output.json")
opt_parser.add_option("--pretty", "-p", action="store_true", dest="pretty", default=False,
                      help="Pretty format output")
(options, args) = opt_parser.parse_args()

instances = InstanceMap()
with open(options.outfile, 'w') as out:
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

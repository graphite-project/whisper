#!/usr/bin/env python
import sys, os, fnmatch
from subprocess import call
from optparse import OptionParser

option_parser = OptionParser(
    usage='''%prog storagePath configPath

storagePath   the Path to the directory containing whisper files (CAN NOT BE A SUBDIR, use --subdir for that)
configPath    the path to your carbon config files
''', version="%prog 0.1")

option_parser.add_option(
    '--doit', default=False, action='store_true',
    help="This is not a drill, lets do it")
option_parser.add_option(
    '-q', '--quiet', default=False, action='store_true',
    help="Display extra debugging info")
option_parser.add_option(
    '--subdir', default=None,
    type='string', help="only process a subdir of whisper files")
option_parser.add_option(
    '--carbonlib', default=None,
    type='string', help="folder where the carbon lib files are if its not in your path already")
option_parser.add_option(
    '--whisperlib', default=None,
    type='string', help="folder where the whisper lib files are if its not in your path already")
option_parser.add_option(
    '--confirm', default=False, action='store_true',
    help="ask for comfirmation prior to resizing a whisper file")
option_parser.add_option(
    '-x', '--extra_args', default='',
    type='string', help="pass any additional arguments to the whisper-resize.py script")

(options, args) = option_parser.parse_args()

if len(args) < 2:
    option_parser.print_help()
    sys.exit(1)

storagePath = args[0]
configPath  = args[1]

#check to see if we are processing a subfolder
# we need to have a separate config option for this since
# otherwise the metric test thinks the metric is at the root
# of the storage path and can match schemas incorrectly
if options.subdir is None:
    processPath = args[0]
else:
    processPath = options.subdir

# Injecting the Whisper Lib Path if needed
if options.whisperlib is not None:
    sys.path.insert(0, options.whisperlib)

try:
    import whisper
except ImportError:
    raise SystemExit('[ERROR] Can\'t find the whisper module, try using --whisperlib to explicitly include the path')

# Injecting the Carbon Lib Path if needed
if options.carbonlib is not None:
    sys.path.insert(0, options.carbonlib)

try:
    from carbon import conf
    from carbon.conf import settings
except ImportError:
    raise SystemExit('[ERROR] Can\'t find the carbon module, try using --carbonlib to explicitly include the path')

#carbon.conf not seeing the config files so give it a nudge
settings.CONF_DIR = configPath
settings.LOCAL_DATA_DIR = storagePath

# import these once we have the settings figured out
from carbon.storage import loadStorageSchemas, loadAggregationSchemas

# Load the Defined Schemas from our config files
schemas = loadStorageSchemas()
agg_schemas = loadAggregationSchemas()

# check to see if a metric needs to be resized based on the current config
def processMetric(fullPath, schemas, agg_schemas):
    """
        method to process a given metric, and resize it if necessary

        Parameters:
            fullPath    - full path to the metric whisper file
            schemas     - carbon storage schemas loaded from config
            agg_schemas - carbon storage aggregation schemas load from confg

    """
    schema_config_args = ''
    schema_file_args   = ''
    rebuild = False
    messages = ''

    # get archive info from whisper file
    info = whisper.info(fullpath)

    # get graphite metric name from fullPath
    metric = getMetricFromPath(fullpath)

    # loop the carbon-storage schemas
    for schema in schemas:
        if schema.matches(metric):
            # returns secondsPerPoint and points for this schema in tuple format
            archive_config = [archive.getTuple() for archive in schema.archives]
            break

    # loop through the carbon-aggregation schemas
    for agg_schema in agg_schemas:
        if agg_schema.matches(metric):
            xFilesFactor, aggregationMethod = agg_schema.archives
            break

    # loop through the bucket tuples and convert to string format for resizing
    for retention in archive_config:
        current_schema = '%s:%s ' % (retention[0], retention[1])
        schema_config_args += current_schema

    # loop through the current files bucket sizes and convert to string format to compare for resizing
    for fileRetention in info['archives']:
        current_schema  = '%s:%s ' % (fileRetention['secondsPerPoint'], fileRetention['points'])
        schema_file_args += current_schema

    # check to see if the current and configured schemas are the same or rebuild
    if (schema_config_args != schema_file_args):
        rebuild = True
        messages += 'updating Retentions from: %s to: %s \n' % (schema_file_args, schema_config_args)

    # only care about the first two decimals in the comparison since there is floaty stuff going on.
    info_xFilesFactor = "{0:.2f}".format(info['xFilesFactor'])
    str_xFilesFactor =  "{0:.2f}".format(xFilesFactor)

    # check to see if the current and configured aggregationMethods are the same
    if (str_xFilesFactor != info_xFilesFactor):
        rebuild = True
        messages += '%s xFilesFactor differs real: %s should be: %s \n' % (metric, info_xFilesFactor, str_xFilesFactor)
    if (aggregationMethod != info['aggregationMethod']):
        rebuild = True
        messages += '%s aggregation schema differs real: %s should be: %s \n' % (metric, info['aggregationMethod'], aggregationMethod)

    # check to see if the current and configured xFilesFactor are the same
    if (xFilesFactor != info['xFilesFactor']):
        rebuild = True
        messages += '%s xFilesFactor differs real: %s should be: %s \n' % (metric, info['xFilesFactor'], xFilesFactor)

    # if we need to rebuild, lets do it.
    if (rebuild == True):
        cmd = 'whisper-resize.py "%s" %s --xFilesFactor=%s --aggregationMethod=%s %s' % (fullPath, options.extra_args, xFilesFactor, aggregationMethod, schema_config_args)
        if (options.quiet != True or options.confirm == True):
            print(messages)
            print(cmd)

        if (options.confirm == True):
            options.doit = confirm("Would you like to run this command? [y/n]: ")
            if (options.doit == False):
                print("Skipping command \n")

        if (options.doit == True):
            exitcode = call(cmd, shell=True)
            # if the command failed lets bail so we can take a look before proceeding
            if (exitcode > 0):
                print('Error running: %s' % (cmd))
                sys.exit(1)

def getMetricFromPath(filePath):
    """
        this method takes the full file path of a whisper file an converts it to a gaphite metric name

        Parameters:
            filePath - full file path to a whisper file

        Returns a string representing the metric name
    """
    # sanitize directory since we may get a trailing slash or not, and if we don't it creates a leading .
    data_dir = os.path.normpath(settings.LOCAL_DATA_DIR) + os.sep

    # pull the data dir off and convert to the graphite metric name
    metric_name = filePath.replace(data_dir, '')
    metric_name = metric_name.replace('.wsp', '')
    metric_name = metric_name.replace('/', '.')
    return metric_name

def confirm(question, error_response='Valid options : yes or no'):
    """
         ask the user if they would like to perform the action

         Parameters:
             question       - the question you would like to ask the user to confirm.
             error_response - the message to display if an invalid option is given.
    """
    while True:
        answer = raw_input(question).lower()
        if answer in ('y', 'yes'):
            return True
        if answer in ('n', 'no' ):
            return False
        print(error_response)

for root, _, files in os.walk(processPath):
    # we only want to deal with non-hidden whisper files
    for f in fnmatch.filter(files, '*.wsp'):
        fullpath = os.path.join(root, f)
        processMetric(fullpath, schemas, agg_schemas)


#!/usr/bin/env python
import sys, os, fnmatch, logging
from subprocess import call
from optparse import OptionParser

try:
    import whisper
    from whisper import log
except ImportError:
    raise SystemExit('[ERROR] Can\'t find the whisper module, try using --whisperlib to explicitly include the path')

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
    help='Run in quiet mode. Only error messages are displayed.')
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
option_parser.add_option(
    '-s', '--statistics', default=False, action='store_true',
    help='Print some statistics')

(options, args) = option_parser.parse_args()

if options.quiet:
    log.set_log_level(logging.ERROR)

if len(args) < 2:
    option_parser.print_help()
    sys.exit(1)

storagePath = args[0]
configPath = args[1]

#check to see if we are processing a subfolder
# we need to have a seperate config option for this since
# otherwise the metric test thinks the metric is at the root
# of the storage path and can match schemas incorrectly
if options.subdir is None:
    processPath = args[0]
else:
    processPath = options.subdir

# Injecting the Whisper Lib Path if needed
if options.whisperlib is not None:
    sys.path.insert(0, options.whisperlib)

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
def rebuild_metric(aggregationMethod, fullPath, messages, schema_config_args, xFilesFactor):
    do_it = options.doit
    cmd = 'whisper-resize.py %s %s %s --xFilesFactor=%s --aggregationMethod=%s %s' % \
          (['', '-q'][options.quiet], fullPath, options.extra_args, xFilesFactor, aggregationMethod,
           schema_config_args)

    if options.confirm:
        print messages
        print cmd
        do_it = confirm("Would you like to run this command? [y/n]: ")
        if not do_it:
            print "Skipping command \n"

    if do_it:
        if call(cmd, shell=True) != 0:
            log.error('Error running: %s' % (cmd))
            return StatisticsCollector.REBUILD_ERROR
        else:
            return StatisticsCollector.REBUILD_DONE
    else:
        log.info("Would have resized %s but didn't." % fullPath)
        return StatisticsCollector.REBUILD_REQUIRED


def processMetric(fullPath, schemas, agg_schemas):
    """
        method to process a given metric, and resize it if necessary

        Parameters:
            fullPath    - full path to the metric whisper file
            schemas     - carbon storage schemas loaded from config
            agg_schemas - carbon storage aggregation schemas load from confg

    """
    schema_config_args = ''
    schema_file_args = ''
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
        current_schema = '%s:%s ' % (fileRetention['secondsPerPoint'], fileRetention['points'])
        schema_file_args += current_schema

    # check to see if the current and configured schemas are the same or rebuild
    if (schema_config_args != schema_file_args):
        rebuild = True
        messages += 'updating Retentions from: %s to: %s \n' % (schema_file_args, schema_config_args)

    log.info("xFilesFactor: %s (wsp file) / %s (storage-aggregation.cfg)" %
             (str(info['xFilesFactor']), str(xFilesFactor)))

    # set xFilesFactor to the wsp files setting if there is nothing configured in storage-aggregation.conf
    if xFilesFactor is None:
        xFilesFactor = info['xFilesFactor']
        log.warn('no configuration for xFilesFactor found, using xFilesFactor from wsp file')

    # only care about the first two decimals in the comparison since there is floaty stuff going on.
    info_xFilesFactor = "{0:.2f}".format(info['xFilesFactor'])
    str_xFilesFactor = "{0:.2f}".format(xFilesFactor)

    # check to see if the current and configured aggregationMethods are the same
    if (str_xFilesFactor != info_xFilesFactor):
        rebuild = True
        messages += '%s xFilesFactor differs real: %s should be: %s \n' % \
                    (metric, info_xFilesFactor, str_xFilesFactor)
    if (aggregationMethod != info['aggregationMethod']):
        rebuild = True
        messages += '%s aggregation schema differs real: %s should be: %s \n' % \
                    (metric, info['aggregationMethod'], aggregationMethod)

    # if we need to rebuild, lets do it.
    if rebuild:
        return rebuild_metric(aggregationMethod, fullPath, messages, schema_config_args, xFilesFactor)
    else:
        return StatisticsCollector.REBUILD_NOT_REQUIRED


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
    log.info("WORKING ON: " + metric_name)
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
        if answer in ('n', 'no'):
            return False
        print error_response


class StatisticsCollector(object):
    REBUILD_NOT_REQUIRED = 0
    REBUILD_DONE = 1
    REBUILD_ERROR = 2
    REBUILD_REQUIRED = 3

    def __init__(self):
        self.statistics = {self.REBUILD_DONE: 0,
                           self.REBUILD_ERROR: 0,
                           self.REBUILD_NOT_REQUIRED: 0,
                           self.REBUILD_REQUIRED: 0}

    def update_counters(self, status_code):
        self.statistics[status_code] += 1

    def has_errors(self):
        return self.statistics[self.REBUILD_ERROR]

    def as_string(self):
        total = sum(self.statistics.values())
        return 'Metrics processed: %s, ok: %s, error: %s, not changed: %s, required but not done %s' % \
               (total,
                self.statistics[self.REBUILD_DONE],
                self.statistics[self.REBUILD_ERROR],
                self.statistics[self.REBUILD_NOT_REQUIRED],
                self.statistics[self.REBUILD_REQUIRED])

statistics_collector = StatisticsCollector()
for root, _, files in os.walk(processPath):
    # we only want to deal with non-hidden whisper files
    for f in fnmatch.filter(files, '*.wsp'):
        fullpath = os.path.join(root, f)
        status = processMetric(fullpath, schemas, agg_schemas)
        statistics_collector.update_counters(status)

if options.statistics:
    print statistics_collector.as_string()

sys.exit(0 if not statistics_collector.has_errors() else 1)

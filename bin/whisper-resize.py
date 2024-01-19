#!/usr/bin/env python

import os
import sys
import time
import bisect
import signal
import optparse
import traceback

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

now = int(time.time())

option_parser = optparse.OptionParser(
    usage='''%prog path timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention
''')

option_parser.add_option(
    '--xFilesFactor', default=None,
    type='float', help="Change the xFilesFactor")
option_parser.add_option(
    '--aggregationMethod', default=None,
    type='string', help="Change the aggregation function (%s)" %
    ', '.join(whisper.aggregationMethods))
option_parser.add_option(
    '--force', default=False, action='store_true',
    help="Perform a destructive change")
option_parser.add_option(
    '--newfile', default=None, action='store',
    help="Create a new database file without removing the existing one")
option_parser.add_option(
    '--sparse', default=True, action='store_true',
    help="Create new whisper as sparse file")
option_parser.add_option(
    '--fallocate', default=False, action='store_true',
    help="Create new whisper and use fallocate (disabling sparse)")
option_parser.add_option(
    '--chown-uid', default=-1,
    type='int', help="Run chown for specific UID")
option_parser.add_option(
    '--chown-gid', default=-1,
    type='int', help="Run chown for specific GID")
option_parser.add_option(
    '--nobackup', action='store_true',
    help='Delete the .bak file after successful execution')
option_parser.add_option(
    '--aggregate', action='store_true',
    help='Try to aggregate the values to fit the new archive better.'
         ' Note that this will make things slower and use more memory.')
option_parser.add_option(
    '--quiet', default=False, action='store_true',
    help='Print less messages')
option_parser.add_option(
    '--silent', default=False, action='store_true',
    help='Print no messages')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path = args[0]

if not os.path.exists(path):
  sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % path)
  option_parser.print_help()
  sys.exit(1)

if not options.silent:
    size = os.stat(path).st_size
    blocks = os.stat(path).st_blocks
    print('Old file: %s (%d bytes, %d blocks*%d=%d bytes on disk)'
          % (path, size, blocks, 512, blocks * 512))

info = whisper.info(path)

new_archives = [whisper.parseRetentionDef(retentionDef)
                for retentionDef in args[1:]]

old_archives = info['archives']
# sort by precision, lowest to highest
old_archives.sort(key=lambda a: a['secondsPerPoint'], reverse=True)

if options.xFilesFactor is None:
  xff = info['xFilesFactor']
else:
  xff = options.xFilesFactor

if options.aggregationMethod is None:
  aggregationMethod = info['aggregationMethod']
else:
  aggregationMethod = options.aggregationMethod

if not options.quiet and not options.silent:
    print('Retrieving all data from the archives')
for archive in old_archives:
  fromTime = now - archive['retention'] + archive['secondsPerPoint']
  untilTime = now
  timeinfo, values = whisper.fetch(path, fromTime, untilTime)
  archive['data'] = (timeinfo, values)

if options.newfile is None:
  tmpfile = path + '.tmp'
  if os.path.exists(tmpfile):
    if not options.quiet and not options.silent:
        print('Removing previous temporary database file: %s' % tmpfile)
    os.unlink(tmpfile)
  newfile = tmpfile
else:
  newfile = options.newfile

if not options.quiet and not options.silent:
    print('Creating new whisper database: %s' % newfile)

try:
    whisper.create(newfile, new_archives, xFilesFactor=xff,
                   aggregationMethod=aggregationMethod, sparse=options.sparse,
                   useFallocate=options.fallocate)
except whisper.WhisperException as exc:
    raise SystemExit('[ERROR] %s' % str(exc))

size = os.stat(newfile).st_size
blocks = os.stat(newfile).st_blocks
if not options.quiet and not options.silent:
    print('Created: %s (%d bytes, %d blocks*%d=%d bytes on disk)'
          % (newfile, size, blocks, 512, blocks * 512))

if options.aggregate:
  # This is where data will be interpolated (best effort)
  if not options.quiet and not options.silent:
    print('Migrating data with aggregation...')
  all_datapoints = []
  for archive in sorted(old_archives, key=lambda x: x['secondsPerPoint']):
    # Loading all datapoints into memory for fast querying
    timeinfo, values = archive['data']
    new_datapoints = list(zip(range(*timeinfo), values))
    new_datapoints.reverse()
    if all_datapoints:
      last_timestamp = all_datapoints[-1][0]
      slice_end = 0
      for i, (timestamp, value) in enumerate(new_datapoints):
        if timestamp < last_timestamp:
          slice_end = i
          break
      all_datapoints += new_datapoints[slice_end:]
    else:
      all_datapoints += new_datapoints
  all_datapoints.reverse()

  oldtimestamps = list(map(lambda p: p[0], all_datapoints))
  oldvalues = list(map(lambda p: p[1], all_datapoints))
  if not options.quiet and not options.silent:
    print("oldtimestamps: %s" % oldtimestamps)
  # Simply cleaning up some used memory
  del all_datapoints

  new_info = whisper.info(newfile)
  new_archives = new_info['archives']

  for archive in new_archives:
    step = archive['secondsPerPoint']
    fromTime = now - archive['retention'] + now % step
    untilTime = now + now % step + step
    if not options.quiet and not options.silent:
      print("(%s,%s,%s)" % (fromTime, untilTime, step))
    timepoints_to_update = range(fromTime, untilTime, step)
    if not options.quiet and not options.silent:
      print("timepoints_to_update: %s" % timepoints_to_update)
    newdatapoints = []
    for tinterval in zip(timepoints_to_update[:-1], timepoints_to_update[1:]):
      # TODO: Setting lo= parameter for 'lefti' based on righti from previous
      #       iteration. Obviously, this can only be done if
      #       timepoints_to_update is always updated. Is it?
      lefti = bisect.bisect_left(oldtimestamps, tinterval[0])
      righti = bisect.bisect_left(oldtimestamps, tinterval[1], lo=lefti)
      newvalues = oldvalues[lefti:righti]
      if newvalues:
        non_none = list(filter(lambda x: x is not None, newvalues))
        if non_none and 1.0 * len(non_none) / len(newvalues) >= xff:
          newdatapoints.append([tinterval[0],
                                whisper.aggregate(aggregationMethod,
                                                  non_none, newvalues)])
    whisper.update_many(newfile, newdatapoints)
else:
  if not options.quiet and not options.silent:
    print('Migrating data without aggregation...')
  for archive in old_archives:
    timeinfo, values = archive['data']
    datapoints = zip(range(*timeinfo), values)
    datapoints = filter(lambda p: p[1] is not None, datapoints)
    whisper.update_many(newfile, datapoints)

if options.newfile is not None:
  sys.exit(0)

backup = path + '.bak'
if not options.quiet and not options.silent:
    print('Renaming old database to: %s' % backup)
os.rename(path, backup)

try:
  if not options.quiet and not options.silent:
    print('Renaming new database to: %s' % path)
  os.rename(tmpfile, path)
except (OSError):
  traceback.print_exc()
  if not options.quiet and not options.silent:
    print('\nOperation failed, restoring backup')
  os.rename(backup, path)
  sys.exit(1)

if options.chown_uid > 0 and options.chown_gid > 0:
  try:
    os.chown(path=path, uid=options.chown_uid, gid=options.chown_gid)
  except (OSError):
    traceback.print_exc()

if not options.silent:
    size = os.stat(path).st_size
    blocks = os.stat(path).st_blocks
    print('New file: %s (%d bytes, %d blocks*%d=%d bytes on disk)'
          % (path, size, blocks, 512, blocks * 512))

if options.nobackup:
  if not options.quiet and not options.silent:
    print("Unlinking backup: %s" % backup)
  os.unlink(backup)

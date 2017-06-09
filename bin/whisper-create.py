#!/usr/bin/env python

import os
import sys
import signal
import optparse
import math

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')


def byte_format(num):
  for x in ['bytes', 'KB', 'MB']:
    if num < 1024.0:
      return "%.3f%s" % (num, x)
    num /= 1024.0
  return "%.3f%s" % (num, 'GB')


# Ignore SIGPIPE
try:
  signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
  # OS=windows
  pass

option_parser = optparse.OptionParser(
    usage='''%prog path timePerPoint:timeToStore [timePerPoint:timeToStore]*
%prog --estimate timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention
''')
option_parser.add_option('--xFilesFactor', default=0.5, type='float')
option_parser.add_option('--aggregationMethod', default='average',
                         type='string',
                         help="Function to use when aggregating values (%s)" %
                         ', '.join(whisper.aggregationMethods))
option_parser.add_option('--overwrite', default=False, action='store_true')
option_parser.add_option('--estimate', default=False, action='store_true',
                         help="Don't create a whisper file, estimate storage "
                              "requirements based on archive definitions")
option_parser.add_option('--sparse', default=False, action='store_true',
                         help="Create new whisper as sparse file")
option_parser.add_option('--fallocate', default=False, action='store_true',
                         help="Create new whisper and use fallocate")

(options, args) = option_parser.parse_args()

if options.estimate:
  if len(args) == 0:
    option_parser.print_usage()
    sys.exit(1)
  if len(args) == 1 and args[0].find(",") > 0:
    args = args[0].split(",")

  archives = 0
  total_points = 0
  for (precision, points) in map(whisper.parseRetentionDef, args):
    print("Archive %s: %s points of %ss precision" % (archives, points, precision))
    archives += 1
    total_points += points

  size = 16 + (archives * 12) + (total_points * 12)
  disk_size = int(math.ceil(size / 4096.0) * 4096)
  print("\nEstimated Whisper DB Size: %s (%s bytes on disk with 4k blocks)\n" %
        (byte_format(size), disk_size))
  for x in [1, 5, 10, 50, 100, 500]:
    print("Estimated storage requirement for %sk metrics: %s" %
          (x, byte_format(x * 1000 * disk_size)))
  sys.exit(0)

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path = args[0]
archives = [whisper.parseRetentionDef(retentionDef)
            for retentionDef in args[1:]]

if os.path.exists(path) and options.overwrite:
  print('Overwriting existing file: %s' % path)
  os.unlink(path)

try:
  whisper.create(path, archives, xFilesFactor=options.xFilesFactor,
                 aggregationMethod=options.aggregationMethod, sparse=options.sparse,
                 useFallocate=options.fallocate)
except whisper.WhisperException as exc:
  raise SystemExit('[ERROR] %s' % str(exc))

size = os.stat(path).st_size
print('Created: %s (%d bytes)' % (path, size))

#!/usr/bin/env python

import sys
import time
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

now = int(time.time())

option_parser = optparse.OptionParser(
  usage='''%prog [options] path [timestamp:value]*

  If no values are passed as arguments, they are read one-per-line from stdin.''')

(options, args) = option_parser.parse_args()

if not args:
  option_parser.print_help()
  sys.exit(1)

path = args[0]
if len(args) >= 2:
  datapoint_strings = args[1:]
else:
  # no argv values, so read from stdin
  datapoint_strings = sys.stdin
datapoint_strings = [point.replace('N:', '%d:' % now)
                     for point in datapoint_strings]
datapoints = [tuple(point.split(':')) for point in datapoint_strings]

try:
  if len(datapoints) == 1:
    timestamp, value = datapoints[0]
    whisper.update(path, value, timestamp)
  else:
    whisper.update_many(path, datapoints)
except whisper.WhisperException as exc:
  raise SystemExit('[ERROR] %s' % str(exc))

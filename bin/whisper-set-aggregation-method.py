#!/usr/bin/env python

import sys
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
try:
  signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
  # windows?
  pass

option_parser = optparse.OptionParser(
    usage='%%prog path <%s> [xFilesFactor]' % '|'.join(whisper.aggregationMethods))

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path = args[0]
aggregationMethod = args[1]

xFilesFactor = None
if len(args) == 3:
  xFilesFactor = args[2]

try:
  oldAggregationMethod = whisper.setAggregationMethod(path, aggregationMethod, xFilesFactor)
except IOError:
  sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % path)
  option_parser.print_help()
  sys.exit(1)
except whisper.WhisperException as exc:
  raise SystemExit('[ERROR] %s' % str(exc))


print('Updated aggregation method: %s (%s -> %s)' % (path, oldAggregationMethod, aggregationMethod))

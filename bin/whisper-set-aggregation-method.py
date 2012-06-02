#!/usr/bin/env python

import os
import sys
from optparse import OptionParser

try:
    import whisper
except ImportError:
    raise SystemExit('[ERROR] Please make sure whisper is installed properly')


option_parser = optparse.OptionParser(
    usage='%%prog path <%s>' % '|'.join(whisper.aggregationMethods))

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_usage()
  sys.exit(1)

path = args[0]
aggregationMethod = args[1]

try:
  oldAggregationMethod = whisper.setAggregationMethod(path, aggregationMethod)
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))


print 'Updated aggregation method: %s (%s -> %s)' % (path,oldAggregationMethod,aggregationMethod)

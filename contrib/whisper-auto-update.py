#!/usr/bin/env python

import sys
import time
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# update this callback to do the logic you want.
# a future version could use a config while in which this fn is defined.


def update_value(timestamp, value):
  if value is None:
    return value
  return value * 1024 * 1024 * 1024


# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

now = int(time.time())
yesterday = now - (60 * 60 * 24)

option_parser = optparse.OptionParser(usage='''%prog [options] path''')
option_parser.add_option(
  '--from', default=yesterday, type='int', dest='_from',
  help=("Unix epoch time of the beginning of "
        "your requested interval (default: 24 hours ago)"))
option_parser.add_option(
  '--until', default=now, type='int',
  help="Unix epoch time of the end of your requested interval (default: now)")
option_parser.add_option(
  '--pretty', default=False, action='store_true',
  help="Show human-readable timestamps instead of unix times")

(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_usage()
  sys.exit(1)

path = args[0]
from_time = int(options._from)
until_time = int(options.until)

try:
  data = whisper.fetch(path, from_time, until_time)
  if not data:
    raise SystemExit('No data in selected timerange')
  (timeInfo, values_old) = data
except whisper.WhisperException as exc:
  raise SystemExit('[ERROR] %s' % str(exc))

(start, end, step) = timeInfo
t = start
for value_old in values_old:
  value_str_old = str(value_old)
  value_new = update_value(t, value_old)
  value_str_new = str(value_new)
  if options.pretty:
    timestr = time.ctime(t)
  else:
    timestr = str(t)

  print("%s\t%s -> %s" % (timestr, value_str_old, value_str_new))
  try:
    if value_new is not None:
      whisper.update(path, value_new, t)
    t += step
  except whisper.WhisperException as exc:
    raise SystemExit('[ERROR] %s' % str(exc))

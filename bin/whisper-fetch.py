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

now = int( time.time() )
yesterday = now - (60 * 60 * 24)

option_parser = optparse.OptionParser(usage='''%prog [options] path''')
option_parser.add_option('--from', default=yesterday, type='int', dest='_from',
  help=("Unix epoch time of the beginning of "
        "your requested interval (default: 24 hours ago)"))
option_parser.add_option('--until', default=now, type='int',
  help="Unix epoch time of the end of your requested interval (default: now)")
option_parser.add_option('--json', default=False, action='store_true',
  help="Output results in JSON form")
option_parser.add_option('--pretty', default=False, action='store_true',
  help="Show human-readable timestamps instead of unix times")

(options, args) = option_parser.parse_args()

if len(args) != 1:
  option_parser.print_usage()
  sys.exit(1)

path = args[0]

from_time = int( options._from )
until_time = int( options.until )


try:
  (timeInfo, values) = whisper.fetch(path, from_time, until_time)
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))


(start,end,step) = timeInfo

if options.json:
  values_json = str(values).replace('None','null')
  print '''{
    "start" : %d,
    "end" : %d,
    "step" : %d,
    "values" : %s
  }''' % (start,end,step,values_json)
  sys.exit(0)

t = start
for value in values:
  if options.pretty:
    timestr = time.ctime(t)
  else:
    timestr = str(t)
  if value is None:
    valuestr = "None"
  else:
    valuestr = "%f" % value
  print "%s\t%s" % (timestr,valuestr)
  t += step

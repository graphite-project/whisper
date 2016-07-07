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

option_parser = optparse.OptionParser(usage='''%prog [options] path''')
option_parser.add_option('--json', default=False, action='store_true',
  help="Output results in JSON form")
option_parser.add_option('--pretty', default=False, action='store_true',
  help="Show human-readable timestamps instead of unix times")

(options, args) = option_parser.parse_args()

if len(args) != 1:
  option_parser.print_help()
  sys.exit(1)

path = args[0]

try:
  last_update = whisper.fetch_lastupdate(path, now)
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

if last_update is None:
  last_update = (0, None)

(t, v) = last_update

if options.json:
  value_json = str(v).replace('None','null')
  print '''{
  "timestamp" : %d,
  "value" : %s
}''' % (t, value_json)
  sys.exit(0)

if options.pretty:
  timestr = time.ctime(t)
else:
  timestr = str(t)
if v is None:
  valuestr = "None"
else:
  valuestr = "%f" % v
print "%s\t%s" % (timestr, valuestr)

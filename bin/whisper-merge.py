#!/usr/bin/env python

import os
import sys
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(
    usage='''%prog [options] from_path to_path''')
option_parser.add_option(
  '--from', default=None, type='int', dest='_from',
  help=("Begining of interval, unix timestamp (default: epoch)"))
option_parser.add_option(
  '--until', default=None, type='int',
  help="End of interval, unix timestamp (default: now)")

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path_from = args[0]
path_to = args[1]

for filename in (path_from, path_to):
  if not os.path.exists(filename):
    raise SystemExit('[ERROR] File "%s" does not exist!' % filename)

whisper.merge(path_from, path_to, options._from, options.until)

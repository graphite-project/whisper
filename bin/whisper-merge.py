#!/usr/bin/env python

import sys
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')


option_parser = optparse.OptionParser(
    usage='''%prog [options] from_path to_path''')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_usage()
  sys.exit(1)

path_from = args[0]
path_to = args[1]

whisper.merge(path_from, path_to)

#!/usr/bin/env python

import os
import sys
import signal
import optparse
import json

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
try:
  signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
  # OS=windows
  pass

option_parser = optparse.OptionParser(usage='''%prog [options] path [field]''')
option_parser.add_option('--json', default=False, action='store_true',
                         help="Output results in JSON form")
(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_help()
  sys.exit(1)

path = args[0]
if len(args) > 1:
  field = args[1]
else:
  field = None

try:
  info = whisper.info(path)
except whisper.WhisperException as exc:
  raise SystemExit('[ERROR] %s' % str(exc))

info['fileSize'] = os.stat(path).st_size

if field:
  if field not in info:
    print('Unknown field "%s". Valid fields are %s' % (field, ','.join(info)))
    sys.exit(1)

  print(info[field])
  sys.exit(0)

if options.json:
  print(json.dumps(info, indent=2, separators=(',', ': ')))
else:
  archives = info.pop('archives')
  for key, value in info.items():
    print('%s: %s' % (key, value))
  print('')

  for i, archive in enumerate(archives):
    print('Archive %d' % i)
    for key, value in archive.items():
      print('%s: %s' % (key, value))
    print('')

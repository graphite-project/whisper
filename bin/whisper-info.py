#!/usr/bin/env python

import os
import sys
import signal
import optparse

try:
  import whisper
  from whisper import log
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
try:
  signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
  #OS=windows
  pass

option_parser = optparse.OptionParser(usage='''%prog path [field]''')
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
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

info['fileSize'] = os.stat(path).st_size

if field:
  if field not in info:
    log.error('Unknown field "%s". Valid fields are %s' % (field, ','.join(info)))
    sys.exit(1)

  log.info(info[field])
  sys.exit(0)


archives = info.pop('archives')
for key,value in info.items():
  log.info('%s: %s' % (key,value))

for i,archive in enumerate(archives):
  log.info('Archive %d' % i)
  for key,value in archive.items():
    log.info('%s: %s' % (key,value))

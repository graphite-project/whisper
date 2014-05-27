#!/usr/bin/env python

import os
import sys
import signal
import optparse
import shutil

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(
    usage='''%prog [options] from_path to_path''')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path_from = args[0]
path_to = args[1]


for filename in (path_from, path_to):
   if not os.path.exists(filename):
       raise SystemExit('[ERROR] File "%s" does not exist!' % filename)


if os.path.isdir(path_from) and os.path.isdir(path_to):
  for root, dirs, files in os.walk(path_from):
    for file in files:
      fullname = os.path.join(root, file)
      relpath = os.path.relpath(fullname, path_from)
      other_file = os.path.join(path_to, relpath)

      if os.path.exists(other_file):
        print 'Merging', relpath
        whisper.merge(fullname, other_file)
      else:
        dirname = os.path.dirname(other_file)
        if not os.path.exists(dirname):
          os.makedirs(dirname)
        print 'Copying', relpath, 'to', path_to
        shutil.copy(fullname, other_file)
else:
   whisper.merge(path_from, path_to)

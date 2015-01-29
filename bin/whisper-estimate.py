#!/usr/bin/env python

import sys
import optparse
import math

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

def byte_format(num):
  for x in ['bytes','KB','MB']:
    if num < 1024.0:
      return "%.3f%s" % (num, x)
    num /= 1024.0
  return "%.3f%s" % (num, 'GB')

option_parser = optparse.OptionParser(usage='''%prog timePerPoint:timeToStore [timePerPoint:timeToStore]*''')
(options, args) = option_parser.parse_args()

if len(args) == 0:
  option_parser.print_usage()
  sys.exit(1)
if len(args) == 1 and args[0].find(",") > 0:
  args = args[0].split(",")

archives = 0
total_points = 0
for (precision, points) in map(whisper.parseRetentionDef, args):
  print "Archive %s: %s points of %ss precision" % (archives, points, precision)
  archives += 1
  total_points += points

size = 16 + (archives * 12) + (total_points * 12)
disk_size = int(math.ceil(size / 4096.0) * 4096)
print "\nEstimated Whisper DB Size: %s (%s bytes on disk with 4k blocks)\n" % (byte_format(size), disk_size)
for x in [1, 5, 10, 50, 100, 500]:
  print "Estimated storage requirement for %sk metrics: %s" % (x, byte_format(x * 1000 * disk_size))

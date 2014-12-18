#!/usr/bin/env python

import struct
import signal
import optparse

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(usage='''%prog path''')
(options, args) = option_parser.parse_args()

if len(args) != 1:
  option_parser.error("require one input file name")
else:
  path = args[0]

def dump_header(header):
  print 'Meta data:'
  print '  aggregation method: %s' % header['aggregationMethod']
  print '  max retention: %d' % header['maxRetention']
  print '  xFilesFactor: %g' % header['xFilesFactor']
  print
  dump_archive_headers(header['archives'])

def dump_archive_headers(archives):
  for i,archive in enumerate(archives):
    print 'Archive %d info:' % i
    print '  offset: %d' % archive['offset']
    print '  seconds per point: %d' % archive['secondsPerPoint']
    print '  points: %d' % archive['points']
    print '  retention: %d' % archive['retention']
    print '  size: %d' % archive['size']
    print

def dump_archives(fm, archives):
  for i,archive in enumerate(archives):
    print 'Archive %d data:' %i
    offset = archive['offset']
    for point in xrange(archive['points']):
      (timestamp, value) = struct.unpack(whisper.pointFormat, fm['map'][offset:offset+whisper.pointSize])
      print '%d: %d, %10.35g' % (point, timestamp, value)
      offset += whisper.pointSize
    print

header = whisper.info(path)
dump_header(header)
fm = whisper.map_path(path, 'r')
dump_archives(fm, header['archives'])

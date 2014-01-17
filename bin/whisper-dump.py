#!/usr/bin/env python

import os
import mmap
import struct
import signal
import optparse

try:
  import whisper
  from whisper import Log
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

def mmap_file(filename):
  fd = os.open(filename, os.O_RDONLY)
  map = mmap.mmap(fd, os.fstat(fd).st_size, prot=mmap.PROT_READ)
  os.close(fd)
  return map

def read_header(map):
  try:
    (aggregationType,maxRetention,xFilesFactor,archiveCount) = struct.unpack(whisper.metadataFormat,map[:whisper.metadataSize])
  except:
    raise CorruptWhisperFile("Unable to unpack header")

  archives = []
  archiveOffset = whisper.metadataSize

  for i in xrange(archiveCount):
    try:
      (offset, secondsPerPoint, points) = struct.unpack(whisper.archiveInfoFormat, map[archiveOffset:archiveOffset+whisper.archiveInfoSize])
    except:
      raise CorruptWhisperFile("Unable to read archive %d metadata" % i)

    archiveInfo = {
      'offset' : offset,
      'secondsPerPoint' : secondsPerPoint,
      'points' : points,
      'retention' : secondsPerPoint * points,
      'size' : points * whisper.pointSize,
    }
    archives.append(archiveInfo)
    archiveOffset += whisper.archiveInfoSize

  header = {
    'aggregationMethod' : whisper.aggregationTypeToMethod.get(aggregationType, 'average'),
    'maxRetention' : maxRetention,
    'xFilesFactor' : xFilesFactor,
    'archives' : archives,
  }
  return header

def dump_header(header):
  Log.info('Meta data:')
  Log.info('  aggregation method: %s' % header['aggregationMethod'])
  Log.info('  max retention: %d' % header['maxRetention'])
  Log.info('  xFilesFactor: %g' % header['xFilesFactor'])
  print
  dump_archive_headers(header['archives'])

def dump_archive_headers(archives):
  for i,archive in enumerate(archives):
    Log.info('Archive %d info:' % i)
    Log.info('  offset: %d' % archive['offset'])
    Log.info('  seconds per point: %d' % archive['secondsPerPoint'])
    Log.info('  points: %d' % archive['points'])
    Log.info('  retention: %d' % archive['retention'])
    Log.info('  size: %d' % archive['size'])

def dump_archives(archives):
  for i,archive in enumerate(archives):
    Log.info('Archive %d data:' %i)
    offset = archive['offset']
    for point in xrange(archive['points']):
      (timestamp, value) = struct.unpack(whisper.pointFormat, map[offset:offset+whisper.pointSize])
      Log.info('%d: %d, %10.35g' % (point, timestamp, value))
      offset += whisper.pointSize

if not os.path.exists(path):
  raise SystemExit('[ERROR] File "%s" does not exist!' % path)

map = mmap_file(path)
header = read_header(map)
dump_header(header)
dump_archives(header['archives'])

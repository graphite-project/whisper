#!/usr/bin/python

import os
import mmap
import struct
import sys
import math
import bisect
import signal
import optparse
import traceback

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)


option_parser = optparse.OptionParser(
    usage='''%prog path 

''')

option_parser.add_option(
    '--xFilesFactor', default=None,
    type='float', help="Change the xFilesFactor")
option_parser.add_option(
    '--aggregationMethod', default=None,
    type='string', help="Change the aggregation function (%s)" %
    ', '.join(whisper.aggregationMethods))
option_parser.add_option(
    '--force', default=False, action='store_true',
    help="Perform a destructive change")
option_parser.add_option(
    '--newfile', default=None, action='store',
    help="Create a new database file without removing the existing one")
option_parser.add_option(
    '--nobackup', action='store_true',
    help='Delete the .bak file after successful execution')

(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_help()
  sys.exit(1)

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
    raise whisper.CorruptWhisperFile("Unable to unpack header")

  archives = []
  archiveOffset = whisper.metadataSize

  for i in xrange(archiveCount):
    try:
      (offset, secondsPerPoint, points) = struct.unpack(whisper.archiveInfoFormat, map[archiveOffset:archiveOffset+whisper.archiveInfoSize])
    except:
      raise whisper.CorruptWhisperFile("Unable to read archive %d metadata" % i)

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

def file_update_archive(fh,archive,points):
  step = archive['secondsPerPoint']
  alignedPoints = [ (timestamp - (timestamp % step), value)
                    for (timestamp,value) in points ]
  #Create a packed string for each contiguous sequence of points
  packedStrings = []
  previousInterval = None
  currentString = ""
  lenAlignedPoints = len(alignedPoints)
  for i in xrange(0,lenAlignedPoints):
    #take last point in run of points with duplicate intervals
    if i+1 < lenAlignedPoints and alignedPoints[i][0] == alignedPoints[i+1][0]:
      continue
    (interval,value) = alignedPoints[i]
    if (not previousInterval) or (interval == previousInterval + step):
      currentString += struct.pack(whisper.pointFormat,interval,value)
      previousInterval = interval
    else:
      numberOfPoints = len(currentString) / whisper.pointSize
      startInterval = previousInterval - (step * (numberOfPoints-1))
      packedStrings.append( (startInterval,currentString) )
      currentString = struct.pack(whisper.pointFormat,interval,value)
      previousInterval = interval
  if currentString:
    numberOfPoints = len(currentString) / whisper.pointSize
    startInterval = previousInterval - (step * (numberOfPoints-1))
    packedStrings.append( (startInterval,currentString) )

  #Read base point and determine where our writes will start
  fh.seek(archive['offset'])
  packedBasePoint = fh.read(whisper.pointSize)
  (baseInterval,baseValue) = struct.unpack(whisper.pointFormat,packedBasePoint)
  if baseInterval == 0: #This file's first update
    baseInterval = packedStrings[0][0] #use our first string as the base, so we start at the start

  #Write all of our packed strings in locations determined by the baseInterval
  for (interval,packedString) in packedStrings:
    timeDistance = interval - baseInterval
    pointDistance = timeDistance / step
    byteDistance = pointDistance * whisper.pointSize
    myOffset = archive['offset'] + (byteDistance % archive['size'])
    fh.seek(myOffset)
    archiveEnd = archive['offset'] + archive['size']
    bytesBeyond = (myOffset + len(packedString)) - archiveEnd

    if bytesBeyond > 0:
      fh.write( packedString[:-bytesBeyond] )
      assert fh.tell() == archiveEnd, "archiveEnd=%d fh.tell=%d bytesBeyond=%d len(packedString)=%d" % (archiveEnd,fh.tell(),bytesBeyond,len(packedString))
      fh.seek( archive['offset'] )
      fh.write( packedString[-bytesBeyond:] ) #safe because it can't exceed the archive (retention checking logic above)
    else:
      fh.write(packedString)

if not os.path.exists(path):
  sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % path)
  option_parser.print_help()
  sys.exit(1)
else:
  map_orig = mmap_file(path)


header_orig = read_header(map_orig)

archives_orig = header_orig['archives']

new_archives = []
for ar in archives_orig:
  new_archives.append((ar['secondsPerPoint'],ar['points']))

# sort by precision, lowest to highest
archives_orig.sort(key=lambda a: a['secondsPerPoint'], reverse=True)


if options.xFilesFactor is None:
  xff = header_orig['xFilesFactor']
else:
  xff = options.xFilesFactor

if options.aggregationMethod is None:
  aggregationMethod = 'sum'
else:
  aggregationMethod = options.aggregationMethod

print 'Retrieving all data from the archives'

if options.newfile is None:
  tmpfile = path + '.tmp'
  if os.path.exists(tmpfile):
    print 'Removing previous temporary database file: %s' % tmpfile
    os.unlink(tmpfile)
  newfile = tmpfile
else:
  newfile = options.newfile

print 'Creating new whisper database: %s' % newfile
whisper.create(newfile, new_archives, xFilesFactor=xff, aggregationMethod=aggregationMethod)
size = os.stat(newfile).st_size
print 'Created: %s (%d bytes)' % (newfile,size)

fh = open(newfile,'r+b')

print 'Converting data...'
for i,archive in enumerate(archives_orig):
  print 'Transformating Archive %d data:' %i
  offset = archive['offset']
  multiply = archive['secondsPerPoint']
  datapoints = []
  for point in xrange(archive['points']):
    (timestamp, value) = struct.unpack(whisper.pointFormat, map_orig[offset:offset+whisper.pointSize])
    #print '%d: %d, %10.35g' % (point, timestamp, value)
    offset += whisper.pointSize
    if value is not None:
      datapoints.append((timestamp, value*multiply));
  file_update_archive(fh,archive,datapoints)

if options.newfile is not None:
  sys.exit(0)

backup = path + '.bak'
print 'Renaming old database to: %s' % backup
os.rename(path, backup)

try:
  print 'Renaming new database to: %s' % path
  os.rename(tmpfile, path)
except:
  traceback.print_exc()
  print '\nOperation failed, restoring backup'
  os.rename(backup, path)
  sys.exit(1)

if options.nobackup:
  print "Unlinking backup: %s" % backup
  os.unlink(backup)

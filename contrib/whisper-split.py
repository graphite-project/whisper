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
this tool split a whisper file in as many other as archive has each one
''')

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

# sort by precision, highest to lowest 
archives_orig.sort(key=lambda a: a['secondsPerPoint'], reverse=False)


for i,ar in enumerate(archives_orig):
  newfile = "%s_archive%s.wsp" % ( os.path.splitext(path)[0] , i ) 
  if os.path.exists(newfile):
    print 'Removing previous database file: %s' % newfile
    os.unlink(newfile)
  new_archives=[]
  new_archives.append((ar['secondsPerPoint'],ar['points']))
  print 'Creating new whisper database: %s' % newfile
  whisper.create(newfile, new_archives, xFilesFactor=header_orig['xFilesFactor'], aggregationMethod=header_orig['aggregationMethod'])
  size = os.stat(newfile).st_size
  print 'Created: %s (%d bytes)' % (newfile,size)
  map_dest = mmap_file(newfile)
  header_dest = read_header(map_dest)
  archive_dest = header_dest['archives'][0];
  fh = open(newfile,'r+b')
  print 'Migrating data...'
  offset = ar['offset']
  datapoints = []
  for point in xrange(ar['points']):
    (timestamp, value) = struct.unpack(whisper.pointFormat, map_orig[offset:offset+whisper.pointSize])
    offset += whisper.pointSize
    if value is not None:
      datapoints.append((timestamp, value))
  file_update_archive(fh,archive_dest,datapoints)
  fh.close()
sys.exit(0)


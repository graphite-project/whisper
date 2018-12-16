# Copyright 2009-Present The Graphite Development Team
# Copyright 2008 Orbitz WorldWide
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# This module is an implementation of the Whisper database API
# Here is the basic layout of a whisper data file
#
# File = Header,Data
#   Header = Metadata,ArchiveInfo+
#       Metadata = aggregationType,maxRetention,xFilesFactor,archiveCount
#       ArchiveInfo = Offset,SecondsPerPoint,Points
#   Data = Archive+
#       Archive = Point+
#           Point = timestamp,value

import itertools
import operator
import os
import platform
import re
import struct
import sys
import time

izip = getattr(itertools, 'izip', zip)
ifilter = getattr(itertools, 'ifilter', filter)

if sys.version_info >= (3, 0):
  xrange = range

try:
  import fcntl
  CAN_LOCK = True
except ImportError:
  CAN_LOCK = False

try:
  import ctypes
  import ctypes.util
  CAN_FALLOCATE = True
except ImportError:
  CAN_FALLOCATE = False

try:
  if sys.version_info >= (3, 0):
    from os import posix_fadvise, POSIX_FADV_RANDOM
  else:
    from fadvise import posix_fadvise, POSIX_FADV_RANDOM
  CAN_FADVISE = True
except ImportError:
  CAN_FADVISE = False

fallocate = None

if CAN_FALLOCATE:
  libc_name = ctypes.util.find_library('c')
  libc = ctypes.CDLL(libc_name)
  c_off64_t = ctypes.c_int64
  c_off_t = ctypes.c_int

  if platform.uname()[0] == 'FreeBSD':
    # offset type is 64-bit on FreeBSD 32-bit & 64-bit platforms to address files more than 2GB
    c_off_t = ctypes.c_int64

  try:
    _fallocate = libc.posix_fallocate64
    _fallocate.restype = ctypes.c_int
    _fallocate.argtypes = [ctypes.c_int, c_off64_t, c_off64_t]
  except AttributeError:
    try:
      _fallocate = libc.posix_fallocate
      _fallocate.restype = ctypes.c_int
      _fallocate.argtypes = [ctypes.c_int, c_off_t, c_off_t]
    except AttributeError:
      CAN_FALLOCATE = False

  if CAN_FALLOCATE:
    def _py_fallocate(fd, offset, len_):
      res = _fallocate(fd.fileno(), offset, len_)
      if res != 0:
        raise IOError(res, 'fallocate')
    fallocate = _py_fallocate
  del libc
  del libc_name

LOCK = False
CACHE_HEADERS = False
AUTOFLUSH = False
FADVISE_RANDOM = False
# Buffering setting applied to all operations that do *not* require
# a full scan of the file in order to minimize cache thrashing.
BUFFERING = 0
__headerCache = {}

longFormat = "!L"
longSize = struct.calcsize(longFormat)
floatFormat = "!f"
floatSize = struct.calcsize(floatFormat)
valueFormat = "!d"
valueSize = struct.calcsize(valueFormat)
pointFormat = "!Ld"
pointSize = struct.calcsize(pointFormat)
metadataFormat = "!2LfL"
metadataSize = struct.calcsize(metadataFormat)
archiveInfoFormat = "!3L"
archiveInfoSize = struct.calcsize(archiveInfoFormat)

aggregationTypeToMethod = dict({
  1: 'average',
  2: 'sum',
  3: 'last',
  4: 'max',
  5: 'min',
  6: 'avg_zero',
  7: 'absmax',
  8: 'absmin'
})
aggregationMethodToType = dict([[v, k] for k, v in aggregationTypeToMethod.items()])
aggregationMethods = aggregationTypeToMethod.values()

debug = startBlock = endBlock = lambda *a, **k: None

UnitMultipliers = {
  'seconds': 1,
  'minutes': 60,
  'hours': 3600,
  'days': 86400,
  'weeks': 86400 * 7,
  'years': 86400 * 365
}


def getUnitString(s):
  for value in ('seconds', 'minutes', 'hours', 'days', 'weeks', 'years'):
    if value.startswith(s):
      return value
  raise ValueError("Invalid unit '%s'" % s)


def parseRetentionDef(retentionDef):
  try:
    (precision, points) = retentionDef.strip().split(':', 1)
  except ValueError:
    raise ValueError("Invalid retention definition '%s'" % retentionDef)

  if precision.isdigit():
    precision = int(precision) * UnitMultipliers[getUnitString('s')]
  else:
    precision_re = re.compile(r'^(\d+)([a-z]+)$')
    match = precision_re.match(precision)
    if match:
      precision = int(match.group(1)) * UnitMultipliers[getUnitString(match.group(2))]
    else:
      raise ValueError("Invalid precision specification '%s'" % precision)

  if points.isdigit():
    points = int(points)
  else:
    points_re = re.compile(r'^(\d+)([a-z]+)$')
    match = points_re.match(points)
    if match:
      points = int(match.group(1)) * UnitMultipliers[getUnitString(match.group(2))] // precision
    else:
      raise ValueError("Invalid retention specification '%s'" % points)

  return (precision, points)


class WhisperException(Exception):

  """Base class for whisper exceptions."""


class InvalidConfiguration(WhisperException):

  """Invalid configuration."""


class InvalidAggregationMethod(WhisperException):

  """Invalid aggregation method."""


class InvalidTimeInterval(WhisperException):

  """Invalid time interval."""


class InvalidXFilesFactor(WhisperException):

  """Invalid xFilesFactor."""


class TimestampNotCovered(WhisperException):

  """Timestamp not covered by any archives in this database."""


class CorruptWhisperFile(WhisperException):

  def __init__(self, error, path):
    Exception.__init__(self, error)
    self.error = error
    self.path = path

  def __repr__(self):
    return "<CorruptWhisperFile[%s] %s>" % (self.path, self.error)

  def __str__(self):
    return "%s (%s)" % (self.error, self.path)


def disableDebug():
  """ Disable writing IO statistics to stdout """
  global open
  try:
    open = _open
  except NameError:
    pass


def enableDebug():
  """ Enable writing IO statistics to stdout """
  global open, _open, debug, startBlock, endBlock
  _open = open

  class open(object):
    def __init__(self, *args, **kwargs):
      self.f = _open(*args, **kwargs)
      self.writeCount = 0
      self.readCount = 0

    def __enter__(self):
      return self

    def __exit__(self, *args):
      self.f.close()

    def write(self, data):
      self.writeCount += 1
      debug('WRITE %d bytes #%d' % (len(data), self.writeCount))
      return self.f.write(data)

    def read(self, size):
      self.readCount += 1
      debug('READ %d bytes #%d' % (size, self.readCount))
      return self.f.read(size)

    def __getattr__(self, attr):
      return getattr(self.f, attr)

  def debug(message):
    print('DEBUG :: %s' % message)

  __timingBlocks = {}

  def startBlock(name):
    __timingBlocks[name] = time.time()

  def endBlock(name):
    debug("%s took %.5f seconds" % (name, time.time() - __timingBlocks.pop(name)))


def __readHeader(fh):
  if CACHE_HEADERS:
    info = __headerCache.get(fh.name)
    if info:
      return info

  originalOffset = fh.tell()
  fh.seek(0)
  packedMetadata = fh.read(metadataSize)

  try:
    (aggregationType, maxRetention, xff, archiveCount) \
        = struct.unpack(metadataFormat, packedMetadata)
  except (struct.error, ValueError, TypeError):
    raise CorruptWhisperFile("Unable to read header", fh.name)

  try:
    aggregationTypeToMethod[aggregationType]
  except KeyError:
    raise CorruptWhisperFile("Unable to read header", fh.name)

  if not 0 <= xff <= 1:
    raise CorruptWhisperFile("Unable to read header", fh.name)

  archives = []

  for i in xrange(archiveCount):
    packedArchiveInfo = fh.read(archiveInfoSize)
    try:
      (offset, secondsPerPoint, points) = struct.unpack(archiveInfoFormat, packedArchiveInfo)
    except (struct.error, ValueError, TypeError):
      raise CorruptWhisperFile("Unable to read archive%d metadata" % i, fh.name)

    archiveInfo = {
      'offset': offset,
      'secondsPerPoint': secondsPerPoint,
      'points': points,
      'retention': secondsPerPoint * points,
      'size': points * pointSize,
    }
    archives.append(archiveInfo)

  fh.seek(originalOffset)
  info = {
    'aggregationMethod': aggregationTypeToMethod.get(aggregationType, 'average'),
    'maxRetention': maxRetention,
    'xFilesFactor': xff,
    'archives': archives,
  }
  if CACHE_HEADERS:
    __headerCache[fh.name] = info

  return info


def setXFilesFactor(path, xFilesFactor):
  """Sets the xFilesFactor for file in path

  path is a string pointing to a whisper file
  xFilesFactor is a float between 0 and 1

  returns the old xFilesFactor
  """

  (_, old_xff) = __setAggregation(path, xFilesFactor=xFilesFactor)

  return old_xff


def setAggregationMethod(path, aggregationMethod, xFilesFactor=None):
  """Sets the aggregationMethod for file in path

  path is a string pointing to the whisper file
  aggregationMethod specifies the method to use when propagating data (see
  ``whisper.aggregationMethods``)
  xFilesFactor specifies the fraction of data points in a propagation interval
  that must have known values for a propagation to occur. If None, the
  existing xFilesFactor in path will not be changed

  returns the old aggregationMethod
  """

  (old_agm, _) = __setAggregation(path, aggregationMethod, xFilesFactor)

  return old_agm


def __setAggregation(path, aggregationMethod=None, xFilesFactor=None):
  """ Set aggregationMethod and or xFilesFactor for file in path"""

  with open(path, 'r+b', BUFFERING) as fh:
    if LOCK:
      fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    info = __readHeader(fh)

    if xFilesFactor is None:
      xFilesFactor = info['xFilesFactor']

    if aggregationMethod is None:
      aggregationMethod = info['aggregationMethod']

    __writeHeaderMetadata(fh, aggregationMethod, info['maxRetention'],
                          xFilesFactor, len(info['archives']))

    if AUTOFLUSH:
      fh.flush()
      os.fsync(fh.fileno())

    if CACHE_HEADERS and fh.name in __headerCache:
      del __headerCache[fh.name]

  return (info['aggregationMethod'], info['xFilesFactor'])


def __writeHeaderMetadata(fh, aggregationMethod, maxRetention, xFilesFactor, archiveCount):
  """ Writes header metadata to fh """

  try:
    aggregationType = aggregationMethodToType[aggregationMethod]
  except KeyError:
    raise InvalidAggregationMethod("Unrecognized aggregation method: %s" %
                                   aggregationMethod)

  try:
    xFilesFactor = float(xFilesFactor)
  except ValueError:
    raise InvalidXFilesFactor("Invalid xFilesFactor %s, not a float" %
                              xFilesFactor)

  if xFilesFactor < 0 or xFilesFactor > 1:
    raise InvalidXFilesFactor("Invalid xFilesFactor %s, not between 0 and 1" %
                              xFilesFactor)

  aggregationType = struct.pack(longFormat, aggregationType)
  maxRetention = struct.pack(longFormat, maxRetention)
  xFilesFactor = struct.pack(floatFormat, xFilesFactor)
  archiveCount = struct.pack(longFormat, archiveCount)

  packedMetadata = aggregationType + maxRetention + xFilesFactor + archiveCount

  fh.seek(0)
  fh.write(packedMetadata)


def validateArchiveList(archiveList):
  """ Validates an archiveList.
  An ArchiveList must:
  1. Have at least one archive config. Example: (60, 86400)
  2. No archive may be a duplicate of another.
  3. Higher precision archives' precision must evenly divide all lower
     precision archives' precision.
  4. Lower precision archives must cover larger time intervals than higher
     precision archives.
  5. Each archive must have at least enough points to consolidate to the next
     archive

  Returns True or False
  """

  if not archiveList:
    raise InvalidConfiguration("You must specify at least one archive configuration!")

  archiveList.sort(key=lambda a: a[0])  # Sort by precision (secondsPerPoint)

  for i, archive in enumerate(archiveList):
    if i == len(archiveList) - 1:
      break

    nextArchive = archiveList[i + 1]
    if not archive[0] < nextArchive[0]:
      raise InvalidConfiguration(
        "A Whisper database may not be configured having "
        "two archives with the same precision (archive%d: %s, archive%d: %s)" %
        (i, archive, i + 1, nextArchive))

    if nextArchive[0] % archive[0] != 0:
      raise InvalidConfiguration(
        "Higher precision archives' precision "
        "must evenly divide all lower precision archives' precision "
        "(archive%d: %s, archive%d: %s)" %
        (i, archive[0], i + 1, nextArchive[0]))

    retention = archive[0] * archive[1]
    nextRetention = nextArchive[0] * nextArchive[1]

    if not nextRetention > retention:
      raise InvalidConfiguration(
        "Lower precision archives must cover "
        "larger time intervals than higher precision archives "
        "(archive%d: %s seconds, archive%d: %s seconds)" %
        (i, retention, i + 1, nextRetention))

    archivePoints = archive[1]
    pointsPerConsolidation = nextArchive[0] // archive[0]
    if not archivePoints >= pointsPerConsolidation:
      raise InvalidConfiguration(
        "Each archive must have at least enough points "
        "to consolidate to the next archive (archive%d consolidates %d of "
        "archive%d's points but it has only %d total points)" %
        (i + 1, pointsPerConsolidation, i, archivePoints))


def create(path, archiveList, xFilesFactor=None, aggregationMethod=None,
           sparse=False, useFallocate=False):
  """create(path,archiveList,xFilesFactor=0.5,aggregationMethod='average')

  path               is a string
  archiveList        is a list of archives, each of which is of the form
                     (secondsPerPoint, numberOfPoints)
  xFilesFactor       specifies the fraction of data points in a propagation interval
                     that must have known values for a propagation to occur
  aggregationMethod  specifies the function to use when propagating data (see
                     ``whisper.aggregationMethods``)
  """
  # Set default params
  if xFilesFactor is None:
    xFilesFactor = 0.5
  if aggregationMethod is None:
    aggregationMethod = 'average'

  # Validate archive configurations...
  validateArchiveList(archiveList)

  # Looks good, now we create the file and write the header
  if os.path.exists(path):
    raise InvalidConfiguration("File %s already exists!" % path)

  with open(path, 'wb', BUFFERING) as fh:
    try:
      if LOCK:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
      if CAN_FADVISE and FADVISE_RANDOM:
        posix_fadvise(fh.fileno(), 0, 0, POSIX_FADV_RANDOM)

      oldest = max([secondsPerPoint * points for secondsPerPoint, points in archiveList])

      __writeHeaderMetadata(fh, aggregationMethod, oldest, xFilesFactor,
                            len(archiveList))

      headerSize = metadataSize + (archiveInfoSize * len(archiveList))
      archiveOffsetPointer = headerSize

      for secondsPerPoint, points in archiveList:
        archiveInfo = struct.pack(archiveInfoFormat, archiveOffsetPointer, secondsPerPoint, points)
        fh.write(archiveInfo)
        archiveOffsetPointer += (points * pointSize)

      # If configured to use fallocate and capable of fallocate use that, else
      # attempt sparse if configure or zero pre-allocate if sparse isn't configured.
      if CAN_FALLOCATE and useFallocate:
        remaining = archiveOffsetPointer - headerSize
        fallocate(fh, headerSize, remaining)
      elif sparse:
        fh.seek(archiveOffsetPointer - 1)
        fh.write(b'\x00')
      else:
        remaining = archiveOffsetPointer - headerSize
        chunksize = 16384
        zeroes = b'\x00' * chunksize
        while remaining > chunksize:
          fh.write(zeroes)
          remaining -= chunksize
        fh.write(zeroes[:remaining])

      if AUTOFLUSH:
        fh.flush()
        os.fsync(fh.fileno())
      # Explicitly close the file to catch IOError on close()
      fh.close()
    except IOError:
      # if we got an IOError above, the file is either empty or half created.
      # Better off deleting it to avoid surprises later
      os.unlink(fh.name)
      raise


def aggregate(aggregationMethod, knownValues, neighborValues=None):
  if aggregationMethod == 'average':
    return float(sum(knownValues)) / float(len(knownValues))
  elif aggregationMethod == 'sum':
    return float(sum(knownValues))
  elif aggregationMethod == 'last':
    return knownValues[-1]
  elif aggregationMethod == 'max':
    return max(knownValues)
  elif aggregationMethod == 'min':
    return min(knownValues)
  elif aggregationMethod == 'avg_zero':
    if not neighborValues:
      raise InvalidAggregationMethod("Using avg_zero without neighborValues")
    values = [x or 0 for x in neighborValues]
    return float(sum(values)) / float(len(values))
  elif aggregationMethod == 'absmax':
    return max(knownValues, key=abs)
  elif aggregationMethod == 'absmin':
    return min(knownValues, key=abs)
  else:
    raise InvalidAggregationMethod(
      "Unrecognized aggregation method %s" % aggregationMethod)


def __propagate(fh, header, timestamp, higher, lower):
  aggregationMethod = header['aggregationMethod']
  xff = header['xFilesFactor']

  lowerIntervalStart = timestamp - (timestamp % lower['secondsPerPoint'])

  fh.seek(higher['offset'])
  packedPoint = fh.read(pointSize)
  try:
    (higherBaseInterval, higherBaseValue) = struct.unpack(pointFormat, packedPoint)
  except struct.error:
    raise CorruptWhisperFile("Unable to read base datapoint", fh.name)

  if higherBaseInterval == 0:
    higherFirstOffset = higher['offset']
  else:
    timeDistance = lowerIntervalStart - higherBaseInterval
    pointDistance = timeDistance // higher['secondsPerPoint']
    byteDistance = pointDistance * pointSize
    higherFirstOffset = higher['offset'] + (byteDistance % higher['size'])

  higherPoints = lower['secondsPerPoint'] // higher['secondsPerPoint']
  higherSize = higherPoints * pointSize
  relativeFirstOffset = higherFirstOffset - higher['offset']
  relativeLastOffset = (relativeFirstOffset + higherSize) % higher['size']
  higherLastOffset = relativeLastOffset + higher['offset']
  fh.seek(higherFirstOffset)

  if higherFirstOffset < higherLastOffset:  # We don't wrap the archive
    seriesString = fh.read(higherLastOffset - higherFirstOffset)
  else:  # We do wrap the archive
    higherEnd = higher['offset'] + higher['size']
    seriesString = fh.read(higherEnd - higherFirstOffset)
    fh.seek(higher['offset'])
    seriesString += fh.read(higherLastOffset - higher['offset'])

  # Now we unpack the series data we just read
  byteOrder, pointTypes = pointFormat[0], pointFormat[1:]
  points = len(seriesString) // pointSize
  seriesFormat = byteOrder + (pointTypes * points)
  try:
    unpackedSeries = struct.unpack(seriesFormat, seriesString)
  except struct.error:
    raise CorruptWhisperFile("Unable to read datapoints", fh.name)

  # And finally we construct a list of values
  neighborValues = [None] * points
  currentInterval = lowerIntervalStart
  step = higher['secondsPerPoint']

  for i in xrange(0, len(unpackedSeries), 2):
    pointTime = unpackedSeries[i]
    if pointTime == currentInterval:
      neighborValues[i // 2] = unpackedSeries[i + 1]
    currentInterval += step

  # Propagate aggregateValue to propagate from neighborValues if we have enough known points
  knownValues = [v for v in neighborValues if v is not None]
  if not knownValues:
    return False

  knownPercent = float(len(knownValues)) / float(len(neighborValues))
  if knownPercent >= xff:  # We have enough data to propagate a value!
    aggregateValue = aggregate(aggregationMethod, knownValues, neighborValues)
    myPackedPoint = struct.pack(pointFormat, lowerIntervalStart, aggregateValue)
    fh.seek(lower['offset'])
    packedPoint = fh.read(pointSize)
    try:
      (lowerBaseInterval, lowerBaseValue) = struct.unpack(pointFormat, packedPoint)
    except struct.error:
      raise CorruptWhisperFile("Unable to read base datapoint", fh.name)

    if lowerBaseInterval == 0:  # First propagated update to this lower archive
      fh.seek(lower['offset'])
      fh.write(myPackedPoint)
    else:  # Not our first propagated update to this lower archive
      timeDistance = lowerIntervalStart - lowerBaseInterval
      pointDistance = timeDistance // lower['secondsPerPoint']
      byteDistance = pointDistance * pointSize
      lowerOffset = lower['offset'] + (byteDistance % lower['size'])
      fh.seek(lowerOffset)
      fh.write(myPackedPoint)

    return True

  else:
    return False


def update(path, value, timestamp=None, now=None):
  """
  update(path, value, timestamp=None)

  path is a string
  value is a float
  timestamp is either an int or float
  """
  value = float(value)
  with open(path, 'r+b', BUFFERING) as fh:
    if CAN_FADVISE and FADVISE_RANDOM:
      posix_fadvise(fh.fileno(), 0, 0, POSIX_FADV_RANDOM)
    return file_update(fh, value, timestamp, now)


def file_update(fh, value, timestamp, now=None):
  if LOCK:
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

  header = __readHeader(fh)
  if now is None:
    now = int(time.time())
  if timestamp is None:
    timestamp = now

  timestamp = int(timestamp)
  diff = now - timestamp
  if not ((diff < header['maxRetention']) and diff >= 0):
    raise TimestampNotCovered(
      "Timestamp not covered by any archives in this database.")

  # Find the highest-precision archive that covers timestamp
  for i, archive in enumerate(header['archives']):
    if archive['retention'] < diff:
      continue
    # We'll pass on the update to these lower precision archives later
    lowerArchives = header['archives'][i + 1:]
    break

  # First we update the highest-precision archive
  myInterval = timestamp - (timestamp % archive['secondsPerPoint'])
  myPackedPoint = struct.pack(pointFormat, myInterval, value)
  fh.seek(archive['offset'])
  packedPoint = fh.read(pointSize)
  try:
    (baseInterval, baseValue) = struct.unpack(pointFormat, packedPoint)
  except struct.error:
    raise CorruptWhisperFile("Unable to read base datapoint", fh.name)

  if baseInterval == 0:  # This file's first update
    fh.seek(archive['offset'])
    fh.write(myPackedPoint)
    baseInterval = myInterval
  else:  # Not our first update
    timeDistance = myInterval - baseInterval
    pointDistance = timeDistance // archive['secondsPerPoint']
    byteDistance = pointDistance * pointSize
    myOffset = archive['offset'] + (byteDistance % archive['size'])
    fh.seek(myOffset)
    fh.write(myPackedPoint)

  # Now we propagate the update to lower-precision archives
  higher = archive
  for lower in lowerArchives:
    if not __propagate(fh, header, myInterval, higher, lower):
      break
    higher = lower

  if AUTOFLUSH:
    fh.flush()
    os.fsync(fh.fileno())


def update_many(path, points, now=None):
  """update_many(path,points)

path is a string
points is a list of (timestamp,value) points
"""
  if not points:
    return
  points = [(int(t), float(v)) for (t, v) in points]
  points.sort(key=lambda p: p[0], reverse=True)  # Order points by timestamp, newest first
  with open(path, 'r+b', BUFFERING) as fh:
    if CAN_FADVISE and FADVISE_RANDOM:
      posix_fadvise(fh.fileno(), 0, 0, POSIX_FADV_RANDOM)
    return file_update_many(fh, points, now)


def file_update_many(fh, points, now=None):
  if LOCK:
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

  header = __readHeader(fh)
  if now is None:
    now = int(time.time())
  archives = iter(header['archives'])
  currentArchive = next(archives)
  currentPoints = []

  for point in points:
    age = now - point[0]

    while currentArchive['retention'] < age:  # We can't fit any more points in this archive
      if currentPoints:  # Commit all the points we've found that it can fit
        currentPoints.reverse()  # Put points in chronological order
        __archive_update_many(fh, header, currentArchive, currentPoints)
        currentPoints = []
      try:
        currentArchive = next(archives)
      except StopIteration:
        currentArchive = None
        break

    if not currentArchive:
      break  # Drop remaining points that don't fit in the database

    currentPoints.append(point)

  # Don't forget to commit after we've checked all the archives
  if currentArchive and currentPoints:
    currentPoints.reverse()
    __archive_update_many(fh, header, currentArchive, currentPoints)

  if AUTOFLUSH:
    fh.flush()
    os.fsync(fh.fileno())


def __archive_update_many(fh, header, archive, points):
  step = archive['secondsPerPoint']
  alignedPoints = [(timestamp - (timestamp % step), value)
                   for (timestamp, value) in points]
  # Create a packed string for each contiguous sequence of points
  packedStrings = []
  previousInterval = None
  currentString = b""
  lenAlignedPoints = len(alignedPoints)
  for i in xrange(0, lenAlignedPoints):
    # Take last point in run of points with duplicate intervals
    if i + 1 < lenAlignedPoints and alignedPoints[i][0] == alignedPoints[i + 1][0]:
      continue
    (interval, value) = alignedPoints[i]
    if (not previousInterval) or (interval == previousInterval + step):
      currentString += struct.pack(pointFormat, interval, value)
      previousInterval = interval
    else:
      numberOfPoints = len(currentString) // pointSize
      startInterval = previousInterval - (step * (numberOfPoints - 1))
      packedStrings.append((startInterval, currentString))
      currentString = struct.pack(pointFormat, interval, value)
      previousInterval = interval
  if currentString:
    numberOfPoints = len(currentString) // pointSize
    startInterval = previousInterval - (step * (numberOfPoints - 1))
    packedStrings.append((startInterval, currentString))

  # Read base point and determine where our writes will start
  fh.seek(archive['offset'])
  packedBasePoint = fh.read(pointSize)
  try:
    (baseInterval, baseValue) = struct.unpack(pointFormat, packedBasePoint)
  except struct.error:
    raise CorruptWhisperFile("Unable to read base datapoint", fh.name)

  if baseInterval == 0:  # This file's first update
    # Use our first string as the base, so we start at the start
    baseInterval = packedStrings[0][0]

  # Write all of our packed strings in locations determined by the baseInterval
  for (interval, packedString) in packedStrings:
    timeDistance = interval - baseInterval
    pointDistance = timeDistance // step
    byteDistance = pointDistance * pointSize
    myOffset = archive['offset'] + (byteDistance % archive['size'])
    fh.seek(myOffset)
    archiveEnd = archive['offset'] + archive['size']
    bytesBeyond = (myOffset + len(packedString)) - archiveEnd

    if bytesBeyond > 0:
      fh.write(packedString[:-bytesBeyond])
      assert fh.tell() == archiveEnd, (
        "archiveEnd=%d fh.tell=%d bytesBeyond=%d len(packedString)=%d" %
        (archiveEnd, fh.tell(), bytesBeyond, len(packedString))
      )
      fh.seek(archive['offset'])
      # Safe because it can't exceed the archive (retention checking logic above)
      fh.write(packedString[-bytesBeyond:])
    else:
      fh.write(packedString)

  # Now we propagate the updates to lower-precision archives
  higher = archive
  lowerArchives = [arc for arc in header['archives']
                   if arc['secondsPerPoint'] > archive['secondsPerPoint']]

  for lower in lowerArchives:
    def fit(i):
      return i - (i % lower['secondsPerPoint'])
    lowerIntervals = [fit(p[0]) for p in alignedPoints]
    uniqueLowerIntervals = set(lowerIntervals)
    propagateFurther = False
    for interval in uniqueLowerIntervals:
      if __propagate(fh, header, interval, higher, lower):
        propagateFurther = True

    if not propagateFurther:
      break
    higher = lower


def info(path):
  """
  info(path)

  path is a string
  """
  try:
    with open(path, 'rb') as fh:
      return __readHeader(fh)
  except (IOError, OSError):
    pass
  return None


def fetch(path, fromTime, untilTime=None, now=None, archiveToSelect=None):
  """fetch(path,fromTime,untilTime=None,archiveToSelect=None)

path is a string
fromTime is an epoch time
untilTime is also an epoch time, but defaults to now.
archiveToSelect is the requested granularity, but defaults to None.

Returns a tuple of (timeInfo, valueList)
where timeInfo is itself a tuple of (fromTime, untilTime, step)

Returns None if no data can be returned
"""
  with open(path, 'rb') as fh:
    return file_fetch(fh, fromTime, untilTime, now, archiveToSelect)


def file_fetch(fh, fromTime, untilTime, now=None, archiveToSelect=None):
  header = __readHeader(fh)
  if now is None:
    now = int(time.time())
  if untilTime is None:
    untilTime = now
  fromTime = int(fromTime)
  untilTime = int(untilTime)

  # Here we try and be flexible and return as much data as we can.
  # If the range of data is from too far in the past or fully in the future, we
  # return nothing
  if fromTime > untilTime:
    raise InvalidTimeInterval(
        "Invalid time interval: from time '%s' is after until time '%s'" %
        (fromTime, untilTime))

  oldestTime = now - header['maxRetention']
  # Range is in the future
  if fromTime > now:
    return None
  # Range is beyond retention
  if untilTime < oldestTime:
    return None
  # Range requested is partially beyond retention, adjust
  if fromTime < oldestTime:
    fromTime = oldestTime
  # Range is partially in the future, adjust
  if untilTime > now:
    untilTime = now

  diff = now - fromTime

  # Parse granularity if requested
  if archiveToSelect:
    retentionStr = str(archiveToSelect) + ":1"
    archiveToSelect = parseRetentionDef(retentionStr)[0]

  for archive in header['archives']:
    if archiveToSelect:
      if archive['secondsPerPoint'] == archiveToSelect:
        break
      archive = None
    else:
      if archive['retention'] >= diff:
        break

  if archiveToSelect and not archive:
    raise ValueError("Invalid granularity: %s" % (archiveToSelect))

  return __archive_fetch(fh, archive, fromTime, untilTime)


def __archive_fetch(fh, archive, fromTime, untilTime):
  """
Fetch data from a single archive. Note that checks for validity of the time
period requested happen above this level so it's possible to wrap around the
archive on a read and request data older than the archive's retention
"""
  step = archive['secondsPerPoint']

  fromInterval = int(fromTime - (fromTime % step)) + step

  untilInterval = int(untilTime - (untilTime % step)) + step

  if fromInterval == untilInterval:
    # Zero-length time range: always include the next point
    untilInterval += step

  fh.seek(archive['offset'])
  packedPoint = fh.read(pointSize)
  try:
    (baseInterval, baseValue) = struct.unpack(pointFormat, packedPoint)
  except struct.error:
    raise CorruptWhisperFile("Unable to read base datapoint", fh.name)

  if baseInterval == 0:
    points = (untilInterval - fromInterval) // step
    timeInfo = (fromInterval, untilInterval, step)
    valueList = [None] * points
    return (timeInfo, valueList)

  # Determine fromOffset
  timeDistance = fromInterval - baseInterval
  pointDistance = timeDistance // step
  byteDistance = pointDistance * pointSize
  fromOffset = archive['offset'] + (byteDistance % archive['size'])

  # Determine untilOffset
  timeDistance = untilInterval - baseInterval
  pointDistance = timeDistance // step
  byteDistance = pointDistance * pointSize
  untilOffset = archive['offset'] + (byteDistance % archive['size'])

  # Read all the points in the interval
  fh.seek(fromOffset)
  if fromOffset < untilOffset:  # If we don't wrap around the archive
    seriesString = fh.read(untilOffset - fromOffset)
  else:  # We do wrap around the archive, so we need two reads
    archiveEnd = archive['offset'] + archive['size']
    seriesString = fh.read(archiveEnd - fromOffset)
    fh.seek(archive['offset'])
    seriesString += fh.read(untilOffset - archive['offset'])

  # Now we unpack the series data we just read (anything faster than unpack?)
  byteOrder, pointTypes = pointFormat[0], pointFormat[1:]
  points = len(seriesString) // pointSize
  seriesFormat = byteOrder + (pointTypes * points)
  try:
    unpackedSeries = struct.unpack(seriesFormat, seriesString)
  except struct.error:
    raise CorruptWhisperFile("Unable to read datapoints", fh.name)

  # And finally we construct a list of values (optimize this!)
  valueList = [None] * points  # Pre-allocate entire list for speed
  currentInterval = fromInterval

  for i in xrange(0, len(unpackedSeries), 2):
    pointTime = unpackedSeries[i]
    if pointTime == currentInterval:
      pointValue = unpackedSeries[i + 1]
      valueList[i // 2] = pointValue  # In-place reassignment is faster than append()
    currentInterval += step

  timeInfo = (fromInterval, untilInterval, step)
  return (timeInfo, valueList)


def merge(path_from, path_to, time_from=None, time_to=None, now=None):
  """ Merges the data from one whisper file into another. Each file must have
  the same archive configuration. time_from and time_to can optionally be
  specified for the merge.
"""
  # Python 2.7 will allow the following commented line
  # with open(path_from, 'rb') as fh_from, open(path_to, 'rb+') as fh_to:
  # But with Python 2.6 we need to use this (I prefer not to introduce
  # contextlib.nested just for this):
  with open(path_from, 'rb') as fh_from:
    with open(path_to, 'rb+') as fh_to:
      return file_merge(fh_from, fh_to, time_from, time_to, now)


def file_merge(fh_from, fh_to, time_from=None, time_to=None, now=None):
  headerFrom = __readHeader(fh_from)
  headerTo = __readHeader(fh_to)
  if headerFrom['archives'] != headerTo['archives']:
    raise NotImplementedError(
      "%s and %s archive configurations are unalike. "
      "Resize the input before merging" % (fh_from.name, fh_to.name))

  if now is None:
    now = int(time.time())

  if (time_to is not None):
    untilTime = time_to
  else:
    untilTime = now

  if (time_from is not None):
    fromTime = time_from
  else:
    fromTime = 0

  # Sanity check: do not mix the from/to values.
  if untilTime < fromTime:
    raise ValueError("time_to must be >= time_from")

  archives = headerFrom['archives']
  archives.sort(key=operator.itemgetter('retention'))

  for archive in archives:
    archiveFrom = fromTime
    archiveTo = untilTime
    if archiveFrom < now - archive['retention']:
      archiveFrom = now - archive['retention']
    # if untilTime is too old, skip this archive
    if archiveTo < now - archive['retention']:
      continue
    (timeInfo, values) = __archive_fetch(fh_from, archive, archiveFrom, archiveTo)
    (start, end, archive_step) = timeInfo
    pointsToWrite = list(ifilter(
      lambda points: points[1] is not None,
      izip(xrange(start, end, archive_step), values)))
    # skip if there are no points to write
    if len(pointsToWrite) == 0:
      continue
    __archive_update_many(fh_to, headerTo, archive, pointsToWrite)


def diff(path_from, path_to, ignore_empty=False, until_time=None, now=None):
  """ Compare two whisper databases. Each file must have the same archive configuration """
  with open(path_from, 'rb') as fh_from:
    with open(path_to, 'rb') as fh_to:
      return file_diff(fh_from, fh_to, ignore_empty, until_time, now)


def file_diff(fh_from, fh_to, ignore_empty=False, until_time=None, now=None):
  headerFrom = __readHeader(fh_from)
  headerTo = __readHeader(fh_to)

  if headerFrom['archives'] != headerTo['archives']:
    # TODO: Add specific whisper-resize commands to right size things
    raise NotImplementedError(
        "%s and %s archive configurations are unalike. "
        "Resize the input before diffing" % (fh_from.name, fh_to.name))

  archives = headerFrom['archives']
  archives.sort(key=operator.itemgetter('retention'))

  archive_diffs = []

  if now is None:
    now = int(time.time())
  if until_time:
    untilTime = until_time
  else:
    untilTime = now

  for archive_number, archive in enumerate(archives):
    diffs = []
    startTime = now - archive['retention']
    (fromTimeInfo, fromValues) = \
        __archive_fetch(fh_from, archive, startTime, untilTime)
    (toTimeInfo, toValues) = __archive_fetch(fh_to, archive, startTime, untilTime)
    (start, end, archive_step) =  \
        (min(fromTimeInfo[0], toTimeInfo[0]),
         max(fromTimeInfo[1], toTimeInfo[1]),
         min(fromTimeInfo[2], toTimeInfo[2]))

    points = map(lambda s: (s * archive_step + start, fromValues[s], toValues[s]),
                 xrange(0, (end - start) // archive_step))
    if ignore_empty:
      points = [p for p in points if p[1] is not None and p[2] is not None]
    else:
      points = [p for p in points if p[1] is not None or p[2] is not None]

    diffs = [p for p in points if p[1] != p[2]]

    archive_diffs.append((archive_number, diffs, points.__len__()))
    untilTime = min(startTime, untilTime)
  return archive_diffs

#!/usr/bin/python

import os
import mmap
import struct
import time
import sys
import signal
import optparse
from datetime import datetime
from datetime import timedelta

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
try:
   signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except AttributeError:
   #OS=windows
   pass


now = int(time.time())

option_parser = optparse.OptionParser(
    usage='''%prog path timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention
''')
option_parser.add_option('--xFilesFactor', default=0.5, type='float')
option_parser.add_option('--aggregationMethod', default='average',
        type='string', help="Function to use when aggregating values (%s)" %
        ', '.join(whisper.aggregationMethods))
option_parser.add_option('--fillmode', default='value', type='string', 
	help = "indicates the type of data to insert ")
option_parser.add_option('--overwrite', default=False, action='store_true')

option_parser.add_option('--fillvalue', default='1.0', type='string', 
        help = '''
	when fillmode=value ( default ) --fillvalue indicates a fix number (default 1.0 ) 
	when fillmode=date --fillvalue should indicate a changing value depending on the timestamp :
		m	=month like executing (date -d $TIMESTAMP +%m)
		d	=day   like executing (date -d $TIMESTAMP +%d)
		H	=hour  like executing (date -d $TIMESTAMP +%H)
		M	=minute like executing (date -d %TIMESTAMP +%M) 
''')

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_help()
  sys.exit(1)

path = args[0]
archives = [whisper.parseRetentionDef(retentionDef)
            for retentionDef in args[1:]]

if os.path.exists(path) and options.overwrite:
    print 'Overwriting existing file: %s' % path
    os.unlink(path)

try:
  whisper.create(path, archives, xFilesFactor=options.xFilesFactor, aggregationMethod=options.aggregationMethod)
except whisper.WhisperException, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

size = os.stat(path).st_size
print 'Created: %s (%d bytes)' % (path,size)

info = whisper.info(path)
old_archives = info['archives']
# sort by precision, lowest to highest
old_archives.sort(key=lambda a: a['secondsPerPoint'], reverse=True)
#creating a unit value from now-retention to now with 
maxRetention = old_archives[0]['retention']
fromTime = now-maxRetention
toTime = now
old_archives.sort(key=lambda a: a['secondsPerPoint'], reverse=False)
precision = old_archives[0]['secondsPerPoint']
#fromTime += precision

data_from_pretty = datetime.fromtimestamp(fromTime).strftime('%Y-%m-%d %H:%M:%S')
data_to_pretty   = datetime.fromtimestamp(toTime).strftime('%Y-%m-%d %H:%M:%S')

print "From : %s | Timestamp: %s " % ( data_from_pretty , fromTime )
print "To:    %s | Timestamp: %s " % ( data_to_pretty , toTime )

print 'Max precision : %s seconds' % precision
print 'Max retention : %d seconds : ( %s )' % (maxRetention, timedelta(seconds=maxRetention))
datapoints = []
timedata=range(fromTime,now,precision)
if options.fillmode == 'value':
  staticval = None
  try:
    staticval=float(options.fillvalue)
  except ValueError:
    print "Error: fillvalue is not a valid number: %s" % options.fillvalue 
    sys.exit(1)

  print "updating datapoints from static value : %f" % staticval
  datapoints = zip(timedata,[staticval] * len(timedata))
else:
  values = []
  if options.fillvalue == 'm':
    print "updating datapoints from the timestamp month "
    for ts in timedata:
      values.append(float(datetime.fromtimestamp(ts).strftime('%m')))
  elif options.fillvalue == 'd':
    print "updating datapoints from the timestamp day "
    for ts in timedata:
      values.append(float(datetime.fromtimestamp(ts).strftime('%d')))
  elif options.fillvalue == 'H':
    print "updating datapoints from the timestamp Hour "
    for ts in timedata:
      values.append(float(datetime.fromtimestamp(ts).strftime('%H')))
  elif options.fillvalue == 'M':
    print "updating datapoints from the timestamp minute "
    for ts in timedata:
      values.append(float(datetime.fromtimestamp(ts).strftime('%M')))
  else:
    print  "no valid fillvalue value : %s " % options.fillvalue
    sys.exit(1)

  datapoints = zip(timedata,values)
print "%d Datapoints updated " %  len(datapoints)
whisper.update_many(path, datapoints)




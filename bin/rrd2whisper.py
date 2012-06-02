#!/usr/bin/env python

import os
import sys
import time
import signal
import optparse

try:
  import rrdtool
except ImportError, exc:
  raise SystemExit('[ERROR] Missing dependency: %s' % str(exc))

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(usage='''%prog rrd_path''')
option_parser.add_option('--xFilesFactor', default=0.5, type='float')

(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_usage()
  sys.exit(1)

rrd_path = args[0]

try:
  rrd_info = rrdtool.info(rrd_path)
except rrdtool.error, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

seconds_per_point = rrd_info['step']

# First get the max retention - we grab the max of all datasources
if 'rra' in rrd_info:
  rras = rrd_info['rra']
else:
  rra_count = max([ int(key[4]) for key in rrd_info if key.startswith('rra[') ]) + 1
  rras = [{}] * rra_count
  for i in range(rra_count):
    rras[i]['pdp_per_row'] = rrd_info['rra[%d].pdp_per_row' % i]
    rras[i]['rows'] = rrd_info['rra[%d].rows' % i]

retention_points = 0
for rra in rras:
  points = rra['pdp_per_row'] * rra['rows']
  if points > retention_points:
    retention_points = points

retention = seconds_per_point * points

datasources = []
if 'ds' in rrd_info:
  datasource_names = rrd_info['ds'].keys()
else:
  ds_keys = [ key for key in rrd_info if key.startswith('ds[') ]
  datasources = list(set( key[3:].split(']')[0] for key in ds_keys ))

for datasource in datasources:
  now = int(time.time())
  path = rrd_path.replace('.rrd','_%s.wsp' % datasource)
  whisper.create(path, [(seconds_per_point,retention_points)], xFilesFactor=options.xFilesFactor)
  size = os.stat(path).st_size
  print 'Created: %s (%d bytes)' % (path,size)

  print 'Migrating data'
  startTime = str(now - retention)
  endTime = str(now)
  (time_info,columns,rows) = rrdtool.fetch(rrd_path, 'AVERAGE', '-s', startTime, '-e', endTime)
  column_index = list(columns).index(datasource)
  rows.pop() #remove the last datapoint because RRD sometimes gives funky values

  values = [row[column_index] for row in rows]
  timestamps = list(range(*time_info))
  datapoints = zip(timestamps,values)
  datapoints = filter(lambda p: p[1] is not None, datapoints)
  print ' migrating %d datapoints...' % len(datapoints)
  whisper.update_many(path, datapoints)

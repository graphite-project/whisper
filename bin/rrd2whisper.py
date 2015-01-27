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

aggregationMethods = whisper.aggregationMethods

# RRD doesn't have a 'sum' or 'total' type
aggregationMethods.remove('sum')
# RRD doesn't have a 'absmax' type
aggregationMethods.remove('absmax')
# RRD doesn't have a 'absmin' type
aggregationMethods.remove('absmin')

option_parser = optparse.OptionParser(usage='''%prog rrd_path''')
option_parser.add_option(
    '--xFilesFactor',
    help="The xFilesFactor to use in the output file. " +
    "Defaults to the input RRD's xFilesFactor",
    default=None,
    type='float')
option_parser.add_option(
    '--aggregationMethod',
    help="The consolidation function to fetch from on input and " +
    "aggregationMethod to set on output. One of: %s" %
    ', '.join(aggregationMethods),
    default='average',
    type='string')

(options, args) = option_parser.parse_args()

if len(args) < 1:
  option_parser.print_help()
  sys.exit(1)

rrd_path = args[0]

try:
  rrd_info = rrdtool.info(rrd_path)
except rrdtool.error, exc:
  raise SystemExit('[ERROR] %s' % str(exc))

seconds_per_pdp = rrd_info['step']

# Reconcile old vs new python-rrdtool APIs (yuck)
# leave consistent 'rras' and 'datasources' lists
if 'rra' in rrd_info:
  rras = rrd_info['rra']
else:
  rra_indices = []
  for key in rrd_info:
    if key.startswith('rra['):
      index = int(key.split('[')[1].split(']')[0])
      rra_indices.append(index)

  rra_count = max(rra_indices) + 1
  rras = []
  for i in range(rra_count):
    rra_info = {}
    rra_info['pdp_per_row'] = rrd_info['rra[%d].pdp_per_row' % i]
    rra_info['rows'] = rrd_info['rra[%d].rows' % i]
    rra_info['cf'] = rrd_info['rra[%d].cf' % i]
    rra_info['xff'] = rrd_info['rra[%d].xff' % i]
    rras.append(rra_info)

datasources = []
if 'ds' in rrd_info:
  datasource_names = rrd_info['ds'].keys()
else:
  ds_keys = [key for key in rrd_info if key.startswith('ds[')]
  datasources = list(set(key[3:].split(']')[0] for key in ds_keys))

# Grab the archive configuration
relevant_rras = []
for rra in rras:
  if rra['cf'] == options.aggregationMethod.upper():
    relevant_rras.append(rra)

if not relevant_rras:
  err = "[ERROR] Unable to find any RRAs with consolidation function: %s" % \
        options.aggregationMethod.upper()
  raise SystemExit(err)

archives = []
xFilesFactor = options.xFilesFactor
for rra in relevant_rras:
  precision = rra['pdp_per_row'] * seconds_per_pdp
  points = rra['rows']
  if not xFilesFactor:
    xFilesFactor = rra['xff']
  archives.append((precision, points))

for datasource in datasources:
  now = int(time.time())
  path = rrd_path.replace('.rrd', '_%s.wsp' % datasource)
  try:
    whisper.create(path, archives, xFilesFactor=xFilesFactor)
  except whisper.InvalidConfiguration, e:
    raise SystemExit('[ERROR] %s' % str(e))
  size = os.stat(path).st_size
  archiveConfig = ','.join(["%d:%d" % ar for ar in archives])
  print "Created: %s (%d bytes) with archives: %s" % (path, size, archiveConfig)

  print "Migrating data"
  archiveNumber = len(archives) - 1
  for precision, points in reversed(archives):
    retention = precision * points
    endTime = now - now % precision
    startTime = endTime - retention
    (time_info, columns, rows) = rrdtool.fetch(
      rrd_path,
      options.aggregationMethod.upper(),
      '-r', str(precision),
      '-s', str(startTime),
      '-e', str(endTime))
    column_index = list(columns).index(datasource)
    rows.pop()  # remove the last datapoint because RRD sometimes gives funky values

    values = [row[column_index] for row in rows]
    timestamps = list(range(*time_info))
    datapoints = zip(timestamps, values)
    datapoints = filter(lambda p: p[1] is not None, datapoints)
    print ' migrating %d datapoints from archive %d' % (len(datapoints), archiveNumber)
    archiveNumber -= 1
    whisper.update_many(path, datapoints)

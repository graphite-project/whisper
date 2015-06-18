#!/usr/bin/env python

import os, sys, time, shutil, whisper
from optparse import OptionParser

now = int( time.time() )
epoch = 0

option_parser = OptionParser(usage='''%prog [options] path1 path2''')
option_parser.add_option('--from', default=epoch, type='int', dest='_from',
  help=("Unix epoch time of the beginning of "
        "your requested interval (default: epoch)"))
option_parser.add_option('--until', default=now, type='int',
  help="Unix epoch time of the end of your requested interval (default: now)")

(options, args) = option_parser.parse_args()

if len(args) != 2:
  option_parser.print_usage()
  sys.exit(1)

path1 = args[0]
path2 = args[1]
bakfile = path2 + '.bak'
shutil.copy2(path2, bakfile)

#print "created backup file %s" % (bakfile)

from_time = int( options._from )
until_time = int( options.until )


info1 = whisper.info(path1)
info2 = whisper.info(path2)
if info1 != info2:
    raise NotImplementedError("%s and %s archive configurations are unalike. "\
    "Resize the input before merging" % (path1, path2))

retentions = [r['retention'] - r['secondsPerPoint']  for r in info1['archives']]
retentions.sort()
last_retention = 0
for retention in retentions:
    from_time = now - retention
    until_time = now - last_retention
    if options._from > from_time:
        from_time  = options._from
    if options.until < until_time:
        until_time = options.until

    (timeInfo, values) = whisper.fetch(path1, from_time, until_time)

    (start,end,step) = timeInfo

    t = start
    for value in values:
        timestr = str(t)
        if value is None:
            next
        else:
            valuestr = "%f" % value
            datapoints = [timestr, valuestr]
            whisper.update(path2, valuestr, timestr)
        t += step
    last_retention = retention

# to avoid wasting storage space, we must delete the backup file 

os.remove(bakfile)

#print "deleted backup file %s" % (bakfile)


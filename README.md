# Whisper

[![Build Status](https://secure.travis-ci.org/graphite-project/whisper.png)](http://travis-ci.org/graphite-project/whisper)

Whisper is one of the components of [Graphite][], and is responsible for
the backend storage of incoming metrics from the network.
Currently [Whisper][] is our stable, supported backend and
[Ceres][] is the work-in-progress future replacement for Whisper.

[Graphite]: https://github.com/graphite-project
[Graphite Web]: https://github.com/graphite-project/graphite-web
[Whisper]: https://github.com/graphite-project/whisper
[Ceres]: https://github.com/graphite-project/ceres

## Overview

Whisper is a fixed-size database, similar in design and purpose to RRD
(round-robin-database). It provides fast, reliable storage of numeric data over
time. Whisper allows for higher resolution (seconds per point) of recent data
to degrade into lower resolutions for long-term retention of historical data.

## Whisper Scripts

rrd2whisper.py
--------------
Convert a rrd file into a whisper (.wsp) file.

```
Usage: rrd2whisper.py rrd_path

Options:
  -h, --help            show this help message and exit
  --xFilesFactor=XFILESFACTOR
                        The xFilesFactor to use in the output file. Defaults
                        to the input RRD's xFilesFactor
  --aggregationMethod=AGGREGATIONMETHOD
                        The consolidation function to fetch from on input and
                        aggregationMethod to set on output. One of: average,
                        last, max, min
```

whisper-create.py
--------------
Create a new whisper database file

```
Usage: whisper-create.py path timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention


Options:
  -h, --help            show this help message and exit
  --xFilesFactor=XFILESFACTOR
  --aggregationMethod=AGGREGATIONMETHOD
                        Function to use when aggregating values (average, sum,
                        last, max, min)
  --overwrite           
```

whisper-dump.py
--------------
Dump the metadata about a whisper file to stdout

```
Usage: whisper-dump.py path

Options:
  -h, --help  show this help message and exit
```

whisper-fetch.py
--------------
Fetch all the metrics stored in a whisper file to stdout.

```
Usage: whisper-fetch.py [options] path

Options:
  -h, --help     show this help message and exit
  --from=_FROM   Unix epoch time of the beginning of your requested interval
                 (default: 24 hours ago)
  --until=UNTIL  Unix epoch time of the end of your requested interval
                 (default: now)
  --json         Output results in JSON form
  --pretty       Show human-readable timestamps instead of unix times
```

whisper-info.py
--------------

```
Usage: whisper-info.py path [field]

Options:
  -h, --help  show this help message and exit
```

whisper-merge.py
--------------
Join to existing whisper files together.

```
Usage: whisper-merge.py [options] from_path to_path

Options:
  -h, --help  show this help message and exit
```

whisper-resize.py
--------------
Change the retention rates of an existing whisper file.

```
Usage: whisper-resize.py path timePerPoint:timeToStore [timePerPoint:timeToStore]*

timePerPoint and timeToStore specify lengths of time, for example:

60:1440      60 seconds per datapoint, 1440 datapoints = 1 day of retention
15m:8        15 minutes per datapoint, 8 datapoints = 2 hours of retention
1h:7d        1 hour per datapoint, 7 days of retention
12h:2y       12 hours per datapoint, 2 years of retention


Options:
  -h, --help            show this help message and exit
  --xFilesFactor=XFILESFACTOR
                        Change the xFilesFactor
  --aggregationMethod=AGGREGATIONMETHOD
                        Change the aggregation function (average, sum, last,
                        max, min)
  --force               Perform a destructive change
  --newfile=NEWFILE     Create a new database file without removing the
                        existing one
  --nobackup            Delete the .bak file after successful execution
  --aggregate           Try to aggregate the values to fit the new archive
                        better. Note that this will make things slower and use
                        more memory.
```

whisper-set-aggregation-method.py
--------------
Change the aggregation method of an existing whisper file.

```
Usage: whisper-set-aggregation-method.py path <average|sum|last|max|min>

Options:
  -h, --help  show this help message and exit
```

whisper-update.py
--------------
Update a whisper file with 1 or many values, must provide a time stamp with the value.

```
Usage: whisper-update.py [options] path timestamp:value [timestamp:value]*

Options:
  -h, --help  show this help message and exit
```

whisper-diff.py
--------------
Check the differences between whisper files.  Use sanity check before merging.
```
Usage: whisper-diff.py [options] path_a path_b

Options:
  -h, --help      show this help message and exit
  --summary       show summary of differences
  --ignore-empty  skip comparison if either value is undefined
  --columns       print output in simple columns
  --no-headers    do not print column headers
```


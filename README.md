# Whisper

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/f00d0b65802742e29de56f3744503ab0)](https://www.codacy.com/app/graphite-project/whisper?utm_source=github.com&utm_medium=referral&utm_content=graphite-project/whisper&utm_campaign=badger)
[![Build Status](https://secure.travis-ci.org/graphite-project/whisper.png)](http://travis-ci.org/graphite-project/whisper)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fgraphite-project%2Fwhisper.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fgraphite-project%2Fwhisper?ref=badge_shield)

## Overview

Whisper is one of three components within the Graphite project:

1. [Graphite-Web](https://github.com/graphite-project/graphite-web), a Django-based web application that renders graphs and dashboards
2. The [Carbon](https://github.com/graphite-project/carbon) metric processing daemons
3. The Whisper time-series database library

![Graphite Components](https://github.com/graphite-project/graphite-web/raw/master/webapp/content/img/overview.png "Graphite Components")

Whisper is a fixed-size database, similar in design and purpose to RRD (round-robin-database). It provides fast, reliable storage of numeric data over time. Whisper allows for higher resolution (seconds per point) of recent data to degrade into lower resolutions for long-term retention of historical data.

## Installation, Configuration and Usage

Please refer to the instructions at [readthedocs](http://graphite.readthedocs.org/).

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
                        last, max, min, avg_zero, absmax, absmin
  --destinationPath=DESTINATIONPATH
                        Path to place created whisper file. Defaults to the
                        RRD file's source path.

```

whisper-create.py
-----------------
Create a new whisper database file.

```
Usage: whisper-create.py path timePerPoint:timeToStore [timePerPoint:timeToStore]*
       whisper-create.py --estimate timePerPoint:timeToStore [timePerPoint:timeToStore]*

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
                        last, max, min, avg_zero, absmax, absmin)
  --overwrite
  --estimate            Don't create a whisper file, estimate storage requirements based on archive definitions
```

whisper-dump.py
---------------
Dump the whole whisper file content to stdout.

```
Usage: whisper-dump.py path

Options:
  -h, --help            show this help message and exit
  --pretty              Show human-readable timestamps instead of unix times
  -t TIME_FORMAT, --time-format=TIME_FORMAT
                        Time format to use with --pretty; see time.strftime()
  -r, --raw             Dump value only in the same format for whisper-update
                        (UTC timestamps)
```

whisper-fetch.py
----------------
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
  -t TIME_FORMAT, --time-format=TIME_FORMAT
                 Time format to use with --pretty; see time.strftime()
  --drop=DROP    Specify 'nulls' to drop all null values. Specify 'zeroes' to
                 drop all zero values. Specify 'empty' to drop both null and
                 zero values.
```

whisper-info.py
---------------
Dump the metadata about a whisper file to stdout.

```
Usage: whisper-info.py [options] path [field]

Options:
  -h, --help  show this help message and exit
  --json      Output results in JSON form
```

whisper-merge.py
----------------
Join two existing whisper files together.

```
Usage: whisper-merge.py [options] from_path to_path

Options:
  -h, --help  show this help message and exit
```

whisper-fill.py
----------------
Copies data from src in dst, if missing.
Unlike whisper-merge, don't overwrite data that's
already present in the target file, but instead, only add the missing
data (e.g. where the gaps in the target file are).  Because no values
are overwritten, no data or precision gets lost.  Also, unlike
whisper-merge, try to take the highest-precision archive to provide
the data, instead of the one with the largest retention.

```
Usage: whisper-fill.py [options] src_path dst_path

Options:
  -h, --help  show this help message and exit
```

whisper-resize.py
-----------------
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
                        max, min, avg_zero, absmax, absmin)
  --force               Perform a destructive change
  --newfile=NEWFILE     Create a new database file without removing the
                        existing one
  --nobackup            Delete the .bak file after successful execution
  --aggregate           Try to aggregate the values to fit the new archive
                        better. Note that this will make things slower and use
                        more memory.
```

whisper-set-aggregation-method.py
---------------------------------
Change the aggregation method of an existing whisper file.

```
Usage: whisper-set-aggregation-method.py path <average|sum|last|max|min|avg_zero|absmax|absmin>

Options:
  -h, --help  show this help message and exit
```

whisper-update.py
-----------------
Update a whisper file with 1 or many values, must provide a time stamp with the value.

```
Usage: whisper-update.py [options] path timestamp:value [timestamp:value]*

Options:
  -h, --help  show this help message and exit
```

whisper-diff.py
---------------
Check the differences between whisper files.  Use sanity check before merging.

```
Usage: whisper-diff.py [options] path_a path_b

Options:
  -h, --help      show this help message and exit
  --summary       show summary of differences
  --ignore-empty  skip comparison if either value is undefined
  --columns       print output in simple columns
  --no-headers    do not print column headers
  --until=UNTIL   Unix epoch time of the end of your requested interval
                  (default: now)
  --json          Output results in JSON form
```

## License

Whisper is licensed under version 2.0 of the Apache License. See the [LICENSE](https://github.com/graphite-project/carbon/blob/master/LICENSE) file for details.

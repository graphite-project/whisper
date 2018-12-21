#!/usr/bin/python -tt

import sys
import optparse
import json

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

option_parser = optparse.OptionParser(usage='''%prog [options] path_a path_b''')
option_parser.add_option('--summary', default=False, action='store_true',
                         help="show summary of differences")
option_parser.add_option('--ignore-empty', default=False, action='store_true',
                         help="skip comparison if either value is undefined")
option_parser.add_option('--columns', default=False, action='store_true',
                         help="print output in simple columns")
option_parser.add_option('--no-headers', default=False, action='store_true',
                         help="do not print column headers")
option_parser.add_option('--until', default=None, type='int',
                         help="Unix epoch time of the end of your requested "
                              "interval (default: None)")
option_parser.add_option('--json', default=False, action='store_true',
                         help="Output results in JSON form")

(options, args) = option_parser.parse_args()

if len(args) != 2:
  option_parser.print_help()
  sys.exit(1)

(path_a, path_b) = args[0::1]

if options.until:
  until_time = int(options.until)
else:
  until_time = None


def print_diffs(diffs, pretty=True, headers=True):
  if pretty:
    h = "%7s %11s %13s %13s\n"
    f = "%7s %11d %13s %13s\n"
  else:
    h = "%s %s %s %s\n"
    f = "%s %d %s %s\n"
  if headers:
    sys.stdout.write(h % ('archive', 'timestamp', 'value_a', 'value_b'))
  for archive, points, total in diffs:
    if pretty:
      sys.stdout.write('Archive %d (%d of %d datapoints differ)\n' %
                       (archive, points.__len__(), total))
      sys.stdout.write(h % ('', 'timestamp', 'value_a', 'value_b'))
    for p in points:
      if pretty:
        sys.stdout.write(f % ('', p[0], p[1], p[2]))
      else:
        sys.stdout.write(f % (archive, p[0], p[1], p[2]))


def print_summary(diffs, pretty=True, headers=True):
  if pretty:
    f = "%7s %9s %9s\n"
  else:
    f = "%s %s %s\n"
  if headers:
    sys.stdout.write(f % ('archive', 'total', 'differing'))
  for archive, points, total in diffs:
    sys.stdout.write(f % (archive, total, points.__len__()))


def print_summary_json(diffs, path_a, path_b):
  print(json.dumps({'path_a': path_a,
                    'path_b': path_b,
                    'archives': [{'archive': archive,
                                  'total': total,
                                  'points': points.__len__()}
                                 for archive, points, total in diffs]},
                   sort_keys=True, indent=2, separators=(',', ' : ')))


def print_diffs_json(diffs, path_a, path_b):
  print(json.dumps({'path_a': path_a,
                    'path_b': path_b,
                    'archives': [{'archive': archive,
                                  'total': total,
                                  'points': points.__len__(),
                                  'datapoint': [{
                                      'timestamp': p[0],
                                      'value_a': p[1],
                                      'value_b': p[2]
                                    } for p in points]}
                                 for archive, points, total in diffs]},
                   sort_keys=True, indent=2, separators=(',', ' : ')))


def main():
  archive_diffs = whisper.diff(path_a, path_b, ignore_empty=options.ignore_empty,
                               until_time=until_time)
  if options.summary:
    if options.json:
      print_summary_json(archive_diffs, path_a, path_b)
    else:
      print_summary(archive_diffs, pretty=(not options.columns),
                    headers=(not options.no_headers))
  else:
    if options.json:
      print_diffs_json(archive_diffs, path_a, path_b)
    else:
      print_diffs(archive_diffs, pretty=(not options.columns),
                  headers=(not options.no_headers))


if __name__ == "__main__":
  main()

#!/usr/bin/env python

import sys
import argparse
import whisper


def main():
    """Set xFilesFactor for existing whisper file"""
    parser = argparse.ArgumentParser(
        description='Set xFilesFactor for existing whisper file')
    parser.add_argument('path', type=str, help='path to whisper file')
    parser.add_argument('xff', metavar='xFilesFactor', type=float,
                        help='new xFilesFactor, a float between 0 and 1')

    args = parser.parse_args()

    try:
        old_xff = whisper.setXFilesFactor(args.path, args.xff)
    except IOError:
        sys.stderr.write("[ERROR] File '%s' does not exist!\n\n" % args.path)
        parser.print_help()
        sys.exit(1)
    except whisper.WhisperException as exc:
        raise SystemExit('[ERROR] %s' % str(exc))

    print('Updated xFilesFactor: %s (%s -> %s)' %
          (args.path, old_xff, args.xff))


if __name__ == "__main__":
    main()

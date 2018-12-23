#!/usr/bin/env python
# encoding: utf-8
"""Find and (optionally) delete corrupt Whisper data files"""
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import os
import sys

import whisper


def walk_dir(base_dir, delete_corrupt=False, verbose=False):
    for dirpath, dirnames, filenames in os.walk(base_dir):
        if verbose:
            print("Scanning %s…" % dirpath)

        whisper_files = (os.path.join(dirpath, i) for i in filenames if i.endswith('.wsp'))

        for f in whisper_files:
            try:
                info = whisper.info(f)
            except whisper.CorruptWhisperFile:
                if delete_corrupt:
                    print('Deleting corrupt Whisper file: %s' % f, file=sys.stderr)
                    os.unlink(f)
                else:
                    print('Corrupt Whisper file: %s' % f, file=sys.stderr)
                continue

            if verbose:
                print('%s: %d points' % (f, sum(i['points'] for i in info.get('archives', {}))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.strip())
    parser.add_argument('--delete-corrupt', default=False, action='store_true',
                        help='Delete reported files')
    parser.add_argument('--verbose', default=False, action='store_true',
                        help='Display progress info')
    parser.add_argument('directories', type=str, nargs='+',
                        metavar='WHISPER_DIR',
                        help='Directory containing Whisper files')
    args = parser.parse_args()

    for d in args.directories:
        d = os.path.realpath(d)
        if not os.path.isdir(d):
            parser.error("%d is not a directory!")

        walk_dir(d, delete_corrupt=args.delete_corrupt, verbose=args.verbose)

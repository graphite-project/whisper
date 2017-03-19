#!/usr/bin/env python
# @package update_storage_schemas.py
# Correct/Update storage schemas\n
# @code
#   # Usage example for update_storage_schemas.py
#   sudo ./update_storage_schemas.py --path /opt/graphite/whisper --cfg /opt/graphite/conf/schemas
# @endcode

import sys
import os
import logging
import subprocess
import argparse
import re
import time
from multiprocessing import Pool, cpu_count
from configobj import ConfigObj
# Assuming Python 2, we'll want scandir if possible, it's much faster
try:
    from scandir import scandir
except ImportError:
    from os import listdir as scandir

RESIZE_BIN = "/opt/graphite/bin/whisper-resize.py"
INFO_BIN = "/opt/graphite/bin/whisper-info.py"
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)
SCHEMA_LIST = {}
# The very basic default retentions
DEFAULT_SCHEMA = {'match': re.compile('.*'),
                  'retentions': '1m:7d'}
DEBUG = False
DRY_RUN = False
BASE_COMMAND = [RESIZE_BIN]
ROOT_PATH = ""


def config_schemas(cfg):
    schema_conf = ConfigObj(cfg)

    for schema in schema_conf.items():
        item = schema[1]['pattern']
        if item == '.*':
            DEFAULT_SCHEMA['retentions'] = schema[1]['retentions']
        else:
            if item[0] == '^':
                item = item[1:]
            SCHEMA_LIST[item] = {'retentions': schema[1]['retentions'],
                                 'match': re.compile(item)}


def _convert_seconds(time):
    seconds_dict = {'s': 1, 'm': 60, 'h': 3600, 'min': 60,
                    'd': 86400, 'w': 604800, 'y': 31536000}
    (points, time) = time.split(':')
    if str.isalpha(time[-1]):
        time = int(time[:-1]) * seconds_dict[time[-1]]
    return time


def _compare_retention(retention, tmp_path):
    # Get the new retention as [(secondsPerPoint, numPoints), ...]
    new_retention = [_convert_seconds(item) for item in list(retention)]
    info_string = [INFO_BIN, tmp_path]
    cur_ret_list = subprocess.Popen(info_string, stdout=subprocess.PIPE)
    cur_ret_list = cur_ret_list.communicate()[0].split('\n')
    cur_retention = [int(line.split(':')[1]) for line in cur_ret_list
                     if 'retention' in line]
    return cur_retention == new_retention


def _find_metrics(path):
    for f in scandir(path):
        if f.is_dir(follow_symlinks=False):
            for sf in _find_metrics(f.path):
                yield sf
        else:
            if not f.is_file(follow_symlinks=False) or \
              not f.name.endswith('.wsp'):
                continue
            yield f.path


def fix_metric(metric):
    if not SCHEMA_LIST:
        LOG.error("Didn't initialize schemas!")
        return []

    if DEBUG:
        LOG.info("Testing %s for modification" % metric)
    devnull = open(os.devnull, 'w')
    command_string = list(BASE_COMMAND) + [metric]

    retention = DEFAULT_SCHEMA['retentions']
    matching = metric[len(ROOT_PATH):]
    for schema, info in SCHEMA_LIST.iteritems():
        if info['match'].search(matching):
            retention = info['retentions']
            break
    command_string.extend(list(retention))
    if DEBUG:
        LOG.info("Created command: %s" % command_string)

    if _compare_retention(retention, metric):
        LOG.debug('%s has the same retention as before!' % metric)
        return [(False, metric)]

    if DRY_RUN:
        res = 0
    else:
        LOG.debug('Retention will be %s' % retention)
        if DEBUG:
            res = subprocess.check_call(command_string)
        else:
            res = subprocess.check_call(command_string,
                                        stdout=devnull)
    devnull.close()
    # wait for a second, so we don't kill I/O on the host
    time.sleep(0.3)
    """
    We have manual commands for every failed file from these
    errors, so we can just go through each of these errors
    after a completed run. There shouldn't be many
    """
    if res != 0:
        LOG.error('Failed to update schemas for %s' % metric)
        LOG.error('Attempted retention: %s' % retention)
        LOG.error('Attempted command string: %s' % command_string)
        return [(False, metric)]
    else:
        return [(True, metric)]


def search_and_fix(subdir):
    if not SCHEMA_LIST:
        LOG.error("Didn't initialize schemas!")
        return

    fpath = os.path.join(ROOT_PATH, subdir)
    pool = Pool(cpu_count())
    LOG.info('Creating new storage schemas for metrics under %s ...' % fpath)

    results = pool.map(fix_metric, _find_metrics(fpath), 100)
    pool.close()
    pool.join()
    return results


# Parse command line options sent to the script
def cli_opts():
    parser = argparse.ArgumentParser("Correct storage settings on multiple whisper files")
    parser.add_argument('--cfg', action='store', dest='cfg',
                        help='The storage-schemas.conf file path',
                        required=True)
    parser.add_argument('--path', action='store', dest='path',
                        help='The root path to find metrics in',
                        required=True)
    parser.add_argument('--debug', action='store_true', dest='debug',
                        help='Display debug information',
                        default=False)
    parser.add_argument('--dry-run', action='store_true', dest='dry_run',
                        help="Don't actually do anything",
                        default=False)
    parser.add_argument('--subdir', action='store', dest='subdir',
                        help="If you only want to process a particular subdir",
                        default='')
    parser.add_argument('--nobackup', action='store_true', dest='nobackup',
                        help="Passed through to whisper-resize.py, don't create a backup",
                        default=False)
    parser.add_argument('--aggregate', action='store_true', dest='aggregate',
                        help="Passed through to whisper-resize.py, roll up values",
                        default=False)
    return parser.parse_args()

if __name__ == '__main__':
    i_args = cli_opts()
    if os.getenv('USER') != 'root':
        print("You must run this script as root!")
        sys.exit(1)

    if i_args.debug:
        LOG.setLevel(logging.DEBUG)
    soh = logging.StreamHandler(sys.stdout)
    LOG.addHandler(soh)

    ROOT_PATH = i_args.path
    DEBUG = i_args.debug
    DRY_RUN = i_args.dry_run
    if i_args.nobackup:
        BASE_COMMAND.append('--nobackup')
    if i_args.aggregate:
        BASE_COMMAND.append('--aggregate')

    config_schemas(i_args.cfg)
    search_and_fix(i_args.subdir)


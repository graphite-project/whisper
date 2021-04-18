#!/usr/bin/env python

import os
import sys
import time
import math
import random
import struct
import errno
from datetime import datetime

from six.moves import StringIO
from six import assertRegex

try:
    from unittest.mock import patch, mock_open
except ImportError:
    from mock import patch, mock_open

try:
    import unittest2 as unittest
except ImportError:
    import unittest

# For py3k in TestWhisper.test_merge
try:
    FileNotFoundError  # noqa
except NameError:
    class FileNotFoundError(Exception):
        pass
import whisper


class SimulatedCorruptWhisperFile(object):
    """
    Simple context manager to be used as a decorator for simulating a
    corrupt whisper file for testing purposes.

    Example:

        >>> whisper.create('test.wsp', [(60, 10)])
        >>> with SimulatedCorruptWhisperFile():
        ...     whisper.info('test.wsp')

    When 'corrupt_archive' is passed as True, the metadata will be left
    intact, but the archive will seem corrupted.
    """
    def __init__(self, corrupt_archive=False):
        self.corrupt_archive = corrupt_archive

        self.metadataFormat = whisper.metadataFormat
        self.archiveInfoFormat = whisper.archiveInfoFormat
        self.CACHE_HEADERS = whisper.CACHE_HEADERS

    def __enter__(self):
        # Force the struct unpack to fail by changing the metadata
        # format. This simulates an actual corrupted whisper file
        if not self.corrupt_archive:
            whisper.metadataFormat = '!ssss'
        else:
            whisper.archiveInfoFormat = '!ssss'

        # Force whisper to reread the header instead of returning
        # the previous (correct) header from the header cache
        whisper.CACHE_HEADERS = False

    def __exit__(self, *args, **kwargs):
        whisper.metadataFormat = self.metadataFormat
        whisper.archiveInfoFormat = self.archiveInfoFormat
        whisper.CACHE_HEADERS = self.CACHE_HEADERS


class AssertRaisesException(object):
    """
    Context manager to not only assert the type of exception raised,
    but also the actual value of the exception matches what is expected


    >>> with AssertRaisesException(ValueError('beer > wine')):
    ...     raise ValueError('beer > wine')

    This is better than unittest.TestCase.assertRaises as it also checks
    the contents of the exception vs just the type raised.
    """
    def __init__(self, exc):
        self.expected_exc = exc

    def __enter__(self):
        yield

    def __exit__(self, e_type, e_value, tracebck):
        # Ensure an exception was actually raised
        if e_type is None:
            raise AssertionError("Exception of type '{}' was not raised".format(
                self.expected_exc.__class__.__name__,
            ))
        elif not isinstance(self.expected_exc, e_type):
            raise AssertionError("Exception type '{}' is not of type '{}'".format(
                getattr(e_type, '__name__', 'None'),
                self.expected_exc.__class__.__name__,
            ))
        # Ensure the actual values are the exact same. Since
        # two instances of an arbitrary exception will never
        # be considered equal, use the __dict__ attr to check
        # that all of the kwargs such as path for exceptions
        # such as CorruptWhisperFile are the exact same.
        elif e_value.__dict__ != self.expected_exc.__dict__:
            raise AssertionError("'{}' != '{}'".format(
                repr(self.expected_exc.__dict__),
                repr(e_value.__dict__),
            ))
        # Some builtin exceptions such as ValueError return {} for
        # ValueError.__dict__, so finally, cast those to strings to compare
        elif str(e_value) != str(self.expected_exc):
            raise AssertionError("String forms of: '{}' != '{}'".format(
                str(self.expected_exc),
                str(e_value),
            ))
        # Context managers need to return True in __exit__ to not
        # re-raise the exception held in the e_value variable
        return True


class WhisperTestBase(unittest.TestCase):
    def setUp(self):
        self.filename = 'db.wsp'
        self.retention = [(1, 60), (60, 60)]

    def tearDown(self):
        self._remove(self.filename)

    @staticmethod
    def _remove(wsp_file):
        try:
            os.unlink(wsp_file)
        except (IOError, OSError, FileNotFoundError):
            pass


class TestWhisper(WhisperTestBase):
    """
    Testing functions for whisper.
    """
    def test_validate_archive_list(self):
        """
        blank archive config
        """
        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    'You must specify at least one archive configuration!')):
            whisper.validateArchiveList([])

    def test_duplicate(self):
        """
        Checking duplicates
        """
        # TODO: Fix the lies with whisper.validateArchiveList() saying it returns True/False
        self.assertIsNone(whisper.validateArchiveList(self.retention))

        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    'A Whisper database may not be configured having two '
                    'archives with the same precision (archive0: (1, 60), '
                    'archive1: (1, 60))')):
            whisper.validateArchiveList([(1, 60), (60, 60), (1, 60)])

    def test_even_precision_division(self):
        """
        even precision division
        """
        whisper.validateArchiveList([(60, 60), (6, 60)])
        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    "Higher precision archives' precision must evenly divide "
                    "all lower precision archives' precision (archive0: 7, "
                    "archive1: 60)")):
            whisper.validateArchiveList([(60, 60), (7, 60)])

    def test_timespan_coverage(self):
        """
        timespan coverage
        """
        whisper.validateArchiveList(self.retention)
        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    'Lower precision archives must cover larger time intervals '
                    'than higher precision archives (archive0: 60 seconds, '
                    'archive1: 10 seconds)')):
            whisper.validateArchiveList([(1, 60), (10, 1)])

    def test_number_of_points(self):
        """
        number of points
        """
        whisper.validateArchiveList(self.retention)
        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    "Each archive must have at least enough points to "
                    "consolidate to the next archive (archive1 consolidates 60 "
                    "of archive0's points but it has only 30 total points)")):
            whisper.validateArchiveList([(1, 30), (60, 60)])

    def test_aggregate(self):
        """
        aggregate functions
        """
        # min of 1-4
        self.assertEqual(whisper.aggregate('min', [1, 2, 3, 4]), 1)
        # max of 1-4
        self.assertEqual(whisper.aggregate('max', [1, 2, 3, 4]), 4)
        # last element in the known values
        self.assertEqual(whisper.aggregate('last', [3, 2, 5, 4]), 4)
        # sum ALL THE VALUES!
        self.assertEqual(whisper.aggregate('sum', [10, 2, 3, 4]), 19)
        # average of the list elements
        self.assertEqual(whisper.aggregate('average', [1, 2, 3, 4]), 2.5)
        avg_zero = [1, 2, 3, 4, None, None, None, None]
        non_null = [i for i in avg_zero if i is not None]
        self.assertEqual(whisper.aggregate('avg_zero', non_null, avg_zero), 1.25)
        # avg_zero without neighborValues
        with self.assertRaises(whisper.InvalidAggregationMethod):
            whisper.aggregate('avg_zero', non_null)
        # absmax with negative max
        self.assertEqual(whisper.aggregate('absmax', [-3, -2, 1, 2]), -3)
        # absmax with positive max
        self.assertEqual(whisper.aggregate('absmax', [-2, -1, 2, 3]), 3)
        # absmin with positive min
        self.assertEqual(whisper.aggregate('absmin', [-3, -2, 1, 2]), 1)
        # absmin with negative min
        self.assertEqual(whisper.aggregate('absmin', [-2, -1, 2, 3]), -1)

        with AssertRaisesException(
                whisper.InvalidAggregationMethod(
                    'Unrecognized aggregation method derp')):
            whisper.aggregate('derp', [12, 2, 3123, 1])

    def _test_create_exception(self, exception_method='write', e=None):
        """
        Behaviour when creating a whisper file on a full filesystem
        """
        m_open = mock_open()
        # Get the mocked file object and override interesting attributes
        m_file = m_open.return_value
        m_file.name = self.filename
        method = getattr(m_file, exception_method)

        if not e:
          e = IOError(errno.ENOSPC, "Mocked IOError")
        method.side_effect = e

        with patch('whisper.open', m_open, create=True):
          with patch('os.unlink') as m_unlink:
            self.assertRaises(e.__class__, whisper.create, self.filename, self.retention)

        return (m_file, m_unlink)

    def test_create_write_ENOSPC(self):
        """
        Behaviour when creating a whisper file on a full filesystem (write)
        """
        (m_file, m_unlink) = self._test_create_exception('write')
        m_unlink.assert_called_with(self.filename)

    def test_create_close_ENOSPC(self):
        """
        Behaviour when creating a whisper file on a full filesystem (close)
        """
        (m_file, m_unlink) = self._test_create_exception('close')
        m_unlink.assert_called_with(self.filename)

    def test_create_close_EIO(self):
        """
        Behaviour when creating a whisper file and getting an I/O error (EIO)
        """
        (m_file, m_unlink) = self._test_create_exception('close', e=IOError(errno.EIO))
        self.assertTrue(m_unlink.called)

    def test_create_close_exception(self):
        """
        Behaviour when creating a whisper file and getting a generic exception
        """
        (m_file, m_unlink) = self._test_create_exception('close', e=Exception("boom!"))
        # Must not call os.unlink on exception other than IOError
        self.assertFalse(m_unlink.called)

    def test_create_and_info(self):
        """
        Create a db and use info() to validate
        """
        # check if invalid configuration fails successfully
        for retention in (0, []):
            with AssertRaisesException(
                    whisper.InvalidConfiguration(
                        'You must specify at least one archive configuration!')):
                whisper.create(self.filename, retention)

        # create a new db with a valid configuration
        whisper.create(self.filename, self.retention)

        # Ensure another file can't be created when one exists already
        with AssertRaisesException(
                whisper.InvalidConfiguration(
                    'File {0} already exists!'.format(self.filename))):
            whisper.create(self.filename, self.retention)

        info = whisper.info(self.filename)

        # check header information
        self.assertEqual(info['maxRetention'],
                         max([a[0] * a[1] for a in self.retention]))
        self.assertEqual(info['aggregationMethod'], 'average')
        self.assertEqual(info['xFilesFactor'], 0.5)

        # check archive information
        self.assertEqual(len(info['archives']), len(self.retention))
        self.assertEqual(info['archives'][0]['points'], self.retention[0][1])
        self.assertEqual(info['archives'][0]['secondsPerPoint'],
                         self.retention[0][0])
        self.assertEqual(info['archives'][0]['retention'],
                         self.retention[0][0] * self.retention[0][1])
        self.assertEqual(info['archives'][1]['retention'],
                         self.retention[1][0] * self.retention[1][1])

    def test_info_bogus_file(self):
        self.assertIsNone(whisper.info('bogus-file'))

        # Validate "corrupt" whisper metadata
        whisper.create(self.filename, self.retention)
        with SimulatedCorruptWhisperFile():
            with AssertRaisesException(
                    whisper.CorruptWhisperFile(
                        'Unable to read header', self.filename)):
                whisper.info(self.filename)

        # Validate "corrupt" whisper archive data
        with SimulatedCorruptWhisperFile(corrupt_archive=True):
            with AssertRaisesException(
                    whisper.CorruptWhisperFile(
                        'Unable to read archive0 metadata', self.filename)):
                whisper.info(self.filename)

    def test_file_fetch_edge_cases(self):
        """
        Test some of the edge cases in file_fetch() that should return
        None or raise an exception
        """
        whisper.create(self.filename, [(1, 60)])

        with open(self.filename, 'rb') as fh:
            msg = "Invalid time interval: from time '{0}' is after until time '{1}'"
            until_time = 0
            from_time = int(time.time()) + 100

            with AssertRaisesException(
                    whisper.InvalidTimeInterval(msg.format(from_time, until_time))):
                whisper.file_fetch(fh, fromTime=from_time, untilTime=until_time)

            # fromTime > now aka metrics from the future
            self.assertIsNone(
                whisper.file_fetch(fh, fromTime=int(time.time()) + 100,
                                   untilTime=int(time.time()) + 200),
            )

            # untilTime > oldest time stored in the archive
            headers = whisper.info(self.filename)
            the_past = int(time.time()) - headers['maxRetention'] - 200
            self.assertIsNone(
                whisper.file_fetch(fh, fromTime=the_past - 1, untilTime=the_past),
            )

            # untilTime > now, change untilTime to now
            now = int(time.time())
            self.assertEqual(
                whisper.file_fetch(fh, fromTime=now, untilTime=now + 200, now=now),
                ((now + 1, now + 2, 1), [None]),
            )

    def test_merge(self):
        """
        test merging two databases
        """
        testdb = "test-%s" % self.filename

        # Create 2 whisper databases and merge one into the other
        self._update()
        self._update(testdb)

        whisper.merge(self.filename, testdb)

    def test_merge_empty(self):
        """
        test merging from an empty database
        """
        testdb_a = "test-a-%s" % self.filename
        testdb_b = "test-b-%s" % self.filename

        # create two empty databases with same retention
        self.addCleanup(self._remove, testdb_a)
        whisper.create(testdb_a, self.retention)
        self.addCleanup(self._remove, testdb_b)
        whisper.create(testdb_b, self.retention)

        whisper.merge(testdb_a, testdb_b)

    def test_merge_bad_archive_config(self):
        testdb = "test-%s" % self.filename

        # Create 2 whisper databases with different schema
        self._update()

        self.addCleanup(self._remove, testdb)
        whisper.create(testdb, [(100, 1)])

        with AssertRaisesException(
                NotImplementedError(
                    'db.wsp and test-db.wsp archive configurations are '
                    'unalike. Resize the input before merging')):
            whisper.merge(self.filename, testdb)

    def test_diff(self):
        testdb = "test-%s" % self.filename

        now = int(time.time())

        self.addCleanup(self._remove, testdb)
        whisper.create(testdb, self.retention)

        whisper.create(self.filename, self.retention)

        whisper.update(testdb, 1.0, now)
        whisper.update(self.filename, 2.0, now)

        results = whisper.diff(testdb, self.filename)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_diff_with_empty(self):
        testdb = "test-%s" % self.filename

        now = time.time()

        self.addCleanup(self._remove, testdb)
        whisper.create(testdb, self.retention)

        whisper.create(self.filename, self.retention)

        whisper.update(testdb, 1.0, now)
        whisper.update(self.filename, 2.0, now)

        # Purposefully insert nulls to strip out
        previous = now - self.retention[0][0]
        whisper.update(testdb, float('NaN'), previous)

        results = whisper.diff(testdb, self.filename, ignore_empty=True)
        self.assertEqual(
            results,
            [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)],
        )

        results_empties = whisper.diff(testdb, self.filename, ignore_empty=False)
        expected = [(0, [(int(previous), float('NaN'), None), (int(now), 1.0, 2.0)], 2), (1, [], 0)]

        # Stupidly, float('NaN') != float('NaN'), so assert that the
        # repr() results are the same :/
        #
        # See this thread:
        #    https://mail.python.org/pipermail/python-ideas/2010-March/006945.html
        self.assertEqual(
            repr(results_empties),
            repr(expected),
        )
        # Since the above test is somewhat of a sham, ensure that there
        # is a nan where there should be.
        self.assertTrue(
            math.isnan(results_empties[0][1][0][1])
        )

    def test_file_diff(self):
        testdb = "test-%s" % self.filename

        now = time.time()

        self.addCleanup(self._remove, testdb)
        whisper.create(testdb, self.retention)

        whisper.create(self.filename, self.retention)

        whisper.update(testdb, 1.0, now)
        whisper.update(self.filename, 2.0, now)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                results = whisper.file_diff(fh_1, fh_2)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_file_diff_invalid(self):
        testdb = "test-%s" % self.filename

        self.addCleanup(self._remove, testdb)
        whisper.create(testdb, [(120, 10)])

        whisper.create(self.filename, self.retention)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                with AssertRaisesException(
                        NotImplementedError(
                            'test-db.wsp and db.wsp archive configurations are '
                            'unalike. Resize the input before diffing')):
                    whisper.file_diff(fh_1, fh_2)

    def test_fetch(self):
        """
        fetch info from database
        """
        # Don't use AssertRaisesException due to a super obscure bug in
        # python2.6 which returns an IOError in the 2nd argument of __exit__
        # in a context manager as a tuple. See this for a minimal reproducer:
        #    http://git.io/cKz30g
        with self.assertRaises(IOError):
            # check a db that doesnt exist
            whisper.fetch("this_db_does_not_exist", 0)

        # SECOND MINUTE HOUR DAY
        retention = [(1, 60), (60, 60), (3600, 24), (86400, 365)]
        whisper.create(self.filename, retention)

        # check a db with an invalid time range
        now = int(time.time())
        past = now - 6000

        msg = "Invalid time interval: from time '{0}' is after until time '{1}'"
        with AssertRaisesException(whisper.InvalidTimeInterval(msg.format(now, past))):
            whisper.fetch(self.filename, now, past)

        fetch = whisper.fetch(self.filename, 0)

        # check time range
        self.assertEqual(fetch[0][1] - fetch[0][0],
                         retention[-1][0] * retention[-1][1])

        # check number of points
        self.assertEqual(len(fetch[1]), retention[-1][1])

        # check step size
        self.assertEqual(fetch[0][2], retention[-1][0])

    def _update(self, wsp=None, schema=None, sparse=False, useFallocate=False):
        wsp = wsp or self.filename
        schema = schema or [(1, 20)]

        num_data_points = 20

        # create sample data
        self.addCleanup(self._remove, wsp)
        whisper.create(wsp, schema, sparse=sparse, useFallocate=useFallocate)
        tn = int(time.time()) - num_data_points

        data = []
        for i in range(num_data_points):
            data.append((tn + 1 + i, random.random() * 10))

        # test single update
        whisper.update(wsp, data[0][1], data[0][0])

        # test multi update
        whisper.update_many(wsp, data[1:])

        return data

    def test_fadvise(self):
        original_fadvise = whisper.FADVISE_RANDOM
        whisper.FADVISE_RANDOM = True

        self._update()

        whisper.FADVISE_RANDOM = original_fadvise

    def test_lock(self):
        original_lock = whisper.LOCK
        whisper.LOCK = True

        self._update()

        whisper.LOCK = original_lock

    def test_autoflush(self):
        original_autoflush = whisper.AUTOFLUSH
        whisper.AUTOFLUSH = True

        self._update()

        whisper.AUTOFLUSH = original_autoflush

    def test_fallocate(self):
        self._update(useFallocate=True)

    def test_sparse(self):
        self._update(sparse=True)

    def test_set_xfilesfactor(self):
        """
        Create a whisper file
        Update xFilesFactor
        Check if update succeeded
        Check if exceptions get raised with wrong input
        """
        whisper.create(self.filename, [(1, 20)])

        target_xff = 0.42
        info0 = whisper.info(self.filename)
        old_xff = whisper.setXFilesFactor(self.filename, target_xff)
        # return value should match old xff
        self.assertEqual(info0['xFilesFactor'], old_xff)
        info1 = whisper.info(self.filename)

        # Other header information should not change
        self.assertEqual(info0['aggregationMethod'],
                         info1['aggregationMethod'])
        self.assertEqual(info0['maxRetention'], info1['maxRetention'])
        self.assertEqual(info0['archives'], info1['archives'])

        # packing and unpacking because
        # AssertionError: 0.20000000298023224 != 0.2
        target_xff = struct.unpack("!f", struct.pack("!f", target_xff))[0]
        self.assertEqual(info1['xFilesFactor'], target_xff)

        with AssertRaisesException(
            whisper.InvalidXFilesFactor('Invalid xFilesFactor zero, not a '
                                        'float')):
            whisper.setXFilesFactor(self.filename, "zero")

        for invalid_xff in -1, 2:
            with AssertRaisesException(
                whisper.InvalidXFilesFactor('Invalid xFilesFactor %s, not '
                                            'between 0 and 1' %
                                            float(invalid_xff))):
                whisper.setXFilesFactor(self.filename, invalid_xff)

    def test_update_single_archive(self):
        """
        Update with a single leveled archive
        """
        retention_schema = [(1, 20)]
        data = self._update(schema=retention_schema)
        # fetch the data
        fetch = whisper.fetch(self.filename, 0)   # all data
        fetch_data = fetch[1]

        for i, (timestamp, value) in enumerate(data):
            # is value in the fetched data?
            self.assertEqual(value, fetch_data[i])

        # check TimestampNotCovered
        with AssertRaisesException(
                whisper.TimestampNotCovered(
                    'Timestamp not covered by any archives in this database.')):
            # in the futur
            whisper.update(self.filename, 1.337, time.time() + 1)

        with AssertRaisesException(
                whisper.TimestampNotCovered(
                    'Timestamp not covered by any archives in this database.')):
            # before the past
            whisper.update(self.filename, 1.337, time.time() - retention_schema[0][1] - 1)

        # When no timestamp is passed in, it should use the current time
        original_lock = whisper.LOCK
        whisper.LOCK = True
        whisper.update(self.filename, 3.7337, None)
        fetched = whisper.fetch(self.filename, 0)[1]
        self.assertEqual(fetched[-1], 3.7337)

        whisper.LOCK = original_lock

    def test_update_many_excess(self):
        # given an empty db
        wsp = "test_update_many_excess.wsp"
        self.addCleanup(self._remove, wsp)
        archive_len = 3
        archive_step = 1
        whisper.create(wsp, [(archive_step, archive_len)])

        # given too many points than the db can hold
        excess_len = 1
        num_input_points = archive_len + excess_len
        test_now = int(time.time())
        input_start = test_now - num_input_points + archive_step
        input_points = [(input_start + i, random.random() * 10)
                        for i in range(num_input_points)]

        # when the db is updated with too many points
        whisper.update_many(wsp, input_points, now=test_now)

        # then only the most recent input points (those at the end) were written
        actual_time_info = whisper.fetch(wsp, 0, now=test_now)[0]
        self.assertEqual(actual_time_info,
                         (input_points[-archive_len][0],
                          input_points[-1][0] + archive_step,  # untilInterval = newest + step
                          archive_step))

    def test_debug(self):
        """
        Test creating a file with debug enabled
        Should print debug messages to stdout
        """
        # debug prints to stdout, redirect it to a variable
        old_stdout = sys.stdout
        sys.stdout = StringIO()

        whisper.disableDebug()
        whisper.enableDebug()
        self._update()
        whisper.disableDebug()

        sys.stdout.seek(0)
        out = sys.stdout.read()

        sys.stdout = old_stdout

        assertRegex(self, out, r'(DEBUG :: (WRITE|READ) \d+ bytes #\d+\n)+')

    # TODO: This test method takes more time than virtually every
    #       single other test combined. Profile this code and potentially
    #       fix the underlying reason
    def test_setAggregation(self):
        """
        Create a db, change aggregation, xFilesFactor, then use info() to validate
        """
        original_lock = whisper.LOCK
        original_caching = whisper.CACHE_HEADERS
        original_autoflush = whisper.AUTOFLUSH

        whisper.LOCK = True
        whisper.AUTOFLUSH = True
        whisper.CACHE_HEADERS = True
        # create a new db with a valid configuration
        whisper.create(self.filename, self.retention)

        with AssertRaisesException(
                whisper.InvalidAggregationMethod(
                    'Unrecognized aggregation method: yummy beer')):
            whisper.setAggregationMethod(self.filename, 'yummy beer')

        # set setting every AggregationMethod available
        for ag in whisper.aggregationMethods:
          for xff in 0.0, 0.2, 0.4, 0.7, 0.75, 1.0:
            # original xFilesFactor
            info0 = whisper.info(self.filename)
            # optional xFilesFactor not passed
            old_ag = whisper.setAggregationMethod(self.filename, ag)

            # should return old aggregationmethod
            self.assertEqual(old_ag, info0['aggregationMethod'])

            # original value should not change
            info1 = whisper.info(self.filename)
            self.assertEqual(info0['xFilesFactor'], info1['xFilesFactor'])

            # the selected aggregation method should have applied
            self.assertEqual(ag, info1['aggregationMethod'])

            # optional xFilesFactor used
            old_ag = whisper.setAggregationMethod(self.filename, ag, xff)
            # should return old aggregationmethod
            self.assertEqual(old_ag, info1['aggregationMethod'])
            # new info should match what we just set it to
            info2 = whisper.info(self.filename)
            # packing and unpacking because
            # AssertionError: 0.20000000298023224 != 0.2
            target_xff = struct.unpack("!f", struct.pack("!f", xff))[0]
            self.assertEqual(info2['xFilesFactor'], target_xff)

            # same aggregationMethod assertion again, but double-checking since
            # we are playing with packed values and seek()
            self.assertEqual(ag, info2['aggregationMethod'])

            with SimulatedCorruptWhisperFile():
                with AssertRaisesException(
                        whisper.CorruptWhisperFile(
                            'Unable to read header', self.filename)):
                    whisper.setAggregationMethod(self.filename, ag)

        whisper.LOCK = original_lock
        whisper.AUTOFLUSH = original_autoflush
        whisper.CACHE_HEADERS = original_caching

    def test_fetch_with_archive_to_select(self):
        """
        fetch info from database providing the archive to select
        """

        # SECOND MINUTE HOUR DAY
        retention = [(1, 60), (60, 60), (3600, 24), (86400, 365)]
        whisper.create(self.filename, retention)

        archives = ["1s", "1m", "1h", "1d"]

        for i in range(len(archives)):
            fetch = whisper.fetch(self.filename, 0, archiveToSelect=archives[i])
            self.assertEqual(fetch[0][2], retention[i][0])

            # check time range
            self.assertEqual(fetch[0][1] - fetch[0][0], retention[-1][0] * retention[-1][1])
        with AssertRaisesException(ValueError("Invalid granularity: 2")):
            fetch = whisper.fetch(self.filename, 0, archiveToSelect="2s")

    def test_resize_with_aggregate(self):
        """resize whisper file with aggregate"""
        # 60s per point save two days
        retention = [(60, 60 * 24 * 2)]
        whisper.create(self.filename, retention)

        # insert data
        now_timestamp = int((datetime.now() - datetime(1970, 1, 1)).total_seconds())
        now_timestamp -= now_timestamp % 60  # format timestamp
        points = [(now_timestamp - i * 60, i) for i in range(0, 60 * 24 * 2)]
        whisper.update_many(self.filename, points)
        data = whisper.fetch(self.filename,
                             fromTime=now_timestamp - 3600 * 25,
                             untilTime=now_timestamp - 3600 * 25 + 60 * 10)
        self.assertEqual(len(data[1]), 10)
        self.assertEqual(data[0][2], 60)  # high retention == 60
        for d in data[1]:
            self.assertIsNotNone(d)
        # resize from high to low
        os.system('whisper-resize.py %s 60s:1d 300s:2d --aggregate --nobackup >/dev/null' % self.filename)  # noqa
        data_low = whisper.fetch(
            self.filename, fromTime=now_timestamp - 3600 * 25,
            untilTime=now_timestamp - 3600 * 25 + 60 * 10)
        self.assertEqual(len(data_low[1]), 2)
        self.assertEqual(data_low[0][2], 300)  # low retention == 300
        for d in data_low[1]:
            self.assertIsNotNone(d)
        data_high = whisper.fetch(
            self.filename, fromTime=now_timestamp - 60 * 10,
            untilTime=now_timestamp
        )
        self.assertEqual(len(data_high[1]), 10)
        self.assertEqual(data_high[0][2], 60)  # high retention == 60
        # resize from low to high
        os.system('whisper-resize.py %s 60s:2d --aggregate --nobackup >/dev/null' % self.filename) # noqa
        data1 = whisper.fetch(
            self.filename, fromTime=now_timestamp - 3600 * 25,
            untilTime=now_timestamp - 3600 * 25 + 60 * 10)
        self.assertEqual(len(data1[1]), 10)
        # noqa data1 looks like ((1588836720, 1588837320, 60), [None, None, 1490.0, None, None, None, None, 1485.0, None, None])
        # data1[1] have two not none value
        self.assertEqual(len(list(filter(lambda x: x is not None, data1[1]))),
                         2)
        data2 = whisper.fetch(
            self.filename, fromTime=now_timestamp - 60 * 15,
            untilTime=now_timestamp - 60 * 5)
        # noqa data2 looks like ((1588925820, 1588926420, 60), [10.0, 11.0, 10.0, 9.0, 8.0, 5.0, 6.0, 5.0, 4.0, 3.0])
        self.assertEqual(len(list(filter(lambda x: x is not None, data2[1]))),
                         10)

        # clean up
        self.tearDown()


class TestgetUnitString(unittest.TestCase):
    def test_function(self):
        for unit in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            value = whisper.getUnitString(unit[0])
            self.assertEqual(value, unit)

    def test_invalid_unit(self):
        with AssertRaisesException(ValueError("Invalid unit 'z'")):
            whisper.getUnitString('z')


# If you send an invalid file, this deadlocks my Fedora 21 / Linux 3.17 laptop
# TODO: Find a way to pass in corrupt whisper files that don't deadlock the testing box
class TestReadHeader(WhisperTestBase):
    def test_normal(self):
        whisper.create(self.filename, [(1, 60), (60, 60)])

        whisper.CACHE_HEADERS = True
        whisper.info(self.filename)
        whisper.info(self.filename)
        whisper.CACHE_HEADERS = False


class TestParseRetentionDef(unittest.TestCase):
    def test_valid_retentions(self):
        retention_map = (
            ('60:10', (60, 10)),
            ('10:60', (10, 60)),
            ('10s:10h', (10, 3600)),
        )
        for retention, expected in retention_map:
            results = whisper.parseRetentionDef(retention)
            self.assertEqual(results, expected)

    def test_invalid_retentions(self):
        retention_map = (
            # From getUnitString
            ('10x:10', ValueError("Invalid unit 'x'")),
            ('60:10x', ValueError("Invalid unit 'x'")),

            # From parseRetentionDef
            ('10', ValueError("Invalid retention definition '10'")),
            ('10X:10', ValueError("Invalid precision specification '10X'")),
            ('10:10$', ValueError("Invalid retention specification '10$'")),
            ('60:10', (60, 10)),
        )
        for retention, expected_exc in retention_map:
            try:
                results = whisper.parseRetentionDef(retention)
            except expected_exc.__class__ as exc:
                self.assertEqual(
                    str(expected_exc),
                    str(exc),
                )
                self.assertEqual(
                    expected_exc.__class__,
                    exc.__class__,
                )
            else:
                # When there isn't an exception raised
                self.assertEqual(results, expected_exc)


class TestCorruptWhisperFile(unittest.TestCase):
    def setUp(self):
        self.path = '/opt/graphite/storage/whisper/moolah.wsp'
        self.error = 'What is the average velocity of an unladen swallow?'

    def test_error(self):
        try:
            raise whisper.CorruptWhisperFile(self.error, self.path)
        except whisper.CorruptWhisperFile as exc:
            self.assertEqual(exc.error, self.error)

    def test_path(self):
        try:
            raise whisper.CorruptWhisperFile(self.error, self.path)
        except whisper.CorruptWhisperFile as exc:
            self.assertEqual(exc.path, self.path)

    def test_repr(self):
        try:
            raise whisper.CorruptWhisperFile(self.error, self.path)
        except whisper.CorruptWhisperFile as exc:
            self.assertEqual(
                repr(exc),
                '<CorruptWhisperFile[%s] %s>' % (self.path, self.error),
            )

    def test_str(self):
        try:
            raise whisper.CorruptWhisperFile(self.error, self.path)
        except whisper.CorruptWhisperFile as exc:
            self.assertEqual(
                str(exc),
                "{0} ({1})".format(self.error, self.path)
            )


if __name__ == '__main__':
    unittest.main()

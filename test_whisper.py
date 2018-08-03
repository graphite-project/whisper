#!/usr/bin/env python

import os
import sys
import time
import math
import random
import struct
import errno

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
import memwhisper


class SimulatedCorruptWhisperFile(object):
    """
    Simple context manager to be used as a decorator for simulating a
    corrupt whisper file for testing purposes.

    Example:

        >>> memwhisper.create('test.wsp', [(60, 10)])
        >>> with SimulatedCorruptWhisperFile():
        ...     memwhisper.info('test.wsp')

    When 'corrupt_archive' is passed as True, the metadata will be left
    intact, but the archive will seem corrupted.
    """
    def __init__(self, corrupt_archive=False):
        self.corrupt_archive = corrupt_archive

        self.metadataFormat = memwhisper.metadataFormat
        self.archiveInfoFormat = memwhisper.archiveInfoFormat
        self.CACHE_HEADERS = memwhisper.CACHE_HEADERS

    def __enter__(self):
        # Force the struct unpack to fail by changing the metadata
        # format. This simulates an actual corrupted whisper file
        if not self.corrupt_archive:
            memwhisper.metadataFormat = '!ssss'
        else:
            memwhisper.archiveInfoFormat = '!ssss'

        # Force whisper to reread the header instead of returning
        # the previous (correct) header from the header cache
        memwhisper.CACHE_HEADERS = False

    def __exit__(self, *args, **kwargs):
        memwhisper.metadataFormat = self.metadataFormat
        memwhisper.archiveInfoFormat = self.archiveInfoFormat
        memwhisper.CACHE_HEADERS = self.CACHE_HEADERS


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
                memwhisper.InvalidConfiguration(
                    'You must specify at least one archive configuration!')):
            memwhisper.validateArchiveList([])

    def test_duplicate(self):
        """
        Checking duplicates
        """
        # TODO: Fix the lies with whisper.validateArchiveList() saying it returns True/False
        self.assertIsNone(memwhisper.validateArchiveList(self.retention))

        with AssertRaisesException(
                memwhisper.InvalidConfiguration(
                    'A Whisper database may not be configured having two '
                    'archives with the same precision (archive0: (1, 60), '
                    'archive1: (1, 60))')):
            memwhisper.validateArchiveList([(1, 60), (60, 60), (1, 60)])

    def test_even_precision_division(self):
        """
        even precision division
        """
        memwhisper.validateArchiveList([(60, 60), (6, 60)])
        with AssertRaisesException(
                memwhisper.InvalidConfiguration(
                    "Higher precision archives' precision must evenly divide "
                    "all lower precision archives' precision (archive0: 7, "
                    "archive1: 60)")):
            memwhisper.validateArchiveList([(60, 60), (7, 60)])

    def test_timespan_coverage(self):
        """
        timespan coverage
        """
        memwhisper.validateArchiveList(self.retention)
        with AssertRaisesException(
                memwhisper.InvalidConfiguration(
                    'Lower precision archives must cover larger time intervals '
                    'than higher precision archives (archive0: 60 seconds, '
                    'archive1: 10 seconds)')):
            memwhisper.validateArchiveList([(1, 60), (10, 1)])

    def test_number_of_points(self):
        """
        number of points
        """
        memwhisper.validateArchiveList(self.retention)
        with AssertRaisesException(
                memwhisper.InvalidConfiguration(
                    "Each archive must have at least enough points to "
                    "consolidate to the next archive (archive1 consolidates 60 "
                    "of archive0's points but it has only 30 total points)")):
            memwhisper.validateArchiveList([(1, 30), (60, 60)])

    def test_aggregate(self):
        """
        aggregate functions
        """
        # min of 1-4
        self.assertEqual(memwhisper.aggregate('min', [1, 2, 3, 4]), 1)
        # max of 1-4
        self.assertEqual(memwhisper.aggregate('max', [1, 2, 3, 4]), 4)
        # last element in the known values
        self.assertEqual(memwhisper.aggregate('last', [3, 2, 5, 4]), 4)
        # sum ALL THE VALUES!
        self.assertEqual(memwhisper.aggregate('sum', [10, 2, 3, 4]), 19)
        # average of the list elements
        self.assertEqual(memwhisper.aggregate('average', [1, 2, 3, 4]), 2.5)
        avg_zero = [1, 2, 3, 4, None, None, None, None]
        non_null = [i for i in avg_zero if i is not None]
        self.assertEqual(memwhisper.aggregate('avg_zero', non_null, avg_zero), 1.25)
        # avg_zero without neighborValues
        with self.assertRaises(memwhisper.InvalidAggregationMethod):
            memwhisper.aggregate('avg_zero', non_null)
        # absmax with negative max
        self.assertEqual(memwhisper.aggregate('absmax', [-3, -2, 1, 2]), -3)
        # absmax with positive max
        self.assertEqual(memwhisper.aggregate('absmax', [-2, -1, 2, 3]), 3)
        # absmin with positive min
        self.assertEqual(memwhisper.aggregate('absmin', [-3, -2, 1, 2]), 1)
        # absmin with negative min
        self.assertEqual(memwhisper.aggregate('absmin', [-2, -1, 2, 3]), -1)

        with AssertRaisesException(
                memwhisper.InvalidAggregationMethod(
                    'Unrecognized aggregation method derp')):
            memwhisper.aggregate('derp', [12, 2, 3123, 1])

    def _test_create_exception(self, exception_method='write', e=None):
        """
        Behaviour when creating a whisper file on a full filesystem
        """
        m_open = mock_open()
        # Get the mocked file object and override interresting attributes
        m_file = m_open.return_value
        m_file.name = self.filename
        method = getattr(m_file, exception_method)

        if not e:
          e = IOError(errno.ENOSPC, "Mocked IOError")
        method.side_effect = e

        with patch('whisper.open', m_open, create=True):
          with patch('os.unlink') as m_unlink:
            self.assertRaises(e.__class__, memwhisper.create, self.filename, self.retention)

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
                    memwhisper.InvalidConfiguration(
                        'You must specify at least one archive configuration!')):
                memwhisper.create(self.filename, retention)

        # create a new db with a valid configuration
        memwhisper.create(self.filename, self.retention)

        # Ensure another file can't be created when one exists already
        with AssertRaisesException(
                memwhisper.InvalidConfiguration(
                    'File {0} already exists!'.format(self.filename))):
            memwhisper.create(self.filename, self.retention)

        info = memwhisper.info(self.filename)

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
        self.assertIsNone(memwhisper.info('bogus-file'))

        # Validate "corrupt" whisper metadata
        memwhisper.create(self.filename, self.retention)
        with SimulatedCorruptWhisperFile():
            with AssertRaisesException(
                    memwhisper.CorruptWhisperFile(
                        'Unable to read header', self.filename)):
                memwhisper.info(self.filename)

        # Validate "corrupt" whisper archive data
        with SimulatedCorruptWhisperFile(corrupt_archive=True):
            with AssertRaisesException(
                    memwhisper.CorruptWhisperFile(
                        'Unable to read archive0 metadata', self.filename)):
                memwhisper.info(self.filename)

    def test_file_fetch_edge_cases(self):
        """
        Test some of the edge cases in file_fetch() that should return
        None or raise an exception
        """
        memwhisper.create(self.filename, [(1, 60)])

        with open(self.filename, 'rb') as fh:
            msg = "Invalid time interval: from time '{0}' is after until time '{1}'"
            until_time = 0
            from_time = int(time.time()) + 100

            with AssertRaisesException(
                    memwhisper.InvalidTimeInterval(msg.format(from_time, until_time))):
                memwhisper.file_fetch(fh, fromTime=from_time, untilTime=until_time)

            # fromTime > now aka metrics from the future
            self.assertIsNone(
                memwhisper.file_fetch(fh, fromTime=int(time.time()) + 100,
                                      untilTime=int(time.time()) + 200),
            )

            # untilTime > oldest time stored in the archive
            headers = memwhisper.info(self.filename)
            the_past = int(time.time()) - headers['maxRetention'] - 200
            self.assertIsNone(
                memwhisper.file_fetch(fh, fromTime=the_past - 1, untilTime=the_past),
            )

            # untilTime > now, change untilTime to now
            now = int(time.time())
            self.assertEqual(
                memwhisper.file_fetch(fh, fromTime=now, untilTime=now + 200, now=now),
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

        memwhisper.merge(self.filename, testdb)

    def test_merge_empty(self):
        """
        test merging from an empty database
        """
        testdb_a = "test-a-%s" % self.filename
        testdb_b = "test-b-%s" % self.filename

        # create two empty databases with same retention
        self.addCleanup(self._remove, testdb_a)
        memwhisper.create(testdb_a, self.retention)
        self.addCleanup(self._remove, testdb_b)
        memwhisper.create(testdb_b, self.retention)

        memwhisper.merge(testdb_a, testdb_b)

    def test_merge_bad_archive_config(self):
        testdb = "test-%s" % self.filename

        # Create 2 whisper databases with different schema
        self._update()

        self.addCleanup(self._remove, testdb)
        memwhisper.create(testdb, [(100, 1)])

        with AssertRaisesException(
                NotImplementedError(
                    'db.wsp and test-db.wsp archive configurations are '
                    'unalike. Resize the input before merging')):
            memwhisper.merge(self.filename, testdb)

    def test_diff(self):
        testdb = "test-%s" % self.filename

        now = int(time.time())

        self.addCleanup(self._remove, testdb)
        memwhisper.create(testdb, self.retention)

        memwhisper.create(self.filename, self.retention)

        memwhisper.update(testdb, 1.0, now)
        memwhisper.update(self.filename, 2.0, now)

        results = memwhisper.diff(testdb, self.filename)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_diff_with_empty(self):
        testdb = "test-%s" % self.filename

        now = time.time()

        self.addCleanup(self._remove, testdb)
        memwhisper.create(testdb, self.retention)

        memwhisper.create(self.filename, self.retention)

        memwhisper.update(testdb, 1.0, now)
        memwhisper.update(self.filename, 2.0, now)

        # Purposefully insert nulls to strip out
        previous = now - self.retention[0][0]
        memwhisper.update(testdb, float('NaN'), previous)

        results = memwhisper.diff(testdb, self.filename, ignore_empty=True)
        self.assertEqual(
            results,
            [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)],
        )

        results_empties = memwhisper.diff(testdb, self.filename, ignore_empty=False)
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
        memwhisper.create(testdb, self.retention)

        memwhisper.create(self.filename, self.retention)

        memwhisper.update(testdb, 1.0, now)
        memwhisper.update(self.filename, 2.0, now)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                results = memwhisper.file_diff(fh_1, fh_2)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_file_diff_invalid(self):
        testdb = "test-%s" % self.filename

        self.addCleanup(self._remove, testdb)
        memwhisper.create(testdb, [(120, 10)])

        memwhisper.create(self.filename, self.retention)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                with AssertRaisesException(
                        NotImplementedError(
                            'test-db.wsp and db.wsp archive configurations are '
                            'unalike. Resize the input before diffing')):
                    memwhisper.file_diff(fh_1, fh_2)

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
            memwhisper.fetch("this_db_does_not_exist", 0)

        # SECOND MINUTE HOUR DAY
        retention = [(1, 60), (60, 60), (3600, 24), (86400, 365)]
        memwhisper.create(self.filename, retention)

        # check a db with an invalid time range
        now = int(time.time())
        past = now - 6000

        msg = "Invalid time interval: from time '{0}' is after until time '{1}'"
        with AssertRaisesException(memwhisper.InvalidTimeInterval(msg.format(now, past))):
            memwhisper.fetch(self.filename, now, past)

        fetch = memwhisper.fetch(self.filename, 0)

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
        memwhisper.create(wsp, schema, sparse=sparse, useFallocate=useFallocate)
        tn = int(time.time()) - num_data_points

        data = []
        for i in range(num_data_points):
            data.append((tn + 1 + i, random.random() * 10))

        # test single update
        memwhisper.update(wsp, data[0][1], data[0][0])

        # test multi update
        memwhisper.update_many(wsp, data[1:])

        return data

    def test_fadvise(self):
        original_fadvise = memwhisper.FADVISE_RANDOM
        memwhisper.FADVISE_RANDOM = True

        self._update()

        memwhisper.FADVISE_RANDOM = original_fadvise

    def test_lock(self):
        original_lock = memwhisper.LOCK
        memwhisper.LOCK = True

        self._update()

        memwhisper.LOCK = original_lock

    def test_autoflush(self):
        original_autoflush = memwhisper.AUTOFLUSH
        memwhisper.AUTOFLUSH = True

        self._update()

        memwhisper.AUTOFLUSH = original_autoflush

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
        memwhisper.create(self.filename, [(1, 20)])

        target_xff = 0.42
        info0 = memwhisper.info(self.filename)
        old_xff = memwhisper.setXFilesFactor(self.filename, target_xff)
        # return value should match old xff
        self.assertEqual(info0['xFilesFactor'], old_xff)
        info1 = memwhisper.info(self.filename)

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
            memwhisper.InvalidXFilesFactor('Invalid xFilesFactor zero, not a '
                                        'float')):
            memwhisper.setXFilesFactor(self.filename, "zero")

        for invalid_xff in -1, 2:
            with AssertRaisesException(
                memwhisper.InvalidXFilesFactor('Invalid xFilesFactor %s, not '
                                            'between 0 and 1' %
                                               float(invalid_xff))):
                memwhisper.setXFilesFactor(self.filename, invalid_xff)

    def test_update_single_archive(self):
        """
        Update with a single leveled archive
        """
        retention_schema = [(1, 20)]
        data = self._update(schema=retention_schema)
        # fetch the data
        fetch = memwhisper.fetch(self.filename, 0)   # all data
        fetch_data = fetch[1]

        for i, (timestamp, value) in enumerate(data):
            # is value in the fetched data?
            self.assertEqual(value, fetch_data[i])

        # check TimestampNotCovered
        with AssertRaisesException(
                memwhisper.TimestampNotCovered(
                    'Timestamp not covered by any archives in this database.')):
            # in the futur
            memwhisper.update(self.filename, 1.337, time.time() + 1)

        with AssertRaisesException(
                memwhisper.TimestampNotCovered(
                    'Timestamp not covered by any archives in this database.')):
            # before the past
            memwhisper.update(self.filename, 1.337, time.time() - retention_schema[0][1] - 1)

        # When no timestamp is passed in, it should use the current time
        original_lock = memwhisper.LOCK
        memwhisper.LOCK = True
        memwhisper.update(self.filename, 3.7337, None)
        fetched = memwhisper.fetch(self.filename, 0)[1]
        self.assertEqual(fetched[-1], 3.7337)

        memwhisper.LOCK = original_lock

    def test_update_many_excess(self):
        # given an empty db
        wsp = "test_update_many_excess.wsp"
        self.addCleanup(self._remove, wsp)
        archive_len = 3
        archive_step = 1
        memwhisper.create(wsp, [(archive_step, archive_len)])

        # given too many points than the db can hold
        excess_len = 1
        num_input_points = archive_len + excess_len
        test_now = int(time.time())
        input_start = test_now - num_input_points + archive_step
        input_points = [(input_start + i, random.random() * 10)
                        for i in range(num_input_points)]

        # when the db is updated with too many points
        memwhisper.update_many(wsp, input_points, now=test_now)

        # then only the most recent input points (those at the end) were written
        actual_time_info = memwhisper.fetch(wsp, 0, now=test_now)[0]
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

        memwhisper.disableDebug()
        memwhisper.enableDebug()
        self._update()
        memwhisper.disableDebug()

        sys.stdout.seek(0)
        out = sys.stdout.read()

        sys.stdout = old_stdout

        assertRegex(self, out, '(DEBUG :: (WRITE|READ) \d+ bytes #\d+\n)+')

    # TODO: This test method takes more time than virtually every
    #       single other test combined. Profile this code and potentially
    #       fix the underlying reason
    def test_setAggregation(self):
        """
        Create a db, change aggregation, xFilesFactor, then use info() to validate
        """
        original_lock = memwhisper.LOCK
        original_caching = memwhisper.CACHE_HEADERS
        original_autoflush = memwhisper.AUTOFLUSH

        memwhisper.LOCK = True
        memwhisper.AUTOFLUSH = True
        memwhisper.CACHE_HEADERS = True
        # create a new db with a valid configuration
        memwhisper.create(self.filename, self.retention)

        with AssertRaisesException(
                memwhisper.InvalidAggregationMethod(
                    'Unrecognized aggregation method: yummy beer')):
            memwhisper.setAggregationMethod(self.filename, 'yummy beer')

        # set setting every AggregationMethod available
        for ag in memwhisper.aggregationMethods:
          for xff in 0.0, 0.2, 0.4, 0.7, 0.75, 1.0:
            # original xFilesFactor
            info0 = memwhisper.info(self.filename)
            # optional xFilesFactor not passed
            old_ag = memwhisper.setAggregationMethod(self.filename, ag)

            # should return old aggregationmethod
            self.assertEqual(old_ag, info0['aggregationMethod'])

            # original value should not change
            info1 = memwhisper.info(self.filename)
            self.assertEqual(info0['xFilesFactor'], info1['xFilesFactor'])

            # the selected aggregation method should have applied
            self.assertEqual(ag, info1['aggregationMethod'])

            # optional xFilesFactor used
            old_ag = memwhisper.setAggregationMethod(self.filename, ag, xff)
            # should return old aggregationmethod
            self.assertEqual(old_ag, info1['aggregationMethod'])
            # new info should match what we just set it to
            info2 = memwhisper.info(self.filename)
            # packing and unpacking because
            # AssertionError: 0.20000000298023224 != 0.2
            target_xff = struct.unpack("!f", struct.pack("!f", xff))[0]
            self.assertEqual(info2['xFilesFactor'], target_xff)

            # same aggregationMethod assertion again, but double-checking since
            # we are playing with packed values and seek()
            self.assertEqual(ag, info2['aggregationMethod'])

            with SimulatedCorruptWhisperFile():
                with AssertRaisesException(
                        memwhisper.CorruptWhisperFile(
                            'Unable to read header', self.filename)):
                    memwhisper.setAggregationMethod(self.filename, ag)

        memwhisper.LOCK = original_lock
        memwhisper.AUTOFLUSH = original_autoflush
        memwhisper.CACHE_HEADERS = original_caching

    def test_fetch_with_archive_to_select(self):
        """
        fetch info from database providing the archive to select
        """

        # SECOND MINUTE HOUR DAY
        retention = [(1, 60), (60, 60), (3600, 24), (86400, 365)]
        memwhisper.create(self.filename, retention)

        archives = ["1s", "1m", "1h", "1d"]

        for i in range(len(archives)):
            fetch = memwhisper.fetch(self.filename, 0, archiveToSelect=archives[i])
            self.assertEqual(fetch[0][2], retention[i][0])

            # check time range
            self.assertEqual(fetch[0][1] - fetch[0][0], retention[-1][0] * retention[-1][1])
        with AssertRaisesException(ValueError("Invalid granularity: 2")):
            fetch = memwhisper.fetch(self.filename, 0, archiveToSelect="2s")


class TestgetUnitString(unittest.TestCase):
    def test_function(self):
        for unit in ('seconds', 'minutes', 'hours', 'days', 'weeks'):
            value = memwhisper.getUnitString(unit[0])
            self.assertEqual(value, unit)

    def test_invalid_unit(self):
        with AssertRaisesException(ValueError("Invalid unit 'z'")):
            memwhisper.getUnitString('z')


# If you send an invalid file, this deadlocks my Fedora 21 / Linux 3.17 laptop
# TODO: Find a way to pass in corrupt whisper files that don't deadlock the testing box
class TestReadHeader(WhisperTestBase):
    def test_normal(self):
        memwhisper.create(self.filename, [(1, 60), (60, 60)])

        memwhisper.CACHE_HEADERS = True
        memwhisper.info(self.filename)
        memwhisper.info(self.filename)
        memwhisper.CACHE_HEADERS = False


class TestParseRetentionDef(unittest.TestCase):
    def test_valid_retentions(self):
        retention_map = (
            ('60:10', (60, 10)),
            ('10:60', (10, 60)),
            ('10s:10h', (10, 3600)),
        )
        for retention, expected in retention_map:
            results = memwhisper.parseRetentionDef(retention)
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
                results = memwhisper.parseRetentionDef(retention)
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
            raise memwhisper.CorruptWhisperFile(self.error, self.path)
        except memwhisper.CorruptWhisperFile as exc:
            self.assertEqual(exc.error, self.error)

    def test_path(self):
        try:
            raise memwhisper.CorruptWhisperFile(self.error, self.path)
        except memwhisper.CorruptWhisperFile as exc:
            self.assertEqual(exc.path, self.path)

    def test_repr(self):
        try:
            raise memwhisper.CorruptWhisperFile(self.error, self.path)
        except memwhisper.CorruptWhisperFile as exc:
            self.assertEqual(
                repr(exc),
                '<CorruptWhisperFile[%s] %s>' % (self.path, self.error),
            )

    def test_str(self):
        try:
            raise memwhisper.CorruptWhisperFile(self.error, self.path)
        except memwhisper.CorruptWhisperFile as exc:
            self.assertEqual(
                str(exc),
                "{0} ({1})".format(self.error, self.path)
            )


if __name__ == '__main__':
    unittest.main()

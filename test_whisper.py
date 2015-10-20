#!/usr/bin/env python

import os
import time
import math
import random
import struct

import errno

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

    def __enter__(self):
        # Force the struct unpack to fail by changing the metadata
        # format. This simulates an actual corrupted whisper file
        if not self.corrupt_archive:
            whisper.metadataFormat = '!ssss'
        else:
            whisper.archiveInfoFormat = '!ssss'

    def __exit__(self, *args, **kwargs):
        whisper.metadataFormat = self.metadataFormat
        whisper.archiveInfoFormat = self.archiveInfoFormat


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
        with AssertRaisesException(whisper.InvalidConfiguration('You must specify at least one archive configuration!')):
            whisper.validateArchiveList([])

    def test_duplicate(self):
        """
        Checking duplicates
        """
        # TODO: Fix the lies with whisper.validateArchiveList() saying it returns True/False
        self.assertIsNone(whisper.validateArchiveList(self.retention))

        with AssertRaisesException(whisper.InvalidConfiguration('A Whisper database may not be configured having two archives with the same precision (archive0: (1, 60), archive1: (1, 60))')):
            whisper.validateArchiveList([(1, 60), (60, 60), (1, 60)])

    def test_even_precision_division(self):
        """
        even precision division
        """
        whisper.validateArchiveList([(60, 60), (6, 60)])
        with AssertRaisesException(whisper.InvalidConfiguration("Higher precision archives' precision must evenly divide all lower precision archives' precision (archive0: 7, archive1: 60)")):
            whisper.validateArchiveList([(60, 60), (7, 60)])

    def test_timespan_coverage(self):
        """
        timespan coverage
        """
        whisper.validateArchiveList(self.retention)
        with AssertRaisesException(whisper.InvalidConfiguration('Lower precision archives must cover larger time intervals than higher precision archives (archive0: 60 seconds, archive1: 10 seconds)')):
            whisper.validateArchiveList([(1, 60), (10, 1)])

    def test_number_of_points(self):
        """
        number of points
        """
        whisper.validateArchiveList(self.retention)
        with AssertRaisesException(whisper.InvalidConfiguration("Each archive must have at least enough points to consolidate to the next archive (archive1 consolidates 60 of archive0's points but it has only 30 total points)")):
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

        with AssertRaisesException(whisper.InvalidAggregationMethod('Unrecognized aggregation method derp')):
            whisper.aggregate('derp', [12, 2, 3123, 1])

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
            with AssertRaisesException(whisper.InvalidConfiguration('You must specify at least one archive configuration!')):
                whisper.create(self.filename, retention)

        # create a new db with a valid configuration
        whisper.create(self.filename, self.retention)

        # Ensure another file can't be created when one exists already
        with AssertRaisesException(whisper.InvalidConfiguration('File {0} already exists!'.format(self.filename))):
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
            with AssertRaisesException(whisper.CorruptWhisperFile('Unable to read header', self.filename)):
                whisper.info(self.filename)

        # Validate "corrupt" whisper archive data
        with SimulatedCorruptWhisperFile(corrupt_archive=True):
            with AssertRaisesException(whisper.CorruptWhisperFile('Unable to read archive0 metadata', self.filename)):
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

            with AssertRaisesException(whisper.InvalidTimeInterval(msg.format(from_time, until_time))):
                whisper.file_fetch(fh, fromTime=from_time, untilTime=until_time)

            # fromTime > now aka metrics from the future
            self.assertIsNone(
                whisper.file_fetch(fh, fromTime=int(time.time()) + 100, untilTime=int(time.time()) + 200),
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
        self._remove(testdb)

    def test_merge_bad_archive_config(self):
        testdb = "test-%s" % self.filename

        # Create 2 whisper databases with different schema
        self._update()
        whisper.create(testdb, [(100, 1)])

        with AssertRaisesException(NotImplementedError('db.wsp and test-db.wsp archive configurations are unalike. Resize the input before merging')):
            whisper.merge(self.filename, testdb)

        self._remove(testdb)

    def test_diff(self):
        testdb = "test-%s" % self.filename

        now = time.time()

        whisper.create(testdb, self.retention)
        whisper.create(self.filename, self.retention)
        whisper.update(testdb, 1.0, now)
        whisper.update(self.filename, 2.0, now)

        results = whisper.diff(testdb, self.filename)
        self._remove(testdb)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_diff_with_empty(self):
        testdb = "test-%s" % self.filename

        now = time.time()

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
        self._remove(testdb)

    def test_file_diff(self):
        testdb = "test-%s" % self.filename

        now = time.time()

        whisper.create(testdb, self.retention)
        whisper.create(self.filename, self.retention)
        whisper.update(testdb, 1.0, now)
        whisper.update(self.filename, 2.0, now)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                results = whisper.file_diff(fh_1, fh_2)
        self._remove(testdb)

        expected = [(0, [(int(now), 1.0, 2.0)], 1), (1, [], 0)]

        self.assertEqual(results, expected)

    def test_file_diff_invalid(self):
        testdb = "test-%s" % self.filename

        whisper.create(testdb, [(120, 10)])
        whisper.create(self.filename, self.retention)

        # Merging 2 archives with different retentions should fail
        with open(testdb, 'rb') as fh_1:
            with open(self.filename, 'rb+') as fh_2:
                with AssertRaisesException(NotImplementedError('test-db.wsp and db.wsp archive configurations are unalike. Resize the input before diffing')):
                    whisper.file_diff(fh_1, fh_2)
        self._remove(testdb)

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

    def _update(self, wsp=None, schema=None):
        wsp = wsp or self.filename
        schema = schema or [(1, 20)]

        num_data_points = 20

        # create sample data
        whisper.create(wsp, schema)
        tn = time.time() - num_data_points
        data = []
        for i in range(num_data_points):
            data.append((tn + 1 + i, random.random() * 10))

        # test single update
        whisper.update(wsp, data[0][1], data[0][0])

        # test multi update
        whisper.update_many(wsp, data[1:])
        return data

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
        with AssertRaisesException(whisper.TimestampNotCovered('Timestamp not covered by any archives in this database.')):
            # in the futur
            whisper.update(self.filename, 1.337, time.time() + 1)

        with AssertRaisesException(whisper.TimestampNotCovered('Timestamp not covered by any archives in this database.')):
            # before the past
            whisper.update(self.filename, 1.337, time.time() - retention_schema[0][1] - 1)

        # When no timestamp is passed in, it should use the current time
        original_lock = whisper.LOCK
        whisper.LOCK = True
        whisper.update(self.filename, 3.7337, None)
        fetched = whisper.fetch(self.filename, 0)[1]
        self.assertEqual(fetched[-1], 3.7337)

        whisper.LOCK = original_lock

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

        with AssertRaisesException(whisper.InvalidAggregationMethod('Unrecognized aggregation method: yummy beer')):
            whisper.setAggregationMethod(self.filename, 'yummy beer')

        #set setting every AggregationMethod available
        for ag in whisper.aggregationMethods:
          for xff in 0.0, 0.2, 0.4, 0.7, 0.75, 1.0:
            # original xFilesFactor
            info0 = whisper.info(self.filename)
            # optional xFilesFactor not passed
            whisper.setAggregationMethod(self.filename, ag)

            # original value should not change
            info1 = whisper.info(self.filename)
            self.assertEqual(info0['xFilesFactor'], info1['xFilesFactor'])

            # the selected aggregation method should have applied
            self.assertEqual(ag, info1['aggregationMethod'])

            # optional xFilesFactor used
            whisper.setAggregationMethod(self.filename, ag, xff)
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
                with AssertRaisesException(whisper.CorruptWhisperFile('Unable to read header', self.filename)):
                    whisper.setAggregationMethod(self.filename, ag)

        whisper.LOCK = original_lock
        whisper.AUTOFLUSH = original_autoflush
        whisper.CACHE_HEADERS = original_caching


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

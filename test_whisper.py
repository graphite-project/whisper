#!/usr/bin/env python

import os
import time
import random
import struct

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import whisper


class TestWhisper(unittest.TestCase):
    """
    Testing functions for whisper.
    """
    db = "db.wsp"

    @classmethod
    def setUpClass(cls):
        cls._removedb()

    @classmethod
    def _removedb(cls):
        """Remove the whisper database file"""
        try:
            if os.path.exists(cls.db):
                os.unlink(cls.db)
        except (IOError, OSError):
            pass

    def test_validate_archive_list(self):
        """blank archive config"""
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([])

    def test_duplicate(self):
        """Checking duplicates"""
        whisper.validateArchiveList([(1, 60), (60, 60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1, 60), (60, 60), (1, 60)])

    def test_even_precision_division(self):
        """even precision division"""
        whisper.validateArchiveList([(60, 60), (6, 60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(60, 60), (7, 60)])

    def test_timespan_coverage(self):
        """timespan coverage"""
        whisper.validateArchiveList([(1, 60), (60, 60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1, 60), (10, 1)])

    def test_number_of_points(self):
        """number of points"""
        whisper.validateArchiveList([(1, 60), (60, 60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1, 30), (60, 60)])

    def test_aggregate(self):
        """aggregate functions"""
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
        # absmax with negative max
        self.assertEqual(whisper.aggregate('absmax', [-3, -2, 1, 2]), -3)
        # absmax with positive max
        self.assertEqual(whisper.aggregate('absmax', [-2, -1, 2, 3]), 3)
        # absmin with positive min
        self.assertEqual(whisper.aggregate('absmin', [-3, -2, 1, 2]), 1)
        # absmin with negative min
        self.assertEqual(whisper.aggregate('absmin', [-2, -1, 2, 3]), -1)
        with self.assertRaises(whisper.InvalidAggregationMethod):
            whisper.aggregate('derp', [12, 2, 3123, 1])

    def test_create(self):
        """Create a db and use info() to validate"""
        retention = [(1, 60), (60, 60)]

        # check if invalid configuration fails successfully
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.create(self.db, [])

        # create a new db with a valid configuration
        whisper.create(self.db, retention)

        # attempt to create another db in the same file, this should fail
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.create(self.db, 0)

        info = whisper.info(self.db)

        # check header information
        self.assertEqual(info['maxRetention'],
                         max([a[0] * a[1] for a in retention]))
        self.assertEqual(info['aggregationMethod'], 'average')
        self.assertEqual(info['xFilesFactor'], 0.5)

        # check archive information
        self.assertEqual(len(info['archives']), len(retention))
        self.assertEqual(info['archives'][0]['points'], retention[0][1])
        self.assertEqual(info['archives'][0]['secondsPerPoint'],
                         retention[0][0])
        self.assertEqual(info['archives'][0]['retention'],
                         retention[0][0] * retention[0][1])
        self.assertEqual(info['archives'][1]['retention'],
                         retention[1][0] * retention[1][1])

        # remove database
        self._removedb()

    def test_merge(self):
        """test merging two databases"""
        testdb = "test-%s" % self.db
        self._removedb()

        try:
          os.unlink(testdb)
        except Exception:
          pass

        # Create 2 whisper databases and merge one into the other
        self._update()
        self._update(testdb)

        whisper.merge(self.db, testdb)

        self._removedb()
        try:
          os.unlink(testdb)
        except Exception:
          pass

    def test_fetch(self):
        """fetch info from database """

        # check a db that doesnt exist
        with self.assertRaises(Exception):
            whisper.fetch("this_db_does_not_exist", 0)

        # SECOND MINUTE HOUR DAY
        retention = [(1, 60), (60, 60), (3600, 24), (86400, 365)]
        whisper.create(self.db, retention)

        # check a db with an invalid time range
        with self.assertRaises(whisper.InvalidTimeInterval):
            whisper.fetch(self.db, time.time(), time.time()-6000)

        fetch = whisper.fetch(self.db, 0)

        # check time range
        self.assertEqual(fetch[0][1] - fetch[0][0],
                         retention[-1][0] * retention[-1][1])

        # check number of points
        self.assertEqual(len(fetch[1]), retention[-1][1])

        # check step size
        self.assertEqual(fetch[0][2], retention[-1][0])

        self._removedb()

    def _update(self, wsp=None, schema=None):
        wsp = wsp or self.db
        schema = schema or [(1, 20)]
        num_data_points = 20

        whisper.create(wsp, schema)

        # create sample data
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
        """Update with a single leveled archive"""
        retention_schema = [(1, 20)]
        data = self._update(schema=retention_schema)
        # fetch the data
        fetch = whisper.fetch(self.db, 0)   # all data
        fetch_data = fetch[1]

        for i, (timestamp, value) in enumerate(data):
            # is value in the fetched data?
            self.assertEqual(value, fetch_data[i])

        # check TimestampNotCovered
        with self.assertRaises(whisper.TimestampNotCovered):
            # in the future
            whisper.update(self.db, 1.337, time.time() + 1)
        with self.assertRaises(whisper.TimestampNotCovered):
            # before the past
            whisper.update(self.db, 1.337,
                           time.time() - retention_schema[0][1] - 1)

        self._removedb()
        
    def test_setAggregation(self):
        """Create a db, change aggregation, xFilesFactor, then use info() to validate"""
        retention = [(1, 60), (60, 60)]

        # create a new db with a valid configuration
        whisper.create(self.db, retention)

        #set setting every AggregationMethod available
        for ag in whisper.aggregationMethods:
          for xff in 0.0,0.2,0.4,0.7,0.75,1.0:
            #original xFilesFactor
            info0 = whisper.info(self.db)
            #optional xFilesFactor not passed
            whisper.setAggregationMethod(self.db, ag)

            #original value should not change
            info1 = whisper.info(self.db)
            self.assertEqual(info0['xFilesFactor'],info1['xFilesFactor'])
            #the selected aggregation method should have applied
            self.assertEqual(ag,info1['aggregationMethod'])

            #optional xFilesFactor used
            whisper.setAggregationMethod(self.db, ag, xff)
            #new info should match what we just set it to
            info2 = whisper.info(self.db)
            #packing and unpacking because
            #AssertionError: 0.20000000298023224 != 0.2
            target_xff = struct.unpack("!f", struct.pack("!f",xff))[0]
            self.assertEqual(info2['xFilesFactor'], target_xff)

            #same aggregationMethod asssertion again, but double-checking since
            #we are playing with packed values and seek()
            self.assertEqual(ag,info2['aggregationMethod'])

        self._removedb()


    @classmethod
    def tearDownClass(cls):
        cls._removedb()

if __name__ == '__main__':
    unittest.main()

import unittest
import whisper
import os
import time
import random

def fileExists(filename):
    try:
        with open(filename) as f:
            return True
    except IOError as e:
        return False

class TestWhisper(unittest.TestCase):
    """
    Testing functions for whisper.
    """

    @classmethod
    def setUpClass(cls):
        # remove old database
        try:
            os.remove("db.wsp")
            print('removed old db.wsp')
        except Exception:
            pass

    def test_validateArchiveList(self):
        """ TESTCASE for blank archive config"""
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([])

        """ TESTCASE for duplicate"""
        whisper.validateArchiveList([(1,60),(60,60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1,60),(60,60),(1,60)])

        """ TESTCASE for even precision division"""
        whisper.validateArchiveList([(60,60),(6,60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(60,60),(7,60)])

        """ TESTCASE for timespan coverage"""
        whisper.validateArchiveList([(1,60),(60,60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1,60),(10,1)])

        """TESTCASE for number of points"""
        whisper.validateArchiveList([(1,60),(60,60)])
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.validateArchiveList([(1,30),(60,60)])

    def test_aggregate(self):
        """TESTCASE for the aggregate functions"""
        self.assertEqual(whisper.aggregate('min',[1,2,3,4]),1)          # minimum of 1,2,3,4
        self.assertEqual(whisper.aggregate('max',[1,2,3,4]),4)          # maximum of 1,2,3,4
        self.assertEqual(whisper.aggregate('last',[3,2,5,4]),4)         # last element in the known values
        self.assertEqual(whisper.aggregate('sum',[10,2,3,4]),19)        # sum all the values
        self.assertEqual(whisper.aggregate('average',[1,2,3,4]),2.5)    # average of the elements
        with self.assertRaises(whisper.InvalidAggregationMethod):
            whisper.aggregate('derp',[12,2,3123,1])


    def test_create(self):
        """TESTCASE for the creating a db, uses info() to validate """
        retention = [(1,60),(60,60)]

        # check if invalid configuration fails successfully
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.create("db.wsp",[])

        # create a new db with a valid configuration
        whisper.create("db.wsp",retention)

        # attempt to create another db in the same file, this should fail
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.create("db.wsp",0)

        info = whisper.info("db.wsp")

        # check header information
        self.assertEqual(info['maxRetention'], max([a[0]*a[1] for a in retention]))
        self.assertEqual(info['aggregationMethod'], 'average')
        self.assertEqual(info['xFilesFactor'], 0.5)

        # check archive information
        self.assertEqual(len(info['archives']),len(retention))
        self.assertEqual(info['archives'][0]['points'],retention[0][1])
        self.assertEqual(info['archives'][0]['secondsPerPoint'],retention[0][0])
        self.assertEqual(info['archives'][0]['retention'],retention[0][0]*retention[0][1])
        self.assertEqual(info['archives'][1]['retention'],retention[1][0]*retention[1][1])

        # remove database

        os.remove("db.wsp")


    def test_fetch(self):
        """ TESTCASE for fetching info from database """

        # check a db that doesnt exist
        with self.assertRaises(Exception):
            whisper.fetch("this_db_does_not_exist",0)

        # SECOND MINUTE HOUR DAY
        retention = [(1,60),(60,60),(3600,24),(86400,365)]
        whisper.create("db.wsp",retention)

        # check a db with an invalid time range
        with self.assertRaises(whisper.InvalidTimeInterval):
            whisper.fetch("db.wsp", time.time(), time.time()-6000)

        fetch = whisper.fetch("db.wsp",0)
        # print(fetch)

        # check time range
        self.assertEqual(fetch[0][1]-fetch[0][0], retention[-1][0]*retention[-1][1])

        # check number of points
        self.assertEqual(len(fetch[1]), retention[-1][1])

        # check step size
        self.assertEqual(fetch[0][2], retention[-1][0])

        os.remove("db.wsp")

    def test_update_single_archive(self):
        """ TEST Update functionality with a single leveled archive"""

        retention_schema = [(1,20)]
        num_data_points = 20

        whisper.create("db.wsp", retention_schema)

        # create sample data
        tn = time.time()-num_data_points
        data = []
        for i in range(num_data_points):
            data.append((tn+1+i, random.random()*10))

        # test single update
        whisper.update("db.wsp", data[0][1], data[0][0])

        # test multi update
        whisper.update_many("db.wsp", data[1:])

        # fetch the data
        fetch = whisper.fetch("db.wsp",0)   # all data
        fetch_data = fetch[1]

        for i, (timestamp, value) in list(enumerate(data)):
            # is value in the fetched data
            self.assertEqual(value, fetch_data[i])

        # check TimestampNotCovered
        with self.assertRaises(whisper.TimestampNotCovered):
            # in the future
            whisper.update("db.wsp", 1.337, time.time()+1)
        with self.assertRaises(whisper.TimestampNotCovered):
            # before the past
            whisper.update("db.wsp", 1.337, time.time()-retention_schema[0][1]-1)

        os.remove("db.wsp")





    @classmethod
    def tearDownClass(cls):
        # make sure file is removed
        try:
            os.remove("db.wsp")
            print('cleaned up db.wsp')
        except Exception:
            pass


if __name__ == '__main__':
    unittest.main()

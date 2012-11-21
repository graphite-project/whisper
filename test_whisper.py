import unittest
import whisper
import os
import time
import random

class TestWhisper(unittest.TestCase):
    """
    Testing functions for whisper. Testing is mostly done on
    random methods, for now
    """
    
    def setUp(self):
        """ SETUP method
        removes any old databases left over from tests 
        """
        # remove old database
        try:
            os.remove("db")
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
            whisper.create("db",[])
                   
        # create a new db with a valid configuration
        whisper.create("db",retention)         
        
        # attempt to create another db in the same file, this should fail
        with self.assertRaises(whisper.InvalidConfiguration):
            whisper.create("db",0)
            
        info = whisper.info("db")
        
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
        os.remove("db")
        
        
    def test_fetch(self):    
        """ TESTCASE for fetching info from database """
        
        # check a db that doesnt exist
        with self.assertRaises(Exception):
            whisper.fetch("this_db_does_not_exist",0)
            
        # SECOND MINUTE HOUR DAY
        retention = [(1,60),(60,60),(3600,24),(86400,365)]
        whisper.create("db",retention)  
            
        # check a db with an invalid time range
        with self.assertRaises(whisper.InvalidTimeInterval):
            whisper.fetch("db", time.time(), time.time()-6000)
    
        fetch = whisper.fetch("db",0)
        # print(fetch)
        
        # check time range 
        self.assertEqual(fetch[0][1]-fetch[0][0], retention[-1][0]*retention[-1][1])
    
        # check number of points
        self.assertEqual(len(fetch[1]), retention[-1][1])
        
        # check step size
        self.assertEqual(fetch[0][2], retention[-1][0])
        
        os.remove("db")
        
    def test_update(self):
        
        whisper.create("db", [(1, 5)])

        tn = time.time()-4
        data = []
        
        for i in range(5):
            data.append((tn, random.random()*10))
            tn = tn+1
        
        whisper.update("db", data[0][1], data[0][0])
        whisper.update_many("db", data[1:])
        
        fetch = whisper.fetch("db",0)
        tstart = fetch[0][0]
        tend = fetch[0][1]
        
        i=0
        for timestamp, value in data:
            
            # is timestamp inside the time range
            
            self.assertGreaterEqual(timestamp, tstart)
            self.assertLessEqual(timestamp, tend)
        
            # is value in the data set
            
            self.assertEqual(value, data[i][1])
            i+=1
        
        # check TimestampNotCovered
        with self.assertRaises(whisper.TimestampNotCovered):
            whisper.update("db", 1.337, time.time()+1)
        with self.assertRaises(whisper.TimestampNotCovered):
            whisper.update("db", 1.337, time.time()-10)
        
        os.remove("db")
        
    
        
    
    def tearDown(self):
        # make sure file is removed
        try:
            os.remove("db")
        except Exception:
            pass
        
        
        
    

if __name__ == '__main__':
    unittest.main()

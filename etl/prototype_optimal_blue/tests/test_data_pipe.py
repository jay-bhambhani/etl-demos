import os
import web
import sys
import unittest
from logging import getLogger, basicConfig, INFO
from scripts.raw_data_pipe import *




basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)



TEST_PART = 

s3_factory = S3Factory()
sftp = SFTPHarvester(OPTIMAL_BLUE_SOURCE)
s3_factory.s3_connect()
sftp.connect()
ftp_connection = sftp.connection
s3_connection = s3_factory.connection



class PipeTestCase(unittest.TestCase):

     def setUp(self):
        """ Setting up for the test """
        logger.info("Pipe Test Setup")
         

    def check_file_concat(self):
        grouped_files = group_files(ftp_connection, sftp)[0]
        self.assertTrue(isinstance(grouped_files, dict))
        self.assertGreaterEqual(len(grouped_files['weekly']), len(grouped_files['daily']), "seems we grouped correctly")

    def check_header_handler(self):
        pass



        # ending the test
    def tearDown(self):
        """Cleaning up after the test"""
        logger.info("Pipe test tear down")



# coding: utf-8

# In[8]:

# lets import some important packages
import os
import sys
import web
import re
import logging
# this does fun things with csv-style data
import csv
from argparse import ArgumentParser
from contextlib import contextmanager
# paramiko is Python's built in OpenSSH package
import pysftp
# this is a credentials thing I created - temporary, 
# but okay for the short term
from ob_sftp_config import logins


# In[9]:

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
SOURCE = 'optimal_blue'
TEST_FILE = 'ATL_fedfile_20151005.txt'
TEST_CLEAN_FILE = 'ATL_fedfile_20151005.csv'
FIELD_NAMES = ['IndexValue', 'Updated', 'Correspondent', 'IndexName', 'CompanyID', 'Points', 'Scenario Name', 'LenderID', 'Rate', 'RateSheetID', 'LockPeriod', 'Margin', None]
POINTS_MAX = 110
POINTS_MIN = 90
CORRESPONDENT = 'True'
IDENTIFIER = 'ATL|PR01P1LA300000LT080F640LD90IO0T01'


# In[23]:

class SFTPHarvester(object):
    """
    This is a basic harvester that will open an SFTP connection to a server and files that you want automatically
    """
    def __init__(self, source):
        """
        Constructor
        """
        self.source = source
        self.connection = None
        self.connected = False

    def create_connection(self):
        """
        Creates connection based on login credentials
        """
        ob_login = logins()
        ob_login.get_credentials([self.source])
        logger.info('obtained credentials %s for source %s' % (ob_login.credentials, self.source))
        self.connection = pysftp.Connection(ob_login.credentials[self.source]['host'], 
                                        username=ob_login.credentials[self.source]['user'], 
                                        password=ob_login.credentials[self.source]['password'])
        self.connected = True
        logger.info('connected to source %s' % self.source)
    
    def collect_files(self, remote_path='.', local_path=None):
        """
        Collects all files in remote path specified, and copies to local folder
        """
        local_path = self.check_local_path(local_path)
        
        target_files = self.connection.listdir(remote_path)
        
        for target_file in target_files:
            if target_file not in os.listdir(local_path):
                logger.info('file %s not in local directory. adding...' % target_file)
                self.get_file(target_file, local_path)
            else:
                logger.info('file %s already in local directory. skipping...' % target_file)
    
    def get_file(self, file_name, local_path=None):
        """
        Copies file to local folder
        """
        if self.connected == True:
            logger.info('getting file %s' % file_name)
            if local_path == '.':
                self.connection.get(file_name)
            else:
                self.connection.get(file_name, localpath=local_path)
        else:
            self.create_connection()
    

    def check_local_path(self, local_path):
        """
        Quick check to see if local path exists and creates if it doesn't
        """
        if not local_path:
            current = self.current_path()
            return current
            
        else:
            if os.getcwd() != local_path:
                os.mkdir(local_path)
                os.chdir(local_path)
            return
    
    @staticmethod
    def current_path():
        local_path = '.'
        return local_path
        

    


# In[50]:

def clean_data(read_file_name, write_file_name):
    """
    Filters data by what you are looking for
    """
    csv_file = open(write_file_name, 'wb') 
    writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES)
    
    with open(read_file_name, 'rU') as data_file:
        reader = csv.DictReader(data_file, delimiter='\t')
        for row in reader:
            filtered_row = filter_row(row, IDENTIFIER, POINTS_MIN, POINTS_MAX, CORRESPONDENT)
            if filtered_row:
                writer.writerow(filtered_row)
            else:
                continue
    
    csv_file.close()


# In[51]:

def filter_row(row, identifier, points_min, points_max, correspondent):
    """
    This is where the filtering logic is
    """
    if (row['Scenario Name'] == identifier) and         (float(row['Points']) >= points_min) and         (float(row['Points']) <= points_max) and         (row['Correspondent'] == correspondent):
            return row
    


# In[53]:

def etl_mortgage_data(read_file_name, write_file_name):
    """
    The function for the above
    """
    optimal = SFTPHarvester(SOURCE)
    optimal.create_connection()
    with optimal.connection:
        optimal.get_file(read_file_name)
        clean_data(read_file_name, write_file_name)
        #optimal.collect_files()
    logger.info('done!')


# In[54]:

etl_mortgage_data(TEST_FILE, TEST_CLEAN_FILE)


# In[ ]:




# In[ ]:




# In[ ]:




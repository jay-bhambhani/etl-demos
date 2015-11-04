
# coding: utf-8

# In[8]:
###########################################################
##### This class borrows from the connection object #######
##### and allows you to harvest your data via sftp  #######
###########################################################

# lets import some important packages
import os
import sys
import web
import re
from logging import getLogger, basicConfig, INFO
# this does fun things with csv-style data
import csv
from argparse import ArgumentParser
from contextlib import contextmanager
# paramiko is Python's built in OpenSSH package
import pysftp
# a way to write to an open file object
from StringIO import StringIO
# this is a credentials thing I created - temporary, 
# but okay for the short term
from credential_manager import CredentialManager
# Our connector object!
from connectors.sftp_connector import SFTPConnector


# In[9]:

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)


# In[23]:

class SFTPHarvester(SFTPConnector):
    """
    This is a basic harvester that will open an SFTP connection to a server and files that you want automatically
    """
    def __init__(self, source):
        """
        Constructor
        """
        super(SFTPHarvester, self).__init__(source)
        self.connected = False

    
    
    def list_files(self, remote_path='.', local_path=None):
        """
        Collects all files in remote path specified, and copies to local folder
        """
        #local_path = self.check_local_path(local_path)
        
        target_files = self.connection.listdir(remote_path)
        
        return target_files
        """
        for target_file in target_files:
            if target_file not in os.listdir(local_path):
                logger.info('file %s not in local directory. adding...' % target_file)
                self.get_file(target_file, local_path)
            else:
                logger.info('file %s already in local directory. skipping...' % target_file)
        """
    
    def get_file(self, file_name, local_path=None):
        """
        Copies file to local folder
        """
        if self.connected == True:
            logger.info('getting file %s' % file_name)
            self.get(file_name, local_path)
            
        else:
            logger.error('not connected')
    

    def get(self, file_name, local_path):
        """
        actually gets the file
        """
        if local_path == '.':
            self.connection.get(file_name)
        else:
            self.connection.get(file_name, localpath=local_path)
    
    
    def open(self, file_name):
        """
        opens a buffer file on sftp server
        """
        
        return self.connection.open(file_name)


    def read(self, file_name):
        """
        reads sftp content into file buffer
        """
        temp_file_object = StringIO()

        self.connection.getfo(file_name, temp_file_object, cb)

        return temp_file_object


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
        
    @staticmethod
    def cb(bytes_transferred, total_bytes):
        f_loaded = bytes_transferred / float(total_bytes)
        p_loaded = f_loaded * float(100)
        logger.info('%s %% uploaded' % int(p_loaded))



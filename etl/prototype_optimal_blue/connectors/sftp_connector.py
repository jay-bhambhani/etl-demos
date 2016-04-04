
# coding: utf-8

# In[8]:
###########################################################
###### The connection object for SFTP with which we #######
######                    harvest                   #######
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
# this is a credentials thing I created - temporary, 
# but okay for the short term
from credential_manager import CredentialManager


# In[9]:

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)


class SFTPConnector(object):

    def __init__(self, source):
        """
        Constructor
        """
        self.source = source
        self.connection = None
        self.connected = False

    def connect(self):
        """
        Creates connection based on login credentials
        """
        login = CredentialManager()
        login.get_credentials(self.source)
        logger.info('obtained credentials %s for source %s' % (login.credentials, self.source))
        self.connection = pysftp.Connection(login.credentials['host'], 
                                        username=login.credentials['user'], 
                                        password=login.credentials['password'])
        self.connected = True
        logger.info('connected to source %s' % self.source)

    def remove_connection(self):
        """
        deletes connection thread
        """
        self.connection = None
        self.connected = False
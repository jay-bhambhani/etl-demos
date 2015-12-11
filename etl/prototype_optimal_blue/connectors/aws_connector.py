# coding: utf-8

# In[8]:
###########################################################
#####        This is how we connect to AWS S3       #######
###########################################################

# lets import some important packages
import os
import sys
import web
import re
from logging import getLogger, basicConfig, INFO
# Here are the main connection package sets
from boto.s3.connection import S3Connection
from boto.emr.connection import EmrConnection
# Manage our credentials
from credential_manager import CredentialManager

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)

# Let's make this puppy!
SOURCE = 'aws'

class AWSConnector(object):

    def __init__(self):
        self.connection = None
        self.connected = False

    def get_credentials(self):
        aws = CredentialManager()
        aws.get_credentials(SOURCE)
        return aws

    def s3_connect(self):
        """
        logs you in to S3!
        """
        aws = self.get_credentials()
        self.connection = S3Connection(aws.credentials['access_key'], aws.credentials['secret_key'])
        logger.info('connected to s3')
        self.connected = True

    def emr_connect(self):
        """
        logs you in to EMR!
        """
        aws = self.get_credentials()
        self.connection = EmrConnection(aws.credentials['access_key'], aws.credentials['secret_key'])
        logger.info('connected to emr')
        self.connected = True
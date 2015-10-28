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
# Here is the main S3 package set
from boto.s3.connection import S3Connection
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

    def s3_connect(self):
        """
        logs you in to S3!
        """
        aws = CredentialManager()
        aws.get_credentials(SOURCE)
        self.connection = S3Connection(aws.credentials['access_key'], aws.credentials['secret_key'])
        logger.info('connected to s3')
        self.connected = True
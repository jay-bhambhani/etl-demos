# coding: utf-8

# In[8]:
###########################################################
#####       This is how we do stuff on AWS S3       #######
###########################################################

# lets import some important packages
import os
import sys
import web
import re
from logging import getLogger, basicConfig, INFO
# Here is the main S3 package set
from boto.s3.connection import S3Connection
from boto.s3.key import Key
# Manage our credentials
from credential_manager import CredentialManager
# Creating file-like objects (have a look at the docs)
from StringIO import StringIO
#Connect to AWS
from connectors.aws_connector import AWSConnector 

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)

# Let's make this puppy!
class S3Factory(AWSConnector):
    """
    This class inherits the connection to aws
    and allows us to do a few fun things
    """

    def __init__(self):
        """
        Connection and inheritance
        """
        self.connected = False
        self.connected = None
        super(S3Factory, self).__init__()


    def get_bucket(self, bucket_name):
        """
        Get s3 bucket by name
        """
        try:
            bucket = self.connection.get_bucket(bucket_name)
        
        except:
            raise KeyError('bucket %s not found' % bucket)
        
        else:
            logger.info('found bucket %s' % bucket)
            return bucket

    def get_key(self, bucket_name, key_name):
        """
        Get s3 key by name
        """
        bucket = self.get_bucket(bucket_name)
        try:
            key = Key(bucket)
            key.key = key_name
        except:
            raise KeyError('key %s not found' % key.key)
        else:
            logger.info('found key %s ' % key.key)
            return key

    def set_bucket(self, bucket_name):
        """
        Create an s3 bucket
        """
        logger.info('setting bucket %s' % bucket_name)
        bucket = self.connection.create_bucket(bucket_name)
        return bucket

    def set_key(self, bucket_name, key_name, contents):
        """
        Create an s3 key, accompanied by contents
        """
        bucket = self.get_bucket(bucket_name)
        key = Key(bucket)
        key.key = key_name
        logger.info('setting key %s' % key.key)
        key.set_contents_from_filename(key_name)
        return key

    def upload_partial_files(self, bucket_name, key_name, parts, clean_headers=False, connection=None):
        """
        Upload pieces of a large file to a key
        """
        bucket = self.get_bucket(bucket_name)
        logger.info('initiating multi-part upload')
        multi_part = bucket.initiate_multipart_upload(key_name)

        for index, file_name in enumerate(parts):
            logger.info('uploading part %s - %s' % (index+1, file_name))
            if connection:
                with connection.open(file_name, mode='r+') as part:
                    self._upload_part(multi_part, part, index + 1, clean_headers)
            else:
                with open(file_name, 'rU') as part:
                    self._upload_part(multi_part, part, index + 1, clean_headers) 

        multi_part.complete_upload()

        return multi_part

    def _upload_part(self, multi_part, part, index, clean_headers):
        """
        Upload individual part
        """
        if clean_headers:
            logger.info('cleaning headers')
            part = self._part_handler(part, index)
        try:
            logger.info('uploading part %s, stats %s' % (index, part.read()))
            multi_part.upload_part_from_file(part, index, cb=self._display_multipart_progress, num_cb=1000)
        except:
            raise TypeError('wrong file format')
        else:
            logger.info('uploaded part %s' % index)


    def delete_multipart_uploads(self, bucket_name, mpu_id=None):
        bucket = self.get_bucket(bucket_name)
        if mpu_id:
            mpus = bucket.list_multipart_uploads()
            mpu = mpus.find(mpu_id)
            bucket.cancel_multipart_upload(mpu.key_name, mpu.id)
        else:
            logger.info('uploads %s' % bucket.list_multipart_uploads())
            for mpu in bucket.list_multipart_uploads():
                logger.info('deleting mpu %s, parts %s' % (mpu.key_name, mpu))
                bucket.cancel_multipart_upload(mpu.key_name, mpu.id)
        
    @staticmethod
    def _display_multipart_progress(bytes_so_far, total_bytes):
        logger.info('%d bytes transferred / %d bytes total' % (bytes_so_far, total_bytes))

    @staticmethod
    def _part_handler(part, index):
        """
        handles things from parts, just headers right now.
        """
        if index == 1:
            prepped_file = part
        else:
            logger.info('skipping redundant header')
            prepped_file = part.next()
    
        return prepped_file
        

   

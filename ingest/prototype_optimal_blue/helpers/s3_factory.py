# coding: utf-8

# In[8]:
###########################################################
#####       This is how we do stuff on AWS S3       #######
###########################################################

# lets import some important packages
import os
import sys
print sys.path
import web
import re
from logging import getLogger, basicConfig, INFO
# Here is the main S3 package set
from boto.s3.connection import S3Connection
from boto.s3.key import Key
# Concurrency!
from multiprocessing import Pool, cpu_count, Queue, Process
# Some funky things for applying pickling for methods
import cPickle as pickle
import dill
# To split methods into bits
from functools import partial
# Manage our credentials
from credential_manager import CredentialManager
# Creating file-like objects (have a look at the docs)
from StringIO import StringIO
#fun things with managing contexts!
from contextlib import contextmanager, closing
#Connect to AWS
from connectors.aws_connector import AWSConnector
from inspect import getmro

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)

CORE_MIN = 1
FILE_MIN = 1


def upload_wrapper(factory, upload_args):
    """
    wrapper function to avoid pickling of instance method
    factory: s3_factory object
    upload_args: tuple of arguments that 
                 upload_part function takes
    """
    if len(upload_args) == 2:
        logger.info('uploading keys')
        key, file_name = upload_args
        factory.set_key(key, file_name)
    else:
        logger.info('uploading parts')
        multi_part, file_name, index, clean_headers = upload_args
        factory.upload_part(multi_part, file_name, index, clean_headers)
    return

# Let's make this puppy!
class S3Factory(AWSConnector):
    """
    This class inherits the connection to aws
    and allows us to do a few fun things
    """

    def __init__(self):
        """
        Connection and inheritance from AWSConnector
        """
        self.connected = False
        self.source = None
        self.cores = max(cpu_count() - 1, 1)
        self.pool = None
        super(S3Factory, self).__init__()

    def add_data_source(self, source):
        """
        connects to data source you may need
        source_object: object of source
        """
        self.source = source

    def connect_to_source(self):
        """
        connects to data source
        """
        self.source.connect()
    

    def get_bucket(self, bucket_name):
        """
        Get s3 bucket
        bucket_name: str
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
        Get s3 key
        bucket_name: str
        key_name: str
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
        bucket_name: str
        """
        logger.info('setting bucket %s' % bucket_name)
        bucket = self.connection.create_bucket(bucket_name)
        return bucket

    def set_keys(self, bucket_name, key_base, files):
        """
        sets multiple s3 keys from files
        bucket_name: str
        key_base: str folder name of key you'd like
        files: list of file name strs
        """
        bucket = self.get_bucket(bucket_name)

        if self.cores > CORE_MIN and len(files) > FILE_MIN:
            logger.info('going the pooled approach')
            self.pooled_key_upload(bucket, key_base, files)

        else:
            for file_name  in files:
                logger.info('sequential it is')
                key_name = '{folder}/{f_name}'.format(folder=key_base, f_name=file_name)
                key = self._create_key(key_name, bucket)
                self.set_key(key, file_name)


    def pooled_key_upload(self, bucket, key_base, files):
        """
        creates parallel job set to upload full files
        bucket: s3 bucket object
        key_base: str folder name of key you'd like
        files: list of file name strs
        """
        upload_jobs = list()
        
        for file_name in files:
            key_name = '{folder}/{f_name}'.format(folder=key_base, f_name=file_name)
            key = self._create_key(key_name, bucket)
            upload_jobs.append((key, file_name))
            logger.info('loaded file %s to queue, size %s' % (file_name, len(upload_jobs)))
        
        self.concurrent_upload(upload_jobs)

    def set_key(self, key, file_name):
        """
        Create an s3 key
        key: object of s3 key
        file_object: file object being uploaded to key
        """
        file_object = self._stream_helper(file_name)

        logger.info('setting key %s' % key)
        try:
            key.set_contents_from_file(file_object, cb=self._display_progress, num_cb=11)
            return key

        finally:
            file_object.close()




    def initiate_multipart_upload(self, bucket_name, key_name):
        """
        Initiate multipart upload on s3
        bucket_name - str
        key_name - str 
        """
        bucket = self.get_bucket(bucket_name)
        logger.info('initiating multi-part upload')
        multi_part = bucket.initiate_multipart_upload(key_name)

        return multi_part

    def upload_partial_files(self, multi_part, parts, clean_headers=False):
        """
        decides how handle partial file uploads
        multi_part - the multi part download object from boto
        parts - list of parts
        clean_headers - bool (removed file headers)
        """
        if self.cores > CORE_MIN and len(parts) > FILE_MIN:
            logger.info('turning to threaded approach')
            self.pooled_partial_files(multi_part, parts, clean_headers)

        else:
            logger.info('turning to sequential approach')
            self.sequential_partial_files(multi_part, parts, clean_headers)

        logger.info('completing upload')
        multi_part.complete_upload()

        

    def pooled_partial_files(self, multi_part, parts, clean_headers):
        """
        Use computer cores to upload multiple parts to key
        multi_part - the multi part download object from boto
        parts - list of parts
        clean_headers - bool (removed file headers)
        """
        upload_jobs = list()
        
        for index, file_name in enumerate(parts):
            upload_jobs.append((multi_part, file_name, index + 1, clean_headers))
            logger.info('loaded file %s to queue, size %s' % (file_name, index + 1))
        
        self.concurrent_upload(upload_jobs)

    def concurrent_upload(self, upload_jobs):
        """
        This pools the upload jobs
        upload_jobs: list of tuple arguments for upload function
        """
        logger.info('creating pool')
        pool = self._initiate_pool()
        logger.info('running map task')
        _bound_upload_wrapper = partial(upload_wrapper, self)
        pool.map(_bound_upload_wrapper, upload_jobs)
        logger.info('tear down')
        pool.close()
        pool.join
                

    def sequential_partial_files(self, multi_part, parts, clean_headers):
        """
        Upload pieces of a large file to a key
        multi_part - the multi part download object from boto
        parts - list of parts
        clean_headers - bool (removed file headers)
        """
        for index, file_name in enumerate(parts):
            logger.info('uploading part %s - %s' % (index+1, file_name))
            self.upload_part(multi_part, part, index + 1, clean_headers)
            part.close()

        return multi_part


    def upload_part(self, multi_part, file_name, index, clean_headers):
        """
        Upload individual part
        multipart - object
        part to upload - object
        index of part - int
        remove headers - bool
        """
        part = self._stream_helper(file_name)

        if clean_headers:
            logger.info('cleaning headers')
            part = self._header_handler(part, index)
        try:
            logger.info('uploading part %s, stats %s' % (index, part))
            multi_part.upload_part_from_file(part, index, cb=self._display_progress, num_cb=11)
        except:
            raise TypeError('wrong file format')
        else:
            logger.info('uploaded part %s' % index)
        finally:
            part.close()


    def delete_multipart_uploads(self, bucket_name, mpu_id=None):
        """
        deletes multi upload task
        bucket_name: str
        mpu_id: str (optional, if added will only delete specific id
                     otherwise all of them)
        """
        bucket = self.get_bucket(bucket_name)
        if mpu_id:
            mpus = bucket.list_multipart_uploads()
            mpu = mpus.find(mpu_id)
            bucket.cancel_multipart_upload(mpu.key_name, mpu.id)
        else:
            logger.info('uploads %s' % bucket.list_multipart_uploads())
            for mpu in bucket.list_multipart_uploads():
                logger.info('deleting mpu %s, parts %s' % (mpu.key_name, mpu.get_all_parts()))
                bucket.cancel_multipart_upload(mpu.key_name, mpu.id)

    
    def _initiate_pool(self):
        "helper method to create a pool of processes"
        pool = Pool(self.cores)
        return pool

        
    def _stream_helper(self, file_name):
        """
        opens file based on connection
        file_name: str
        """
        if self.source:
            self.connect_to_source()
            file_object = StringIO()
            with self.source.connection as source_connection:
                self.source.connection.getfo(file_name, file_object)
            file_object.seek(0)
        else:
            file_object = open(file_name, 'rU')

        return file_object
    

    @staticmethod
    def _display_progress(bytes_so_far, total_bytes):
        """
        Progress calculator
        handled by aws
        """
        f_loaded = bytes_so_far / float(total_bytes)
        p_loaded = f_loaded * float(100)
        logger.info('%s %% uploaded' % int(p_loaded))

    
    @staticmethod
    def _header_handler(part, index):
        """
        handles things from parts, just headers right now.
        part: object part of file
        index: int index of object
        """
        if index == 1:
            prepped_file = part
        else:
            logger.info('skipping redundant header')
            part.next()
            prepped_file = part
    
        return prepped_file


 

    @staticmethod
    def _create_key(key_name, bucket):
        """
        helper method to create a key
        key_name: str of key
        bucket: bucket object
        """
        key = Key(bucket)
        key.key = key_name
        logger.info('creating key %s' % key.key)

        return key

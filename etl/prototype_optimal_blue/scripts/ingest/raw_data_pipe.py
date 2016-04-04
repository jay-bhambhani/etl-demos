# DEN, LOS havested daily
# coding: utf-8

# In[8]:
###########################################################
####              ETL on Optimal Blue Data            #####
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
from contextlib import contextmanager, closing
from datetime import date
# our new sftp and s3 connectors
from helpers.sftp_harvester import SFTPHarvester
from helpers.s3_factory import S3Factory

OPTIMAL_BLUE_SOURCE = 'ob-sftp'
OPTIMAL_BLUE_BUCKET = 'cinchfinancial-mortgage-optimalblue-proto'
DAILY_FOLDER = 'daily'
WEEKLY_FOLDER = 'weekly'
WEEKLY_FILE_NAME = 'weekly_aggregated_%s.txt'

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)

ArgumentParser(description='ETL and storage of optimal blue data')

def pipe():
    """
    Overall ETL structure
    """
    s3_factory = S3Factory()
    sftp = SFTPHarvester(OPTIMAL_BLUE_SOURCE)
    s3_factory.s3_connect()
    sftp.connect()
    

    with sftp.connection:
        grouped_files, max_date_string = group_files(sftp.connection)
    
    sftp.remove_connection()
    s3_factory.add_data_source(sftp)
    logger.info('sftp connection %s, s3 connection %s' % (sftp.connection, s3_factory.source.connection))
    upload_to_s3(s3_factory, grouped_files, max_date_string)

    logger.info('complete!')



def upload_to_s3(s3_factory, grouped_files, max_date_string):
    """
    uploads file to s3
    s3_factory: s3_factory object
    grouped_files: dict of grouped file names
    max_date_string: str of weekly date (max count)
    """
    with closing(s3_factory.connection) as connection:
        upload_weekly_files(s3_factory, grouped_files['weekly'], max_date_string)
        upload_daily_files(s3_factory, grouped_files['daily'])

def upload_weekly_files(s3_factory, files, max_date_string):
    """
    uploads weekly files post aggregation
    s3_factory: s3_factory object
    ftp_connection: ftp connection object
    files: list of file names
    max_date string: str of weekly date (max count)
    """
    logger.info('uploading weekly files %s' % files)
    weekly_aggregated_file = WEEKLY_FILE_NAME % max_date_string
    weekly_key_name = '{folder}/{f_name}'.format(folder=WEEKLY_FOLDER, f_name=weekly_aggregated_file)
    multi_part = s3_factory.initiate_multipart_upload(OPTIMAL_BLUE_BUCKET, weekly_key_name)
    s3_factory.upload_partial_files(multi_part, files, clean_headers=True)
    logger.info('weekly file upload complete')


def upload_daily_files(s3_factory, files):
    """
    uploads daily files one by one to s3
    s3_factory: s3_factory object
    ftp_connection: ftp connection object
    files: list of file names
    """
    logger.info('uploading daily files %s' % files)
    s3_factory.set_keys(OPTIMAL_BLUE_BUCKET, DAILY_FOLDER, files)


def group_files(sftp):
    """
    groups files into daily and weekly
    sftp: sftp_harvester object
    """
    document_groups = dict()
    document_groups['weekly'] = list()
    document_groups['daily'] = list()
    date_total = dict()
    for read_file_name in sftp.listdir():
        current_city, current_file_type, current_date_string = split_file_name(read_file_name)
        max_date_string = find_max_date_count(read_file_name, date_total)
        if current_date_string == max_date_string:
            logger.info('adding %s to weekly files' % read_file_name)
            document_groups['weekly'].append(read_file_name)
        else:
            logger.info('adding %s to daily files' % read_file_name)
            document_groups['daily'].append(read_file_name)

    return document_groups, max_date_string


def find_max_date_count(read_file_name, date_total):
    """
    helper function to find max counts of a date
    in file
    read_file_name: str file name of file to be read 
    date_total: dict of date counts
    """
    city, file_type, date_string = split_file_name(read_file_name)
    add_to_totals(date_string, read_file_name, date_total)

    max_date_string = find_max_count(date_total)

    logger.info('current max date %s' % max_date_string)
    return max_date_string


def find_max_count(dictionary):
    """
    helper function to find max count
    dictionary: dict
    """
    max_count = max(dictionary, key=lambda date_string: dictionary[date_string])

    return max_count


def add_to_totals(date_string, read_file_name, date_total):
    """
    helper function with counts
    date_string: str date 
    read_file_name: str file name of file to be read 
    date_total: dict of date counts
    """
    if date_string in date_total:
        date_total[date_string] += 1
    else:
        date_total[date_string] = 1
    logger.info('current totals %s' % date_total)



def split_file_name(file_name):
    """
    splits file text into important pieces
    file_name: str file name 
    """
    file_pre, file_extension = os.path.splitext(file_name)
    city, file_type, date_string = file_pre.split('_')

    logger.info('city %s file type %s date %s' % (city, file_type, date_string))
    return city, file_type, date_string



if __name__ == '__main__':
    pipe()

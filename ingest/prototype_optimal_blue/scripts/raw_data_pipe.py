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
from contextlib import contextmanager
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
    ftp_connection = sftp.connection
    s3_connection = s3_factory.connection

    with ftp_connection:
        grouped_files, max_date_string = concat_files(ftp_connection, sftp)
        upload_to_s3(s3_connection, ftp_connection, s3_factory, grouped_files, max_date_string)


def concat_files(ftp_connection, sftp):
    logger.info('grouping files')
    grouped_files, max_date_string = group_files(sftp)

    return grouped_files, max_date_string

def upload_to_s3(s3_connection, ftp_connection, s3_factory, grouped_files, max_date_string):
    with closing(s3_connection) as connection:
        upload_weekly_files(s3_factory, ftp_connection, grouped_files['weekly'], max_date_string)
        upload_daily_files(s3_factory, grouped_files['daily'])

def upload_weekly_files(s3_factory, ftp_connection, files, max_date_string):
    logger.info('uploading weekly files %s' % files)
    weekly_aggregated_file = WEEKLY_FILE_NAME % max_date_string
    weekly_key_name = '{folder}/{f_name}'.format(folder=WEEKLY_FOLDER, f_name=weekly_aggregated_file)
    s3_factory.upload_partial_files(OPTIMAL_BLUE_BUCKET, weekly_key_name, files,
                             clean_headers=True, connection=ftp_connection)


def upload_daily_files(s3_factory, files):
    logger.info('uploading daily files %s' % files)
    for file_name in files:
        daily_key_name = '{folder}/{f_name}'.format(folder=DAILY_FOLDER, f_name=file_name)
        with open(file_name) as contents:
            s3_factory.set_key(OPTIMAL_BLUE_BUCKET, daily_key_name, contents)


def group_files(ftp_connection):
    """
    groups files into daily and weekly
    """
    document_groups = dict()
    document_groups['weekly'] = list()
    document_groups['daily'] = list()
    date_total = dict()
    for read_file_name in ftp_connection.list_files():
        current_city, current_file_type, current_date_string = split_file_name(read_file_name)
        max_date_string = find_max_date_count(read_file_name, date_total, ftp_connection)
        if current_date_string == max_date_string:
            logger.info('adding %s to weekly files' % read_file_name)
            document_groups['weekly'].append(read_file_name)
        else:
            logger.info('adding %s to daily files' % read_file_name)
            document_groups['daily'].append(read_file_name)

    return document_groups, max_date_string


def find_max_date_count(read_file_name, date_total, ftp_connection):
    """
    helper function to find max counts of a date
    in file
    """
    city, file_type, date_string = split_file_name(read_file_name)
    add_to_totals(date_string, read_file_name, date_total)

    max_date_string = find_max_count(date_total)

    logger.info('current max date %s' % max_date_string)
    return max_date_string


def find_max_count(dictionary):
    """
    helper function to find max count
    """
    max_count = max(dictionary, key=lambda date_string: dictionary[date_string])

    return max_count


def add_to_totals(date_string, read_file_name, date_total):
    """
    helper function with counts
    """
    if date_string in date_total:
        date_total[date_string] += 1
    else:
        date_total[date_string] = 0
    logger.info('current totals %s' % date_total)



def split_file_name(file_name):
    """
    splits file text into important pieces
    """
    file_pre, file_extension = os.path.splitext(file_name)
    city, file_type, date_string = file_pre.split('_')

    logger.info('city %s file type %s date %s' % (city, file_type, date_string))
    return city, file_type, date_string

@contextmanager
def closing(connection):
    """
    closing connections
    """
    try:
        yield connection
    
    finally:
        connection.close()

if __name__ == '__main__':
    pipe()

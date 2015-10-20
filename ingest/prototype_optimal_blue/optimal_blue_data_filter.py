
# coding: utf-8

# In[21]:

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
# Our harvester!
from sftp_harvester import SFTPHarvester


# In[22]:

# setting up our logger
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


# In[23]:

# Our test variables - these are preset for now

SOURCE = 'optimal_blue'
WRITE_FILE_BASE = 'filtered_%s.csv'
FIELD_NAMES = ['IndexValue', 'Updated', 'Correspondent', 'IndexName', 'CompanyID', 'Points', 'Scenario Name', 'LenderID', 'Rate', 'RateSheetID', 'LockPeriod', 'Margin', None]


# In[24]:

# We are going to give you some arguments so you can filter as needed:

parser = ArgumentParser(description='filters optimal blue csv by criteria')
parser.add_argument('--points_max', help="filter rows by max points", type=float)
parser.add_argument('--points_min', help="filter rows by min points", type=float)
parser.add_argument('--identifier', help="identifier string to filter rows by")
parser.add_argument('--correspondent', help='correspondent value - True or False')


# In[25]:

def etl_mortgage_data(filters):
    """
    The function for getting what you need from the data
    """
    optimal = SFTPHarvester(SOURCE)
    optimal.create_connection()
    with optimal.connection:
        #optimal.get_file(read_file_name)
        optimal.collect_files()
        logger.info('all files collected')
        for read_file_name in os.listdir(os.getcwd()):
            if read_file_name.find('.txt') > -1:
                logger.info('filtering file %s' % read_file_name)
                clean_data(read_file_name, filters)
    logger.info('done!')


# In[26]:

def filter_row(row, filters):
    """
    This is where the filtering logic is: essentially we just check that you've met
    all necessary conditions
    """
    logger.info('filtering row {row} with params: {ide}, {pmin}, {pmax}, {corr}'.format(row=row,
                                                                                       ide=filters['identifier'],
                                                                                       pmin=filters['points_min'],
                                                                                       pmax=filters['points_max'],
                                                                                       corr=filters['correspondent']))

    
    logger.info('row identifier - %s, filter identifier - %s' % (parse_city_name(row['Scenario Name'], '|'), 
                                                                  str(filters['identifier'] or '')))
    logger.info('row points - %s, filter points max - %s' % (float(row['Points']), 
                                                              str(filters['points_max'] or '')))
    logger.info('row points - %s, filter points min - %s' % (float(row['Points']), 
                                                              str(filters['points_min'] or '')))
    logger.info('row correspondent - %s, filter correspondent - %s' % (row['Correspondent'], 
                                                                        str(filters['correspondent'] or '')))
    
    filter_results = list()
    filter_criteria = filter_logic()
    for filter_criterion in filter_criteria:
        if filters[filter_criterion]:
            filter_results.append(filter_criteria[filter_criterion](row, filters))
    
    print filter_results
    if len(set(filter_results)) == 1 and filter_results[0] == True:
                
        logger.info('got row %s' % row)
        return row


# In[27]:

def filter_logic():
    """
    Here's the filter set!
    """
        
    return {'points_max': (lambda row, filters: float(row['Points']) <= filters['points_max']),
            'points_min': (lambda row, filters: float(row['Points']) >= filters['points_min']),
            'correspondent': (lambda row, filters: row['Correspondent'] == filters['correspondent']),
            'identifier': (lambda row, filters: parse_city_name(row['Scenario Name'], '|')[1] == filters['identifier'])
            }


# In[28]:

def parse_city_name(text, delim):
    """
    breaks up city into identifiers
    """
    splitter = text.find(delim)
    city = text[:splitter]
    other = text[splitter+1:]
    
    return city, other
    


# In[29]:

def clean_data(read_file_name, filters):
    """
    Filters data by what you are looking for
    """
    #city, extra = parse_city_name(read_file_name, '_')
    city_file_name = WRITE_FILE_BASE % read_file_name
    csv_file = open(city_file_name, 'wb') 
    
    writer = csv.DictWriter(csv_file, fieldnames=FIELD_NAMES)
    logger.info('writing file %s opened' % city_file_name)
    
    with open(read_file_name, 'rU') as data_file:
        reader = csv.DictReader(data_file, delimiter='\t')
        logger.info('reading file %s opened' % read_file_name)
        
        for row in reader:
            
            filtered_row = filter_row(row, filters)
            if filtered_row:    
                logger.info('writing filtered row %s' % filtered_row)
                writer.writerow(filtered_row)
            else:
                continue
    
    csv_file.close()


# In[30]:

if __name__ == '__main__':
    
    args = parser.parse_args()
    filters = vars(args)
    print filters
    
    etl_mortgage_data(filters)


# In[ ]:




# In[ ]:




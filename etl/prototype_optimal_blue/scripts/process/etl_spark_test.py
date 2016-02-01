
# coding: utf-8

# In[1]:

from collections import namedtuple
from datetime import datetime

TIME_FMT = "%m/%d/%y %I:%M %p"
POINTS_MIN = 99.2
POINTS_MAX = 100.8
SCENARIO_FIELDS = ('ScenarioName','RateSheetID', 'LenderID', 'Rate', 'LockPeriod', 
          'Points', 'CompanyID', 'Margin', 'IndexName', 'IndexValue', 
          'Correspondent', 'Updated', 'Void')
MARGIN_FIELDS = ('UserName', 'Entity_Index', 'TotalMarkup', 'InvestorEntity_Index',
                 'LenderName', 'Points', 'VolumeType', 'VolumeTypeName', 'Rate', 'HierarchyName',
                 'ProductSet_Index', 'IsYourRate', 'TotalFees', 'User_Index', 'ProductSetName',
                 'EntityName', 'InvestorEntityName', 'TotalCompensation', 'ScenarioName')

Scenario = namedtuple('Scenario', SCENARIO_FIELDS)
Margin = namedtuple('Margin', MARGIN_FIELDS)

def process_interest_rates(hdfs_paths):
    """
    processes interest rates to
    filter down comparison
    hdfs_path: dict of file types, HDFS paths
    """
    for_merge = list()
    for data_type in hdfs_paths:
        values = prep_data(hdfs_paths[data_type], data_type)
        for_merge.append(values)
    for data_set in for_merge:
        for key, scenario_data in data_set.collect():
            print key, data_set
    


def prep_data(hdfs_path, data_type):
    """
    Cleans and filters data, also groups
    by a key we set
    hdfs_path: str of file location in HDFS
    """
    raw = sc.textFile("hdfs://localhost:9000/%s" % hdfs_path)
    header = raw.first()
    split_data = raw.filter(lambda line:line != header).map(lambda line: line.split("\t"))
    parsed_data = parse_data(split_data, data_type)
    points_filtered_data = parsed_data.filter(lambda line: (line.Points > POINTS_MIN and line.Points < POINTS_MAX))
    correspondence_filtered_data = correspondence_filter(points_filtered_data, data_type)
    keyed = correspondence_filtered_data.map(lambda line: ('%sPV%s'%(line.ScenarioName, line.Points), line))
    
    return keyed

def parse_data(data, data_type):
    """
    A helpers to parse a row based on data type
    data: rdd of data being parsed
    data_type: str of data type either scenario or margin
    """
    if data_type == 'scenario':
        parsed_data = data.map(parse_scenario)
    else:
        parsed_data = data.map(parse_margin)
    
    return parsed_data

def correspondence_filter(data, data_type):
    """
    Cleans up the correspondence data, if it exists
    """
    if data_type == 'scenario':
        filtered_data = data.filter(lambda line: (line.Correspondent == True))
    else:
        filtered_data = data
    return filtered_data

def parse_scenario(row):
    """
    Parses the raw scenario csv data
    row: list of str data
    """
    row[1] = int(row[1])
    row[2] = int(row[2])
    row[3] = float(row[3])
    row[4] = int(row[4])
    row[5] = round(float(row[5]),2)
    row[6] = int(row[6])
    row[7] = float(row[7])
    row[9] = float(row[9]) if row[9] else None
    row[10] = bool(row[10])
    row[11] = datetime.strptime(row[11], TIME_FMT)
    
    return Scenario(*row[:])
 
def parse_margin(row):
    """
    Parses the raw margin csv data
    row: list of str data
    """
    row[1] = int(row[1])
    row[2] = float(row[2])
    row[3] = int(row[3])
    row[5] = round(float(row[5]),2)
    row[6] = int(row[6])
    row[8] = float(row[8])
    row[10] = int(row[10])
    row[11] = bool(row[11])
    row[12] = float(row[12])
    row[13] = int(row[13])
    row[17] = float(row[17])
    
    return Margin(*row[:])


# In[ ]:

process_interest_rates({'scenario': 'Python/weekly_aggregated_20151114.txt', 'margin': 'Python/margin_aggregated_20151228.txt'})


# In[ ]:




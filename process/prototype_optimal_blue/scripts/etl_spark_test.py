
# coding: utf-8

# In[11]:

from collections import namedtuple
from datetime import datetime

TIME_FMT = "%m/%d/%y %I:%M %p"
POINTS_MIN = 99.2
POINTS_MAX = 100.8
FIELDS = ('ScenarioName','RateSheetID', 'LenderID', 'Rate', 'LockPeriod', 
          'Points', 'CompanyID', 'Margin', 'IndexName', 'IndexValue', 
          'Correspondent', 'Updated', 'Void')
Scenario = namedtuple('Scenario', FIELDS)


def process_interest_rates(hdfs_path):
    """
    processes interest rates to
    filter down comparison
    hdfs_path: str of file location in HDFS
    """
    scenario_values = scenario_grouping(hdfs_path)
    for key, scenario_data in scenario_values.collect():
        print key, scenario_data
    


def scenario_grouping(hdfs_path):
    """
    Cleans and filters data, also groups
    by a key we set
    hdfs_path: str of file location in HDFS
    """
    raw = sc.textFile("hdfs://localhost:9000/%s" % hdfs_path)
    header = raw.first()
    split_data = raw.filter(lambda line:line !=header).map(lambda line: line.split("\t"))
    filtered_data = split_data.map(parse_scenario).filter(lambda line: (line.Points > POINTS_MIN and line.Points < POINTS_MAX))                    .filter(lambda line: (line.Correspondent == True))
    keyed = filtered_data.map(lambda line: ('%sPV%s'%(line.ScenarioName, line.Points), line))
    sorted_scenarios = keyed.sortByKey()
    
    return sorted_scenarios

def parse_scenario(row):
    """
    Parses the raw csv data
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
 


# In[12]:

process_interest_rates("Python/weekly_agg_test.txt")


# In[ ]:




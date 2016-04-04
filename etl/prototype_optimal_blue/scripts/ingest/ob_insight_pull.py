
# coding: utf-8

# In[5]:

import os
import sys
from itertools import product, izip
from requests import Session
import simplejson as json
import pprint
from time import sleep
from random import random
from logging import getLogger, basicConfig, INFO
from csv import DictWriter
from datetime import datetime


# In[6]:

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)


# In[7]:

PARAMS={"ARMFixedTerm":0,
        "AUS":278,
        "AmortizationType":133,
        "Citizenship":"NaN",
        "CreateDate":"0001-01-01T00:00:00",
        "CurrentHierarchyPresent":"false",
        "DTI":40,
        "Entity_Index":0,
        "FICO":740,
        "IsDiagnosticUser":"false",
        "LTV":80,
        "LoanAmount":225000,
        "LoanOfficer_Index":0,
        "LockPeriod":30,
        "MSALocation_Index":68,
        "MarketAnalysisCriteria":"DefaultCriteria",
        "ModifyDate":"0001-01-01T00:00:00",
        "NumberOfStories":1,
        "NumberOfUnits":123,
        "Occupancy":2,
        "PrepaymentPenalty":211,
        "Price":100,
        "ProductType":127,
        "PropertyType":115,
        "PropertyValue":281250,
        "Purpose":106,
        "Rate":0,
        "SavedSearch_Index":0,
        "SearchByPrice":"true",
        "SearchName":'',
        "SearchStatus_Index":1,
        "SelfEmployed":110,
        "ShowAdditionalFields":"false",
        "Term":136,
        "User_Index":0
        }


# In[8]:

WRITE_FILE_PATH = '/Users/jbhambhani/cinch/mortgage-interest-data-prototype/ob_insight/ob_insight_%s.csv'                    % (datetime.strftime(datetime.now(),'%Y%m%d'))
UA = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:39.0) Gecko/20100101 Firefox/39.0"}
OB_BASE = 'https://optimalblue.com/%s'
HOME = 'insight'
LOGIN_URL = '/Insight/Home/Login/%5Bobject%20Object%5D'
SEARCH_URL = '/Insight/Search/SubmitSearch'
USER_COOKIE = {"tbUserName": "%22chip%40cinchfinancil%22"}
PASSWORD_PAYLOAD = {"user":{"UserName":"chip@cinchfinancial.com","Password":"Password1"}}
SLEEP_TIME = 5 * random()
#MSAS = {33: 'BOS', 48: 'CHI', 153: 'LOS', 179: 'NYC'}
#AMOUNTS = {300000: 'LA300000', 650000: 'LA650000'}
#PRODUCT_TYPES = {127: 'PR01', 128: 'PRO2', 129: 'PR03'} # Assume PR03 < 300k loan, PR01 < 300K, PR02 > 625
#PURPOSE = {106: 'P1', 111: 'P2'}
#FICO = {640:'F640', 720: 'F720'}
#LTV = {80: 'LT080', 95: 'LT095'}
TERMS = {139: 'T02',136: 'T05'}
ARM = {271: 'T14'}
PRODUCT_TYPES = {127: 'PR01', 128: 'PRO2'}
INTEREST_VAR = {134: 1}
INTEREST_FIX = {133: 0}
PURPOSE = {106: 'P1'}
FICO = {720: 'F720'}
LTV = {80: 'LT080'}
AMOUNTS = {300000: 'LA300000'}
MSAS = {33: 'BOS', 48: 'CHI'}
QUERY_INPUTS = {"MSALocation_Index": MSAS,
                "AmortizationType": INTEREST_FIX,
                "ProductType": PRODUCT_TYPES, 
                "Term": TERMS,
                "FICO": FICO,
                "LTV": LTV,
                "LoanAmount": AMOUNTS,
                "Purpose": PURPOSE
               }


# In[9]:

def get_data():
    with Session() as session:
        session.headers.update(UA)
        homepage = session.get(OB_BASE % HOME)
        sleep(SLEEP_TIME)
        session.headers.update({'X-Requested-With': 'XMLHttpRequest'})
        session.headers.update({"Referer": 'https://optimalblue.com/insight'})
        logger.info('logging in')
        login = session.post(url = OB_BASE % LOGIN_URL,
                             cookies = {"tbUserName": "%22chip%40cinchfinancil%22"},
                             json=PASSWORD_PAYLOAD)
        sleep(SLEEP_TIME)
        run_queries(session, 'fixed')
        logger.info('done fixed queries')
        run_queries(session, 'variable')
        logger.info('done variable queries')
        logger.info('finished with status 0')


# In[10]:

def run_queries(session, state):

                           
    query_inputs = term_handler(state)
    
    combinations = cartesian_product(query_inputs)
    
    for query in combinations:
        PARAMS.update(query)
        logger.info('query')
        sleep(SLEEP_TIME)
        page = session.get(url = OB_BASE % SEARCH_URL,
                                params = PARAMS)
        logger.info('got page')
        pricing_data = page.json()
        with open(WRITE_FILE_PATH, 'wb') as output_file:
            fieldnames = pricing_data['PricingRecords'][0].keys()
            fieldnames.append('Scenario')
            print 'FIELDNAMES %s' % fieldnames
            logger.info('header %s' % fieldnames)
            csv_output = DictWriter(output_file, fieldnames=fieldnames)
            csv_output.writeheader()
            for row in pricing_data['PricingRecords']:
                row['Scenario'] = '{msa}|{product}{purpose}{amount}{ltv}{fico}LD30IO0{term}'.format(msa=query_inputs['MSALocation_Index'][query['MSALocation_Index']],
                                                                                                    product=query_inputs["ProductType"][query["ProductType"]],
                                                                                                    purpose=query_inputs["Purpose"][query["Purpose"]],
                                                                                                    amount=query_inputs["LoanAmount"][query["LoanAmount"]],
                                                                                                    ltv=query_inputs["LTV"][query["LTV"]],
                                                                                                    fico=query_inputs["FICO"][query["FICO"]],
                                                                                                    term=query_inputs["Term"][query["Term"]])
                
        
                logger.info('adding row %s' % row)
                csv_output.writerow(row)


# In[11]:

def term_handler(state):
    query_inputs = QUERY_INPUTS
    if state == 'variable':
        variable_params = {"AmortizationType": INTEREST_VAR,
                           "ARMFixedTerm": ARM,
                           "Term": {136: TERMS.get(136)}
                           }
                          
        query_inputs.update(variable_params)
    
    else:
        pass
    

    return query_inputs


# In[12]:

def cartesian_product(query_inputs):
    return (dict(izip(query_inputs, x)) for x in product(*query_inputs.itervalues()))


# In[13]:

get_data()


# In[ ]:




# In[ ]:





# coding: utf-8

# In[257]:

import os
import sys
from requests import Session
import simplejson as json
from time import sleep
from random import random


# In[260]:

PARAMS={
        "ARMFixedTerm":0,
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


# In[264]:

UA = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:39.0) Gecko/20100101 Firefox/39.0"}
OB_BASE = 'https://optimalblue.com/%s'
HOME = 'insight'
LOGIN_URL = '/Insight/Home/Login/%5Bobject%20Object%5D'
SEARCH_URL = '/Insight/Search/SubmitSearch'
USER_COOKIE = {"tbUserName": "%22chip%40cinchfinancil%22"}
PASSWORD_PAYLOAD = {"user":{"UserName":"chip@cinchfinancial.com","Password":"Password1"}}
SLEEP_TIME = 5 * random()


# In[267]:

def get_data():
    with Session() as session:
        session.headers.update(UA)
        homepage = session.get(OB_BASE % HOME)
        sleep(SLEEP_TIME)
        session.headers.update({'X-Requested-With': 'XMLHttpRequest'})
        session.headers.update({"Referer": 'https://optimalblue.com/insight'})
        login = session.post(url = OB_BASE % LOGIN_URL,
                             cookies = {"tbUserName": "%22chip%40cinchfinancil%22"},
                             json=PASSWORD_PAYLOAD)
        sleep(SLEEP_TIME)
        query = session.get(url = OB_BASE % SEARCH_URL,
                            params = PARAMS)
        
        print query.text
        


# In[268]:

get_data()


# In[ ]:




# In[ ]:




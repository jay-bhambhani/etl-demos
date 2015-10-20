
# coding: utf-8

# In[ ]:

# Configs for Optimal Blue Server

class logins(object):
    def __init__(self):
        self.credentials = dict()
    
    def get_credentials(self, data_sources):
        if 'optimal_blue' in data_sources:
            source = 'optimal_blue'
            credential = dict()
            credential['user'] = 'CinchFinancial'
            credential['host'] = 'sftp.optimalblue.com'
            credential['password'] = 'd4ech7tabuwU'
            self.credentials[source] = credential


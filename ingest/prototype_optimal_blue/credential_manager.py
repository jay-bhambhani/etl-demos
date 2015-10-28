
# coding: utf-8

# In[ ]:

# Simple credential manager for the prototype
import os
import sys
import web
from ConfigParser import SafeConfigParser
from logging import getLogger, basicConfig, INFO

basicConfig(stream=sys.stdout, level=INFO)
logger = getLogger(__name__)

CONFIG_PARSER_PATH = 'proto.ini'

class CredentialManager(object):
    def __init__(self):
        self.credentials = dict()
        self.parser = SafeConfigParser()

    
    def get_credentials(self, source):
        """
        will get you the credentials for a given source platform
        """
        credential = dict()
        self.parser.read(CONFIG_PARSER_PATH)
        options = self.parser.options(source)
        for option in options:
            try:
                credential[option] = self.parser.get(source, option)
                if credential[option] == -1:
                    logger.info("skipping: %s" % option)
            except:
                logger.warn("exception on %s!" % option)
                credential[option] = None
        self.credentials = credential


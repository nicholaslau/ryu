__author__ = 'root'

from ryu.cfg import CONF
import logging
import  time

TYPE = 'Type'
DOMAINID = 'DomainId'

class DomainReplyController(object):

    def __init__(self):

        self.name = 'DomainReplyController'

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

        self.logger.info("I am reply controller")

    def TaskAssin(self, jsonMsg, domaincontroller):

        assert jsonMsg[TYPE] == 'TaskAssin'


    def TaskDelete(self, jsomMsg, domaincontroller):

        assert jsomMsg[TYPE] == 'TaskDelete'

    def KeepAlive(self, jsonMsg, domaincontroller):

        assert jsonMsg[TYPE] == 'KeepAlive'

        domainId = jsonMsg[DOMAINID]

        if domainId is not domaincontroller.domain_id:
            self.logger.debug("receive a keepalive in wrong way")
            return

        domaincontroller.super_last_echo = time.time()











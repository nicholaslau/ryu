__author__ = 'root'

import  logging
import time

TYPE = 'Type'
DOMAINID = 'DomainID'
DPID = 'dpid'
SRCSWITCH = 'SrcSwitch'
DSTSWITCH = 'DstSwitch'
SRCPORT = 'SrcPort'
DSTPORT = 'DstPort'
PORTSTATUS = 'PortStats'
DOMAINWSGIIP = 'domainWsgiIp'
DOMAINWSGIPORT = 'domainWsgiPort'
DOMAINLASRECHO = 'domainLastEcho'

REST_TASK_ID =  'task_id'

TASKSTATUS = 'task_status'
ESTABLISHED = 'established'
UNCONFIRMED = 'unconfirmed'

class SuperReplyController(object):

    def __init__(self):
        self.name = 'SuperReplyController'
        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)
        self.logger.info("I am Super Reply Controller!")


    def SwitchEnter(self, jsonMsg, superController):

        assert jsonMsg[TYPE] == 'SwitchEnter'
        domainId = jsonMsg[DOMAINID]
        switch_list = superController.switch_domains.setdefault(domainId, [])
        if not switch_list:
            self.logger.info("DomainA")

        dpid = jsonMsg[DPID]
        if dpid not in switch_list:
            switch_list.append(dpid)
            superController.all_topo.add_node(dpid)
            self.logger.info("Switch %016x enter in global topo" % dpid)

        if dpid not in superController.dpiToDomainId:
            superController.dpiToDomainId[dpid] = domainId

    def SwitchLeave(self, jsonMsg, superController):
        assert jsonMsg[TYPE] == 'SwitchLeave'

        domainid = jsonMsg[DOMAINID]

        if domainid in superController.switch_domains:
            switch_list = superController.switch_domains[domainid]
        else:
            self.logger.fatal("Switch leave from no_exist domain. DomianId: %d" % domainid)
            return

        dpid = jsonMsg[DPID]

        if dpid in switch_list:
            switch_list.remove(dpid)
            if dpid in superController.all_topo.nodes():
                superController.all_topo.remove_node(dpid)
                self.logger.info("Switch %16x leave from global topo" % dpid)
        else:
            self.logger.fatal("Switch %16x not in Domain %d" % (dpid, domainid))
            return

        if dpid in superController.dpiToDomainId:
            del superController.dpiToDomainId[dpid]

        if not switch_list:
            del superController.switch_domains[domainid]
            self.logger.info("Domain %d Leave" % domainid)

    def LinkAdd(self, jsonMsg, supercontroller):
        assert jsonMsg[TYPE] == 'LindAdd'

        domainid = jsonMsg[DOMAINID]
        link_list = supercontroller.link_domains.setdefault(domainid, [])
        src_switch = jsonMsg[SRCSWITCH]
        dst_switch = jsonMsg[DSTSWITCH]
        src_port = jsonMsg[SRCPORT]
        dst_port = jsonMsg[DSTPORT]

        link = (src_switch, src_port, dst_switch, dst_port)

        if link not in link_list:
            link_list.append(link)

        if (src_switch, dst_switch) not in supercontroller.all_topo.edges():
            supercontroller.all_topo.add_edge(src_switch, dst_switch)
            self.logger.info("Glolb Topo Lind add: src %16x port_no %8x-> dst %16x %8x" % (src_switch, src_port,
                                                                                           dst_switch, dst_port))
    def LinkDelete(self, jsonMsg, supercontroller):

        assert jsonMsg[TYPE] == 'LinkDelete'

        domainid = jsonMsg[DOMAINID]

        if domainid in supercontroller.link_domains:
            link_list = supercontroller.link_domains[domainid]
        else:
            self.logger.fatal("Link delete from no-exist domain. Domain: %d", domainid)
            return
        src_switch = jsonMsg[SRCSWITCH]
        dst_switch = jsonMsg[DSTSWITCH]
        src_port = jsonMsg[SRCPORT]
        dst_port = jsonMsg[DSTPORT]

        to_delete = (src_switch, src_port, dst_switch, dst_port)

        if to_delete in link_list:
            link_list.remove(to_delete)
            if (src_switch, dst_switch) in supercontroller.all_topo.edges():
                supercontroller.all_topo.remove_edge(src_switch, dst_switch)

    def PortStatus(self, jsomMsg, supercontroller):

        assert jsomMsg[TYPE] == 'PortStatus'



    def KeepAlive(self, jsomMsg, supercontroller):
        domain_wsgi_info = supercontroller.domain_wsgi_info

        assert jsomMsg[TYPE] == 'KeepAlive'

        domainId = jsomMsg[DOMAINID]

        if not domainId in domain_wsgi_info:
            info = domain_wsgi_info.setdefault(domainId, {})
            info[DOMAINWSGIIP] = None
            info[DOMAINWSGIPORT] = None
            info[DOMAINLASRECHO] = None

        info = domain_wsgi_info[domainId]

        domainWsgiIp = jsomMsg[DOMAINWSGIIP]
        domainWsgiPort = jsonMsg[DOMAINWSGIPORT]
        info[DOMAINWSGIIP] = domainWsgiIp
        info[DOMAINWSGIPORT] = domainWsgiPort

        timenow = time.time()

        info[DOMAINLASRECHO ] = timenow


    def TaskAssignReply(self, jsonMsg, supercontroller):

        assert jsonMsg[TYPE] == 'TaskAssignReply'

        taskId = jsonMsg[REST_TASK_ID]

        task_info_dict = supercontroller.task_info_dict
        if taskId not in task_info_dict.keys():
            self.logger.debug("receive a task assign reply to a task not assign")
            return
        else:
            task_info  =task_info_dict[taskId]
            unconfirm_domian_list = task_info['unconfirm_domain_list']

        domainId = jsonMsg[DOMAINID]

        if domainId in unconfirm_domian_list:
            unconfirm_domian_list.remove(domainId)

        if not unconfirm_domian_list  and task_info[TASKSTATUS] ==  UNCONFIRMED:
            task_info[TASKSTATUS]  == ESTABLISHED

    def TaskAssignReply(self, jsonMsg, supercontroller):

        assert jsonMsg[TYPE] == 'TaskDeleteReply'

        taskId = jsonMsg[REST_TASK_ID]

        task_delete_info = supercontroller.task_delete_info
        try:
            unconfirm_domain_list = task_delete_info[taskId]
        except:
            self.logger.debug("receive a task delete reply for a task not assign")
            return

        domianId = jsonMsg[DOMAINID]
        if domianId in unconfirm_domain_list:
            unconfirm_domain_list.remove(domianId)

        if not unconfirm_domain_list:
            task_info_dict = supercontroller.task_info_dict
            if taskId in task_info_dict:
                del task_info_dict[taskId]

__author__ = 'root'

import networkx as nx
import json
import  logging
import time

from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication
from ryu.app.wsgi import route

from ryu.base import app_manager

from ryu.app.super_reply_controller import SuperReplyController

super_instance = 'super_controller_instance'
super_reply_controller_instance = 'super_reply_controller_instance'

super_url_base = 'super'

TYPE = 'Type'

DOMAINWSGIIP = 'domainWsgiIp'
DOMAINWSGIPORT = 'domainWsgiPort'


REST_TRAFFIC_ID = 'id'
REST_SRC_SWITCH = 'src_switch'
REST_DST_SWITCH = 'dst_switch'
REST_MID_SWITCH = 'mid_switch'
REST_TASK_ID =  'task_id'
REST_BANDWITH = 'bandwith'
REST_DURATION = 'duration'
REST_DST_IP = 'nw_dst'
REST_SRC_IP = 'nw_src'

TASKSTATUS = 'task_status'
ESTABLISHED = 'established'
UNCONFIRMED = 'unconfirmed'

DOMAINID = 'DomainId'

class SuperController(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(SuperController, self).__init__(*args, **kwargs)

        self.all_topo = nx.DiGraph()
        self.switch_domains = {}    # domainid  <----> switchlist
        self.link_domains = {}
        self.domain_wsgi_info = {}  # domainid  <----->   domain wsig info {'ip':"xxxxx", 'prot':1111}

        self.dpiToDomainId = {}   # dpid <------->   domainId
        self.task_info_dict = {}
        self.task_delete_info = {}
        self.label_list = self.gen_label_list()

        wsgi = kwargs['wsgi']
        wsgi.register(SuperWsgiController, {super_instance: self, super_reply_controller_instance: SuperReplyController()})

    def gen_label_list(self):


        return []


    def _to_commad(self, send_message, returnType=False):

        command = 'curl -X '
        if returnType:
            command += 'GET -d \''
        else:
            command += 'PUT -d \''
        command += send_message
        command += '\' http://'
        command += self.super_wsgi_ip
        command += ':'
        command += str(self.super_wsgi_port)
        command += super_url_no_reutrn


        return command

    # @staticmethod
    def send_no_return_command(self, command):
        try:
            os.popen(command)
        except:
            self.logger.debug('command exceute fail.Fail Command: %s' % command)
            return


    def make_task_assign_message(self, domianid, task_id, dst_ip, bandwith, duration, path_info):
        to_send = {}
        to_send[DOMAINID] = domainid
        to_send[TYPE] = 'TaskAssign'
        to_send[REST_TASK_ID] = task_id
        to_send[REST_DST_IP] = dst_ip
        to_send[REST_BANDWITH] = bandwith
        to_send[REST_DURATION] = duration
        to_send['path'] = path_info

        send_message = json.dumps(to_send)

        return send_message

    def make_task_delete_message(self, domainid, task_id):
        to_send = {}
        to_send[DOMAINID] = domainid
        to_send[TYPE] = 'TaskDelete'
        to_send[REST_TASK_ID] =  task_id

        send_message = json.dumps(to_send)

        return send_message

    def _to_commad(self, send_message, returnType=False):

        domainId = eval(send_message)[DOMAINID]
        domain_wsgi_ip = self.domain_wsgi_info[DOMAINID][DOMAINWSGIIP]

        domain_wsgi_port = self.domain_wsgi_info[DOMAINID][DOMAINWSGIPORT]

        command = 'curl -X '
        if returnType:
            command += 'GET -d \''
        else:
            command += 'PUT -d \''
        command += send_message
        command += '\' http://'
        command += domain_wsgi_ip
        command += ':'
        command += str(domain_wsgi_port)
        command += super_url_no_reutrn

        return command


class SuperWsgiController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(SuperWsgiController, self).__init__(req, link, data, **config)
        self.name = 'SuperWsgiController'
        self.SuperController = data[super_instance]
        self.super_reply_controller = data[super_reply_controller_instance]

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

    @route('super', super_url_base + '/noreturn', methods=['PUT'], requirements=None)
    def noreturned_command_handler(self, req):
        msgbody = eval(req.body)
        type = msgbody[TYPE]

        try:
            func = getattr(self.super_reply_controller, type)
        except:
            self.logger.fatal('no such type')

        func(msgbody, self.SuperController)

    @route('super', super_url_base + '/return', methods=['PUT'], requirements=None)
    def returned_command_handler(self, req):
        msgbody = eval(req.body)
        try:
            type = msgbody[TYPE]
        except Exception, e:
            self.logger.error("bad commands")
            return

        try:
            func = getattr(self.super_reply_controller, type)
        except:
            self.logger.error('Can not find handler')
            return

        func(msgbody, self.SuperController)


    @route('super', super_url_base + '/task/assign', methods=['PUT'], requirements=None)
    def taskAssign(self, rep):
        SC = self.SuperController

        rest = eval(req)

        taskId = rest[REST_TASK_ID]

        if taskId in SC.task_info_dict:
            if SC.task_info_dict[taskId]['status'] == ESTABLISHED:
                self.logger.debug("Task of this taskID has be ESTABLISHED!!!!")
                self.logger.warning("Please check out!!!!!!")
                return

        task_info = SC.task_info_dict.setdefault(taskId, {})
        task_info[REST_TASK_ID] = taskId

        src_switch = rest[REST_SRC_SWITCH]
        dst_switch = rest[REST_DST_SWITCH]

        task_info[REST_SRC_SWITCH] = src_switch
        task_info[REST_DST_SWITCH] = dst_switch

        switch_list = nx.shorestpath(SC.all_topo, src_switch, dst_switch)

        task_info['complete_path'] = switch_list

        bandwith = rest[REST_BANDWITH]
        duration = rest[REST_DURATION]
        dstIp = rest[REST_DST_IP]
        srcIp = rest[REST_SRC_IP]
        task_info[REST_SRC_IP] = srcIp
        task_info[REST_DST_IP] = dstIp
        task_info[REST_BANDWITH] =  bandwith
        task_info[REST_DURATION] = duration

        sectorial_path = self.get_sectorial_path(switch_list, SC.dpidToDomainID)

        task_info['unconfirm_domain_list'] = sectorial_path.keys()

        task_info['sectorial_path'] = sectorial_path

        sectorial_path_with_labels = self.assin_mpls_lable(sectorial_path, SC.label_list)



        for i in sectorial_path_with_labels:
            path_info = sectorial_path_with_labels[i]
            send_message = SC.make_task_assign_message(domianid=i, task_id=taskId, dst_ip=dst_ip,
                                                  bandwith=bandwith, duration=duration, path_info=path_info)

            command =  SC._to_commad(send_message)

            SuperController.send_no_return_command(command=command)

        task_info['create_time'] = time.time()
        task_info[TASKSTATUS] = UNCONFIRMED
        return
    @route('super', super_url_base + '/task/delete', methods=['PUT'], requirements=None)
    def taskDelete(self, req):
        SC = self.SuperController

        rest = eval(req)

        try:
            taskId = rest[REST_TASK_ID]
        except:
            self.logger.debug("No task marked as taskId")
            return

        task_info = SC.task_info_dict[taskId]
        domainList = task_info['sectorial_path'].keys()

        for i in domainList:
            send_message = SC.make_task_delete_message(domainid=i, task_id=taskId)
            command = SC._to_commad(send_message)

            SuperController.send_no_return_command(command)

        SC.task_del_info[taskId] = domainList




    def get_sectorial_path(self, switchlist, switchdic):
        sectorial_path = {}
        return sectorial_path


    def assin_mpls_lable(self, sectorial_path, lable_list):

        for i in sectorial_path:
            labels = []
            sectorial_swtich_lists = i['switches']
            length = len(sectorial_swtich_lists)
            for j in range(0, length-1):
                label = lable_list[0]
                label_list.remove(label)
                labels.append(label)
            i['labels'] = labels

        return sectorial_path
__author__ = 'root'

import networkx as nx
import json
import logging
import time
import os
import random
import copy

from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication
from ryu.app.wsgi import route

from ryu.base import app_manager

from ryu.app.super_reply_controller import SuperReplyController

super_instance = 'super_controller_instance'
super_reply_controller_instance = 'super_reply_controller_instance'

super_url_base = '/super'

domain_url_no_reutrn = '/domain/noreturn'
domain_url_reutrn = '/domain/return'

TYPE = 'Type'

DOMAINWSGIIP = 'domainWsgiIp'
DOMAINWSGIPORT = 'domainWsgiPort'


REST_TRAFFIC_ID = 'id'
REST_SRC_SWITCH = 'src_switch'
REST_DST_SWITCH = 'dst_switch'
REST_MID_SWITCH = 'mid_switch'
REST_TASK_ID =  'task_id'
REST_BANDWITH = 'bandwidth'
REST_DURATION = 'duration'
REST_DST_IP = 'nw_dst'
REST_SRC_IP = 'nw_src'

TASKSTATUS = 'task_status'
ESTABLISHED = 'established'
UNCONFIRMED = 'unconfirmed'

DOMAINID = 'DomainID'

class SuperController(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(SuperController, self).__init__(*args, **kwargs)

        self.all_topo = nx.DiGraph()
        self.all_topo_weight = nx.DiGraph()
        self.switch_domains = {}    # domainid  <----> switchlist
        self.link_domains = {}
        self.domain_wsgi_info = {1:{'domainWsgiIp':'10.108.90.200','domainWsgiPort':8080}}  # domainid  <----->   domain wsig info {'ip':"xxxxx", 'prot':1111}


        self.traffic_balance = False
        self.dpidToDomainID = {}   # dpid <------->   domainId
        self.task_info_dict = {}
        self.task_delete_info = {}
        self.label_list = self.genMPLSLableLib()

        self.last_collect= {}

        wsgi = kwargs['wsgi']
        wsgi.register(SuperWsgiController, {super_instance: self, super_reply_controller_instance: SuperReplyController()})


    # def _to_commad(self, send_message, returnType=False):
    #
    #     command = 'curl -X '
    #     if returnType:
    #         command += 'GET -d \''
    #     else:
    #         command += 'PUT -d \''
    #     command += send_message
    #     command += '\' http://'
    #     command += self.super_wsgi_ip
    #     command += ':'
    #     command += str(self.super_wsgi_port)
    #     command += super_url_no_reutrn
    #
    #     return command

    # @staticmethod
    def send_no_return_command(self, command):
        try:
            os.popen2(command)
        except:
            self.logger.debug('command exceute fail.Fail Command: %s' % command)
            return


    def make_task_assign_message(self, domainid, task_id, src_ip, dst_ip, bandwidth, duration, path_info, type):
        to_send = {}
        to_send[DOMAINID] = domainid
        to_send[TYPE] = 'TaskAssign'
        to_send['main'] = type
        to_send[REST_TASK_ID] = task_id
        to_send[REST_SRC_IP] = src_ip
        to_send[REST_DST_IP] = dst_ip
        to_send[REST_BANDWITH] = bandwidth
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
        domain_wsgi_info = self.domain_wsgi_info.get(domainId, None)
        domain_wsgi_ip = domain_wsgi_info[DOMAINWSGIIP]

        domain_wsgi_port = domain_wsgi_info[DOMAINWSGIPORT]

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
        if not returnType:
            command += domain_url_no_reutrn
        else:
            command += domain_url_reutrn

        return command

    def genMPLSLableLib(self):
        min = 1
        max = 65536
        num = 1000
        result = []
        while len(result) <= num:
            item = random.randint(min, max + 1)
            if item not in result:
                result.append(item)

        return result


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
    def taskAssign(self, req):

        SC = self.SuperController
        rest = eval(req.body)
        taskId = rest.get(REST_TASK_ID, None)
        if not taskId:
            return Response(status=200, body="Input a task Id\n")
        task_info_dict = SC.task_info_dict
        if taskId in task_info_dict:
            status = task_info_dict[taskId].get(TASKSTATUS, None)
            if  status and status == ESTABLISHED:
                return Response(status=200, body="taskId duplicated!\n")

        task_info = task_info_dict.setdefault(taskId, {})

        src_switch = rest[REST_SRC_SWITCH]
        dst_switch = rest[REST_DST_SWITCH]
        bandwith = rest[REST_BANDWITH]
        duration = rest[REST_DURATION]
        dstIp = rest[REST_DST_IP]
        srcIp = rest[REST_SRC_IP]

        if SC.traffic_balance:
            try:
                main_switch_list = nx.dijkstra_path(SC.all_topo_wight, src_switch, dst_switch)
            except:
                self.logger("no path between switch %d and %d" % (src_switch, dst_switch))
                del task_info_dict[taskId]
                return Response(status=200, body="no path between switch %d and %d\n" % (src_switch, dst_switch))
            main_edges = self.get_edges_from_switchlist(main_switch_list)
            temp_topo = self.get_topo_for_backup(SC.all_topo_wight, main_edges)

            try:
                backup_switch_list = nx.dijkstra_path(temp_topo, src_switch, dst_switch)
            except:
                self.logger.info("no backup path between switch %d and %d" % (src_switch, dst_switch))
                backup_switch_list = []
        else:
            try:
                main_switch_list = nx.shortest_path(SC.all_topo, src_switch, dst_switch)
            except:
                self.logger.info("no path between switch %d and %d" % (src_switch, dst_switch))
                del task_info_dict[taskId]
                return Response(status=200, body="no path between switch %d and %d\n" % (src_switch, dst_switch))
            main_edges = self.get_edges_from_switchlist(main_switch_list)
            temp_topo = self.get_topo_for_backup(SC.all_topo, main_edges)

            try:
                backup_switch_list = nx.shortest_path(temp_topo, src_switch, dst_switch)
            except:
                self.logger.info("no backup path between switch %d and %d" % (src_switch, dst_switch))
                backup_switch_list = []

        main_path_info = task_info.setdefault('main', {})
        main_path_info['complete_path'] = main_switch_list
        main_sectorial_path = self.get_sectorial_path(main_switch_list, SC.dpidToDomainID)
        main_sectorial_path_with_labels = self.assign_mpls_lable(main_sectorial_path, SC.label_list)

        backup_path_info = task_info.setdefault('backup', {})
        backup_path_info['complete_path'] = backup_switch_list
        backup_sectorial_path = self.get_sectorial_path(backup_path_info, SC.dpidToDomainID)
        backup_sectorial_path_with_labels = self.assign_mpls_lable(backup_sectorial_path, SC.dpidToDomainID)

        for i in main_sectorial_path_with_labels:
            path_info = main_sectorial_path_with_labels[i]
            send_message = SC.make_task_assign_message(domainid=i, task_id=taskId, src_ip=srcIp, dst_ip=dstIp,
                                                       bandwidth=bandwith, duration=duration,
                                                       path_info=path_info, type='main')
            command = SC._to_commad(send_message)
            SC.send_no_return_command(command=command)

        for j in backup_sectorial_path_with_labels:
            path_info = backup_sectorial_path_with_labels[j]
            send_message = SC.make_task_assign_message(domainid=i, task_id=taskId, src_ip=srcIp, dst_ip=dstIp,
                                                       bandwidth=bandwith, duration=duration,
                                                       path_info=path_info, type='backup')
            command = SC._to_commad(send_message)
            SC.send_no_return_command(command)

        uncnfirm_domian_list = task_info.setdefault('unconfirm_domain_list', {})
        uncnfirm_domian_list['main'] = main_sectorial_path.keys()
        uncnfirm_domian_list['backup'] = backup_sectorial_path.keys()
        task_info['create_time'] = time.time()
        task_info[TASKSTATUS] = UNCONFIRMED

    def start_backup_hanlder(self, taskId):

        SC = self.SuperController
        to_send = {}
        to_send[TYPE] = 'StartBackupHanlder'
        to_send[DOMAINID] = taskId[DOMAINID]
        to_send[REST_TASK_ID] = taskId['id']

        send_message = json.dumps(to_send)
        command = SC._to_commad(send_message)

        SC.send_no_return_command(command)


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


    def assign_mpls_lable(self, sectorial_path, label_list):

        for key in sectorial_path:
            i = sectorial_path[key]
            labels = []
            sectorial_swtich_lists = i['list']
            length = len(sectorial_swtich_lists)
            for j in range(0, length-1):
                label = label_list[0]
                label_list.remove(label)
                labels.append(label)
            i['labels'] = labels

        return sectorial_path

    def get_sectorial_path(self, pathlist, dict):
        length = len(pathlist)

        path_dict = {}
        for node in pathlist:
            if node in dict:
                node_domainId = dict[node]
                part_path = path_dict.setdefault(node_domainId, {})
                part_path_list = part_path.setdefault('list', [])
                if node not in part_path_list:
                    part_path_list.append(node)
            else:
                break

        for i in path_dict:
            part = path_dict[i]
            first = part['list'][0]
            last = part['list'][-1]
            first_index = pathlist.index(first)
            if first_index  == 0:
                part['pre'] = 0
            else:
                part['pre'] = pathlist[first_index - 1]

            last_index = pathlist.index(last)
            if last_index == length - 1:
                part['post'] = 0
            else:
                part['post'] = pathlist[last_index + 1]

        return path_dict


    def get_backward_info(self, path_info):

        backward_info = path_info
        backward_info['complete_path'].reverse()
        backward_info[REST_SRC_SWITCH], backward_info[REST_DST_SWITCH] \
            = backward_info[REST_DST_SWITCH], backward_info[REST_SRC_SWITCH]
        backward_info[REST_SRC_IP] , backward_info[REST_DST_IP]  \
            = backward_info[REST_DST_IP], backward_info[REST_SRC_IP]
        for i in backward_info['sectorial_path']:
            backward_info['sectorial_path']['labels'].reverse()
            backward_info['sectorial_path']['list'].reverse()
            backward_info['sectorial_path']['pre'], backward_info['sectorial_path']['post']=\
                backward_info['sectorial_path']['post'], backward_info['sectorial_path']['pre']

        return backward_info

    def get_topo_for_backup(self, topo, edges):
        tempTopo = copy.deepcopy(topo)
        tempTopo.remove_edges_from(edges)

        return tempTopo

    def get_edges_from_switchlist(self, switchlist):
        length = len(switchlist)
        edges = []
        for i in range(length - 1):
            edges.append((switchlist[i], switchlist[i+1]))

        return edges
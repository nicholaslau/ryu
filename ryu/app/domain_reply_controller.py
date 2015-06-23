__author__ = 'root'

from ryu.cfg import CONF
import logging
import time
import json
TYPE = 'Type'
DOMAINID = 'DomainID'

REST_SRC_SWITCH = 'src_switch'
REST_DST_SWITCH = 'dst_switch'
REST_MID_SWITCH = 'mid_switch'
REST_TASK_ID =  'task_id'
REST_BANDWITH = 'bandwidth'
REST_DURATION = 'duration'
REST_DST_IP = 'nw_dst'
REST_SRC_IP = 'nw_src'


REST_PORT_NAME = 'port_name'
REST_QUEUE_TYPE = 'type'
REST_QUEUE_MAX_RATE = 'max_rate'
REST_QUEUE_MIN_RATE = 'min_rate'
REST_QUEUE_ID = 'qos_id'
REST_PARENT_MAX_RATE = 'parent_max_queue'

QUEUEINUSETYPE = 'inUse'
QUEUEINUSE = 'yes'
QUEUENOTINUSE = 'no'
MAXRATE = 'max_rate'
MINRATE = 'min_rate'

class DomainReplyController(object):

    def __init__(self):

        self.name = 'DomainReplyController'

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

        self.logger.info("I am reply controller")

    def TaskDelete(self, jsonMsg, domaincontroller):

        assert jsonMsg[TYPE] == 'TaskDelete'


    def TaskAssign(self, jsonMsg, domaincontroller):
        DC = domaincontroller

        assert jsonMsg[TYPE] == 'TaskAssign'
        task_id = jsonMsg[REST_TASK_ID]
        task_info = DC.task_info.setdefault(task_id, {})
        task_type = jsonMsg['main']
        bandwidth = jsonMsg[REST_BANDWITH]
        srcIp = jsonMsg[REST_SRC_IP]
        dstIp = jsonMsg[REST_DST_IP]
        duration = jsonMsg[REST_DURATION]
        pathInfo = jsonMsg['path']
        switch_list = pathInfo['list']
        preswitch = pathInfo['pre']
        postswitch = pathInfo['post']
        max_rate = bandwidth['peak']
        min_rate = bandwidth['guranteed']
        lables = pathInfo['labels']

        match_info = task_info.setdefault('match', {})
        main_match_info = match_info.setdefault('main', {})
        backup_match_info = match_info.setdefault('backup', {})


        port_pair = DC.port_pair
        device_info = DC.device_info

        length = len(switch_list)
        if length > 1:

            for i in switch_list:
                index = switch_list.index(i)
                if index == 0:
                    next_switch = switch_list[index + 1]
                    for pair in port_pair:
                        if pair[0] == i and port_pair[pair][0] == next_switch:
                            out_port = pair
                            break
                    out_port_no = out_port[1]
                    out_port_name = device_info[i]['ports'][out_port_no]['name']

                    queueId = DC.get_queueid(i, out_port_no, max_rate, min_rate)
                    if queueId == -1:
                        self.logger.info("No more queue")
                        return

                    rest = self.make_queue_rest(out_port_name, max_rate, min_rate, queueId)

                    ovs_bridge = DC.QOS_dict[i]
                    status, msg = ovs_bridge.set_queue(rest)
                    if status:
                        self.logger.debug(msg)
                        return

                    push_lable = lables[index]
                    if task_type == 'main':
                        match = DC.push_mpls_flow(i, push_lable, srcIp, dstIp, out_port_no, queueId, duration)
                    elif task_type == 'backup' and preswitch == 0:
                        match, mod = DC.get_flow_mod(i, push_lable, srcIp, dstIp, out_port_no, queueId, duration)
                        task_backup_mod = DC.task_back.setdefault(task_id, {})
                        task_backup_mod['mod'] = mod

                elif index == len(switch_list) - 1:
                    next_switch = postswitch
                    if next_switch != 0:
                        for pair in port_pair:
                            if pair[0] == i and port_pair[pair][0] == next_switch:
                                out_port = pair
                                break
                        out_port_no = out_port[1]
                    else:
                        out_port_no = 2

                    out_port_name = device_info[i]['ports'][out_port_no]['name']
                    queueId = DC.get_queueid(i, out_port_no, max_rate, min_rate)
                    if queueId == -1:
                        self.logger.info("no more queue")
                        return

                    rest = self.make_queue_rest(out_port_name, max_rate, min_rate, queueId)

                    ovs_bridge = DC.QOS_dict[i]
                    status, msg = ovs_bridge.set_queue(rest)
                    if status:
                        self.logger.debug(msg)
                        return
                    pop_lable = lables[-1]
                    match = DC.pop_mpls_flow(i, pop_lable, out_port_no, 0, duration)
                else:
                    next_switch = switch_list[index + 1]
                    for pair in port_pair:
                        if pair[0] == i and port_pair[pair][0] == next_switch:
                            out_port = pair
                            break
                    out_port_no = out_port[1]
                    out_port_name = device_info[i]['ports'][out_port_no]['name']
                    queueId = DC.get_queueid(i, out_port_no, max_rate, min_rate)
                    if queueId == -1:
                        self.logger.info('no more queue')
                        return
                    rest = self.make_queue_rest(out_port_name, max_rate, min_rate, queueId)
                    ovs_bridge = DC.QOS_dict[i]
                    status, msg = ovs_bridge.set_queue(rest)
                    if status:
                        self.logger.debug(msg)
                        return
                    pop_lable = lables[index - 1]
                    push_lable = lables[index]

                    match = DC.swap_mpls_flow(i, pop_lable, push_lable, out_port_no, queueId, duration)
            # match_info[i] = {'match': match, 'port_no': out_port_no, 'queue_id': queueId}
                if task_type == 'main':
                    info =main_match_info.setdefault(i, {})
                elif task_type == 'backup':
                    info = backup_match_info.setdefault(i, {})
                info['match'] = match
                info['port_no'] = out_port_no
                info['queue_id'] = queueId

                DC.queue_info[i][out_port_no][queueId][QUEUEINUSETYPE] = QUEUEINUSE

            # match_info_per_switch = match_info.setdefault(i, {})
            # match_info_per_switch['match'] = match
            # match_info_per_switch['port_no'] = out_port_no
            # match_info_per_switch['queue_id'] = queueId
        elif length == 1:
            i = switch_list[0]
            next_switch = postswitch
            if next_switch != 0:
                for pair in port_pair:
                    if pair[0] == i and port_pair[pair][0] == next_switch:
                        out_port = pair
                        break
                out_port_no = out_port[1]
            else:
                out_port_no = 2

            out_port_no = out_port[1]
            out_port_name = device_info[i]['ports'][out_port_no]['name']

            queueId = DC.get_queueid(i, out_port_no, max_rate, min_rate)
            if queueId == -1:
                self.logger.info("No more queue")
                return

            rest = self.make_queue_rest(out_port_name, max_rate, min_rate, queueId)

            ovs_bridge = DC.QOS_dict[i]
            status, msg = ovs_bridge.set_queue(rest)
            if status:
                self.logger.debug(msg)
                return

            if task_type == 'main':
                match = DC.no_mpls_hanlder(i, srcIp, dstIp, out_port_no, queueId, duration)
            elif task_type == 'backup' and preswitch == 0:
                match, mod = DC.no_mpls_get_mod(i, srcIp, dstIp, out_port_no, queueId, duration)
                task_backup_mod = DC.task_back.setdefault(task_id, {})
                task_backup_mod['mod'] = mod
            if task_type == 'main':
                info =main_match_info.setdefault(i, {})
            elif task_type == 'backup':
                info = backup_match_info.setdefault(i, {})
            info['match'] = match
            info['port_no'] = out_port_no
            info['queue_id'] = queueId

            DC.queue_info[i][out_port_no][queueId][QUEUEINUSETYPE] = QUEUEINUSE




        to_send = {}
        to_send[DOMAINID] = DC.domain_id
        to_send[REST_TASK_ID] = task_id
        to_send[TYPE] = 'TaskAssignReply'
        to_send['main'] = task_type


        send_message = json.dumps(to_send)
        commamd = DC._to_commad(send_message)
        self.logger.info('send task reply')
        DC.send_no_return_command(commamd)


    def StartBackupHanlder(self, jsonMsg, domaincontroller):

        DC = domaincontroller

        assert jsonMsg[TYPE] == 'StartBackupHanlder'
        assert jsonMsg[DOMAINID] == DC.domain_id
        task_id = jsonMsg[REST_TASK_ID]
        src_switch = jsonMsg[REST_SRC_SWITCH]
        task_back = DC.task_back[task_id]
        mod = task_back['mod']
        datapath = mod.datapath

        datapath.send_msg(mod)

        queue_info = DC.queue_info

        task_info_main = DC.task_info[task_id]['match']['main']
        switch_list = task_info_main.keys()
        for i in switch_list:
            info = task_info_main[i]
            port_no = info['port_no']
            queue_id = info['queue_id']
            queue_info[i][port_no][queue_id][QUEUEINUSETYPE] = QUEUENOTINUSE
            if i is not src_switch:
                datapath = DC.dps[i]
                match = info['match']
                new_match = datapath.ofproto_parser.OFPMatch()
                new_match._fields2 = match._fields2
                DC.remove_flow(datapath, new_match)

        task_info_main = DC.task_info[task_id]['match']['backup']
        DC.task_info[task_id]['backup'] = {}














    def make_queue_rest(self, port_name, max_rate, min_rate, queue_id, parent_max_rate=10000000):
        rest = {}
        rest[REST_PORT_NAME] = port_name
        rest[REST_PARENT_MAX_RATE] = parent_max_rate
        rest[REST_QUEUE_MAX_RATE] = max_rate
        rest[REST_QUEUE_MIN_RATE] = min_rate
        rest[REST_QUEUE_ID] = queue_id

        return rest

    def KeepAlive(self, jsonMsg, domaincontroller):

        assert jsonMsg[TYPE] == 'KeepAlive'

        domainId = jsonMsg[DOMAINID]

        if domainId is not domaincontroller.domain_id:
            self.logger.debug("receive a keepalive in wrong way")
            return

        domaincontroller.super_last_echo = time.time()











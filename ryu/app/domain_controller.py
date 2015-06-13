__author__ = 'root'

import os
from operator import attrgetter
import time

import logging
# from ryu import cfg
import networkx as nx
import json

from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication
from ryu.app.wsgi import route

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import DEAD_DISPATCHER
from ryu.controller.handler import CONFIG_DISPATCHER

from ryu.ofproto import ofproto_v1_0, ofproto_v1_3
from ryu.ofproto import ofproto_v1_0_parser, ofproto_v1_3_parser

from ryu.topology import switches
from ryu.topology import event

from ryu.lib import hub
from ryu.lib.packet import packet, ethernet

from ryu.app.domain_reply_controller import DomainReplyController

domain_instance = 'domain_controller_api'
domain_url_base = '/domain'
super_url_no_reutrn = '/super/noreturn'
test_instance = 'test_instance'
domain_reply_controller_instance = 'domain_reply_controller_instance'

DOMAINID = 'domain_id'
DPID = 'dpid'
RETURN = 'return'
TYPE = 'type'
LINKSRC = 'link_src'
LINKDST = 'link_dst'
SRCPORT = 'src_port'
DSTPORT = 'dst_port'
COLLECTTIME = 'ctime'
PORTSTATUS = 'portStatusInfo'

LOG = logging.getLogger(__name__)


# CONF = cfg.CONF
#
# CONF.register_cli_opts([
#     cfg.BoolOpt('observe-links', default=True,
#                 help='observe link discovery events.'),
#     cfg.IntOpt('domain_id', default=None,
#                help='the identifle of this domain controller'),
#     cfg.StrOpt('super_wsgi_IP', default='x.x.x.x',
#               help='Ip address of super\'s wsgi'),
#     cfg.IntOpt('super_wsgi_port', default=8080,
#                help='port no of super\'s wsgi'),
#     cfg.BoolOpt('super_exist', default=False,
#                 help='to notification whether super controller exists'),
#     cfg.BoolOpt('moniter_thread_flag', default=False,
#                 help='start a thread to get ports status')
# ])


class DomainController(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication,
                 'switches': switches.Switches}

    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION, ofproto_v1_3.OFP_VERSION]


    def __init__(self, *args, **kwargs):
        super(DomainController, self).__init__(*args, **kwargs)
        self.domain_id = self.CONF.domain_id
        self.local_topo = nx.DiGraph()
        self.switches_app = kwargs['switches']
        self.local_links = []
        self.dps = {}
        self.switch = []
        self.port_pair = {}       # map the ports between switches
        self.sleep = 2
        self.state_len = 3
        #########
        self.port_stats = {}
        self.port_speed = {}
        self.flow_stats = {}
        self.flow_speed = {}
        #########

        self.link_endpoint = {}

        self.device_info={}

        self.super_exist = self.CONF.super_exist
        if self.super_exist:
            self.super_wsgi_ip = self.CONF.super_wsgi_ip
            self.super_wsgi_port = self.CONF.super_wsgi_port

        self.super_last_echo = time.time()

        self.monitor_thread_flag = self.CONF.monitor_thread_flag
        if self.monitor_thread_flag:
            self.monitor_thread = hub.spawn(self._monitor)
            self.last_collect= {}

        wsgi = kwargs['wsgi']
        wsgi.register(DomainWsgiController, {domain_instance: self, test_instance: test(),
                                             domain_reply_controller_instance: DomainReplyController()})


    # @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg

        in_port = msg.match['in_port']
        if in_port is 4:
            pkt = packet.Packet(msg.data)
            eth = pkt.get_protocols(ethernet.ethernet)
            self.logger.info(str(eth))

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        self.logger.debug('switch features ev %s', msg)

        dpid = datapath.id

        device_info = self.device_info.setdefault(dpid,{})
        device_info['dpid'] = dpid
        device_info['version'] = msg.version
        device_info['capabilities']= msg.capabilities
        device_info['n_buffers'] = msg.n_buffers
        device_info['n_tables'] = msg.n_tables
        device_info['auxiliary_id'] = msg.auxiliary_id

        if datapath.ofproto.OFP_VERSION < 0x04:
            device_info['ports'] = msg.ports
        else:
            device_info['ports'] = {}

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, CONFIG_DISPATCHER)
    def multipart_reply_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        dpid = datapath.id
        device_info = self.device_info[dpid]
        port_info = device_info.setdefault('port',{})
        for port in msg.body:
            port_no = port.port_no
            if port_no > ofproto_v1_3.OFPP_LOCAL:
                device_info['name'] = port.name
            each_port_info = port_info.setdefault(port_no, {})
            each_port_info['port_no'] = port_no
            each_port_info['hw_addr'] = port.hw_addr
            each_port_info['name'] = port.name
            each_port_info['config'] = port.config
            each_port_info['state'] = port.state
            each_port_info['curr'] = port.curr
            each_port_info['advertiesd'] = port.advertised
            each_port_info['supported'] = port.supported
            each_port_info['peer'] = port.peer
            each_port_info['cur_speed'] = port.curr_speed
            each_port_info['max_speed'] = port.max_speed

    def add_flow(self, datapath, match, actions, priority=None, buffer_id=None):

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        if priority == None:
            priority = ofproto.OFP_DEFAULT_PRIORITY

        if ofproto.OFP_VERSION == ofproto_v1_0.OFP_VERSION:
            mod = parser.OFPFlowMod(datapath=datapath, match=match, cookie=0, priority=priority,
                                    command=ofproto.OFPFC_ADD, flags=ofproto.OFPFF_SEND_FLOW_REM,
                                    actions=actions)
        elif ofproto.OFP_VERSION == ofproto_v1_3.OFP_VERSION:
            inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)

        datapath.send_msg(mod)


    def remove_flow(self, datapath, match):

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        if ofproto.OFP_VERSION == ofproto_v1_0.OFP_VERSION:
            mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_DELETE,
                                    out_port=ofproto.OFPP_ANY, match=match)
        elif ofproto.OFP_VERSION == ofproto_v1_3.OFP_VERSION:
            mod = parser.OFPFlowMod(datapath=datapath, command=ofproto.OFPFC_DELETE,
                                    out_group=ofproto.OFPG_ANY,out_port=ofproto.OFPP_ANY,
                                    match=match)

        datapath.send_msg(mod)

    @set_ev_cls(event.EventSwitchEnter, [MAIN_DISPATCHER, CONFIG_DISPATCHER])
    def switch_enter_handler(self, ev):
        switch = ev.switch
        dp = switch.dp
        dpid = dp.id
        if dpid not in self.switch:
            self.switch.append(dpid)
            self.dps[dpid] = dp

        if dpid in self.local_topo.nodes():
            self.logger.debug("Switch %016x already in topo", dpid)
            self.local_topo.remove_node(dpid)

        self.local_topo.add_node(dpid)
        self.logger.info("Switch %016x enter in local topo", dpid)

        if self.super_exist:
            to_send = {}
            to_send[DOMAINID] = self.domain_id
            to_send[TYPE] = 'switch_enter'
            to_send[DPID] = dpid
            send_message = json.dumps(to_send)
            command = self._to_commad(send_message)
            self.send_no_return_command(command)
            self.logger.info("To super controller: Switch Enter->dpid %16x", dpid)

    @set_ev_cls(event.EventSwitchLeave, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def switch_leave_handler(self, ev):
        switch = ev.switch
        dp = switch.dp
        dpid = dp.id

        if dpid in self.switch:
            self.switch.remove(dpid)
        if dpid in self.dps:
            del self.dps[dpid]

        if dpid not in self.local_topo.nodes():
            self.logger.debug("Swtich %016x not in local topo", dpid)
        else:
            self.local_topo.remove_node(dpid)

        if self.super_exist:
            to_send = {}
            to_send[DOMAINID] = self.domain_id
            to_send[TYPE] = 'switch_leave'
            to_send[DPID] = dpid
            send_message = json.dump(to_send)

            self.send_no_return_command(send_message)
            self.logger.info("To super controller: Switch Leave->dpid %016x", dpid)

    @set_ev_cls(event.EventLinkAdd, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def link_add_handler(self, ev):
        link = ev.link
        src = link.src
        dst = link.dst
        src_switch = src.dpid
        dst_switch = dst.dpid
        src_port = src.port_no
        dst_port = dst.port_no

        src_point = (src_switch, src_port)
        if src_point in self.port_pair:
            del self.port_pair[src_port]

        dst_point = (dst_switch, dst_port)
        self.port_pair[src_point] = dst_point

        edge = (src_switch, dst_switch)
        if dst_point not in self.link_endpoint:
            self.link_endpoint[dst_point] = edge

        if edge not in self.local_topo.edges():
            self.local_topo.add_edge(src_switch, dst_switch)
            self.logger.info("Lind add: src %16x port_no %8x-> dst %16x %8x" % (src_switch, src_port, dst_switch, dst_port))
            if self.super_exist:
                send_message = self.make_link_message(True, src_switch, src_port, dst_switch, dst_port)
                command = self._to_commad(send_message)
                self.send_no_return_command(command)
                self.logger.info("TO Super controller.Lind add: src %16x port_no %8x-> dst %16x %8x"
                                 % (src_switch, src_port, dst_switch, dst_port))

    @set_ev_cls(event.EventLinkDelete, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def link_del_handler(self, ev):
        self.logger.debug("EventLinkDelete")
        link = ev.link
        src = link.src
        dst = link.dst
        src_switch = src.dpid
        dst_switch = dst.dpid
        src_port = src.port_no
        dst_port = dst.port_no

        src_point = (src_switch, src_port)
        if src_point in self.port_pair:
            del self.port_pair[src_point]

        edge = (src_switch, dst_switch)
        if edge in self.local_topo.edges():
            self.local_topo.remove_edge(src_switch, dst_switch)
            self.logger.info("Lind delete: src %16x port_no %8x-> dst %16x %8x"
                            % (src_switch, src_port, dst_switch, dst_port))
            if self.super_exist:
                send_message = self.make_link_message(False, src_switch, src_port, dst_switch, dst_port)
                self.send_no_return_command(send_message)
                self.logger.info("TO Super controller.Lind del: src %16x port_no %8x-> dst %16x %8x"
                                 % (src_switch, src_port, dst_switch, dst_port))

    def _monitor(self):
        while True:
            # for dp in self.dps.values:
            for k in self.dps.keys():
                # self._request_stats(dp)
                self._request_stats(self.dps[k])
            hub.sleep(self.sleep)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats_reply_handler(self, ev):
        body = ev.msg.body
        self.logger.info('datapath         in_port  eth_dst  out_port packets  bytes    ')
        self.logger.info('---------------- -------- -------- -------- -------- -------- ')
        for stat in sorted([flow for flow in body if flow.priority == 1],
                           key=lambda flow: (flow.match['in_port'],
                                             flow.match['eth_dst'])):
            self.logger.info('%016x %8x %17s %8x %8d %8d',
                             ev.msg.datapath.id, stat.match['in_port'],
                             stat.match['eth_dst'], stat.instructions[0].actions[0].port,
                             stat.packet_count, stat.byte_count)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        body = ev.msg.body
        dpid = ev.msg.datapath.id
        port_stats = {}
        self.logger.info('datapath         port     rx-pkts  rx-bytes'
                         ' rx-error tx-pkts  tx-bytes tx-error ')
        self.logger.info('---------------- -------- -------- --------'
                         ' -------- -------- -------- -------- ')
        # port_stats_message = ""
        last_switch_collet = self.last_collect.setdefault(dpid, {})
        last_col_time = last_switch_collet.setdefault("last_time", 0)
        time_now = time.time()
        time_gap = time_now - last_col_time
        last_switch_collet["last_time"] = time_now
        last_port_status = last_switch_collet.setdefault("port_stauts",{})
        edges = []
        for stat in sorted(body, key=attrgetter('port_no')):
            if stat.port_no is not ofproto_v1_3.OFPP_LOCAL:
                self.logger.info('%016x %8x %8d %8d %8d %8d %8d %8d',
                                ev.msg.datapath.id, stat.port_no,
                                stat.rx_packets, stat.rx_bytes, stat.rx_errors,
                                stat.tx_packets, stat.tx_bytes, stat.tx_errors)
                # port_stats[str(stat.port_no)] = stat.rx_bytes
                # port_stats_message += str(stat.port_no) + ':' + str(stat.tx_bytes) + '|'
                endpoint = (dpid, stat.port_no)
                if endpoint in self.link_endpoint:
                    print "jj"
                    last_status = last_port_status.setdefault(stat.port_no, 0)
                    last_port_status[stat.port_no] = stat.rx_bytes
                    port_speed = int((stat.rx_bytes - last_status) / time_gap)
                    edge = self.link_endpoint[endpoint]
                    edges.append((edge[0], edge[1],{'weight':port_speed}))
        self.local_topo.add_edges_from(edges)

        self.logger.info(self.local_topo.edges())


        if self.super_exist:
            send_message = self._make_port_status_message(dpid, body)
            command = self._to_commad(send_message, returnType=False)
            print command
            self.send_no_return_command(command)

    def _make_port_status_message(self, dpid, body):

        to_send = {}
        to_send[DOMAINID] = self.domain_id
        to_send[TYPE] = 'PortStatus'
        to_send[DPID] = dpid
        info = []
        for stat in sorted(body, key=attrgetter('port_no')):
            temp ={}
            temp['port_no'] = stat.port_no
            temp['rx_packets'] = stat.rx_packets
            temp['tx_packets'] = stat.tx_packets
            temp['rx_bytes'] = stat.rx_bytes
            temp['tx_bytes'] = stat.tx_bytes
            info.append(temp)
        to_send['info'] = info

        return json.dumps(to_send)


    def _request_stats(self, datapath):
        self.logger.info('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # req = parser.OFPFlowStatsRequest(datapath)
        # datapath.send_msg(req)
        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
        datapath.send_msg(req)

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
        # print command
        to_print = os.popen(command).read()
        print "to_print:", to_print

    def make_link_message(self, addel, src_id, src_port, dst_id, dst_port):
        to_send = {}
        to_send[DOMAINID] = self.domain_id
        if addel:
            to_send[TYPE] = 'link_add'
        else:
            to_send[TYPE] = 'link_del'
        to_send[LINKSRC] = src_id
        to_send[LINKDST] = dst_id
        to_send[SRCPORT] = src_port
        to_send[DSTPORT] = dst_port

        send_message = json.dumps(to_send)
        return send_message


class DomainWsgiController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(DomainWsgiController, self).__init__(req, link, data, **config)
        # self.instance = DomainWsgiController()
        self.name = 'I am Domain Wsgi Controller'

        self.test = data[test_instance]
        self.DomainController = data[domain_instance]
        self.domian_reply_controller = data[domain_reply_controller_instance]

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

    @route('domain', domain_url_base + '/noreturned', methods=['PUT'], requirements=None)
    def noreturned_command_hanlder(self, req):
        msgbody = eval(req.body)
        type = msgbody[TYPE]
        try:
            func = getattr(self.domian_reply_controller, type)
        except:
            self.logger.fatal("no such type")

    @route('domain', domain_url_base + '/returned', methods=['PUT'], requirements=None)
    def returned_command_handler(self, req):
        msgbody = eval(req.body)
        type = msgbody[TYPE]

        func = getattr(self.domian_reply_controller, type)

        return_msg = func(msgbody, self.DomainController)
        return Response(status=400, body=return_msg)

    @route('domain', '/super/noreturn', methods=['PUT'], requirements=None)
    def test(self, req):
        print eval(req.body)


class test(object):

    def __init__(self):
        pass
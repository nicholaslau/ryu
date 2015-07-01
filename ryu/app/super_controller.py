__author__ = 'root'

import networkx as nx
import json
import logging
import time
import os

from ryu.app.net import Task
from ryu.app.topo import TopoInfo
from ryu.app.labelsManager import MplsLabelsPool
from ryu.app.domainInfo import DomainInfo, SwitchInfo

from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication
from ryu.app.wsgi import route
from ryu.base import app_manager
from ryu.app.net import TASK_DICT, delTask, assertTaskInDict, getTask, registerTask

from ryu.app.super_reply_controller import SuperReplyController


SUPERCONTROLLER = 'SuperController'
SUPERREPLYCONTROLLER = 'SuperReplyController'

SUPERBASEURL = '/super'
DOMAINURLNORETURN= '/domain/noreturn'
DOMAINURLRETURN = '/domain/return'


DOMAINID = 'domainId'
TYPE = 'type'
PATHTYPE = 'pathType'
TASK_ID = 'taskId'
SRC_IP = 'srcIp'
DST_IP = 'dstIp'
SRC_SWITCH = 'srcSwitch'
DST_SWITCH = 'dstSwitch'
BANDWIDTH = 'bandwidth'
PARTPATH = 'path'
LABELS = 'labels'
DOMAINWSGIIP = 'domainWsgiIp'
DOMAINWSGIPORT = 'domainWsgiPort'



class SuperController(app_manager.RyuApp):

    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(SuperController, self).__init__(*args, **kwargs)

        self.wsgiIp = None
        self.wsgiPort = None
        ##################################################
        self.topo = TopoInfo()
        self.trafficBalance = True
        ##################################################
        self.LabelsPool = MplsLabelsPool()
        self.LabelsPool.initPool()
        ##################################################
        self.domains = {}

        ###################################################
        wsgi = kwargs['wsgi']
        data = {}
        data[SUPERCONTROLLER] = self
        data[SUPERREPLYCONTROLLER] = SuperReplyController()
        wsgi.register(SuperWsgiController, data)


    def startBackupHandler(self, taskId):
        taskInstance = getTask(taskId)
        backupPathDomains = taskInstance.getBackupCrossDomains()
        if not backupPathDomains:
            self.logger.info('NO Backup Path for this Task')
            return

        mainPathDomains = taskInstance.getMainCrossDomains()

        handlerDomains = self._add_diff_from_list(backupPathDomains, mainPathDomains)

        for domainId in handlerDomains:
            self.sendStartBackupPathMsg(domainId, taskId)

        taskInstance.changeBackupToMain()

    def _add_diff_from_list(self, list1, list2):
        list_ = []
        for i in list1:
            list_.append(i)

        for j in list2:
            if j not in list_:
                list_.append(j)

        return list_

    def sendStartBackupPathMsg(self, domainId, taskId):
        send_message = self._make_start_backup_msg(domainId, taskId)
        command = self._to_commad(send_message)
        print "start backup: ", command
        self.send_no_return_command(command)

    def _make_start_backup_msg(self, domainId, taskId):

        to_send = {}
        to_send[TYPE] = 'startBackup'
        to_send[DOMAINID] = domainId
        to_send[TASK_ID] = taskId

        send_message = json.dumps(to_send)
        return send_message


    ##dai xiu gai
    def setNewBackupPath(self, taskId):
        taskInstance = getTask(taskId)
        completePathMain = taskInstance.getMainCompletePath()
        assert len(completePathMain) > 1  # to make sure we set a backupPath for a task having a mainPath
        mainEdges = taskInstance.getMainEdges()
        newTopo = self.topo.getNewTopoExceptSE(mainEdges)

        srcSwitch = taskInstance.getSrcSwtich()
        dstSwtich = taskInstance.getDstSwitch()
        srcIp = taskInstance.getSrcIp()
        dstIp = taskInstance.getDstIp()
        if self.trafficBalance:
            newCompletePathBackup = newTopo.getWeightPath(srcSwitch, dstSwtich)
        else:
            newCompletePathBackup = newTopo.getShortestPath(srcSwitch, dstSwitch)

        if not  newCompletePathBackup:
            self.logger.warning("can not assign a new backupPath for this task ")
            return

        taskInstance.setBackupCompletePath(newCompletePathBackup)
        nodeToDomain = self.topo.nodeToDomainId
        newBackupSectorialPath = taskInstance.getBackupSectorialPath(nodeToDomain)

        newAllBackupPathMpls = self.LabelsPool.getLabels(len(newCompletePathBackup))
        noUseLabels = taskInstance.assignBackuPathMpls(newAllBackupPathMpls)
        self.LabelsPool.recycleLabels(noUseLabels)

        for i in newBackupSectorialPath:
            send_message = taskInstance.makeDoaminTaskAssign(i,  type= 'backup')
            command = self._to_commad(send_message)
            print 'newbackup: ', command
            self.send_no_return_command(command)
            taskInstance.addBackupUnconfirmDomain(i)






    #############

    def send_no_return_command(self, command):
        try:
            os.popen2(command)
        except:
            self.logger.debug('command exceute fail.Fail Command: %s' % command)
            return

    def _to_commad(self, send_message, returnType=False):

        message = eval(send_message)
        domainId = message.get(DOMAINID)
        domainInstance = self.domains.get(domainId)
        domainWsgiIp = domainInstance.getWsgiIp()
        domainWsgiPort = domainInstance.getWsgiPort()

        command = 'curl -X '
        if returnType:
            command += 'GET -d \''
        else:
            command += 'PUT -d \''
        command += send_message
        command += '\' http://'
        command += domainWsgiIp
        command += ':'
        command += str(domainWsgiPort)
        if not returnType:
            command += DOMAINURLNORETURN
        else:
            command += DOMAINURLRETURN

        command += ' 2> /dev/null'

        return command


class SuperWsgiController(ControllerBase):

    def __init__(self, req, link, data, **config):
        super(SuperWsgiController, self).__init__(req, link, data, **config)
        self.name = 'SuperWsgiController'
        self.SuperController = data[SUPERCONTROLLER]
        self.SuperReplyController = data[SUPERREPLYCONTROLLER]

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

    @route('super', SUPERBASEURL + '/noreturn', methods=['PUT'], requirements=None)
    def noreturned_command_handler(self, req):
        msgbody = eval(req.body)
        assert TYPE in msgbody
        type = msgbody.get(TYPE, None)
        if not type:
            self.logger.fatal("Not type in msgbody")
            return

        try:
            func = getattr(self.SuperReplyController, type)
        except:
            self.logger.fatal('can not find handler')
            return

        func(msgbody, self.SuperController)

    @route('super', SUPERBASEURL + '/return', methods=['PUT'], requirements=None)
    def returned_command_handler(self, req):
        msgbody = eval(req.body)
        assert TYPE in msgbody
        type = msgbody.get(TYPE, None)
        if not type:
            self.logger.fatal("Not type in msgbody")
            return

        try:
            func = getattr(self.super_reply_controller, type)
        except:
            self.logger.error('Can not find handler')
            return

        func(msgbody, self.SuperController)


    @route('super', SUPERBASEURL + '/task/assign', methods=['PUT'], requirements=None)
    def taskAssign(self, req):

        SC = self.SuperController
        body = req.body
        rest = eval(body)
        taskId = rest[TASK_ID]

        if not taskId:
            return Response(status=200, body="Input a task Id\n")
        if assertTaskInDict(taskId):
            taskInstance = getTask(taskId)
        else:
            taskInstance = Task(taskId)

        if taskInstance.isEstablished():

            return Response(status=200, body="taskId duplicated!\n")

        srcSwitch = rest[SRC_SWITCH]
        dstSwitch = rest[DST_SWITCH]
        bandwith = rest[BANDWIDTH]
        # duration = rest[]
        dstIp = rest[DST_IP]
        srcIp = rest[SRC_IP]
        taskInstance.taskSetFields(srcSwitch=srcSwitch, dstSwitch=dstSwitch, srcIp=srcIp, dstIp=dstIp, bandwidth=bandwith)


        if SC.trafficBalance:
            completePathMain = SC.topo.getWeightPath(srcSwitch, dstSwitch)
            if not completePathMain:
                self.logger.warning("no main path between switch %d and %d" % (srcSwitch, dstSwitch))
                return Response(status=200, body="no main path between switch %d and %d\n" % (srcSwitch, dstSwitch))

            taskInstance.setMainCompletePath(completePathMain)
            mainEdges = taskInstance.getMainEdges()
            newTopo = SC.topo.getNewTopoExceptSE(mainEdges)
            completePathBackup = newTopo.getWeightPath(srcSwitch, dstSwitch)
            if not completePathBackup:
                self.logger.warning("no backup path between switch %d and %d" % (srcSwitch, dstSwitch))

            taskInstance.setBackupCompletePath(completePathBackup)
        else:
            completePathMath = SC.topo.getShortestPath(srcSwitch, dstSwitch)
            if not completePathMath:
                self.logger.warning("no main path between switch %d and %d" % (srcSwitch, dstSwitch))
                return Response(status=200, body="no main path between switch %d and %d\n" % (srcSwitch, dstSwitch))

            taskInstance.setMainCompletePath(completePathMath)
            mainEdges = taskInstance.getMainEdges()
            newTopo = SC.topo.getNewTopoExceptSE(mainEdges)
            completePathBackup = newTopo.getShorestPath(srcSwitch, dstSwitch)
            if not completePathBackup:
                self.logger.warning("no backup path between switch %d and %d" % (srcSwitch, dstSwitch))

            taskInstance.setBackupCompletePath(completePathBackup)

        nodeToDomian = SC.topo.nodeToDomainId
        mainSectorialPath = taskInstance.getMainSectorialPath(nodeToDomian)
        backupSectorialPath = taskInstance.getBackupSectorialPath(nodeToDomian)
        # print mainSectorialPath
        # print backupSectorialPath


        allMainPathMpls = SC.LabelsPool.getLabels(len(completePathMain))
        noUseLabels = taskInstance.assignMainPathMpls(allMainPathMpls)
        SC.LabelsPool.recycleLabels(noUseLabels)

        allBackupPathMpls = SC.LabelsPool.getLabels(len(completePathBackup))
        noUseLabels = taskInstance.assignBackuPathMpls(allBackupPathMpls)
        SC.LabelsPool.recycleLabels(noUseLabels)

        registerTask(taskInstance)
        # print "main: ", completePathMain
        # print "backup: ", completePathBackup
        # print "nodeToDomain: ", nodeToDomian

        for i in mainSectorialPath:
            send_message = taskInstance.makeDoaminTaskAssign(i)

            command = SC._to_commad(send_message)
            print "main: ", command
            SC.send_no_return_command(command)
            taskInstance.addMainUnconfirmDomain(i)

        

        for j in backupSectorialPath:
            send_message = taskInstance.makeDoaminTaskAssign(j, type='backup')

            command = SC._to_commad(send_message)
            print "backup: ",command
            SC.send_no_return_command(command)
            taskInstance.addBackupUnconfirmDomain(j)


    #####  dai  xiu gai
    @route('super', SUPERBASEURL + '/task/delete', methods=['PUT'], requirements=None)
    def taskDelete(self, req):
        SC = self.SuperController

        rest = eval(req)

        try:
            taskId = rest[TASK_ID]
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



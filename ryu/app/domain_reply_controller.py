__author__ = 'root'

from ryu.cfg import CONF
import logging
import  time

# from ryu.app.domain_controller import DomainController

from ryu.app.domain_task import DomainTask, TaskList

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

SINGLESWITCH = 1


class DomainReplyController(object):

    def __init__(self):

        self.name = 'DomainReplyController'

        if hasattr(self.__class__, 'LOGGER_NAME'):
            self.logger = logging.getLogger(self.__class__.LOGGER_NAME)
        else:
            self.logger = logging.getLogger(self.name)

        self.logger.info("I am reply controller")

    def taskAssign(self, jsonMsg, DC):

        assert jsonMsg[TYPE] == 'taskAssign'
        # DC = DomainController
        print jsonMsg

        taskId = jsonMsg[TASK_ID]
        TASK_LIST = DC.TASK_LIST
        taskInstance = TASK_LIST.setdefault(taskId, DomainTask(taskId))
        # taskInstance = DomainTask(taskId)
        pathType = jsonMsg[PATHTYPE]
        srcSwitch = jsonMsg[SRC_SWITCH]
        dstSwitch = jsonMsg[DST_SWITCH]
        srcIp = jsonMsg[SRC_IP]
        dstIp = jsonMsg[DST_IP]
        bandwidth = jsonMsg[BANDWIDTH]
        pathInfo = jsonMsg[PARTPATH]
        labels = jsonMsg[LABELS]

        taskInstance.setFields(srcSwitch=srcSwitch, dstSwitch=dstSwitch, srcIp=srcIp, dstIp=dstIp,
                               bandwidth=bandwidth, path=pathInfo, labels=labels, pathType=pathType)

        preSwitch = taskInstance.getPreSwitch(pathType)
        postSwitch = taskInstance.getPostSwitch(pathType)
        switchList = taskInstance.getSwitchList(pathType)
        length =taskInstance.getSwithListLength(pathType)

        maxRate = taskInstance.getMaxRate()
        minRate = taskInstance.getMinRate()

        domainTopo = DC.topo
        DEVICEINFO = DC.deviceInfo

        if length is not SINGLESWITCH:
            for i in switchList:
                index = switchList.index(i)
                if index == 0:
                    nextSwitch = switchList[index + 1]
                    outPortNo = domainTopo.getLinkOutPort(i, nextSwitch)
                    switchInfo = DEVICEINFO[i]
                    outPortName = switchInfo.getPortName(outPortNo)

                    # queueId = DC.get_queueid(i, outPortName, maxRate, minRate)
                    # if queueId == -1:
                    #     self.logger.info("No more queue")
                    #     return
                    #
                    # rest = self.make_queue_rest(i, outPortName, maxRate, minRate, queueId)
                    #
                    # ovs_bridge = DC.QOS_dict[i]
                    # status, msg = ovs_bridge.set_queue(rest)
                    # if status:
                    #     self.logger.debug(msg)
                    #     return
                    pushLabel = labels[index]

                    # match, mod = DC.pushMplsFlow(i, pushLabel, srcIp, dstIp, outPortNo, queueId, pathType)
                    match, mod = DC.pushMplsFlow(i, pushLabel, srcIp, dstIp, outPortNo, 1, pathType)
                    if pathType == 'backup':
                        taskInstance.setBackupMod(mod)


                elif index == length - 1:
                    nextSwitch = postSwitch
                    if nextSwitch != 0:
                        outPortNo = domainTopo.getLinkOutPort(i, nextSwitch)
                    else:
                        # raise ValueError("can not find out port, I think you should input a specify port no")
                        outPortNo = 6
                    switchInfo = DEVICEINFO[i]
                    outPortName = switchInfo.getPortName(outPortNo)

                    # queueId = DC.get_queueid(i, outPortName, maxRate, minRate)
                    # if queueId == -1:
                    #     self.logger.info("No more queue")
                    #     return
                    #
                    # rest = self.make_queue_rest(i, outPortName, maxRate, minRate, queueId)
                    #
                    # ovs_bridge = DC.QOS_dict[i]
                    # status, msg = ovs_bridge.set_queue(rest)
                    # if status:
                    #     self.logger.debug(msg)
                    #     return

                    popLabel = labels[-1]
                    match, mod = DC.popMplsFlow(i, popLabel, outPortNo, 1)
                    # match, mod = DC.popMplsFlow(i, popLabel, outPortNo, queueId)

                else:
                    nextSwitch = switchList[index + 1]
                    outPortNo = domainTopo.getLinkOutPort(i, nextSwitch)
                    switchInfo = DEVICEINFO[i]
                    outPortName = switchInfo.getPortName(outPortNo)

                    # queueId = DC.get_queueid(i, outPortName, maxRate, minRate)
                    # if queueId == -1:
                    #     self.logger.info("No more queue")
                    #     return
                    #
                    # rest = self.make_queue_rest(i, outPortName, maxRate, minRate, queueId)
                    #
                    # ovs_bridge = DC.QOS_dict[i]
                    # status, msg = ovs_bridge.set_queue(rest)
                    # if status:
                    #     self.logger.debug(msg)
                    #     return

                    pushLabel = labels[index]
                    popLabel = labels[index - 1]

                    # match, mod =DC.swapMplsFlow(i, pushLabel, popLabel, outPortNo, queueId)
                    match, mod =DC.swapMplsFlow(i, pushLabel, popLabel, outPortNo, 1)

                if pathType == 'main':
                    taskInstance.addMainMatchInfo(i, match)
                elif pathType == 'backup':
                    taskInstance.addBackupMatchInfo(i, match)

        else:
            i = switchList[0]
            nextSwitch = postSwitch
            if nextSwitch != 0:
                outPortNo = domainTopo.getLinkOutPort(i, nextSwitch)
            else:
                # raise ValueError("can not find out port, I think you should input a specify port no")
                outPortNo = 6

            # switchInfo = DEVICEINFO[i]
            # outPortName = switchInfo.getPortName(outPortNo)
            #
            # queueId = DC.get_queueid(i, outPortName, maxRate, minRate)
            # if queueId == -1:
            #     self.logger.info("No more queue")
            #     return
            #
            # rest = self.make_queue_rest(i, outPortName, maxRate, minRate, queueId)
            #
            # ovs_bridge = DC.QOS_dict[i]
            # status, msg = ovs_bridge.set_queue(rest)
            # if status:
            #     self.logger.debug(msg)
            #     return

            # match, mod = DC.noMplsFlow(i, srcIp, dstIp, outPortNo, queueId, pathType)
            match, mod = DC.noMplsFlow(i, srcIp, dstIp, outPortNo, 1, pathType)
            if pathType == 'main':
                taskInstance.addMainMatchInfo(i, match)
            elif pathType == 'backup':
                taskInstance.addBackupMatchInfo(i, match)


        DC.sendTaskAssignReply(taskId, pathType)

    def startBackup(self, jsonMsg, DC):

        assert jsonMsg[TYPE] == 'startBackup'
        assert jsonMsg[DOMAINID] == DC.domainId
        taskId = jsonMsg[TASK_ID]
        taskInstance = DC.TASK_LIST[taskId]
        # taskInstance = DomainTask(taskId)

        main_ = taskInstance.mainPath
        backup_ = taskInstance.backupPath

        if main_:
            mainList = taskInstance.getSwitchList('main')
            if backup_:
                backupList = taskInstance.getSwitchList('backup')
                if mainList[0] == backupList[0]:
                    switch = backupList[0]
                    mod = taskInstance.getBackupMod()
                    datapath = DC._get_datapath(switch)
                    datapath.send_msg(mod)
                    for i in range(1, len(mainList)):
                        switch = mainList[i]
                        match = taskInstance.getMainMatchInfo(switch)
                        datapath = DC._get_datapath(switch)
                        newMatch = self._get_new_match(datapath, match)
                        DC.remove_flow(datapath, newMatch)
                    taskInstance.changeBackupToMain()
                else:
                    mod = taskInstance.getBackupMod()
                    switch =backupList[0]
                    datapath = DC._get_datapath(switch)
                    datapath.send_msg(mod)
                    for switch in mainList:
                        match = taskInstance.getMainMatchInfo(switch)
                        datapath = DC._get_datapath(switch)
                        newMatch = self._get_new_match(datapath, match)
                        DC.remove_flow(datapath, newMatch)
                    taskInstance.changeBackupToMain()
            else:
                for switch in mainList:
                    match = taskInstance.getMainMatchInfo(switch)
                    datapath = DC._get_datapath(switch)
                    newMatch = self._get_new_match(datapath, match)
                    DC.remove_flow(datapath, newMatch)

                del DC.TASK_LIST[taskId]


        elif backup_:
            mod = taskInstance.getBackupMod()
            datapath = DC._get_datapath(taskInstance.getSwitchList('backup')[0])
            datapath.send_msg(mod)
            taskInstance.changeBackupToMain()

    def _get_new_match(self, datapath, match):
        newMatch = datapath.ofproto_parser.OFPMatch()
        newMatch._fields2 = match._fields2
        return newMatch


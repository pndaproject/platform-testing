"""
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of
Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright,
international treaties, patent, and/or contract. Any use of the material herein must be in
accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.

Purpose:    Zookeeper tests

"""

import argparse
import sys
import os
import logging
import time
import math
from prettytable import PrettyTable
from pnda_plugin import PndaPlugin
from pnda_plugin import Event
from pnda_plugin import MonitorStatus
from plugins.common.zkclient import ZkClient, ZkError
from plugins.common.defcom import ZkNodesHealth, ZkNode, ZkMonitorSummary

sys.path.insert(0, '../..')

TESTBOTPLUGIN = lambda: ZookeeperBot()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TESTBOTPLUGIN")

def do_display(results_summary, zk_data, zknodes=ZkNodesHealth(-1, -1, -1, -1, -1)):
    '''
        Receive a summary tuples, and then build a display
        on the standard output as a result of the monitoring running.
        The second object is the test result from prod2cons.
    '''

    LOGGER.debug("do_display start")

    table = PrettyTable(['Zookeeper', 'Port', 'Id', 'other', 'Valid'])
    table.align['zookeeper'] = 'l'

    if zk_data and zk_data.list_zk:
        for node in zknodes.list:
            table.add_row([node.host, node.port, "", "", node.alive])

    if zk_data:
        print table.get_string(sortby='Zookeeper')
        print
        print 'List of zk:                 %s' % zk_data.list_zk
        print 'List of zk (ko):            %s' % zk_data.list_zk_ko
        print 'Number of zk nodes (ok):    %d' % zk_data.num_zk_ok
        print 'Number of zk nodes (ko):    %d' % zk_data.num_zk_ko

    print '-' * 50
    print 'overall status: ',
    print "OK" if results_summary.value == MonitorStatus["green"] else \
          "WARN" if results_summary.value == MonitorStatus["amber"] else \
          "ERROR"
    if results_summary.value != MonitorStatus["green"]:
        print 'causes:'
        print results_summary.causes
    print '-' * 50
    LOGGER.debug("do_display finished")

def analyse_results(zk_data, zk_election):
    '''
    Analyse the partition summary and Prod2Cons
    Then set the the test result flag accordingly
    I the test flag is not green, put a reason explaining why
    Then return a json
    '''
    analyse_status = MonitorStatus["green"]
    analyse_causes = []
    analyse_metric = 'zookeeper.health'
    zk_majority = int(math.ceil(float(len(zk_data.list_zk.split(",")))/2))

    if zk_data and zk_data.list_zk_ko:
        if zk_data.num_zk_ok >= zk_majority:
            LOGGER.warn("analyse_results : at least one zookeeper node failed")
            analyse_status = MonitorStatus["amber"]
            analyse_causes.append("zookeeper node(s) unreachable (%s)" % zk_data.list_zk_ko)
        else:
            LOGGER.error("analyse_results : at least one zookeeper node failed")
            analyse_status = MonitorStatus["red"]
            analyse_causes.append("zookeeper node(s) unreachable (%s)" % zk_data.list_zk_ko)
    elif zk_election is False:
        LOGGER.error("analyse_results : zookeeper election not done, check nodes mode")
        analyse_status = MonitorStatus["red"]
        analyse_causes.append("zookeeper election not done, check nodes mode")
    return Event(TIMESTAMP_MILLIS(),
                 'zookeeper',
                 analyse_metric,
                 analyse_causes,
                 analyse_status)

def getzknodes(zconnect):
    '''
        Returns a list of zknodes tuples, where each tuple represents
        a zk node with host/port and alive status.
    '''

    LOGGER.debug("getzknodes started")
    zok = 0
    zko = 0
    seq = []
    bconnect = ""
    berror = ""
    zconnectsplit = zconnect.split(",")
    for zpart in zconnectsplit:
        if ':' in zpart:
            host, port = zpart.split(':', 1)
            port = int(port)
            if bconnect != "":
                bconnect += ","
            bconnect += "%s:%d" % (host, port)
            try:
                client = ZkClient(host, port)
                if client.ping():
                    seq.append(ZkNode(host, port, True))
                    zok += 1
                else:
                    if berror != "":
                        berror += ","
                    berror += "%s:%d" % (host, port)
                    seq.append(ZkNode(host, port, False))
                    zko += 1
                    LOGGER.error(
                        "Zookeeper node unreachable (%s:%d)", host, port)
            except ZkError:
                LOGGER.error(
                    "Zookeeper node unreachable (%s:%d)", host, port)
                zko += 1
                seq.append(ZkNode(host, port, False))
    LOGGER.debug("getzknodes finished")
    return ZkNodesHealth(bconnect, berror, zok, zko, seq)

class ProcessorError(Exception):
    '''
    Exception in processor
    '''
    def __init__(self, msg):
        Exception.__init__(msg)
        self.msg = msg

    def __str__(self):
        return self.msg


class ZookeeperBot(PndaPlugin):
    '''
    Main body of plugin
    '''
    def __init__(self):
        self.zconnect = ""
        self.postjson = False
        self.display = False
        self.consumer_timeout = 1  # max number of second to wait for
        self.results = []

    def read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            usage='%(prog)s [options]',
            description='Show state of Zk-Kafka cluster',
            add_help=False)
        parser.add_argument('--zconnect', default='localhost:2181', help= \
            'comma separated host:port pairs, \
                            each corresponding to a zk host (default: localhost:2181)')
        return parser.parse_args(args)

    def process(self, zknodes):
        '''
        Returns a named tuple of type ZkMonitorSummary
        '''

        LOGGER.debug("process started")

        self.results.append(Event(TIMESTAMP_MILLIS(), 'zookeeper', \
                                  'zookeeper.nodes', [], zknodes.connect))
        self.results.append(Event(TIMESTAMP_MILLIS(), 'zookeeper', \
                                  'zookeeper.nodes.ok', [], zknodes.num_ok))
        self.results.append(Event(TIMESTAMP_MILLIS(), 'zookeeper', \
                                  'zookeeper.nodes.ko', [], zknodes.num_ko))

        LOGGER.debug("process finished")
        return ZkMonitorSummary(
            list_zk=zknodes.connect,
            list_zk_ko=zknodes.error,
            num_zk_ok=zknodes.num_ok,
            num_zk_ko=zknodes.num_ko
        )

    def runner(self, args, display=True):
        '''
            Main section.
        '''
        LOGGER.debug("runner started")
        array_args = args.split(" ")
        options = self.read_args(array_args)
        self.zconnect = options.zconnect
        self.display = display
        # split zonnect in pair of zhost, zport
        zknodes = getzknodes(self.zconnect)
        LOGGER.debug(zknodes)
        zk_data = None
        zk_election = False
        zid = 0
        for zkn in zknodes.list:
            LOGGER.debug("processing %s", zkn)
            if zkn.alive is True:
                try:
                    zk_data = self.process(zknodes)
                    zkelect = os.popen("echo stat | nc %s %s | grep Mode" %
                                       (zkn.host, zkn.port)) \
                                       .read().replace("Mode: ", "") \
                                       .rstrip('\r\n')
                    if zkelect == "leader" or zkelect == "standalone":
                        zk_election = True
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                                              'zookeeper',
                                              'zookeeper.%d.mode' % (zid), [], zkelect)
                                       )
                except ZkError, ex:
                    LOGGER.error('Failed to access Zookeeper: %s', str(ex))
                    break
                except ProcessorError, ex:
                    LOGGER.error('Failed to process: %s', str(ex))
                    break
            else:
                self.results.append(Event(TIMESTAMP_MILLIS(),
                                          'zookeeper',
                                          'zookeeper.%d.mode' % (zid), [], MonitorStatus["red"])
                                   )
            zid += 1
        if not zk_data:
            zk_data = ZkMonitorSummary(
                list_zk=self.zconnect,
                list_zk_ko=self.zconnect,
                num_zk_ok=0,
                num_zk_ko=len(zknodes)
            )

        # ----------------------------------------
        # Lets'build the global result structure
        # ----------------------------------------
        results_summary = analyse_results(zk_data, zk_election)
        # ----------------------------------------
        # if output display is required
        # ----------------------------------------
        if self.display:
            do_display(results_summary, zk_data, zknodes)
        LOGGER.debug("runner finished")
        self.results.append(results_summary)
        return self.results

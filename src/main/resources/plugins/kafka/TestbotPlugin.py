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

Purpose:    Kafka tests

"""

# check http://kafka.apache.org/documentation.html#monitoring for more
# information on JMX

# todo: clean up get functions in order to make them more generic

import time
import argparse
import sys
import os
import math
import logging
import json
from decimal import Decimal
import requests
from prettytable import PrettyTable
from plugins.common.zkclient import ZkClient, ZkError
from plugins.kafka.prod2cons import Prod2Cons
from plugins.common.defcom import MonitorSummary, PartitionState, TestbotResult
from plugins.common.defcom import ZkNodesHealth, ZkNode, KkBroker
from pnda_plugin import PndaPlugin
from pnda_plugin import Event
from pnda_plugin import MonitorStatus

sys.path.insert(0, '../..')

TESTBOTPLUGIN = lambda: KafkaWhitebox()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TESTBOTPLUGIN")
NBTEST = 10

def get_broker_by_id(brokers, search):
    '''
    Get broker by id
    '''
    for broker in brokers.list:
        if broker.id == search:
            return KkBroker(
                search, broker.host, broker.port, broker.jmx_port, broker.alive)
    return None

class ProcessorError(Exception):
    '''
    Processor errors
    '''
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.msg = msg

    def __str__(self):
        return self.msg

class KafkaWhitebox(PndaPlugin):
    '''
    Whitebox test plugin for Kafka
    '''

    def __init__(self):
        self.broker_list = []
        self.display = False
        self.results = []
        self.topic_list = []
        self.prod2cons = False
        self.whitebox_error_code = -1
        self.activecontrollercount = -1

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
        parser.add_argument('--brokerlist', default='localhost:9092',
                            help='comma separated host:port pairs, each corresponding to ' + \
          'a kafka broker (default: localhost:9092)')
        parser.add_argument('--zkconnect', default='localhost:2181',
                            help='comma separated host:port pairs, each corresponding to a ' + \
                            'zk host (default: localhost:2181)')
        parser.add_argument('--prod2cons', action='store_const', const=True,
                            help='Run a producer/consumer test')
        return parser.parse_args(args)

    def get_brokertopicmetrics(self, host, topic, broker_id):
        '''
        Get brokertopicmetrics
        '''
        for jmx_path_name in ["BytesInPerSec", "BytesOutPerSec", \
                              "MessagesInPerSec"]:
            for jmx_data in ["RateUnit", "OneMinuteRate", \
                             "EventType", "Count", "FifteenMinuteRate",
                             "FiveMinuteRate", "MeanRate"]:
                url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                                "kafka.server:type=BrokerTopicMetrics,"
                                "name=%s,topic=%s/%s") % (host, jmx_path_name, topic, jmx_data)

                response = requests.get(url_jmxproxy)
                if response.status_code == 200:
                    LOGGER.debug("Getting %s - %s", response.text, url_jmxproxy)
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                                              'kafka',
                                              'kafka.brokers.%d.topics.%s.%s.%s' %
                                              (broker_id,
                                               topic,
                                               jmx_path_name,
                                               jmx_data), [], response.text)
                                       )
                elif response.status_code == 404:
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                                              'kafka',
                                              'kafka.brokers.%d.topics.%s.%s.%s' %
                                              (broker_id,
                                               topic,
                                               jmx_path_name,
                                               jmx_data), [], '0')
                                       )
                else:
                    LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        return None

    def get_activecontrollercount(self, host, broker_id):
        '''
        Get activecontrollercount
        '''
        url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                        "kafka.controller:type=KafkaController,"
                        "name=ActiveControllerCount/Value") % host

        response = requests.get(url_jmxproxy)
        if response.status_code == 200:
            LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
            self.results.append(Event(TIMESTAMP_MILLIS(),
                                      'kafka',
                                      'kafka.brokers.%d.ActiveControllerCount' %
                                      broker_id, [], response.text))
            if self.activecontrollercount != -1 and response.text == 1:
                self.activecontrollercount = 1
            elif self.activecontrollercount == 1 and response.text == 1:
                self.whitebox_error_code = 102

        else:
            LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

        return None

    def get_uncleanleaderelections(self, host, broker_id):
        '''
        Get UncleanLeaderElectionsPerSec
        '''
        unclean_count = None
        unclean_rate = None
        for jmx_data in ["RateUnit",
                         "OneMinuteRate",
                         "EventType",
                         "Count",
                         "FifteenMinuteRate",
                         "FiveMinuteRate",
                         "MeanRate"]:
            url_jmxproxy = ("http://127.0.0.1:8000/jmxproxy/%s/"
                            "kafka.controller:type=ControllerStats,"
                            "name=UncleanLeaderElectionsPerSec/%s") % (host, jmx_data)

            response = requests.get(url_jmxproxy)
            if response.status_code == 200:
                LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
                self.results.append(Event(TIMESTAMP_MILLIS(),
                                          'kafka',
                                          ('kafka.brokers.%d.'
                                           'controllerstats.UncleanLeaderElections.%s') %
                                          (broker_id, jmx_data), [], response.text))

                if jmx_data == "Count":
                    unclean_count = int(response.text)
                elif jmx_data == "FifteenMinuteRate":
                    unclean_rate = Decimal(response.text)

            else:
                LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)
        if unclean_count is not None and unclean_rate is not None:
            if unclean_rate > 0.0002:
                LOGGER.debug("broker %d threshold is %f and current rate is %f",
                             broker_id, 0.0002, unclean_rate)
                self.whitebox_error_code = 104

        return None

    def process(self, zknodes, gbrokers, partitions):
        '''
        Returns a named tuple of type PartitionsSummary.
        '''
        LOGGER.debug("process started")
        topic_ok = 0
        topic_ko = 0
        process_results = []
        for obj in partitions:
            parts_object = obj.partitions["list"]
            if obj.partitions["valid"] is True:
                for parts in parts_object:
                    # Get the partition leader
                    for part, partinfo in parts.iteritems():
                        leader_read = partinfo['leader']
                        broker = get_broker_by_id(
                            gbrokers, '%d' % leader_read)

                        if broker is not None:
                            process_results.append(
                                PartitionState(
                                    broker.host,
                                    broker.port,
                                    obj.id,
                                    part,
                                    obj.partitions["valid"]))
                    topic_ok += 1
            else:
                topic_ko += 1
                LOGGER.error("Topic not in a good state (%s)", obj.id)
                process_results.append(PartitionState(None,
                                                      None,
                                                      obj.id,
                                                      None,
                                                      obj.partitions["valid"]))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes',
                                  [],
                                  gbrokers.connect))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes.ok',
                                  [],
                                  gbrokers.num_ok))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.nodes.ko',
                                  [],
                                  gbrokers.num_ko))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.partitions.ok',
                                  [],
                                  topic_ok))

        self.results.append(Event(TIMESTAMP_MILLIS(),
                                  'kafka',
                                  'kafka.partitions.ko',
                                  [],
                                  topic_ko))

        LOGGER.debug("process finished")
        return MonitorSummary(num_partitions=len(process_results),
                              list_brokers=gbrokers.connect,
                              list_brokers_ko=gbrokers.error,
                              num_brokers_ok=gbrokers.num_ok,
                              num_brokers_ko=gbrokers.num_ko,
                              list_zk=zknodes.connect,
                              list_zk_ko=zknodes.error,
                              num_zk_ok=zknodes.num_ok,
                              num_zk_ko=zknodes.num_ko,
                              num_part_ok=topic_ok,
                              num_part_ko=topic_ko,
                              partitions=tuple(process_results)
                             )

    def getzknodes(self, zconnect):
        '''
            Returns a list of zknodes tuples, where each tuple represents
            a zk node with host/port and alive status.
        '''

        LOGGER.debug("getzknodes started")
        zok = 0
        zko = 0
        node_list = []
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
                        node_list.append(ZkNode(host, port, True))
                        zok += 1
                    else:
                        if berror != "":
                            berror += ","
                        berror += "%s:%d" % (host, port)
                        node_list.append(ZkNode(host, port, False))
                        zko += 1
                        LOGGER.error(
                            "Zookeeper node unreachable (%s:%d)", host, port)
                except ZkError:
                    LOGGER.error(
                        "Zookeeper node unreachable (%s:%d)", host, port)
                    zko += 1
                    node_list.append(ZkNode(host, port, False))
        LOGGER.debug("getzknodes finished")
        return ZkNodesHealth(bconnect, berror, zok, zko, node_list)

    def analyse_results(self, zk_data, test_result):
        '''
        Analyse the partition summary and Prod2Cons
        Then set the the test result flag accordingly
        I the test flag is not green, put a reason explaining why
        Then return a json
        '''
        analyse_status = MonitorStatus["green"]
        analyse_causes = []
        analyse_metric = 'kafka.health'
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

        if zk_data and zk_data.list_brokers_ko:
            LOGGER.error("analyse_results : at least one broker failed")
            analyse_status = MonitorStatus["red"]
            analyse_causes.append("broker(s) unreachable (%s)" %
                                  zk_data.list_brokers_ko)

        if zk_data and zk_data.num_part_ko > 0:
            LOGGER.error("analyse_results : at least one topic / partition inconsistency")
            if analyse_status != MonitorStatus["red"]:
                analyse_status = MonitorStatus["amber"]
            analyse_causes.append(
                "topic / partition inconsistency in zookeeper")

        if self.prod2cons:
            if test_result.sent == test_result.received \
             and test_result.notvalid == 0:
                LOGGER.debug("analyse_results - test for messages sent / received is valid")
            else:
                LOGGER.error("analyse_results - test for messages sent / received failed")
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("producer / consumer failed " + \
                    "(sent %d, rcv_ok %d, rcv_ko %d)" %
                                      (test_result.sent,
                                       test_result.received,
                                       test_result.notvalid))

        # whitebox analysis
        if self.whitebox_error_code != -1:

            if self.whitebox_error_code == 101:
                LOGGER.warn("UnderReplicatedPartitions should be 0")
                if analyse_status != MonitorStatus["red"]:
                    analyse_status = MonitorStatus["amber"]
                analyse_causes.append(
                    "UnderReplicatedPartitions should be 0")
            elif self.whitebox_error_code == 102:
                LOGGER.warn(
                    "ActiveControllerCount only one broker in the cluster should have 1")
                if analyse_status != MonitorStatus["red"]:
                    analyse_status = MonitorStatus["amber"]
                analyse_causes.append(
                    "ActiveControllerCount only one broker in the cluster should have 1")
            elif self.whitebox_error_code == 104:
                LOGGER.warn("analyse_results : Unclean leader election rate, should be 0")
                if analyse_status != MonitorStatus["red"]:
                    analyse_status = MonitorStatus["amber"]
                analyse_causes.append(
                    "Unclean leader election rate, should be 0")

        return Event(
            TIMESTAMP_MILLIS(), 'kafka', \
            analyse_metric, analyse_causes, analyse_status)

    def process_brokers(self):
        '''
        Process the brokers
        '''
        # todo see brokerID
        jmx_config = json.load(open("%s/%s" % (HERE, "jmx_config.json")))
        self.results.append(Event(TIMESTAMP_MILLIS(),
                                 'kafka',
                                 'kafka.available.topics',
                                 [],
                                 '%s' % (map(str, self.topic_list))))

        for broker_index in xrange(1, len(self.broker_list) + 1):
            broker = self.broker_list[broker_index - 1]
            for topic in self.topic_list:
                self.get_brokertopicmetrics(broker, topic, broker_index)
                for jmx_data in jmx_config["mBeans"]:
                    url_jmxproxy = "http://127.0.0.1:8000/jmxproxy/" + \
                    "%s/%s" % (broker, jmx_data["path"])
                    LOGGER.info(url_jmxproxy)
                    response = requests.get(url_jmxproxy)
                    if response.status_code == 200:
                        LOGGER.debug("Getting %s fo %s", response.text, url_jmxproxy)
                        self.results.append(Event(TIMESTAMP_MILLIS(),
                                                  'kafka',
                                                  'kafka.brokers.%d.%s' %
                                                  (broker_index, jmx_data["label"]),
                                                  [],
                                                  response.text))
                        if 'expect_value' in jmx_data and (int(response.text) != jmx_data["expect_value"]):
                            self.whitebox_error_code = jmx_data["error_code"]

                    else:
                        LOGGER.error("ERROR for url_jmxproxy: %s", url_jmxproxy)

            self.get_activecontrollercount(broker, broker_index)
            self.get_uncleanleaderelections(broker, broker_index)
        return None

    def do_display(self, results_summary, zk_data, test_result):
        '''
            Receive a summary tuples, and then build a friendly
            on the standard output as a result of the monitoring running.
            The second object is the test result from prod2cons.
        '''

        LOGGER.debug("do_display start")

        table = PrettyTable(['Broker', 'Port', 'Topic', 'PartId', 'Valid'])
        table.align['broker'] = 'l'

        if zk_data and zk_data.partitions:
            for part in zk_data.partitions:
                table.add_row(
                    [part.broker, part.port, part.topic, \
                    part.partId, part.alive])

        if zk_data:
            print table.get_string(sortby='Broker')
            print
            print 'List of brokers:            %s' % zk_data.list_brokers
            print 'List of brokers (ko):       %s' % zk_data.list_brokers_ko
            print 'Number of brokers (ok):     %d' % zk_data.num_brokers_ok
            print 'Number of brokers (ko):     %d' % zk_data.num_brokers_ko
            print 'List of zk:                 %s' % zk_data.list_zk
            print 'List of zk (ko):            %s' % zk_data.list_zk_ko
            print 'Number of zk nodes (ok):    %d' % zk_data.num_zk_ok
            print 'Number of zk nodes (ko):    %d' % zk_data.num_zk_ko
            print 'Number of partitions (ok):  %d' % zk_data.num_part_ok
            print 'Number of partitions (ko):  %d' % zk_data.num_part_ko
            print 'Number of partitions:       %d' % zk_data.num_partitions
            print 'Run (total):                %d' % NBTEST
            print 'Run (sent):                 %d' % test_result.sent
            print 'Run (rcv):                  %d' % test_result.received
            print 'Run (total):                %d' % test_result.notvalid
            print 'Run (avg ms):               %d' % test_result.avg_ms

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

    def runner(self, args, display=True):
        '''
            Main section.
        '''
        LOGGER.debug("runner started")

        plugin_args = args.split() \
            if args is not None and args.strip() \
            else ""

        options = self.read_args(plugin_args)

        self.broker_list = options.brokerlist.split(",")
        self.prod2cons = options.prod2cons

        zknodes = self.getzknodes(options.zkconnect)
        LOGGER.debug(zknodes)
        prev_zk_data = None
        zk_data = None
        brokers = None
        for zkn in zknodes.list:
            LOGGER.debug("processing %s:%d", zkn.host, zkn.port)
            if zkn.alive is True:
                try:
                    client = ZkClient(zkn.host, zkn.port)
                    brokers = client.brokers()
                    topics = client.topics()
                    for topic in topics:
                        if not topic.id in self.topic_list:
                            self.topic_list.append(topic.id)
                            LOGGER.debug(
                                "adding %s to the topic list", topic.id)

                    zk_data = self.process(zknodes, brokers, topics)
                except ZkError, exc:
                    LOGGER.error('Failed to access Zookeeper: %s', str(exc))
                    break
                except ProcessorError, exc:
                    LOGGER.error('Failed to process: %s', str(exc))
                    break
                if prev_zk_data is not None:
                    if (
                            prev_zk_data.num_partitions != zk_data.num_partitions or
                            prev_zk_data.num_part_ok != zk_data.num_part_ok or
                            prev_zk_data.num_part_ko != zk_data.num_part_ko):
                        LOGGER.error("Inconsistency found in zk (%s,%d) tree comparison",
                                     zkn.host, zkn.port)
                    else:
                        LOGGER.debug("No inconsistency found in zk (%s,%d) tree comparison", \
                                     zkn.host, zkn.port)
                prev_zk_data = zk_data
        if not zk_data:
            zk_data = MonitorSummary(num_partitions=-1,
                                     list_brokers="",
                                     list_brokers_ko="",
                                     num_brokers_ok=-1,
                                     num_brokers_ko=-1,
                                     list_zk=zknodes,
                                     list_zk_ko=zknodes,
                                     num_zk_ok=0,
                                     num_zk_ko=len(zknodes.list),
                                     num_part_ok=-1,
                                     num_part_ko=-1,
                                     partitions=tuple()
                                    )
        test_result = TestbotResult(-1, -1, -1, -1)
        if self.prod2cons:
            LOGGER.debug("=> E2E producer / consumer test required")
            # Now, pick up a broker and run a prod2cons test run
            if brokers and brokers.connect:
                # beta1: use the first of the list
                pairbrokers = brokers.connect.split(',')
                shost, sport = pairbrokers[0].split(':')
                try:
                    test_runner = Prod2Cons(shost,
                                            int(sport),
                                            "%s/%s" % (HERE, "dataplatform-raw.avsc"),
                                            "avro.internal.testbot",
                                            NBTEST)
                    msgsent = test_runner.prod()
                    LOGGER.debug("prod sent %d messages", msgsent)
                    test_result = test_runner.cons()
                except ValueError as error:
                    LOGGER.error("Error on Prod2Cons %s", str(error))
            else:
                LOGGER.error("No valid broker found for running prod2cons run")


        LOGGER.debug("Perform white box test on topics %s", \
          '-'.join(self.topic_list))
        self.process_brokers()

        results_summary = self.analyse_results(zk_data, test_result)
        self.results.append(results_summary)

        LOGGER.debug("runner finished")

        if display:
            self._do_display(self.results)
            self.do_display(results_summary, zk_data, test_result)

        return self.results

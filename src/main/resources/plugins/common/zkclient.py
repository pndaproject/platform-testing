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

Purpose:    Browse zookeeper tree with kafka context in mind

"""


import json
import socket
import logging
import re

from kafka.client import KafkaClient
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError

from plugins.common.defcom import ZkPartitions, KkBrokers, KkBrokersHealth

LOGGER = logging.getLogger("TestbotPlugin")

class ZkError(Exception):
    '''
    Zookeeper errors
    '''
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.msg = msg

    def __str__(self):
        return self.msg

class ZkClient(object):
    '''
    Zookeeper client wrapper
    '''
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.default_zk_timeout = 3.0
        self.client = KazooClient(hosts=':'.join([host, str(port)]),
                                  timeout=2.01,
                                  max_retries=0,
                                  read_only=True)
        self._internal_endpoint_regex = re.compile(r'^PLAINTEXT://(.*):([0-9]+)$')

    def __enter__(self):
        self.client.start(timeout=self.default_zk_timeout)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.stop()

    @classmethod
    def _zjoin(cls, parts):
        return '/'.join(parts)

    def generic_zk_list(self, path):
        '''
        Internal method for browsing zookeeper node at path location
        and get child info
        '''
        details = {}

        if path:
            children = self.client.get_children(path)
            for child in children:
                try:
                    child_path = "%s/%s" % (path, child)
                    detail = self.client.get(child_path)[0]
                    details[child] = detail
                except NoNodeError:
                    LOGGER.error(
                        "zookeeper  (%s:%d) - failed to get child from %s",
                        self.host,
                        self.port,
                        child_path)

        return details

    def ping(self):
        '''
        Returns True or False if / is reachable
        '''
        vroot = '/'
        try:
            rootelts = self.generic_zk_list(vroot)
            return rootelts is not None
        except NoNodeError:
            LOGGER.error(
                "zookeeper root node unreachable - no root node (%s:%d)",
                self.host,
                self.port)
        except KazooTimeoutError:
            LOGGER.error(
                "zookeeper root node timeout (%s:%d)", self.host, self.port)
        return False

    def topics(self):
        '''
        Returns a list of ZkPartitions tuples, where each tuple represents
        a partition.
        '''
        seq = []
        vroot = '/brokers/topics'
        try:
            for topic in self.generic_zk_list(vroot).keys():
                partitions = []
                try:
                    for part in self.generic_zk_list( \
                        self._zjoin([vroot, topic, 'partitions'])).keys():
                        for part_value in self.generic_zk_list( \
                self._zjoin([vroot, topic, 'partitions', part])).values():

                            val = json.loads(part_value)
                            partitions.append(\
                {part: {'leader': val["leader"], 'isr': val["isr"]}})

                    seq.append(ZkPartitions(topic, {'valid': True, 'list': partitions}))
                except NoNodeError:
                    LOGGER.error("zookeeper (%s:%d) - failed to get %s details",
                                 self.host,
                                 self.port,
                                 topic)
                    seq.append(
                        ZkPartitions(topic, {'valid': False, 'list': []}))
        except NoNodeError:
            LOGGER.error("zookeeper (%s:%d) - %s tree do not exist",
                         self.host,
                         self.port,
                         vroot)
            raise ZkError("zookeeper (%s:%d) - %s tree do not exist" %
                          (self.host,
                           self.port,
                           vroot))
        return tuple(seq)

    def _parse_endpoint_data(self, json_data):
        found = None
        data = json.loads(json_data)
        for endpoint in data['endpoints']:
            candidate = self._internal_endpoint_regex.match(endpoint)
            if candidate is not None and len(candidate.groups()) == 2:
                found = (candidate.group(1), int(candidate.group(2)), data['jmx_port'])
                break
        return found

    def brokers(self):
        '''
        Returns a list of KkBrokers tuples, where each tuple represents
        a broker with host/port and alive status.
        '''
        bok = 0
        bko = 0
        seq = []
        vroot = '/brokers/ids'
        bconnect = ""
        berror = ""
        try:
            for kkey, kkinfo in self.generic_zk_list(vroot).items():
                # Let's check the broker is alive
                endpoint = self._parse_endpoint_data(kkinfo)
                if endpoint is not None:
                    host, port, jmx = endpoint
                    if bconnect != "":
                        bconnect += ","
                    bconnect += "%s:%d" % (host, port)
                    try:
                        k = KafkaClient(bootstrap_servers="%s:%d" % (host, port))
                        if k is not None:
                            seq.append(KkBrokers(kkey, host, port, jmx, True))
                            bok += 1
                    except socket.gaierror:
                        LOGGER.error("broker (%s:%d) - not reachable", host, port)
                        if berror != "":
                            berror += ","
                        berror += "%s:%d" % (host, port)
                        seq.append(KkBrokers(kkey, host, port, jmx, False))
                        bko += 1
        except NoNodeError:
            LOGGER.error("zookeeper (%s:%d) - %s tree do not exist",
                         self.host, self.port, vroot)
            raise ZkError("zookeeper (%s:%d) - %s tree do not exist" %
                          (self.host, self.port, vroot))
        return KkBrokersHealth(bconnect, berror, bok, bko, seq)

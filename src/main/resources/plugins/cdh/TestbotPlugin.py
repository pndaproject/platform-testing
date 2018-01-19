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

Purpose:    CDH whitebox tests, proxied through CM API

"""

import time
import argparse
from cm_api.api_client import ApiResource
from pnda_plugin import PndaPlugin
from pnda_plugin import Event

TESTBOTPLUGIN = lambda: CDHPlugin()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class CDHPlugin(PndaPlugin):
    '''
    Plugin for retrieving metrics from CM API
    '''

    def __init__(self):

        self._metrics = {
            "capacity_remaining" :
                "select capacity_remaining where serviceType = HDFS",
            "dfs_capacity_used_non_hdfs" :
                "select dfs_capacity_used_non_hdfs where serviceType = HDFS",
            "under_replicated_blocks" :
                "select under_replicated_blocks where serviceType = HDFS",
            "files_total" :
                "select files_total where serviceType = HDFS",
            "jvm_heap_used_mb" :
                "select jvm_heap_used_mb where serviceType = HDFS",
            "live_datanodes" :
                "select live_datanodes where serviceType = HDFS",
            "dead_datanodes" :
                "select dead_datanodes where serviceType = HDFS",
            "blocks_total" :
                "select blocks_total where serviceType = HDFS",
            "total_dfs_capacity_across_datanodes" :
                "select total_dfs_capacity_across_datanodes where serviceType = HDFS",
            "total_dfs_capacity_used_across_datanodes" :
                "select total_dfs_capacity_used_across_datanodes where serviceType = HDFS",
            "total_available_vcores_across_yarn_pools" :
                "select total_available_vcores_across_yarn_pools where serviceType = YARN",
            "total_available_memory_mb_across_yarn_pools" :
                "select total_available_memory_mb_across_yarn_pools where serviceType = YARN",
            "allocated_vcores_across_yarn_pools" :
                "select total_allocated_vcores_across_yarn_pools where serviceType = YARN",
            "allocated_memory_mb_across_yarn_pools" :
                "select total_allocated_memory_mb_across_yarn_pools where serviceType = YARN"
        }


    def _read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, usage='%(prog)s [options]',
                                         description='Key metrics from CDH cluster')
        parser.add_argument('--cmhost', default='localhost', help='CM host e.g. localhost')
        parser.add_argument('--cmport', default='7180', help='CM port e.g. 7180')
        parser.add_argument('--cmuser', default='admin', help='CM user e.g. admin')
        parser.add_argument('--cmpassword', default='admin', help='CM password e.g. admin')

        return parser.parse_args(args)

    def runner(self, args, display=True):
        '''
        Main section.
        '''
        plugin_args = args.split() if args is not None and args.strip() else ""

        options = self._read_args(plugin_args)

        api = ApiResource(server_host=options.cmhost,
                          server_port=options.cmport,
                          username=options.cmuser,
                          password=options.cmpassword,
                          version=11)

        def fetch(key, query):
            '''
            Do the work of getting a metric from CM, run me over a list of metrics to fetch
            '''
            try:
                items = api.query_timeseries(query)
                value = items[0].timeSeries[-1].data[-1].value
                service = items[0].timeSeriesQuery.split('=')[1].strip()
                source = items[0].timeSeries[-1].metadata.attributes['serviceName']
                return Event(TIMESTAMP_MILLIS(), source, 'hadoop.%s.%s' % (service, key), [], value)
            except:
                return None

        events = filter(None, map(lambda metric: fetch(*metric), self._metrics.iteritems()))

        if display:
            self._do_display(events)

        return events

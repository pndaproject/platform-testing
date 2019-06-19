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

Purpose:    hdfs-namenode whitebox tests

"""

import argparse
import time
import json
import requests
from pnda_plugin import Event, PndaPlugin

TESTBOTPLUGIN = lambda: HDFSPlugin()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class HDFSPlugin(PndaPlugin): # pylint: disable=too-few-public-methods
    '''
    Plugin for retrieving metrics from HDFS JMX API
    '''

    def __init__(self):

        self._metrics = {
            "Hadoop:name=NameNodeInfo,service=NameNode": {
                "capacity_remaining": "Free",
                "dfs_capacity_used_non_hdfs": "NonDfsUsedSpace",
                "files_total": "TotalFiles",
                "live_datanodes": "LiveNodes",
                "dead_datanodes": "DeadNodes",
                "blocks_total": "TotalBlocks",
                "total_dfs_capacity_across_datanodes": "Total",
                "total_dfs_capacity_used_across_datanodes": "Used"
            }
        }


    def _read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, usage='%(prog)s [options]',
                                         description='Key metrics from hdfs-namenode')
        parser.add_argument('--host', default='localhost', help='hdfs-namenode host e.g. localhost')
        parser.add_argument('--port', default='50070', help='hdfs-namenode port e.g. 8080')

        return parser.parse_args(args)

    def runner(self, args, display=True):
        '''
        Main section.
        '''
        plugin_args = args.split() if args is not None and args.strip() else ""

        options = self._read_args(plugin_args)

        events = []
        for section in self._metrics:
            uri = 'http://%s:%s/jmx?qry=%s' % (options.host, options.port, section)
            metrics_data = requests.get(uri).json()
            print(metrics_data)
            metrics_values = metrics_data['beans'][0]

            for metric in self._metrics[section]:
                value = metrics_values[self._metrics[section][metric]]
                if metric == 'live_datanodes' or metric == 'dead_datanodes':
                    # special handling to count the number of live / dead datanodes
                    value = len(json.loads(metrics_values[self._metrics[section][metric]]))
                events.append(Event(TIMESTAMP_MILLIS(),
                                    'HDFS',
                                    'hadoop.%s.%s' % ('HDFS', metric), [], value))

        if display:
            self._do_display(events)

        return events

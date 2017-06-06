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

Purpose:    HDP whitebox tests, proxied through Ambari API

"""

import argparse
import time
import json

import requests

from pnda_plugin import Event, PndaPlugin

TestbotPlugin = lambda: HDPPlugin() # pylint: disable=invalid-name

TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class HDPPlugin(PndaPlugin):
    '''
    Plugin for retrieving metrics from Ambari API
    '''

    def __init__(self):

        self._metrics = {
            "HDFS/components/NAMENODE": {
                "capacity_remaining": "/metrics/dfs/FSNamesystem/CapacityRemaining",
                "dfs_capacity_used_non_hdfs": "/ServiceComponentInfo/NonDfsUsedSpace",
                "under_replicated_blocks": "/metrics/dfs/FSNamesystem/UnderReplicatedBlocks",
                "files_total": "/metrics/dfs/FSNamesystem/TotalFiles",
                "jvm_heap_used_mb": "/metrics/jvm/memHeapUsedM",
                "live_datanodes": "/metrics/dfs/namenode/LiveNodes",
                "dead_datanodes": "/metrics/dfs/namenode/DeadNodes",
                "blocks_total": "/metrics/dfs/FSNamesystem/BlocksTotal",
                "total_dfs_capacity_across_datanodes": "/metrics/dfs/FSNamesystem/CapacityTotal",
                "total_dfs_capacity_used_across_datanodes": "/metrics/dfs/FSNamesystem/CapacityUsed"
            },
            "YARN/components/RESOURCEMANAGER": {
                "total_available_vcores_across_yarn_pools": "/metrics/yarn/Queue/root/AvailableVCores",
                "total_available_memory_mb_across_yarn_pools": "/metrics/yarn/Queue/root/AvailableMB",
                "allocated_vcores_across_yarn_pools": "/metrics/yarn/Queue/root/AllocatedVCores",
                "allocated_memory_mb_across_yarn_pools": "/metrics/yarn/Queue/root/AllocatedMB"
            }
        }


    def _read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, usage='%(prog)s [options]',
                                         description='Key metrics from HDP cluster')
        parser.add_argument('--cmhost', default='localhost', help='CM host e.g. localhost')
        parser.add_argument('--cmport', default='8080', help='CM port e.g. 8080')
        parser.add_argument('--cmuser', default='admin', help='CM user e.g. admin')
        parser.add_argument('--cmpassword', default='admin', help='CM password e.g. admin')
        parser.add_argument('--cluster_name', default='cluster', help='Cluster name e.g. cluster')

        return parser.parse_args(args)

    def runner(self, args, display=True):
        '''
        Main section.
        '''
        plugin_args = args.split() if args is not None and (len(args.strip()) > 0) else ""

        options = self._read_args(plugin_args)

        ambari_api = 'http://%s:%s/api/v1' % (options.cmhost, options.cmport)
        http_headers = {'X-Requested-By': options.cmuser}
        http_auth = (options.cmuser, options.cmpassword)
        cluster_name = options.cluster_name

        def flatten(json_obj, flat_values=None, cur_key=None):
            if cur_key is None:
                cur_key = ''
            if flat_values is None:
                flat_values = {}

            for key in json_obj:
                full_key = cur_key + '/' + key
                value = json_obj[key]
                if isinstance(value, dict):
                    flatten(value, flat_values, full_key)
                else:
                    flat_values[full_key] = value
            return flat_values

        events = []
        for section in self._metrics:
            uri = '%s/clusters/%s/services/%s?fields=' % (ambari_api, cluster_name, section)
            for metric in self._metrics[section]:
                uri += "%s," % self._metrics[section][metric][1:]
            metrics_data = requests.get(uri, auth=http_auth, headers=http_headers).json()
            metrics_values = flatten(metrics_data)
            for metric in self._metrics[section]:
                value = metrics_values[self._metrics[section][metric]]
                if metric == 'live_datanodes' or metric == 'dead_datanodes':
                    # special handling to count the number of live / dead datanodes
                    value = len(json.loads(metrics_values[self._metrics[section][metric]]))
                service = section.split('/')[0]
                source = service
                events.append(Event(TIMESTAMP_MILLIS(), source, 'hadoop.%s.%s' % (service, metric), [], value))

        if display:
            self._do_display(events)

        return events

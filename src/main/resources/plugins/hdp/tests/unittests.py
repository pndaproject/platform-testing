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

Purpose:    Unit testing

"""

import json
import unittest

from mock import patch

from plugins.hdp.TestbotPlugin import HDPPlugin

HDFS_RESPONSE = json.loads('''{
            "href" : "http://hdp-test-hadoop-edge:8080/api/v1/clusters/hdp-test/services/HDFS/components/NAMENODE?fields=metrics/dfs/FSNamesystem/BlocksTotal,metrics/dfs/FSNamesystem/CapacityTotal,metrics/dfs/FSNamesystem/CapacityUsed,metrics/dfs/FSNamesystem/TotalFiles,metrics/dfs/FSNamesystem/UnderReplicatedBlocks,metrics/jvm/memHeapUsedM,metrics/dfs/namenode/DeadNodes,metrics/dfs/namenode/LiveNodes,metrics/dfs/FSNamesystem/CapacityRemaining,ServiceComponentInfo/NonDfsUsedSpace",
            "ServiceComponentInfo" : {
                "NonDfsUsedSpace" : 2824994612,
                "cluster_name" : "hdp-test",
                "component_name" : "NAMENODE",
                "service_name" : "HDFS"
            },
            "metrics" : {
                "dfs" : {
                "FSNamesystem" : {
                    "BlocksTotal" : 730,
                    "CapacityRemaining" : 14460960934,
                    "CapacityTotal" : 19914805248,
                    "CapacityUsed" : 1399910604,
                    "TotalFiles" : 838,
                    "UnderReplicatedBlocks" : 728
                },
                "namenode" : {
                    "DeadNodes" : "{}",
                    "LiveNodes" : "{\\"hdp-test-hadoop-dn-0:50010\\":{\\"infoAddr\\":\\"10.0.1.94:50075\\",\\"infoSecureAddr\\":\\"10.0.1.94:0\\",\\"xferaddr\\":\\"10.0.1.94:50010\\",\\"lastContact\\":1,\\"usedSpace\\":1399910604,\\"adminState\\":\\"In Service\\",\\"nonDfsUsedSpace\\":2824994612,\\"capacity\\":19914805248,\\"numBlocks\\":728,\\"version\\":\\"2.7.3.2.6.0.3-8\\",\\"used\\":1399910604,\\"remaining\\":14460960934,\\"blockScheduled\\":2,\\"blockPoolUsed\\":1399910604,\\"blockPoolUsedPercent\\":7.0294976,\\"volfails\\":0}}"
                }
                },
                "jvm" : {
                "memHeapUsedM" : 305.80264
                }
            }
        }''')

YARN_RESPONSE = json.loads('''{
            "href" : "some_uri",
            "ServiceComponentInfo" : {
                "cluster_name" : "hdp-test",
                "component_name" : "RESOURCEMANAGER",
                "service_name" : "YARN"
            },
            "metrics" : {
                "yarn" : {
                "Queue" : {
                    "root" : {
                    "AllocatedMB" : 0,
                    "AllocatedVCores" : 0,
                    "AvailableMB" : 5120,
                    "AvailableVCores" : 8
                    }
                }
                }
            }
        }''')

# This method will be used by the mock to replace requests.get
# pylint: disable=unused-argument
def mocked_requests_get(*args, **kwargs):
    class MockResponse(object):
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data
    print args[0]
    if args[0].startswith('http://10.60.18.144:8080/api/v1/clusters/hdp-test/services/HDFS/components/NAMENODE'):
        return MockResponse(HDFS_RESPONSE, 200)
    elif args[0].startswith('http://10.60.18.144:8080/api/v1/clusters/hdp-test/services/YARN/components/RESOURCEMANAGER'):
        return MockResponse(YARN_RESPONSE, 200)

    return MockResponse(None, 404)

class TestHDPPlugin(unittest.TestCase):

    '''
    Set of unit tests designed to validate HDP Plugin
    '''
    @patch('requests.get', side_effect=mocked_requests_get)
    # pylint: disable=unused-argument
    def test_normal_use(self, requests_mock):
        '''
        Mock the Ambari API with known data and check process output is what we expect
        '''
        plugin = HDPPlugin()

        values = plugin.runner(("--cmhost 10.60.18.144 --cmport 8080 --cmuser user --cmpassword password --cluster_name=hdp-test"), True)

        found_key = False
        for value in values:
            if value[1] == 'HDFS' and value[2] == 'hadoop.HDFS.files_total':
                self.assertEquals(838, value[4])
                found_key = True

        self.assertEquals(True, found_key)
if __name__ == '__main__':
    unittest.main()

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
import unittest

from mock import patch
from plugins.common.defcom import ZkPartitions

class TestKafkaWhitebox(unittest.TestCase):

    @patch('requests.get')
    @patch('plugins.common.zkclient.ZkClient')
    def test_normal_use(self, zk_mock, requests_mock):
        from plugins.kafka.TestbotPlugin import KafkaWhitebox
        zk_mock.return_value.ping.return_value = True
        zk_mock.return_value.topics.return_value = [ZkPartitions('avro.internal.test', {'valid': True, 'list': [{0: {'leader': 1, 'isr': [1]}}]})]
        #requests_mock.return_value = mocked_requests_get
        requests_mock.return_value = type('obj', (object,), {'status_code' : 200, 'text': 0.0})
        plugin = KafkaWhitebox()
        values = plugin.runner(("--brokerlist 127.0.0.1:9050 --zkconnect 127.0.0.1:2181"), True)
        self.assertEqual(plugin.topic_list, ['avro.internal.test'])
        self.assertEqual(plugin.broker_list, ['127.0.0.1:9050'])

        self.assertEqual(78, len(values))
        i = 6

        for jmx_path_name in ["BytesInPerSec", "BytesOutPerSec",
                              "MessagesInPerSec"]:
            for jmx_data in ["RateUnit", "OneMinuteRate",
                             "EventType", "Count", "FifteenMinuteRate",
                             "FiveMinuteRate", "MeanRate"]:

                self.assertEqual(values[i].source, 'kafka')
                self.assertEqual(values[i].metric, 'kafka.brokers.1.topics.avro.internal.test.%s.%s' % (jmx_path_name, jmx_data))
                self.assertEqual(values[i].causes, [])
                self.assertEqual(values[i].value, 0.0)
                i = i + 1

        for jmx_data in ["OpenFileDescriptorCount",
                         "CommittedVirtualMemorySize",
                         "FreePhysicalMemorySize",
                         "SystemLoadAverage",
                         "Arch",
                         "ProcessCpuLoad",
                         "FreeSwapSpaceSize",
                         "TotalPhysicalMemorySize",
                         "Name",
                         "ObjectName",
                         "TotalSwapSpaceSize",
                         "ProcessCpuTime",
                         "MaxFileDescriptorCount",
                         "SystemCpuLoad",
                         "Version",
                         "AvailableProcessors"]:
            self.assertEqual(values[i].source, 'kafka')
            self.assertEqual(values[i].metric, 'kafka.brokers.1.system.%s' % (jmx_data))
            self.assertEqual(values[i].causes, [])
            self.assertEqual(values[i].value, 0.0)
            i = i + 1

if __name__ == '__main__':
    unittest.main()

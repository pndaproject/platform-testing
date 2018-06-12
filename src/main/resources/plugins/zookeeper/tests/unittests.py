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
from cStringIO import StringIO
import unittest

from mock import patch
from pnda_plugin import Event

class TestKafkaBlackbox(unittest.TestCase):
    @patch('os.popen')
    @patch('plugins.common.zkclient.ZkClient')
    def test_normal_use(self, zk_mock, popen_mock):
        from plugins.zookeeper.TestbotPlugin import ZookeeperBot
        zk_mock.return_value.ping.return_value = True
        popen_mock.return_value = StringIO("Mode: standalone")
        plugin = ZookeeperBot()
        values = plugin.runner(("--zconnect 127.0.0.1:2181"), True)
        self.assertEqual(5, len(values))
        i = 0
        test_ok = []
        test_ok.append(Event(0, 'zookeeper', 'zookeeper.nodes', [], "127.0.0.1:2181"))
        test_ok.append(Event(0, 'zookeeper', 'zookeeper.nodes.ok', [], 1))
        test_ok.append(Event(0, 'zookeeper', 'zookeeper.nodes.ko', [], 0))
        for data in test_ok:
            self.assertEqual(values[i].source, data.source)
            self.assertEqual(values[i].metric, data.metric)
            self.assertEqual(values[i].causes, data.causes)
            self.assertEqual(values[i].value, data.value)
            i = i + 1


if __name__ == '__main__':
    unittest.main()

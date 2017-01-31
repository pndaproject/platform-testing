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

import time
import unittest

from collections import namedtuple

import mock

from plugins.cdh.TestbotPlugin import CDHPlugin

class TestCDHPlugin(unittest.TestCase):
    '''
    Set of unit tests designed to validate CDH Plugin
    '''
    @mock.patch('plugins.cdh.TestbotPlugin.ApiResource')
    def test_normal_use(self, api_mock):
        '''
        Mock the CM API with known data and check process output is what we expect
        '''
        item = namedtuple('item', ['timeSeries', 'timeSeriesQuery'])
        times = namedtuple('times', ['metadata', 'data'])
        metadata = namedtuple('metadata', ['attributes'])
        datum = namedtuple('datum', ['value'])
        times = namedtuple('times', ['metadata', 'data'])
        metadata = namedtuple('metadata', ['attributes'])
        datum = namedtuple('datum', ['value'])

        _datum = datum(145.19345)
        _metadata = metadata({u'active': u'true',
                              u'category': u'ROLE',
                              u'serviceName': u'hdfs01',
                              u'serviceType': u'HDFS'})
        _times = times(_metadata, [_datum])
        _item = item([_times], u'select jvm_heap_used_mb where serviceType = HDFS')
        _items = [_item]

        then = time.time() * 1000
        time.sleep(0.1)
        api_mock.return_value.query_timeseries.return_value = _items

        plugin = CDHPlugin()

        values = plugin.runner(("--cmhost 10.60.18.144 --cmhost 7777 "
                                "--cmuser user --cmpassword password"), True)

        api_mock.assert_called_with(password='password', server_host='7777',
                                    server_port='7180', username='user', version=11)
        self.assertEqual(14, len(values))
        self.assertEqual(values[0].source, 'hdfs01')
        self.assertEqual(values[0].metric, 'hadoop.HDFS.jvm_heap_used_mb')
        self.assertEqual(values[0].causes, [])
        self.assertEqual(values[0].value, 145.19345)
        self.assertGreater(values[0].timestamp, then)

    @mock.patch('plugins.cdh.TestbotPlugin.ApiResource')
    def test_exception_condition(self, api_mock):
        '''
        Test that plugin behaves correctly when CM API throws an exception
        Exception forced via Mock side_effect
        '''
        api_mock.return_value.query_timeseries.side_effect = Exception()

        plugin = CDHPlugin()

        values = plugin.runner(("--cmhost 10.60.18.144 --cmhost 7777 "
                                "--cmuser user --cmpassword password"), True)

        api_mock.assert_called_with(password='password', server_host='7777',
                                    server_port='7180', username='user', version=11)
        self.assertEqual(values, [])

if __name__ == '__main__':
    unittest.main()

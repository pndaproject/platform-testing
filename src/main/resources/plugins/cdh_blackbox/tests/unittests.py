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

import mock

from plugins.cdh_blackbox.TestbotPlugin import CDHBlackboxPlugin
from plugins.cdh_blackbox.cm_health import CDHData

from pnda_plugin import Event


class TestCDHBlackboxPlugin(unittest.TestCase):
    '''
    Set of unit tests designed to validate cdh-blackbox Plugin
    '''
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.ApiResource')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.happybase.Connection')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.jPype')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.jdbApi')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.connect')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_name',
                lambda s, x: {'HBASE': 'hbase01', 'IMPALA': 'impala01', 'HIVE': 'hive01'}[x])
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_type',
                lambda s, x: {'hbase01': 'HBASE', 'impala01': 'IMPALA', 'hive01': 'HIVE'}[x])
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_hbase_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_hive_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_impala_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_status_indicators',
                lambda s: [])
    def test_pass_simple(self, impala_connect_mock, hive_mock, jpype_mock, happybase_connection_mock, api_mock):

        # mock HBase connection.table.row
        _table = mock.MagicMock()
        _table.row.return_value = {'cf:column': 'value'}
        _hbase_conn = mock.MagicMock()
        _hbase_conn.table.return_value = _table
        happybase_connection_mock.return_value = _hbase_conn

        # mock Impala connection.cursor.fetchall
        _cursor = mock.MagicMock()
        _cursor.fetchall.return_value = [[None, 'value']]
        _impala_conn = mock.MagicMock()
        _impala_conn.cursor.return_value = _cursor
        impala_connect_mock.return_value = _impala_conn

        # mock JayDeBe connection and JPype
        _jpype = mock.MagicMock()
        _jpype.getDefaultJVMPath.return_value = None
        _jpype.startJVM.return_value = None
        jpype_mock.return_value = _jpype
        _hive_cursor = mock.MagicMock()
        _hive_cursor.fetchall.return_value = [[None, 'value']]
        _hive_conn = mock.MagicMock()
        _hive_conn.cursor.return_value = _hive_cursor
        hive_mock.return_value = _hive_conn

        plugin = CDHBlackboxPlugin()

        values = plugin.runner(("--cmhost 10.60.18.144 --cmhost 7777 "
                                "--cmuser user --cmpassword password --hadoopdistro CDH"), True)

        api_mock.assert_called_with(password='password', server_host='7777',
                                    server_port='7180', username='user', version=11)

        num_res = [('hbase01', 'hadoop.HBASE.create_table_time_ms', [], 5),
                   ('hbase01', 'hadoop.HBASE.write_time_ms', [], 7),
                   ('hbase01', 'hadoop.HBASE.read_time_ms', [], 1),
                   ('hive01', 'hadoop.HIVE.connection_time_ms', [], 7),
                   ('hive01', 'hadoop.HIVE.create_metadata_time_ms', [], 2),
                   ('impala01', 'hadoop.IMPALA.connection_time_ms', [], 0),
                   ('impala01', 'hadoop.IMPALA.read_time_ms', [], 4),
                   ('hive01', 'hadoop.HIVE.drop_table_time_ms', [], 3),
                   ('hbase01', 'hadoop.HBASE.drop_table_time_ms', [], 7)]
        gen_res = [('hbase01', 'hadoop.HBASE.create_table_succeeded', [], True),
                   ('hbase01', 'hadoop.HBASE.write_succeeded', [], True),
                   ('hbase01', 'hadoop.HBASE.read_succeeded', [], True),
                   ('hive01', 'hadoop.HIVE.connection_succeeded', [], True),
                   ('hive01', 'hadoop.HIVE.create_metadata_succeeded', [], True),
                   ('impala01', 'hadoop.IMPALA.connection_succeeded', [], True),
                   ('impala01', 'hadoop.IMPALA.read_succeeded', [], True),
                   ('hive01', 'hadoop.HIVE.drop_table_succeeded', [], True),
                   ('hbase01', 'hadoop.HBASE.drop_table_succeeded', [], True),
                   ('hbase01', 'hadoop.HBASE.health', [], 'OK'),
                   ('impala01', 'hadoop.IMPALA.health', [], 'OK'),
                   ('hive01', 'hadoop.HIVE.health', [], 'OK')
                  ]

        self.assertEqual(len(values), len(num_res) + len(gen_res))
        index = 0
        for check in num_res:
            self.assertEqual(check[0], values[index].source)
            self.assertEqual(check[1], values[index].metric)
            self.assertEqual(check[2], values[index].causes)
            self.assertTrue(isinstance(values[index].value, (int, long)))
            index += 1

        for check in gen_res:
            self.assertEqual(check[0], values[index].source)
            self.assertEqual(check[1], values[index].metric)
            self.assertEqual(check[2], values[index].causes)
            self.assertEqual(check[3], values[index].value)
            index += 1

    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.ApiResource')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.happybase.Connection')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.jPype')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.jdbApi')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.connect')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_name',
                lambda s, x: {'HBASE': 'hbase01', 'IMPALA': 'impala01',
                              'HIVE': 'hive01', 'HDFS': 'hdfs01'}[x])
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_type',
                lambda s, x: {'hbase01': 'HBASE', 'impala01': 'IMPALA',
                              'hive01': 'HIVE', 'hdfs01': 'HDFS'}[x])
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_hbase_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_hive_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_impala_endpoint',
                lambda s: '0.0.0.0')
    @mock.patch('plugins.cdh_blackbox.TestbotPlugin.CDHData.get_status_indicators',
                lambda s: [Event(0, 'hdfs01', 'hadoop.HDFS.cm_indicator', [], 'OK'),
                           Event(0, 'hbase01', 'hadoop.HBASE.cm_indicator', ['Cause A'], 'WARN'),
                           Event(0, 'hive01', 'hadoop.HIVE.cm_indicator', ['Cause B'], 'ERROR'),
                           Event(0, 'impala01', 'hadoop.IMPALA.cm_indicator',
                                 ['Cause C', 'Cause D'], 'WARN'),
                          ])
    def test_merge_simple(self, impala_connect_mock, hive_mock, jpype_mock, happybase_connection_mock, api_mock):

        # mock HBase connection.table.row with success
        _table = mock.MagicMock()
        _table.row.return_value = {'cf:column': 'value'}
        _hbase_conn = mock.MagicMock()
        _hbase_conn.table.return_value = _table
        happybase_connection_mock.return_value = _hbase_conn

        # mock Impala connection.cursor.fetchall with failure
        _cursor = mock.MagicMock()
        _cursor.fetchall.side_effect = Exception()
        _impala_conn = mock.MagicMock()
        _impala_conn.cursor.return_value = _cursor
        impala_connect_mock.return_value = _impala_conn

        # mock JayDeBe connection and JPype
        _jpype = mock.MagicMock()
        _jpype.getDefaultJVMPath.return_value = None
        _jpype.startJVM.return_value = None
        jpype_mock.return_value = _jpype
        _hive_cursor = mock.MagicMock()
        _hive_cursor.fetchall.return_value = [[None, 'value']]
        _hive_conn = mock.MagicMock()
        _hive_conn.cursor.return_value = _hive_cursor
        hive_mock.return_value = _hive_conn

        plugin = CDHBlackboxPlugin()

        values = plugin.runner(("--cmhost 10.60.18.144 --cmhost 7777 "
                                "--cmuser user --cmpassword password --hadoopdistro CDH"), True)

        api_mock.assert_called_with(password='password', server_host='7777',
                                    server_port='7180', username='user', version=11)

        for value in values:
            if value.metric == 'hadoop.HDFS.health':
                self.assertEqual(value.value, 'OK')
            if value.metric == 'hadoop.HBASE.health':
                self.assertEqual(value.value, 'WARN')
                self.assertEqual(value.causes, ['Cause A'])
            if value.metric == 'hadoop.HIVE.health':
                self.assertEqual(value.value, 'ERROR')
                self.assertEqual(value.causes, ['Cause B'])
            if value.metric == 'hadoop.IMPALA.health':
                self.assertEqual(value.value, 'ERROR')
                self.assertEqual(value.causes, ['Failed to SELECT from Impala',
                                                'Cause C', 'Cause D'])

    def test_cdhdata(self):
        '''
        Test that CDHData behaves as expected given a particular response from CM
        '''

        scheck = {'name': 'service check',
                  'explanation': 'service broken', 'summary': 'BAD'}
        rcheck = {'name': 'role check',
                  'explanation': 'role broken', 'summary': 'BAD'}
        hcheck = {'name': 'host check',
                  'explanation': 'host broken', 'summary': 'BAD'}

        role1 = mock.MagicMock(type='HBASETHRIFTSERVER', healthChecks=[rcheck],
                               hostRef=mock.MagicMock(hostId=42))
        role2 = mock.MagicMock(type='HIVESERVER2', healthChecks=[rcheck],
                               hostRef=mock.MagicMock(hostId=42))
        role3 = mock.MagicMock(type='IMPALAD', healthChecks=[rcheck],
                               hostRef=mock.MagicMock(hostId=42))

        service1 = mock.MagicMock(type='HBASE', healthSummary='CONCERNING', healthChecks=[scheck])
        service1.name = 'hbase01'
        service1.get_all_roles.return_value = [role1]
        service2 = mock.MagicMock(type='HIVE', healthSummary='WARN', healthChecks=[scheck])
        service2.name = 'hive01'
        service2.get_all_roles.return_value = [role2]
        service3 = mock.MagicMock(type='IMPALA', healthSummary='BAD', healthChecks=[scheck])
        service3.name = 'impala01'
        service3.get_all_roles.return_value = [role3]

        host = mock.MagicMock(hostname='hostA', healthChecks=[hcheck])

        api_mock = mock.Mock()
        cluster_mock = mock.Mock()
        cluster_mock.get_all_services = mock.Mock(return_value=[service1, service2, service3])
        api_mock.get_host = mock.Mock(return_value=host)

        cdhdata = CDHData(api_mock, cluster_mock)

        self.assertEqual(cdhdata.get_hive_endpoint(), 'hostA')
        self.assertEqual(cdhdata.get_hbase_endpoint(), 'hostA')
        self.assertEqual(cdhdata.get_impala_endpoint(), 'hostA')
        indicators = cdhdata.get_status_indicators()
        self.assertEqual(len(indicators), 3)
        for indicator in indicators:
            self.assertTrue(indicator.source in ['hive01', 'hbase01', 'impala01'])
            if indicator.source == 'hbase01':
                self.assertEqual(indicator.metric, 'hadoop.HBASE.cm_indicator')
                self.assertEqual(indicator.value, 'WARN')
            if indicator.source == 'hive01':
                self.assertEqual(indicator.metric, 'hadoop.HIVE.cm_indicator')
                self.assertEqual(indicator.value, 'WARN')
            if indicator.source == 'impala01':
                self.assertEqual(indicator.metric, 'hadoop.IMPALA.cm_indicator')
                self.assertEqual(indicator.value, 'ERROR')


if __name__ == '__main__':
    unittest.main()

"""
Unit Test for OPENTSDB PLUGIN
"""
import json
import unittest
import requests
from mock import patch
from plugins.opentsdb.TestbotPlugin import OpenTSDBWhiteBox

#constants
HOST = "127.0.0.1:4242,127.0.0.2:4242"


class TestOpenTSDBWhiteBox(unittest.TestCase):
    """
    Unittest opentsdb plugin
    """
    @patch("requests.post")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.write")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.read")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.delete")
    def test_stat_pass(self, opentsd_delete, opentsd_read, opentsd_write, post_requests_mock):
        """
        Test starts
        """
        stat_res_dict = [{"timestamp": 1503405425, "source": "opentsdb", "metric": "type.open.tsd.connectionmgr.connections", "causes": [], "value": "1"}, 
{"timestamp": 1503405425, "source": "opentsdb", "metric": "type.rejected.tsd.connectionmgr.connections", "causes": [], "value": "0"},
{"timestamp": 1503405425, "source": "opentsdb", "metric": "type.total.tsd.connectionmgr.connections", "causes": [], "value": "9"},
{"timestamp": 1503405425, "source": "opentsdb", "metric": "type.closed.tsd.connectionmgr.exceptions", "causes": [], "value": "0"}]
        post_requests_mock.return_value = type('obj', (object,), {'status_code' : 200, \
        'text': json.dumps(stat_res_dict)})
        opentsd_write.return_value = True
        opentsd_read.return_value = True
        opentsd_delete.return_value = True
        plugin = OpenTSDBWhiteBox()
        values = plugin.runner("%s %s" %("--hosts", HOST), False)
        assert_metric_list = ["type.open.tsd.connectionmgr.connections",
                              "type.rejected.tsd.connectionmgr.connections",
                              "type.total.tsd.connectionmgr.connections",
                              "type.closed.tsd.connectionmgr.exceptions"]
        index = 0
        for _ in HOST.split(','):
            for metric in assert_metric_list:
                self.assertEqual(values[index].source, "opentsdb")
                self.assertIn(metric, values[index].metric)
                self.assertEqual(values[index].causes, [])
                index += 1
            index += 1

    @patch("requests.post")
    #Test failure condition in api_stats function
    def test_stat_fail(self, post_requests_mock):
        """
        Testing requests
        """
        post_requests_mock.return_value = type('obj', (object,), {'status_code' : 413, \
        'text': '{"error": {"message": "bad request"}}'})
        plugin = OpenTSDBWhiteBox()
        values = plugin.runner("%s %s" %("--hosts", HOST), False)
        assert_metric_list = ['STATS']
        index = 0
        for _ in HOST.split(','):
            for metric in assert_metric_list:
                self.assertEqual(values[index].source, "opentsdb")
                self.assertIn(metric, values[index].metric)
                self.assertIsNotNone(values[index].timestamp)
                self.assertEqual(values[index].value, "0")
                self.assertEqual(values[index].causes, ["bad request"])
                index += 1
            self.assertIn("health", values[index].metric)
            self.assertEqual(values[index].value, "ERROR")
            index += 1

    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.write")
    @patch("requests.post")
    @patch("requests.delete")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.api_stats")
    #Execute read and delete function
    def test_request_rd(self, opentsd_stat, post_requests_mock, delete_requests_mock, \
    opentsd_write):
        """
        Testing requests
        """
        opentsd_write.return_value = True
        opentsd_stat.return_value = True
        post_requests_mock.return_value = type('obj', (object,), {'status_code' : 200, 'text': 0.0})
        delete_requests_mock.return_value = type('obj', (object,), \
        {'status_code' : 200, 'text': 0.0})
        plugin = OpenTSDBWhiteBox()
        values = plugin.runner("%s %s" %("--hosts", HOST), False)
        assert_metric_list = ['READ', 'DELETE']
        index = 0
        for _ in HOST.split(','):
            for metric in assert_metric_list:
                self.assertEqual(values[index].source, "opentsdb")
                self.assertIn(metric, values[index].metric)
                self.assertEqual(values[index].value, "1")
                index += 1
            self.assertIn("health", values[index].metric)
            self.assertEqual(values[index].value, "OK")
            index += 1

    @patch("requests.post")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.api_stats")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.create_uid")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.read")
    @patch("plugins.opentsdb.TestbotPlugin.OpenTSDBWhiteBox.delete")
    #execute write function
    def test_request_wr(self, opentsd_delete, opentsd_read, openstd_uid, opentsd_stat, \
    requests_mock):
        """
        Testing requests
        """
        requests_mock.return_value = type('obj', (object,), {'status_code' : 204, 'text': 0.0})
        opentsd_stat.return_value = True
        openstd_uid.return_value = True
        opentsd_read.return_value = True
        opentsd_delete.return_value = True
        plugin = OpenTSDBWhiteBox()
        values = plugin.runner("%s %s" %("--hosts", HOST), False)
        index = 0
        metric = "WRITE"
        for _ in HOST.split(','):
            self.assertEqual(values[index].source, "opentsdb")
            self.assertIn(metric, values[index].metric)
            self.assertEqual(values[index].value, "1")
            index += 1
            self.assertIn("health", values[index].metric)
            self.assertEqual(values[index].value, "OK")
            index += 1

if __name__ == "__main__":
    unittest.main()


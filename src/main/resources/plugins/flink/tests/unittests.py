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

import requests
from mock import patch, MagicMock
from plugins.flink.TestbotPlugin import Flink


# This method will be used by the mock to replace requests.get
def mocked_requests_get(status_code, text, msg):
    class MockResponse:
        def __init__(self, ret_code, ret_text, ret_msg, json_data):
            self.json_data = json_data
            self.status_code = ret_code
            self.text = ret_text
            self.msg = ret_msg

        def json(self):
            return self.json_data

    return MockResponse(status_code, text, msg, {"key1": "value1"})


class TestFlink(unittest.TestCase):
    @patch('requests.get')
    def test_normal_use(self, requests_mock):

        # Flink History Server Produces - 200
        response = mocked_requests_get(200, '{"flink-version": "1.4.0", "finished": [1]}',
                                       'Flink History Server- 200: Success (request path=/config)')
        requests_mock.return_value = response
        plugin = Flink()
        values = plugin.runner("--fhendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('OK', health[4])

        # Flink History Server Produces - 404
        response = mocked_requests_get(404, '404: Not Found',
                                       'Flink History Server- 404: Not Found(request path=/config)')
        requests_mock.return_value = response
        plugin = Flink()
        values = plugin.runner("--fhendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Flink History Server Produces - 500
        response = mocked_requests_get(500, '500: Server Error',
                                       'Flink History Server- 500: Server Error(request path=/config)')
        requests_mock.return_value = response
        plugin = Flink()
        values = plugin.runner("--fhendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])


if __name__ == '__main__':
    unittest.main()

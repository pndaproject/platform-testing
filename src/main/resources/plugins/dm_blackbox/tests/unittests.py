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

import requests
from mock import patch
from plugins.dm_blackbox.TestbotPlugin import DMBlackBox


# This method will be used by the mock to replace requests.get
def mocked_requests_get(status_code, text):
    class MockResponse(object):
        def __init__(self, code, msg, json_data):
            self.json_data = json_data
            self.status_code = code
            self.text = msg

        def json(self):
            return self.json_data

    return MockResponse(status_code, text, {"key1": "value1"})


class TestKafkaWhitebox(unittest.TestCase):
    @patch('requests.get')
    def test_normal_use(self, requests_mock):
        response = mocked_requests_get(200, '')
        requests_mock.return_value = response

        # API Request Successful
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('OK', health[4])

        # Deployment Manager Produces - 404: Not Found
        response = mocked_requests_get(404, 'Deployment Manager - 404: Not Found (request path=/packages)')
        requests_mock.return_value = response
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Deployment Manager Produces - Internal Server Error
        response = mocked_requests_get(500, 'Deployment Manager - '
                                            '500: Internal Server Error (request path=/packages)')
        requests_mock.return_value = response
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Package repository Manager not reachable
        response = mocked_requests_get(503, 'Deployment Manager - '
                                            'Unable to connect to the Package Repository Manager (request path=/xyz)')
        requests_mock.return_value = response
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Package repository Manager Produces - 404: Not Found
        response = mocked_requests_get(404, 'Package Repository Manager - '
                                            '404: Not Found (request path=/packages)')
        requests_mock.return_value = response
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Package repository Manager produces - internal server error
        response = mocked_requests_get(500, 'Package Repository Manager - '
                                            '500: Internal Server Error (request path=/packages)')
        requests_mock.return_value = response
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])

        # Deployment Manager Not reachable
        requests_mock.side_effect = requests.exceptions.RequestException
        plugin = DMBlackBox()
        values = plugin.runner("--dmendpoint http://localhost", True)
        health = values[-1]
        self.assertEquals('ERROR', health[4])


if __name__ == '__main__':
    unittest.main()

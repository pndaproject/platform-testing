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

Purpose:      Blackbox test for the Deployment manager

"""

import json
import re
import time
import argparse
import requests
from requests.exceptions import RequestException
import eventlet

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

TIMESTAMP_MILLIS = lambda: int(round(time.time() * 1000))
TESTBOTPLUGIN = lambda: DMBlackBox()


class DMBlackBox(PndaPlugin):
    '''
    Blackbox test plugin for the Deployment Manager
    '''

    def __init__(self):
        pass

    def read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, \
         usage='%(prog)s [options]', description='Key metrics from CDH cluster')
        parser.add_argument('--dmendpoint', default='http://localhost:5000', \
         help='DM endpoint e.g. http://localhost:5000')

        return parser.parse_args(args)

    @staticmethod
    def parse_err_msg_in_html_response(html_str):
        title_tag = re.search('<title>(.+?)<.*/title>', html_str)
        if title_tag:
            cause_msg = re.sub('<[A-Za-z\/][^>]*>', '', title_tag.group())
            return cause_msg
        return html_str

    @staticmethod
    def validate_api_response(response, path):
        expected_codes = [200]

        if response.status_code in expected_codes:
            return 'SUCCESS', None

        else:
            error_msg = response.text
            if response.status_code == 500:
                try:
                    json_obj = json.loads(response.text)
                    error_msg = json_obj.get('information', 'Not Available')
                except ValueError:
                    error_msg = response.text

            cause_msg = DMBlackBox.parse_err_msg_in_html_response(error_msg)
            if 'Package Repository Manager -' not in cause_msg:
                cause_msg = 'Deployment Manager - {} (request path = {})'.format(cause_msg, path)

            return 'FAIL', cause_msg

    def runner(self, args, display=True):
        '''
        Main section.
        '''
        plugin_args = args.split() \
        if args is not None and args.strip() \
        else ""

        options = self.read_args(plugin_args)
        cause = []
        values = []

        packages_available_ok, packages_deployed_ok = False, False
        packages_available_count, packages_deployed_count = -1, -1
        packages_available_ms, packages_deployed_ms = -1, -1

        # noinspection PyBroadException
        try:
            path = '/repository/packages'
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s%s" % (options.dmendpoint, path), timeout=20)
            end = TIMESTAMP_MILLIS()

            packages_available_ms = end - start
            status, msg = DMBlackBox.validate_api_response(req, path)

            if status == 'SUCCESS':
                packages_available_ok = True
                packages_available_count = len(req.json())
            else:
                cause.append(msg)

        except RequestException:
            cause.append('Unable to connect to the Deployment Manager (request path = {})'.format(path))

        except Exception as ex:
            cause.append('Platform Testing Client Error- ' + str(ex))

        # noinspection PyBroadException
        try:
            path = '/packages'
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s%s" % (options.dmendpoint, path), timeout=20)
            end = TIMESTAMP_MILLIS()

            packages_deployed_ms = end - start
            status, msg = DMBlackBox.validate_api_response(req, path)

            if status == 'SUCCESS':
                packages_deployed_ok = True
                packages_deployed_count = len(req.json())
            else:
                cause.append(msg)

        except RequestException:
            cause.append('Unable to connect to the Deployment Manager (request path = {})'.format(path))

        except Exception as ex:
            cause.append('Platform Testing Client Error- ' + str(ex))

        values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager",
                            "deployment-manager.packages_available_time_ms", [], packages_available_ms))

        values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager",
                            "deployment-manager.packages_available_succeeded", [], packages_available_ok))

        values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager",
                            "deployment-manager.packages_available_count", [], packages_available_count))

        values.append(Event(TIMESTAMP_MILLIS(), "deployment-manager",
                            "deployment-manager.packages_deployed_time_ms", [], packages_deployed_ms))

        values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager',
                            "deployment-manager.packages_deployed_succeeded", [], packages_deployed_ok))

        values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager',
                            "deployment-manager.packages_deployed_count", [], packages_deployed_count))

        health = "OK"
        if not packages_available_ok or not packages_deployed_ok:
            health = "ERROR"

        values.append(Event(TIMESTAMP_MILLIS(), 'deployment-manager',
                            'deployment-manager.health', cause, health))
        if display:
            self._do_display(values)
        return values

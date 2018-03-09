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

import time
import argparse
import requests
import eventlet
import json
from requests.exceptions import RequestException

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

TIMESTAMP_MILLIS = lambda: int(round(time.time() * 1000))
TESTBOTPLUGIN = lambda: Flink()


class Flink(PndaPlugin):
    '''
    Flink test plugin for the Flink History Server
    '''

    def __init__(self):
        pass

    def read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__,
                                         usage='%(prog)s [options]', description='Key metrics from CDH cluster')

        parser.add_argument('--fhendpoint', default='http://localhost:8082',
                            help='Flink History Server endpoint e.g. http://localhost:8082')
        return parser.parse_args(args)

    @staticmethod
    def validate_api_response(response, path, other_exp_codes=[]):
        expected_codes = [200]
        expected_codes.extend(other_exp_codes)

        if response.status_code in expected_codes:
            return 'SUCCESS', None
        else:
            cause_msg = 'Flink History Server - {} (request path = {})'.format(response.text.strip(), path)
            return 'FAIL', cause_msg

    def runner(self, args, display=True):
        """
        Main section.
        """
        plugin_args = args.split() \
        if args is not None and args.strip() \
        else ""

        options = self.read_args(plugin_args)
        cause = []
        values = []

        history_server_available_success, history_server_completed_jobs_success = False, False
        history_server_available_ms, history_server_completed_jobs_ms = -1, -1
        installed_flink_version, completed_job_count = '', -1

        # noinspection PyBroadException
        try:
            path = '/config'
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s%s" % (options.fhendpoint, path), timeout=20)
            end = TIMESTAMP_MILLIS()

            history_server_available_ms = end - start
            status, msg = Flink.validate_api_response(req, path)

            if status == 'SUCCESS':
                installed_flink_version = json.loads(req.text).get("flink-version", '')
                history_server_available_success = True
            else:
                cause.append(msg)

        except RequestException:
            cause.append('Unable to connect to the Flink History Server (request path = {})'.format(path))

        except Exception as e:
            cause.append('Platform Testing Client Error- ' + str(e))

        # noinspection PyBroadException
        try:
            path = '/joboverview'
            start = TIMESTAMP_MILLIS()
            with eventlet.Timeout(100):
                req = requests.get("%s%s" % (options.fhendpoint, path), timeout=20)
            end = TIMESTAMP_MILLIS()

            history_server_completed_jobs_ms = end - start

            # 404 - added to the expected response codes because,
            # Flink history server return 404, unless at least one flink job is executed.
            status, msg = Flink.validate_api_response(req, path, [404])

            if status == 'SUCCESS':
                if req.status_code == 200:
                    completed_job_count = len(json.loads(req.text).get('finished'))
                elif req.status_code == 404:
                    completed_job_count = 0
                history_server_completed_jobs_success = True
            else:
                cause.append(msg)

        except RequestException:
            cause.append('Unable to connect to the Flink History Server (request path = {})'.format(path))

        except Exception as e:
            cause.append('Platform Testing Client Error- ' + str(e))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.history_server_available_success", [], history_server_available_success))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.installed_flink_version", [], installed_flink_version))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.history_server_available_ms", [], history_server_available_ms))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.history_server_completed_jobs_success", [], history_server_completed_jobs_success))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.history_server_completed_jobs_count", [], completed_job_count))

        values.append(Event(TIMESTAMP_MILLIS(), "flink",
                            "flink.history_server_completed_jobs_ms", [], history_server_completed_jobs_ms))

        health = "OK"
        if not history_server_available_success or not history_server_completed_jobs_success:
            health = "ERROR"

        values.append(Event(TIMESTAMP_MILLIS(), 'flink',
                            'flink.health', cause, health))

        if display:
            self._do_display(values)
        return values

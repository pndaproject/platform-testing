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

Purpose:    Main program for starting a plugin stored in plugins/<plugin_name>.
            Plugins extend PndaPlugin and implement logic to probe aspects of platform,
            returning Event objects which are forwarded to a REST endpoint passed in
            the postjson argument if supplied.

"""

import argparse
import os
import sys
import logging
import logging.config
import json
import time
import importlib
import requests

from pnda_plugin import PluginException

HERE = os.path.abspath(os.path.dirname(__file__))
logging.config.fileConfig("%s/logging.conf" % HERE)
LOGGER = logging.getLogger("monitor")
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

def load_plugin(plugin_dir):
    '''
    Load a plugin that implemets TestbotPlugin.runner()
    '''
    LOGGER.debug('Plugin %s loading', plugin_dir)

    cls = None
    try:
        module_name = "%s.TestbotPlugin" % plugin_dir
        module = importlib.import_module(module_name)
        cls = getattr(module, "TESTBOTPLUGIN")
    except ValueError, ex:
        LOGGER.error('Unable to load module %s (%s)', module_name, ex)
    except TypeError, ex:
        LOGGER.error('Unable to load module %s (%s)', module_name, ex)

    return cls()


def read_args():
    '''
    Program argument parser
    '''
    parser = argparse.ArgumentParser(description= \
        'Monitor: collects test output from a specified plugin and sends via HTTP')

    parser.add_argument('--plugin', type=str, help='plugin to run', required=True)
    parser.add_argument('--postjson', type=str, help='endpoint for publishing results')
    parser.add_argument('--display', action='store_const', const=True, \
                            help='display results to stdout', default=False)
    parser.add_argument('--extra', type=str, help='arg string for the plugin to run')

    return parser.parse_args()


class TestbotCollector(object):
    '''
    Collects events via plugins and sends to console
    '''

    def __init__(self, opts):
        self._options = opts

    def runner(self):
        '''
        Main section
        '''
        plugin = load_plugin('plugins.%s' % self._options.plugin)

        if plugin is not None:
            LOGGER.debug('Plugin %s starting', self._options.plugin)

            events = []
            try:
                events = plugin.runner(self._options.extra, self._options.display)
            except PluginException, ex:
                logging.error('Plugin threw exception %s', ex)
                import traceback
                traceback.print_exc()

            if self._options.postjson is not None:
                self._send(events)
            else:
                LOGGER.debug('postjson not enabled, not sending')

            LOGGER.debug('Plugin %s finished', self._options.plugin)

    def _send(self, events):
        '''
        Send all the events in one pass in one payload
        '''

        LOGGER.debug("_send started")

        if len(events) > 0:

            json_datas = [{
                "data": [(lambda ev: {
                    "source": "%s" % ev.source,
                    "metric": "%s" % ev.metric,
                    "value": ev.value,
                    "causes": "%s" % json.dumps(ev.causes),
                    "timestamp": ev.timestamp
                    })(ev) for ev in events],
                "timestamp": TIMESTAMP_MILLIS()
            }]

            # 100kB limit from nodjs body parser https://github.com/expressjs/body-parser#limit-3
            if len(json.dumps(json_datas[0])) > 102400:
                json_datas = [{
                    "data": [{
                        "source": "%s" % ev.source,
                        "metric": "%s" % ev.metric,
                        "value": ev.value,
                        "causes": "%s" % json.dumps(ev.causes),
                        "timestamp": ev.timestamp
                    }],
                    "timestamp": TIMESTAMP_MILLIS()
                } for ev in events]

            headers = {'Content-Type': 'application/json', 'Connection':'close'}
            for json_data in json_datas:

                LOGGER.debug("_send data \n %s", json_data)

                try:
                    response = requests.post(self._options.postjson, data=json.dumps(json_data), headers=headers)
                    if response.status_code != 200:
                        LOGGER.error("_send failed: %s", response.status_code)
                except requests.exceptions.RequestException as ex:
                    LOGGER.error("_send failed: %s", ex)
        else:
            LOGGER.debug("_send - no events to send")

        LOGGER.debug("_send finished")


if __name__ == '__main__':

    TestbotCollector(read_args()).runner()
    sys.exit(0)

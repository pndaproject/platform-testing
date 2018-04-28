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

Purpose:    Gobblin health tests

"""

import time
import os
from datetime import datetime
import argparse

from pnda_plugin import PndaPlugin
from pnda_plugin import Event

TESTBOTPLUGIN = lambda: GobblinPlugin()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class GobblinPlugin(PndaPlugin):
    '''
    Health tests for Gobblin
    '''

    def __init__(self):
        pass

    def _read_args(self, args):
        '''
        Parse plugin options
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, usage='%(prog)s [options]',
                                         description='Gobblin health tests')
        parser.add_argument('--sentinel', help='Path to ingest sentinel file', required=True)
        parser.add_argument('--interval', help='Ingest interval in minutes', required=True)
        parser.add_argument('--ingest-delay-threshold', help='Acceptable time for one ingest run, expressed as percentage of interval', default=50)

        return parser.parse_args(args)

    def _read_sentinel(self, path):
        '''
        Parse sentinel file to last run exit status and timestamp
        '''
        try:
            with open(path) as sentinel:
                status = int(sentinel.read())
        except IOError:
            status = None

        try:
            timestamp = datetime.utcfromtimestamp(os.path.getmtime(path))
        except OSError:
            timestamp = None

        return (status, timestamp)

    def _check_sentinel(self, status, timestamp, options):
        '''
        Health check based on sentinel details
        '''
        delay = (datetime.utcnow() - timestamp).total_seconds() / 60.0
        age_threshold = options.interval + (options.interval/100.0) * options.ingest_delay_threshold

        causes = []
        health = None

        def update_health(current, update):
            if current == 'ERROR' or update == 'ERROR':
                return 'ERROR'
            if current == 'WARN' or update == 'WARN':
                return 'WARN'
            return 'OK'

        if status is None:
            causes.append('Unable to determine last ingest run status')
            health = update_health(health, 'ERROR')

        if timestamp is None:
            causes.append('Unable to determine last ingest run time')
            health = update_health(health, 'ERROR')

        if status != 0:
            causes.append('Gobblin encountered an error during ingest')
            health = update_health(health, 'ERROR')

        if delay > (options.ingest_interval * 2):
            causes.append('Gobblin ingest processing time exceeded ingest interval')
            health = update_health(health, 'ERROR')

        if delay > age_threshold:
            causes.append('Gobblin ingest delay threshold exceeded')
            health = update_health(health, 'WARN')

        if status == 0:
            health = update_health(health, 'OK')

        assert health is not None

        return Event(TIMESTAMP_MILLIS(), 'gobblin', 'gobblin.health', causes, health)


    def runner(self, args, display=True):
        '''
        Run all tests and return events
        '''
        plugin_args = args.split() if args is not None and args.strip() else ""

        options = self._read_args(plugin_args)

        status, timestamp = self._read_sentinel(options.sentinel)

        events = [self._check_sentinel(status, timestamp, options)]

        if display:
            self._do_display(events)

        return events

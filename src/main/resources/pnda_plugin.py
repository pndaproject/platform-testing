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

Purpose:    Base class for PNDA test plugins

"""

from collections import OrderedDict
from collections import namedtuple
from prettytable import PrettyTable

MonitorStatus = OrderedDict([("green", "OK"), ("amber", "WARN"), ("red", "ERROR")]) # pylint: disable=invalid-name

Event = namedtuple('Event',
                   [
                       'timestamp',
                       'source',
                       'metric',
                       'causes',
                       'value'
                   ])

class PluginException(Exception):
    '''
    Exception indicating problem in plugin
    '''
    pass

class PndaPlugin(object):
    '''
    Base class for PNDA plugins
    '''

    def _do_display(self, events):
        '''
        Receive event tuples and display on stdout in presentable format
        '''

        table = PrettyTable(['Time', 'Source', 'Metric', 'Causes', 'Value'])
        table.align['Metric'] = 'l'
        table.align['Value'] = 'l'

        for event in events:
            table.add_row([event.timestamp, event.source, event.metric, event.causes, event.value])

        print(table.get_string(sortby='Time'))


    def runner(self, args, display=True):
        '''
        Implements the body of the plugin

        Each plugin must return a sequence of Event objects (defined above)

        General events can be named as the plugin deems appropriate and take any value.

        Health events are signalled by a metric name of *.health and are expected to
        take a value from the MonitorStatus enumeration above (OK, WARN or ERROR). These are
        generally used to display overall health in the PNDA console.

        Where possible a sequence of causes should be populated in the Event.

        display:    whether to display results to stdout
        args:       command line argument list to be passed to the plugin
        '''
        raise NotImplementedError()

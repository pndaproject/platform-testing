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

Purpose:    Retrieves CM health status indicators

"""

import time
import requests

from pnda_plugin import Event

TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class HadoopData(object):
    '''
    Takes care of obtaining data and metadata from CDH via CM API for the purpose of
    blackbox testing. This includes CM's view of health and endpoints used in further tests
    '''
    def __init__(self):
        self._metadata = {}
        self._values = []
        self.update()

    def get_hbase_endpoint(self):
        '''
        Accessor for HBase endpoint
        '''
        return self._metadata['hbase_endpoint']

    def get_hive_endpoint(self):
        '''
        Accessor for Hive endpoint
        '''
        return self._metadata['hive_endpoint']

    def get_impala_endpoint(self):
        '''
        Accessor for Impala endpoint
        '''
        return self._metadata['impala_endpoint']

    def get_type(self, name):
        '''
        Accessor for service type index
        '''
        return self._metadata['types'][name]

    def get_name(self, _type):
        '''
        Accessor for service name index
        '''
        return self._metadata['names'][_type]

    def get_status_indicators(self):
        '''
        Accessor for CM health indicator list
        '''
        return self._values

    def update(self):
        '''
        Retrieve and cache data from Hadoop cluster
        '''
        pass

    def _update_health(self, current, updated):
        '''
        Given current health and and an update return new current health
        '''
        updated_health = current

        if current != 'ERROR' and (updated == 'CONCERNING' or updated == 'WARN'):
            updated_health = 'WARN'
        elif updated == 'BAD' or updated == 'ERROR':
            updated_health = 'ERROR'

        return updated_health

class CDHData(HadoopData):
    '''
    Takes care of obtaining data and metadata from CDH via CM API for the purpose of
    blackbox testing. This includes CM's view of health and endpoints used in further tests
    '''
    def __init__(self, api, cluster):
        self._api = api
        self._cluster = cluster
        super(CDHData, self).__init__()

    def update(self):
        '''
        Retrieve endpoint metadata & overall health indicators from CM plus any reason codes

        Returns sequence of Event tuples with metrics taking the form of hadoop.%s.cm_indicator
        '''
        self._values = []
        self._metadata = {'names':{}, 'types':{}}

        def is_bad(summary):
            '''
            Designated 'bad' status results
            '''
            return summary in ["BAD", "CONCERNING", "ERROR", "WARN"]

        def get_causes(health_checks):
            '''
            Extract causes from health check results
            '''
            return ["%s%s" % (chk['name'], ":" + chk['explanation']
                              if 'explanation' in chk.keys() else '')
                    for chk in health_checks if is_bad(chk['summary'])]

        # Main body of function - single pass over all services picking up endpoints,
        # health of each service and causes in the case of poor health

        for service in self._cluster.get_all_services():

            self._metadata['names'][service.type] = service.name
            self._metadata['types'][service.name] = service.type

            service_health = self._update_health('OK', service.healthSummary)
            causes = get_causes(service.healthChecks)

            for role in service.get_all_roles():

                if role.type == "HBASETHRIFTSERVER":
                    self._metadata['hbase_endpoint'] = \
                        self._api.get_host(role.hostRef.hostId).hostname
                if role.type == "HIVESERVER2":
                    self._metadata['hive_endpoint'] = \
                        self._api.get_host(role.hostRef.hostId).hostname
                if role.type == "IMPALAD":
                    self._metadata['impala_endpoint'] = \
                        self._api.get_host(role.hostRef.hostId).hostname

                host = self._api.get_host(role.hostRef.hostId)
                causes.extend(get_causes(self._api.get_host(host.hostId).healthChecks))
                causes.extend(get_causes(role.healthChecks))

            self._values.append(Event(TIMESTAMP_MILLIS(),
                                      service.name,
                                      "hadoop.%s.cm_indicator" % service.type,
                                      list(set(causes)),
                                      service_health))

class HDPData(HadoopData):
    '''
    Takes care of obtaining data and metadata from HDP via Ambari's API for the purpose of
    blackbox testing. This includes Ambari's view of health and endpoints used in further tests
    '''
    def __init__(self, api_host, api_user, api_pass):
        self._ambari_api = 'http://%s:8080/api/v1' % api_host
        self._http_headers = {'X-Requested-By': api_user}
        self._http_auth = (api_user, api_pass)
        super(HDPData, self).__init__()

    def update(self):
        '''
        Retrieve endpoint metadata & overall health indicators from Ambari plus any reason codes

        Returns sequence of Event tuples with metrics taking the form of hadoop.%s.cm_indicator
        '''
        self._values = []
        self._metadata = {'names':{}, 'types':{}}

        def get_health_state(alert_state):
            '''
            Convert alert state to health state
            '''
            if alert_state == 'CRITICAL':
                return "ERROR"
            elif alert_state == 'WARNING':
                return "WARN"
            return "OK"

        # get cluster name
        cluster_uri = requests.get('%s/clusters' % self._ambari_api,
                                   auth=self._http_auth,
                                   headers=self._http_headers).json()['items'][0]['href']

        # get all alerts and aggregate a health summary from the alert list
        alerts = requests.get('%s/alerts?fields=Alert/component_name,Alert/text,Alert/'
                              'label,Alert/state&Alert/maintenance_state.in(OFF)' % cluster_uri,
                              auth=self._http_auth,
                              headers=self._http_headers).json()['items']
        self._metadata['names']['HQUERY'] = 'HQUERY'
        self._metadata['types']['HQUERY'] = 'HQUERY'
        service_health_store = {}
        service_health_causes = {}
        for alert_item in alerts:
            alert_info = alert_item['Alert']
            service_name = alert_info['service_name']
            if service_name == 'SPARK':
                service_type = 'SPARK_ON_YARN'
            elif service_name == 'AMBARI':
                service_type = 'CLUSTER_MANAGER'
            elif service_name == 'AMBARI_METRICS':
                service_name = 'AMBARI'
                service_type = 'CLUSTER_MANAGER'
            else:
                service_type = service_name

            self._metadata['names'][service_type] = service_name
            self._metadata['types'][service_name] = service_type

            current_health = service_health_store[service_name] if service_name in service_health_store else 'OK'
            new_health = get_health_state(alert_info['state'])
            updated_health = self._update_health(current_health, new_health)
            service_health_store[service_name] = updated_health
            if new_health in ['ERROR', 'WARN']:
                current_causes = service_health_causes[service_name] if service_name in service_health_causes else []
                current_causes.append('%s: %s - %s' % (alert_info['host_name'],
                                                       alert_info['label'],
                                                       alert_info['text']))
                service_health_causes[service_name] = current_causes


        # Write out an event for each service
        for service_name in service_health_store:
            self._values.append(Event(TIMESTAMP_MILLIS(),
                                      service_name,
                                      "hadoop.%s.cm_indicator" % self._metadata['types'][service_name],
                                      list(set(service_health_causes[service_name] if service_name in service_health_causes else [])),
                                      service_health_store[service_name]))

        # Grab endpoints used by other tests
        query = 'fields=host_components'
        self._metadata['hbase_endpoint'] = requests.get('%s/services/HBASE/components/HBASE_MASTER?%s' %
                                                        (cluster_uri, query),
                                                        auth=self._http_auth,
                                                        headers=self._http_headers).json()['host_components'][0]['HostRoles']['host_name']
        self._metadata['hive_endpoint'] = requests.get('%s/services/HIVE/components/HIVE_SERVER?%s' %
                                                       (cluster_uri, query),
                                                       auth=self._http_auth,
                                                       headers=self._http_headers).json()['host_components'][0]['HostRoles']['host_name']
        self._metadata['impala_endpoint'] = None

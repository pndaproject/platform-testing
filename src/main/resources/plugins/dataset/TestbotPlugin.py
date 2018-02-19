"""
Copyright (c) 2017 Cisco and/or its affiliates.
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
Purpose:    Gobblin/Dataset tests
"""

import time
import argparse
import sys
import os
import os.path
import datetime
import re
import socket
import json
import logging
import requests

from pnda_plugin import PndaPlugin
from pnda_plugin import Event
from pnda_plugin import MonitorStatus

sys.path.insert(0, '../..')
TESTBOTPLUGIN = lambda: DatasetWhitebox()
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
HERE = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("TESTBOTPLUGIN")


class DatasetWhitebox(PndaPlugin):
    '''
    Whitebox test plugin for Master Dataset Health
    '''

    def __init__(self):
        self.display = False
        self.results = []
        self.runtesttopic = "avro.internal.testbot"
        self.dataset_name = "testbot"
        self.cluster_name = None
        self.data_service_ip = None
        self.cron_interval = None
        self.gobblin_log_path = None
        self.hdfs_url = None
        self.elk_ip = None
        self.metric_console = None
        self.num_attempts = None
        self.uri = None
        self.query = None
        self.master_dataset_dir = None
        self.quarantine_dir = None
        self.console_user = None
        self.console_password = None
        self.file_path = '/var/log/master_dataset.txt'
        self.year = None
        self.month = None
        self.day = None
        self.hour = None

    def read_args(self, args):
        '''
            This class argument parser.
            This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            usage='%(prog)s [options]',
            description='Show state of Dataset cluster',
            add_help=False)
        parser.add_argument('--cron_interval', default=30,
                            help='CRON interval that has been set for Gobblin run: 30')
        parser.add_argument('--data_service', default='http://10.0.1.20:7000', help='DATA_SERVICE IP ADDRESS')
        parser.add_argument('--cluster_name', default='cluster_name', help="PNDA CLUSTER NAME")
        parser.add_argument('--gobblin_log_path', default='/var/log/pnda/gobblin/gobblin-current.log',
                            help="Gobblin Log path")
        parser.add_argument('--elk_ip', default='localhost:9200', help="Elasticsearch IP address")
        parser.add_argument('--metric_console', default='localhost:3123', help="Console metric IP address")
        parser.add_argument('--num_attempts', default=10, help="retry count to check gobblin health")
        parser.add_argument('--master_dataset_dir', default='/user/pnda/PNDA_datasets/datasets', help="dataset dir")
        parser.add_argument('--quarantine_dir', default='/user/pnda/PNDA_datasets', help="quarantine dir")
        parser.add_argument('--console_user', default='pnda', help="PNDA console username for PAM auth")
        parser.add_argument('--console_password', default='pnda', help="PNDA console password for PAM auth")
        return parser.parse_args(args)

    # Get Kafka health metric from KAFKA platform-testing
    def get_kafka_health(self):
        kafka_health = False
        creds = {'username': self.console_user, 'password': self.console_password.encode('base64')}
        url = self.metric_console + '/metrics/kafka.health'
        try:
            pam_resp = requests.post(self.metric_console + '/pam/login', data=creds)
            if pam_resp.status_code is not 200:
                LOGGER.warning("Error while trying to access console by PAM auth")
                return kafka_health
            response = requests.get(url, cookies=pam_resp.cookies)
            if response.status_code is 200:
                kafka_health_dict = json.loads(response.text)
                if kafka_health_dict['currentData']['value'] == 'OK':
                    kafka_health = True
        except Exception as ex:
            LOGGER.exception("Exception while checking KAFKA health due to %s", ex)
        return kafka_health

    # Query elasticsearch to get log file which is specific to gobblin parameters
    def query_es(self):
        hits = []
        try:
            response = requests.post(self.uri, data=self.query)
            if response.status_code is 200:
                results = json.loads(response.text)
                hits = results.get('hits', {}).get('hits', [])
        except Exception as ex:
            LOGGER.exception("Exception while querying elasticsearch due to %s", ex)
        return hits

    # Write status variables to a file to track each operation
    def save_status_vars(self, status_dict):
        try:
            with open(self.file_path, 'w') as status_file:
                status_file.write(json.dumps(status_dict))
        except Exception as ex:
            LOGGER.exception("Exception while saving status variables to file due to %s", ex)

    # Reading contents from a status file to track the status of each operation
    def read_file_contents(self):
        contents = {}
        try:
            with open(self.file_path) as upd_file:
                contents = json.loads(upd_file.read())
        except IOError as ex:
            LOGGER.exception("Unable to open file due to %s", ex)
        return contents

    # Check dataset creation whether it exists or not
    def check_dataset(self):
        url = "%s" % (self.data_service_ip + '/api/v1/datasets')
        try:
            response = requests.get(url)
            if response.status_code is 200:
                LOGGER.debug("Response for check_dataset API is successful with status 200")
                response_dict = json.loads(response.text)
                data_list = response_dict.get('data', [])
                if data_list:
                    for item in data_list:
                        # Checking whether dataset name matches from the list of id's
                        if item['id'] == self.dataset_name:
                            LOGGER.debug("testbot exists in the list of datasets")
                            dataset_exists = True
                            return dataset_exists
                LOGGER.warning("Dataset doesn't exist in the whole list")
                dataset_exists = False
            else:
                LOGGER.warning("Response for check_dataset API isn't successful")
                dataset_exists = False
        except Exception as ex:
            LOGGER.exception("Exception while running check dataset function due to %s", ex)
            dataset_exists = False
        return dataset_exists

    # Get NAME_NODE_IP to check HDFS directory/file status
    def get_name_node(self):
        name_node_status = False
        try:
            # Using socket module to get IP address of NAME NODE from given hostname
            name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-1')
            name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&' \
                                                                  'op=GETFILESTATUS'
            name_node_response = requests.get(name_node_url)
            if name_node_response.status_code == 200:
                self.hdfs_url = 'http://' + name_node_ip + ':50070'
                name_node_status = True
                LOGGER.debug("Found Ip address of name_node(hadoop-mgr-1) where webhdfs is running")
            # Get second NAME_NODE IP address if the response from the first NAME NODE is 403
            elif name_node_response.status_code == 403:
                name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-2')
                name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&' \
                                                                      'op=GETFILESTATUS'
                new_node_response = requests.get(name_node_url)
                if new_node_response.status_code is 200:
                    self.hdfs_url = 'http://' + name_node_ip + ':50070'
                    name_node_status = True
                    LOGGER.debug("Found Ip address of name_node(hadoop-mgr-2) where webhdfs is running")
                else:
                    LOGGER.warning("Couldn't get any response from hadoop-mgr-2 node")
            else:
                LOGGER.warning("Couldn't get response from hadoop-mgr-1 node")
        except Exception as ex:
            LOGGER.exception("Exception while executing get_name_node due to %s", ex)
        return name_node_status

    # Check whether file has been modified recently with in the given time
    def get_latest_modified_file(self, dataset_data):
        avro_file_name = ''
        try:
            response_dict = json.loads(dataset_data.text)
            file_data = response_dict['FileStatuses']['FileStatus']
            if file_data:
                LOGGER.debug("Found file data regarding the given dataset")
                for avro_file in file_data:
                    # Calculating current_time, cron interval time in milliseconds
                    current_milli_time = int(time.time() * 1000)
                    ref_time = int(current_milli_time - (self.cron_interval * 60000))
                    modified_milli_time = avro_file.get('modificationTime')

                    # Checking whether the modified time lies in between before_time, now_time.
                    if modified_milli_time in range(ref_time, current_milli_time):
                        LOGGER.debug("File has been modified recently with in last cron interval time")
                        avro_file_name = avro_file.get('pathSuffix')
                        break
            else:
                LOGGER.warning("Didn't get any data related to the above file path")
        except Exception as ex:
            LOGGER.exception("Checking HDFS modified time status failed due to exception %s", ex)
        return avro_file_name

    # Check if dataset source directory(source=<dataset>) exists in HDFS filesystem
    def check_hdfs_source_dir(self):
        hdfs_src_dir_present = False
        src_url = (self.hdfs_url + '/webhdfs/v1%s/source=%s?user.name=pnda&op=LISTSTATUS') % (self.master_dataset_dir, self.dataset_name)
        try:
            src_response = requests.get(src_url)
            if src_response.status_code is 200:
                hdfs_src_dir_present = True
        except Exception as ex:
            LOGGER.warning("Dataset source directory isn't available in HDFS due to %s", ex)
        return hdfs_src_dir_present

    # Query elasticsearch to check if Gobblin run is successful by checking "dataset commit status"
    def query_es_for_commit_msg(self):
        hits = []
        commit_msg_query = '{"sort":[{"@timestamp":{"order":"desc"}}], "query":{"bool":{"must":[{"match":' \
                     '{"source":"gobblin"}}, {"match":{"path": "%s"}}, ' \
                     '{"match_phrase":{"message":"completed successfully"}}]}},"size":1}' % self.gobblin_log_path
        try:
            response = requests.post(self.uri, data=commit_msg_query)
            if response.status_code is 200:
                results = json.loads(response.text)
                hits = results.get('hits', {}).get('hits', [])
        except Exception as ex:
            LOGGER.exception("Exception while querying elasticsearch due to %s", ex)
        return hits

    # Query WEBHDFS to check master dataset directory contents
    def check_dataset_dir(self):
        hdfs_data_matched = False
        try:
            # Getting current time in datetime format and extracting each variable from it to form a directory path.
            current_time = datetime.datetime.now()

            # Adding leading zeros if the value is single digit to match the format of the directory path.
            self.year = current_time.year
            self.month = "%02d" % (current_time.month,)
            self.day = "%02d" % (current_time.day,)
            self.hour = "%02d" % (current_time.hour,)
            url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/hour=%s?user.name=pnda&op='
                                   'LISTSTATUS') % (self.master_dataset_dir, self.dataset_name, self.year, self.month, self.day, self.hour)
            dataset_response = requests.get(url)
            if dataset_response.status_code is not 200:
                hour_before_time = datetime.datetime.now() - datetime.timedelta(hours=1)

                # Adding leading zeros if the value is single digit to match the format of the directory path.
                self.year = hour_before_time.year
                self.month = "%02d" % (hour_before_time.month,)
                self.day = "%02d" % (hour_before_time.day,)
                self.hour = "%02d" % (hour_before_time.hour,)
                url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/hour=%s?user.name=pnda&'
                                       'op=LISTSTATUS') % (self.master_dataset_dir, self.dataset_name, self.year, self.month, self.day,
                                                           self.hour)
                dataset_response = requests.get(url)
                if dataset_response.status_code is not 200:
                    return hdfs_data_matched
            # Checking whether the above url path file has been recently modified or not.
            file_name = self.get_latest_modified_file(dataset_response)
            if not file_name:
                return hdfs_data_matched
            file_url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/hour=%s/%s?user.name='
                                        'pnda&op=OPEN') % (self.master_dataset_dir, self.dataset_name, self.year, self.month, self.day,
                                                           self.hour, file_name)
            file_response = requests.get(file_url)
            if file_response.status_code is not 200:
                return hdfs_data_matched

            # Check if unique string 'avrodata' exists in the file response
            if 'avrodata' in file_response.content:
                hdfs_data_matched = True
        except Exception as ex:
            LOGGER.exception("Error on check_dataset_dir due to %s", ex)
        return hdfs_data_matched

    # Check status of quarantine dataset directory
    def check_quarantine_dir(self):
        quar_data_matched = False
        try:
            # Getting current time in datetime format and extracting each variable from it to form a directory path.
            current_time = datetime.datetime.now()

            # Adding leading zeros if the value is single digit to match the format of the directory path.
            self.year = current_time.year
            self.month = "%02d" % (current_time.month,)
            self.day = "%02d" % (current_time.day,)
            url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s?user.name=pnda&op='
                                   'LISTSTATUS') % (self.quarantine_dir, self.runtesttopic, self.year, self.month,
                                                    self.day)
            quarantine_response = requests.get(url)
            if quarantine_response.status_code is not 200:
                day_before_time = datetime.datetime.now() - datetime.timedelta(days=1)

                # Adding leading zeros if the value is single digit to match the format of the directory path.
                self.year = day_before_time.year
                self.month = "%02d" % (day_before_time.month,)
                self.day = "%02d" % (day_before_time.day,)
                url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s?user.name=pnda&op='
                                       'LISTSTATUS') % (self.quarantine_dir, self.runtesttopic, self.year, self.month,
                                                        self.day)
                quarantine_response = requests.get(url)
                if quarantine_response.status_code is not 200:
                    return quar_data_matched

            file_name = self.get_latest_modified_file(quarantine_response)
            if not file_name:
                return quar_data_matched
            # Open and read quarantine files to check if data matches with produced data
            file_url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s/%s?user.name=pnda&'
                                        'op=OPEN') % (self.quarantine_dir, self.runtesttopic, self.year, self.month,
                                                      self.day, file_name)
            file_response = requests.get(file_url)
            if file_response.status_code is not 200:
                return quar_data_matched
            # Check if unique string 'quardata' matches with the quarantine file response
            if 'quardata' in file_response.content:
                quar_data_matched = True
        except Exception as ex:
            LOGGER.exception("Exception while executing quarantine_status due to %s", ex)
        return quar_data_matched

    # Run whole dataset flow to check dataset content stored in HDFS filesystem
    def check_dataset_content(self, initial_gobblin_run):
        analyse_status = MonitorStatus["green"]
        analyse_causes = []
        analyse_metric = 'dataset.health'
        try:
            if not self.check_dataset():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("Dataset doesn't exist")
                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', analyse_metric, analyse_causes,
                                          analyse_status))
                return
            if not self.get_name_node():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("Unable to get right name_node ip address")
                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', analyse_metric, analyse_causes,
                                          analyse_status))
                return

            '''
	    Since there is a possibility of Gobblin service(Initial manual run) being executed before
	    producing data to KAFKA topic, Check if dataset source directory exists in HDFS filesystem.
	    Query Elasticsearch to check if there is a successful dataset commit message in gobblin log
	    which notifies that Gobblin has ran it's hadoop job successfully but since there is no data
	    in kafka topic it couldn't create dataset source directory.
	    '''
            if initial_gobblin_run:
		# Check if dataset source directory exists in HDFS filesystem
                # Dataset creation happens only if there is data in KAFKA topic
                if not self.check_hdfs_source_dir():
		    # Report dataset health as error if there is no "commit successful" message in gobblin log
                    if not self.query_es_for_commit_msg():
                        analyse_status = MonitorStatus["red"]
                        analyse_causes.append("Didn't store any data in HDFS due to Hadoop job(Gobblin) Failure")

                    self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', analyse_metric, analyse_causes, analyse_status))
                    return

            if not self.check_dataset_dir():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("HDFS dataset content check failed")
            if not self.check_quarantine_dir():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("HDFS quarantine content check failed")
        except Exception as ex:
            LOGGER.exception("Exception while checking dataset content due to %s", ex)
            analyse_status = MonitorStatus["red"]
            analyse_causes.append("Exception while checking dataset content")
        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', analyse_metric, analyse_causes, analyse_status))
        return

    def exec_test(self):
        try:
            # Create a status file if not present to track status of each health check
            if not os.path.isfile(self.file_path):
                status_dict = {}
                status_dict.update({'first-run-gobblin-wait': 1, 'error-reported': 0, 'retries': 0,
                                    'lastlogtimestamp': ''})
                with open(self.file_path, 'w') as new_file:
                    new_file.write(json.dumps(status_dict))

            # Read file contents if file is already present
            file_data = self.read_file_contents()
            first_run_gobblin_wait = file_data.get('first-run-gobblin-wait', 1)
            retry_count = file_data.get('retries', 0)
            lastlogtimestamp = file_data.get('lastlogtimestamp', '')
            error_report = file_data.get('error-reported', 0)

            # Check Kafka health before proceeding to gobblin and dataset health check
            kafka_health = self.get_kafka_health()
            if kafka_health:
                # Initial Gobblin health check(Wait until it finishes it's max retries)
                if first_run_gobblin_wait:
                    # Query Elasticsearch to check if there is any gobblin log generated
                    gobblin_log_status = self.query_es()
                    if gobblin_log_status:
                        # Extract timestamp of the log file from the hits list
                        logtimestamp = gobblin_log_status[0]['_source']['@timestamp']
                        # Update status dict with newly generated timestamp
                        updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                        'lastlogtimestamp': logtimestamp}
                        # Save updated status dict to status file
                        self.save_status_vars(updated_dict)
                        # Report initial gobblin health metric
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health',
                                                  [], "OK"))
                        # Check HDFS filesystem to see if data exists and also if content matches.
                        self.check_dataset_content(first_run_gobblin_wait)
                    # Retry querying elasticsearch until it meets the max retries count
                    else:
                        retry_count = retry_count + 1
                        # Update status dict with retry count
                        updated_dict = {'first-run-gobblin-wait': 1, 'error-reported': 0, 'retries': retry_count,
                                        'lastlogtimestamp': ''}
                        # Save updated status dict to status file
                        self.save_status_vars(updated_dict)

                        # Check if retry count exceeded the max number of retries
                        if retry_count >= self.num_attempts:
                            # Update status flag "first-run-gobblin-wait" to 0 which notifies that initial check is done
                            updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                            'lastlogtimestamp': ''}
                            # Save updated dict to status file
                            self.save_status_vars(updated_dict)
                            # Report health metric "initial gobblin health" as ERROR
                            cause = "Initial Gobblin health check failed after max retries"
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health', [cause],
                                                      "ERROR"))
                    return
                # Enters only if status flag "first_run_gobblin_wait" is 0(Either initial gobblin check is OK or it
                # exceeded max retry count.
                else:
                    # Query Elasticsearch to check if there is any new generated log
                    gobblin_log_status = self.query_es()
                    if gobblin_log_status:
                        # Extract timestamp from the captured log
                        logtimestamp = gobblin_log_status[0]['_source']['@timestamp']
                        # Check if previous log timestamp and current logtimestamp are same
                        if lastlogtimestamp == logtimestamp:
                            # Report error(gobblin health) only if isn't reported previously(When timestamps are same)
                            if not error_report:
                                # Check if current log timestamp has crossed the cron interval specified.
                                ref_time = datetime.datetime.now() - datetime.timedelta(minutes=self.cron_interval)
                                logtimestamp = datetime.datetime(*map(int, re.split('[^\d]', logtimestamp)[:-1]))
                                '''
                                If the current log timestamp has crossed it's cron interval and still there is no new
                                log that has been generated, give few retry attempts to check if there is any new log.
                                '''
                                if (logtimestamp - ref_time).total_seconds() < 0:
                                    retry_count = retry_count + 1
				    # Update status dict with retry_count
                                    updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': retry_count,
                                                    'lastlogtimestamp': lastlogtimestamp}
				    # Save updated status dict to status file
                                    self.save_status_vars(updated_dict)
                                    # Check if it has exceeded maximum retries to report error.
                                    if retry_count >= self.num_attempts:
                                        cause = "Didn't receive new gobblin log even after maximum retries"
                                        # Report health metric gobblin health as error
                                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                                                  'gobblin.health', [cause], "ERROR"))
                                        # Update status flags "error-reported" to 1
                                        updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 1, 'retries': 0,
                                                        'lastlogtimestamp': lastlogtimestamp}
                                        # Save updated variables to status file
                                        self.save_status_vars(updated_dict)
                        # Enters if there is a new log that has been generated.
                        else:
                            # Update status flags to 0 and lastlogtimestamp to newly generated log timestamp.
                            updated_dict = {'first-run-gobblin-wait': 0, 'error-reported': 0, 'retries': 0,
                                            'lastlogtimestamp': logtimestamp}
                            self.save_status_vars(updated_dict)
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health',
                                                      [], "OK"))
                            # Check dataset content to see if data exists and also content matches
                            self.check_dataset_content(first_run_gobblin_wait)
                    # Didn't get any response from Elasticsearch related to gobblin logs.
                    else:
                        cause = "Error reading gobblin log for ELK"
                        # Report Gobblin and dataset health metrics as "UNAVAILABLE"
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'gobblin.health', [cause],
                                                  "UNAVAILABLE"))
                        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset', 'dataset.health', [cause],
                                                  "UNAVAILABLE"))
        except Exception as ex:
            LOGGER.exception("Exception while executing whole test flow due to %s", ex)

    def runner(self, args, display=True):
        """
        Main section.
        """
        plugin_args = args.split() \
            if args is not None and (len(args.strip()) > 0) \
            else ""

        options = self.read_args(plugin_args)
        self.data_service_ip = options.data_service.split(",")[0]
        self.cluster_name = options.cluster_name.split(",")[0]
        self.elk_ip = options.elk_ip.split(",")[0]
        self.cron_interval = int(options.cron_interval)
        self.num_attempts = int(options.num_attempts)
        self.gobblin_log_path = options.gobblin_log_path
        self.metric_console = options.metric_console.split(",")[0]
        self.master_dataset_dir = options.master_dataset_dir
        self.quarantine_dir = options.quarantine_dir
        self.console_user = options.console_user
        self.console_password = options.console_password

        # Form elasticsearch url and query to check if there is any gobblin related log from es response.
        self.uri = self.elk_ip + "/logstash-*/_search?pretty=true"
        self.query = '{"sort":[{"@timestamp":{"order":"desc"}}], "query":{"bool":{"must":[{"match":' \
                     '{"source":"gobblin"}}, {"match":{"path": "%s"}}, ' \
                     '{"match_phrase":{"message":"Shutting down the application"}}]}},"size":1}' % self.gobblin_log_path
        self.exec_test()
        if display:
            self._do_display(self.results)
        return self.results

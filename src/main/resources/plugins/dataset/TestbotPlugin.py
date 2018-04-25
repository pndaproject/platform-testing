"""
Copyright (c) 2018 Cisco and/or its affiliates.
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
    """
    Whitebox test plugin for Master Dataset Health
    """

    def __init__(self):
        self.display = False
        self.results = []
        self.runtesttopic = "avro.internal.testbot"
        self.dataset_name = "testbot"
        self.cluster_name = None
        self.data_service_ip = None
        self.cron_interval = None
        self.hdfs_url = None
        self.metric_console = None
        self.num_attempts = None
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
        """
            This class argument parser.
            This shall come from main runner in the extra arg
        """
        parser = argparse.ArgumentParser(
            prog=self.__class__.__name__,
            usage='%(prog)s [options]',
            description='Show state of Dataset cluster',
            add_help=False)
        parser.add_argument('--cron_interval', default=30,
                            help='CRON interval that has been set for Gobblin run: 30')
        parser.add_argument('--data_service', default='http://10.0.1.20:7000',
                            help='DATA_SERVICE IP ADDRESS')
        parser.add_argument('--cluster_name', default='cluster_name', help="PNDA CLUSTER NAME")
        parser.add_argument('--metric_console', default='localhost:3123',
                            help="Console metric IP address")
        parser.add_argument('--num_attempts', default=10,
                            help="retry count to check gobblin health")
        parser.add_argument('--master_dataset_dir',
                            default='/user/pnda/PNDA_datasets/datasets', help="dataset dir")
        parser.add_argument('--quarantine_dir',
                            default='/user/pnda/PNDA_datasets', help="quarantine dir")
        parser.add_argument('--console_user',
                            default='pnda', help="PNDA console username for PAM auth")
        parser.add_argument('--console_password',
                            default='pnda', help="PNDA console password for PAM auth")
        return parser.parse_args(args)

    def get_kafka_health(self):
        """
        Get Kafka health metric from KAFKA platform-testing
        """
        kafka_health = False
        creds = {'username': self.console_user, 'password': self.console_password.encode('base64')}
        url = self.metric_console + '/metrics/kafka.health'
        try:
            pam_resp = requests.post(self.metric_console + '/pam/login', data=creds)
            if pam_resp.status_code != 200:
                LOGGER.warning("Error while trying to access console by PAM auth")
                return kafka_health
            response = requests.get(url, cookies=pam_resp.cookies)
            if response.status_code == 200:
                kafka_health_dict = json.loads(response.text)
                if kafka_health_dict['currentData']['value'] == 'OK':
                    kafka_health = True
        except Exception as ex:
            LOGGER.exception('Failed to access kafka health: %s', str(ex))
        return kafka_health

    def check_dataset(self):
        """
        Check dataset creation
        """
        dataset_exists = False
        url = "%s" % (self.data_service_ip + '/api/v1/datasets/' + self.dataset_name)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                LOGGER.debug("Response for check_dataset API is successful with status 200")
                response_dict = json.loads(response.text)
                dataset_status = response_dict.get('status', '')
                if dataset_status == "success":
                    dataset_exists = True
        except Exception as ex:
            LOGGER.exception('Failed to check dataset creation: %s', str(ex))
        return dataset_exists

    def save_status_vars(self, status_dict):
        """
        Write status variables to a file to track each operation
        """
        try:
            with open(self.file_path, 'w') as status_file:
                status_file.write(json.dumps(status_dict))
        except Exception as ex:
            LOGGER.exception('Failed to update status variables: %s', str(ex))

    def read_file_contents(self):
        """
        Reading contents from a status file to track the status of each operation
        """
        contents = {}
        try:
            with open(self.file_path) as upd_file:
                contents = json.loads(upd_file.read())
        except IOError as ex:
            LOGGER.exception('Failed to open and read file: %s', str(ex))
        return contents

    def get_name_node(self):
        """
        Get NAME_NODE_IP to check HDFS directory/file status
        """
        name_node_status = False
        try:
            # Using socket module to get IP address of NAME NODE from given hostname
            name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-1')
            name_node_url = 'http://' + name_node_ip + ':50070' + \
                            '/webhdfs/v1/user/pnda?user.name=pnda&op=GETFILESTATUS'
            name_node_response = requests.get(name_node_url)
            if name_node_response.status_code == 200:
                self.hdfs_url = 'http://' + name_node_ip + ':50070'
                name_node_status = True
                LOGGER.debug("Found Ip address of name_node(hadoop-mgr-1) where webhdfs is running")
            # Get second NAME_NODE IP address if the response from the first NAME NODE is 403
            elif name_node_response.status_code == 403:
                name_node_ip = socket.gethostbyname(self.cluster_name + '-' + 'hadoop-mgr-2')
                name_node_url = 'http://' + name_node_ip + ':50070' + \
                                '/webhdfs/v1/user/pnda?user.name=pnda&op=GETFILESTATUS'
                new_node_response = requests.get(name_node_url)
                if new_node_response.status_code == 200:
                    self.hdfs_url = 'http://' + name_node_ip + ':50070'
                    name_node_status = True
                    LOGGER.debug("Found Ip address of name_node(hadoop-mgr-2)"
                                 "where webhdfs is running")
                else:
                    LOGGER.warning("Couldn't get any response from hadoop-mgr-2 node")
            else:
                LOGGER.warning("Couldn't get response from hadoop-mgr-1 node")
        except Exception as ex:
            LOGGER.exception('Failed to get name_node ip: %s', str(ex))
        return name_node_status

    def check_modified_time(self, dataset_data):
        """
        Get directory latest modified time and check if exists with in interval
        """
        dir_modified_time = False
        try:
            response_dict = json.loads(dataset_data.text)
            response_data = response_dict['FileStatus']
            if response_data:
                LOGGER.debug("Found file data regarding the given dataset")
                # Calculating current_time, cron interval time in milliseconds
                current_milli_time = int(time.time() * 1000)
                ref_time = int(current_milli_time - (self.cron_interval * 60000))
                modified_milli_time = response_data['modificationTime']
                # Checking whether the modified time lies in between before_time, now_time.
                if modified_milli_time in range(ref_time, current_milli_time):
                    LOGGER.debug("File has been modified recently")
                    dir_modified_time = True
        except Exception as ex:
            LOGGER.exception('Failed to check file modified time: %s', str(ex))
        return dir_modified_time

    def get_latest_modified_file(self, dataset_data):
        """
        Get file latest modified time and return file name if recently modified
        """
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
                        LOGGER.debug("File has been modified recently "
                                     "with in last cron interval time")
                        avro_file_name = avro_file.get('pathSuffix')
                        break
            else:
                LOGGER.warning("Didn't get any data related to the above file path")
        except Exception as ex:
            LOGGER.exception('Failed to get file name: %s', str(ex))
        return avro_file_name

    def check_dataset_dir(self):
        """
        Query WEBHDFS to check master dataset directory contents
        """
        hdfs_data_matched = False
        try:
            current_time = datetime.datetime.now()

            # Adding leading zeros if the value is single digit to
            # match the format of the directory path.
            self.year = current_time.year
            self.month = "%02d" % (current_time.month,)
            self.day = "%02d" % (current_time.day,)
            self.hour = "%02d" % (current_time.hour,)
            url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/hour=%s?'
                                   'user.name=pnda&op=LISTSTATUS') % \
                  (self.master_dataset_dir, self.dataset_name,
                   self.year, self.month, self.day, self.hour)
            dataset_response = requests.get(url)
            if dataset_response.status_code != 200:
                hour_before_time = datetime.datetime.now() - datetime.timedelta(hours=1)

                # Adding leading zeros if the value is single digit to
                # match the format of the directory path.
                self.year = hour_before_time.year
                self.month = "%02d" % (hour_before_time.month,)
                self.day = "%02d" % (hour_before_time.day,)
                self.hour = "%02d" % (hour_before_time.hour,)
                url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/hour=%s?'
                                       'user.name=pnda&op=LISTSTATUS') % \
                      (self.master_dataset_dir, self.dataset_name, self.year,
                       self.month, self.day, self.hour)
                dataset_response = requests.get(url)
                if dataset_response.status_code != 200:
                    return hdfs_data_matched
            # Checking whether the above url path file has been recently modified or not
            file_name = self.get_latest_modified_file(dataset_response)
            if not file_name:
                return hdfs_data_matched
            file_url = (self.hdfs_url + '/webhdfs/v1%s/source=%s/year=%s/month=%s/day=%s/'
                                        'hour=%s/%s?user.name=pnda&op=OPEN') % \
                       (self.master_dataset_dir, self.dataset_name, self.year,
                        self.month, self.day, self.hour, file_name)
            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                return hdfs_data_matched

            # Check if unique string 'avrodata' exists in the file response
            if 'avrodata' in file_response.content:
                hdfs_data_matched = True
        except Exception as ex:
            LOGGER.exception('Failed to query webhdfs: %s', str(ex))
        return hdfs_data_matched

    def check_quarantine_dir(self):
        """
        Check status of quarantine dataset directory
        """
        quar_data_matched = False
        try:
            current_time = datetime.datetime.now()

            # Adding leading zeros
            self.year = current_time.year
            self.month = "%02d" % (current_time.month,)
            self.day = "%02d" % (current_time.day,)
            url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s?'
                                   'user.name=pnda&op=LISTSTATUS') % \
                  (self.quarantine_dir, self.runtesttopic, self.year, self.month, self.day)
            quarantine_response = requests.get(url)
            if quarantine_response.status_code != 200:
                day_before_time = datetime.datetime.now() - datetime.timedelta(days=1)

                # Adding leading zeros
                self.year = day_before_time.year
                self.month = "%02d" % (day_before_time.month,)
                self.day = "%02d" % (day_before_time.day,)
                url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s?'
                                       'user.name=pnda&op=LISTSTATUS') % \
                      (self.quarantine_dir, self.runtesttopic, self.year, self.month, self.day)
                quarantine_response = requests.get(url)
                if quarantine_response.status_code != 200:
                    return quar_data_matched

            file_name = self.get_latest_modified_file(quarantine_response)
            if not file_name:
                return quar_data_matched
            # Open and read quarantine files to check if data matches with produced data
            file_url = (self.hdfs_url + '/webhdfs/v1%s/source_topic=%s/year=%s/month=%s/day=%s/%s?'
                                        'user.name=pnda&op=OPEN') % \
                       (self.quarantine_dir, self.runtesttopic, self.year,
                        self.month, self.day, file_name)
            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                return quar_data_matched
            # Check if unique string 'quardata' matches with the quarantine file response
            if 'quardata' in file_response.content:
                quar_data_matched = True
        except Exception as ex:
            LOGGER.exception('Failed to query webhdfs: %s', str(ex))
        return quar_data_matched

    def check_gobblin_content(self):
        gobblin_status = False
        gobblin_url = self.hdfs_url + '/webhdfs/v1/user/pnda/gobblin/work/metrics?' \
                                      'user.name=pnda&op=GETFILESTATUS'
        try:
            gobblin_response = requests.get(gobblin_url)
            if gobblin_response.status_code != 200:
                return gobblin_status
            gobblin_content = self.check_modified_time(gobblin_response)
            if gobblin_content:
                gobblin_status = True
        except Exception as ex:
            LOGGER.exception("Failed to check gobblin data:%s", str(ex))
        return gobblin_status

    def check_dataset_content(self):
        """
        Check dataset creation and content
        """
        analyse_status = MonitorStatus["green"]
        analyse_causes = []
        analyse_metric = 'dataset.health'
        try:
            if not self.check_dataset():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("Dataset doesn't exist")
                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                          analyse_metric, analyse_causes, analyse_status))
                return

            if not self.check_dataset_dir():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("HDFS dataset content check failed")
            if not self.check_quarantine_dir():
                analyse_status = MonitorStatus["red"]
                analyse_causes.append("HDFS quarantine content check failed")
        except Exception as ex:
            LOGGER.exception('Failed to check dataset creation and content: %s', str(ex))
            analyse_status = MonitorStatus["red"]
            analyse_causes.append("Failed to check dataset creation and content")
        self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                  analyse_metric, analyse_causes, analyse_status))
        return

    def exec_test(self):
        """
        Execute Gobblin and dataset health check
        """
        try:
            # Create a status file if not present to track the status of health checks
            if not os.path.isfile(self.file_path):
                status_dict = {}
                status_dict.update({'first-run-gobblin-wait': 1, 'retries': 0})
                with open(self.file_path, 'w') as new_file:
                    new_file.write(json.dumps(status_dict))

            # Read file contents if file is already present
            file_data = self.read_file_contents()
            first_run_gobblin_wait = file_data.get('first-run-gobblin-wait', 1)
            retry_count = file_data.get('retries', 0)

            # Check Kafka health before proceeding to gobblin and dataset health check
            kafka_health = self.get_kafka_health()
            if kafka_health:
                # Get Name_Node Ip to query WebHDFS
                if not self.get_name_node():
                    LOGGER.error("Couldn't get NameNode IP Address")
                    cause = "Couldn't get NameNode IP Address"
                    self.results.append(Event(TIMESTAMP_MILLIS(),
                                              'master-dataset', 'gobblin.health', [cause], "ERROR"))
                    return
                # Initial Gobblin health check(Wait until it finishes it's max retries)
                if first_run_gobblin_wait:
                    # Query HDFS to check if mr.pull job exists in gobblin dir
                    gobblin_status = self.check_gobblin_content()
                    if gobblin_status:
                        # Update status dict with newly generated timestamp
                        updated_dict = {'first-run-gobblin-wait': 0, 'retries': 0}
                        # Save updated status dict to status file
                        self.save_status_vars(updated_dict)
                        # Report initial gobblin health metric
                        self.results.append(Event(TIMESTAMP_MILLIS(),
                                                  'master-dataset', 'gobblin.health', [], "OK"))
                    # Retry querying WebHDFS until it meets the max retries count
                    else:
                        retry_count = retry_count + 1
                        # Update status dict with retry count
                        updated_dict = {'first-run-gobblin-wait': 1, 'retries': retry_count}
                        # Save updated status dict to status file
                        self.save_status_vars(updated_dict)

                        # Check if retry count exceeded the max number of retries
                        if retry_count >= self.num_attempts:
                            # Update status flag "first-run-gobblin-wait" to 0
                            # which notifies that initial check is done
                            updated_dict = {'first-run-gobblin-wait': 0, 'retries': 0}
                            # Save updated dict to status file
                            self.save_status_vars(updated_dict)
                            # Report health metric "initial gobblin health" as ERROR
                            cause = "Initial Gobblin health check failed after max retries"
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                                      'gobblin.health', [cause], "ERROR"))
                    return

                # Enters if initial gobblin check is OK or it exceeded max retry count
                else:
                    # Check Gobblin & Dataset Health at post 5 minutes delay to cron_interval
                    current_time = datetime.datetime.now()
                    gobblin_post_interval_1 = (self.cron_interval * 2) % 60
                    gobblin_post_interval_2 = self.cron_interval + 5
                    if current_time.minute == gobblin_post_interval_1 or \
                            current_time.minute == gobblin_post_interval_2:
                        gobblin_status = self.check_gobblin_content()
                        if not gobblin_status:
                            retry_count = retry_count + 1
                            # Update status dict with retry_count
                            updated_dict = {'first-run-gobblin-wait': 0, 'retries': retry_count}
                            # Save updated status dict to status file
                            self.save_status_vars(updated_dict)
                            # Check if it has exceeded maximum retries to report error.
                            if retry_count >= self.num_attempts:
                                cause = "Gobblin run failed"
                                # Report health metric gobblin health as error
                                self.results.append(Event(TIMESTAMP_MILLIS(),
                                                          'master-dataset', 'gobblin.health', [cause], "ERROR"))
                        else:
                            updated_dict = {'first-run-gobblin-wait': 0, 'retries': 0}
                            self.save_status_vars(updated_dict)
                            self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                                      'gobblin.health', [], "OK"))
                            # Check dataset content to see if data exists and also content matches
                            self.check_dataset_content()
            else:
                LOGGER.error("Kafka Health: Error")
                cause = "Kafka Health: Error"
                self.results.append(Event(TIMESTAMP_MILLIS(), 'master-dataset',
                                          'gobblin.health', [cause], "ERROR"))
        except Exception as ex:
            LOGGER.exception('Failed to check Gobblin and dataset health: %s', str(ex))
        return

    def runner(self, args, display=True):
        """
        Main section
        """
        plugin_args = args.split() \
            if args is not None and (len(args.strip()) > 0) \
            else ""

        options = self.read_args(plugin_args)
        self.data_service_ip = options.data_service.split(",")[0]
        self.cluster_name = options.cluster_name.split(",")[0]
        self.cron_interval = int(options.cron_interval)
        self.num_attempts = int(options.num_attempts)
        self.metric_console = options.metric_console.split(",")[0]
        self.master_dataset_dir = options.master_dataset_dir
        self.quarantine_dir = options.quarantine_dir
        self.console_user = options.console_user
        self.console_password = options.console_password

        self.exec_test()
        if display:
            self._do_display(self.results)
        return self.results

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
Purpose:    Master_Dataset tests
"""

"""
DataSetWhiteBox Testing
"""

import json
import time
import datetime
import logging
import sys
import socket
import requests

# Constants
sys.path.insert(0, "../..")
LOGGER = logging.getLogger("TestbotPlugin")
now_year = None
now_month = None
now_day = None
now_hour = None
before_year = None
before_month = None
before_day = None
before_hour = None
hdfs_url = None


# Check dataset creation whether it exists or not
def check_dataset(ip_data_service):
    url = "%s" % (ip_data_service + '/api/v1/datasets')
    try:
        response = requests.get(url)
        if response.status_code is 200:
            LOGGER.debug("Response for check_dataset API is successful with status 200")
            response_dict = json.loads(response.text)
            data_list = response_dict.get('data', [])
            if data_list:
                for item in data_list:
                    # Checking whether dataset name matches from the list of id's
                    if item['id'] == 'testbot':
                        LOGGER.debug("testbot exists in the list of datasets")
                        dataset_exists = True
                        return dataset_exists
            LOGGER.warning("Dataset doesn't exist in the whole list")
            dataset_exists = False
        else:
            LOGGER.warning("Response for check_dataset API isn't successful")
            dataset_exists = False
    except Exception as e:
        LOGGER.error("Exception while running check dataset function due to %s" % e)
        dataset_exists = False
    return dataset_exists


# Get NAME_NODE_IP to check HDFS directory/file status
def get_name_node(cluster_name):
    try:
        global hdfs_url
	# Using socket module to get IP address of NAME NODE from given hostname
        name_node_ip = socket.gethostbyname(cluster_name + '-' + 'hadoop-mgr-1')
        name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&op=GETFILESTATUS'
        name_node_response = requests.get(name_node_url)
        if name_node_response.status_code == 200:
            hdfs_url = 'http://' + name_node_ip + ':50070'
            name_node = True
            LOGGER.debug("Found Ip address of name_node(hadoop-mgr-1) where webhdfs is running")
	# Get second NAME_NODE IP address if the response from the first NAME NODE is 403
        elif name_node_response.status_code == 403:
            name_node_ip = socket.gethostbyname(cluster_name + '-' + 'hadoop-mgr-2')
            name_node_url = 'http://' + name_node_ip + ':50070' + '/webhdfs/v1/user/pnda?user.name=pnda&op=GETFILESTATUS'
            new_node_response = requests.get(name_node_url)
            if new_node_response.status_code is 200:
                hdfs_url = 'http://' + name_node_ip + ':50070'
                name_node = True
                LOGGER.debug("Found Ip address of name_node(hadoop-mgr-2) where webhdfs is running")
            else:
                name_node = False
                LOGGER.warning("Couldn't get any response from hadoop-mgr-2 node")
        else:
            name_node = False
            LOGGER.warning("Couldn't get any response from hadoop-mgr-1 node")
    except Exception as e:
        LOGGER.error("Exception while executing create_name_node_url due to %s" % e)
        name_node = False
    return name_node


# Check response of an HTTP request and return response content only if response is 200
def check_dir(url):
    try:
        response = requests.get(url)
        if response.status_code is 200:
            LOGGER.debug("Response is successful from check_dir API")
            status_response = response
        else:
            LOGGER.warning("Response isn't successful from check_dir API")
            status_response = ''
    except Exception as e:
        LOGGER.error("Exception while executing check_dir function due to %s" % e)
        status_response = ''
    # If the response status_code is 200 it will return the response data otherwise it just returns an empty string.
    return status_response


# Check whether file has been modified recently with in the given time
def get_latest_modified_file(dataset_data):
    avro_file_name = ''
    try:
        response_dict = json.loads(dataset_data.text)
        file_data = response_dict['FileStatuses']['FileStatus']
        if file_data:
            LOGGER.debug("Found file data regarding the given dataset")
            for avro_file in file_data:
                # Calculating current_time, last_30minutes_time in milliseconds because modified time is given in ms.
                current_milli_time = int(time.time() * 1000)
                last_30_min_time = int(current_milli_time - 30*60000)
                modified_milli_time = avro_file.get('modificationTime')

                # Checking whether the modified time lies in between before_time, now_time.
                if modified_milli_time in range(last_30_min_time, current_milli_time):
                    LOGGER.debug("File has been modified recently with in last 30 minutes")
                    avro_file_name = avro_file.get('pathSuffix')
                    return avro_file_name
        else:
            LOGGER.warning("Didn't get any data related to the above file path")
    except Exception as e:
        LOGGER.error("Checking HDFS modified time status failed due to exception %s" % e)
    return avro_file_name


# Check dataset directory to see whether file has been updated with latest data and also if data matches
def check_dataset_dir():
    hdfs_data_matched = False
    try:
        # Getting current time in datetime format and extarcting each variable from it to form a directory path.
        current_time = datetime.datetime.now()

        # Adding leading zeros if the value is single digit to match the format of the directory path.
        global now_year, now_month, now_day, now_hour
        now_year = current_time.year
        now_month = "%02d" % (current_time.month,)
        now_day = "%02d" % (current_time.day,)
        now_hour = "%02d" % (current_time.hour,)

        # Creating the whole url path by combining all the variables fetched above.
        url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/year=%s/month=%s/'
                          'day=%s/'
                          'hour=%s?user.name=pnda&op=LISTSTATUS') % (now_year, now_month, now_day, now_hour)
        # Checking whether the above url path exists or not.
        data = check_dir(url)
        if data:
            LOGGER.debug("Url path exists")
            # Checking whether the above url path file has been recently modified or not.
            file_name = get_latest_modified_file(data)
            if file_name:
		# Open and read file to check if the data matches with the produced data
                file_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/year=%s/month=%s/'
                                       'day=%s/hour=%s/%s?user.name=pnda&op=OPEN') % \
                           (now_year, now_month, now_day, now_hour, file_name)
                file_response = check_dir(file_url)
                if file_response:
		    # Check if unique string 'avrodata' exists in the file response
                    if 'avrodata' in file_response.content:
                        hdfs_data_matched = True
        else:
            # If the above url path doesn't exist, we create a new url path with hour before datetime.
	    # Whenever hour changes gobblin is writing it's newly arrived data in the previous hour file.
            LOGGER.warning("Url path doesn't exist so trying to create a new one with hour before datetime")

            # Getting one hour before time from the current time
            hour_before = datetime.datetime.now() - datetime.timedelta(hours=1)

            # Adding leading zeros if the value is single digit to match the format of the directory path.
            global before_year, before_month, before_day, before_hour
            before_year = hour_before.year
            before_month = "%02d" % (hour_before.month,)
            before_day = "%02d" % (hour_before.day,)
            before_hour = "%02d" % (hour_before.hour,)

            # Creating an hour_before_url to check whether this url exists or not.
            hour_before_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/'
                                          'year=%s/'
                                          'month=%s/day=%s/hour=%s?user.name=pnda&op=LISTSTATUS') % \
                              (before_year, before_month, before_day, before_hour)

            # Checking whether the new hour_before url exists or not.
            new_data = check_dir(hour_before_url)
            if new_data:
                LOGGER.debug("Hour_before url path exists")
                # Checking whether the new url path file has been modified recently or not.
                file_name = get_latest_modified_file(new_data)
	        # Open and read a file to check if data matches
                if file_name:
                    file_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/datasets/source=testbot/'
                                                  'year=%s/'
                                                  'month=%s/day=%s/hour=%s/%s?user.name=pnda&op=OPEN') % \
                                      (before_year, before_month, before_day, before_hour, file_name)
		    # Check if unique string 'avrodata' matches with the response data
                    file_response = check_dir(file_url)
                    if file_response:
                        if 'avrodata' in file_response.content:
                            hdfs_data_matched = True
            else:
                LOGGER.warning("Hour_before url path doesn't exists")
    except Exception as e:
        LOGGER.error("Exception while executing hdfs_status function due to %s" % e)
    return hdfs_data_matched


# Checking the status of HDFS quarantine directory by calling check_dir and check_modified_time functions.
def check_quarantine_dir():
    quar_data_matched = False
    try:
        # Forming quarantine url using the same datetime variables which have been used to check hdfs_status.
        quar_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/source_topic=avro.internal.testbot/'
                               'year=%s/'
                               'month=%s/day=%s?user.name=pnda&op=LISTSTATUS') % (now_year, now_month, now_day)

        # Checking whether the url path exists
        data = check_dir(quar_url)
        if data:
            LOGGER.debug("Quarantine Url path exists")
            # Checking whether the above url path file has been recently modified or not.
            quar_file_name = get_latest_modified_file(data)
            if quar_file_name:
		# Open and read quarantine files to check if data matches with produced data
                quar_file_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/'
                                            'source_topic=avro.internal.testbot/year=%s/month=%s/day=%s/%s?'
                                            'user.name=pnda&op=OPEN') % (now_year, now_month, now_day, quar_file_name)
                quar_file_response = check_dir(quar_file_url)
                if quar_file_response:
		    # Check if unique string 'quardata' matches with the quarantine file response
                    if 'quardata' in quar_file_response.content:
                        quar_data_matched = True
        else:
            # Since the above url doesn't exist, creating a new url with hour before time.
            day_before_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/'
                                         'source_topic=avro.internal.testbot/'
                                         'year=%s/month=%s/day=%s?user.name=pnda&op=LISTSTATUS') % \
                             (before_year, before_month, before_day)

            # Checking whether the new day before quarantine url exists or not.
            new_data = check_dir(day_before_url)
            if new_data:
                LOGGER.debug("Day before Quarantine url path exists")
                # Checking whether the above url path file has been recently modified or not.
                quar_file_name = get_latest_modified_file(new_data)
                if quar_file_name:
		    # Open and read quarantine file to check if data matches
                    quar_file_url = (hdfs_url + '/webhdfs/v1/user/pnda/PNDA_datasets/quarantine/'
                                                 'source_topic=avro.internal.testbot/'
                                                 'year=%s/month=%s/day=%s/%s?user.name=pnda&op=OPEN') % \
                                     (before_year, before_month, before_day, quar_file_name)
                    quar_file_response = check_dir(quar_file_url)
                    if quar_file_response:
		        # Check if unique string 'quardata' matches with the quarantine file response
                        if 'quardata' in quar_file_response.content:
                            quar_data_matched = True
            else:
                LOGGER.warning("Day before Quarantine url path doesn't exists")
    except Exception as e:
        LOGGER.error("Exception while executing quarantine_status due to %s" % e)
    return quar_data_matched

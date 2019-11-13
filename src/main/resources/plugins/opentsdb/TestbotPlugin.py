"""
OpenTSDBWhiteBox Testing
"""
import argparse
import json
import time
import logging
import sys
import requests
from requests.utils import quote
from prettytable import PrettyTable
from pnda_plugin import PndaPlugin, Event, MonitorStatus

#Constants
METRIC_NAME = "tsd.host"
METRIC_VAL = 1
TAGK = "host"
TAGV = "tsd.host"
UID_EXISTS = "Name already exists with UID"
DELETE_ENABLED_STATUS = "Deleting data is not enabled"

TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
sys.path.insert(0, "../..")
TESTBOTPLUGIN = lambda: OpenTSDBWhiteBox()
LOGGER = logging.getLogger("TESTBOTPLUGIN")

class OpenTSDBWhiteBox(PndaPlugin):
    """
    OpenTSDBWhiteBox
    """
    def __init__(self):
        self.hosts = []
        self.results = []
        self.cause = []
        self.test_start_timestamp = None

    def read_args(self, args):
        """
        Program argument parser
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--hosts", default="10.0.1.68:4242,10.0.1.102:4242", \
                            help="The Hostname with port to pass a api query.", type=str)
        return parser.parse_args(args)

    def process_resp(self, msg, operation, status, index):
        """
        process response get from requests
        """
        metric = "%s.%d.%s" % (METRIC_NAME, index, operation)
        self.results.append(Event(TIMESTAMP_MILLIS(), "opentsdb", metric, msg, status))
        if status == "0":
            self.cause.extend(msg)
            metric = "%s.%d.%s" % (METRIC_NAME, index, "health")
            analyse_status = MonitorStatus["red"]
            self.results.append(Event(TIMESTAMP_MILLIS(), "opentsdb", metric, msg, analyse_status))

    def api_stats(self, host, index):
        """
        Api endpoint stats
        """
        msg = []
        url = "%s%s%s" % ("http://", host, "/api/stats")
        try:
            response = requests.post(url)
            if response.status_code == 200:
                response_dict = json.loads(response.text)
                for item in response_dict:
                    stat_metric_tag_val = ""
                    for key in item.keys():
                        if isinstance(item[key], dict):
                            for subkey in item[key].keys():
                                if subkey != "host":
                                    if not stat_metric_tag_val:
                                        stat_metric_tag_val = "%s.%s" % (subkey, item[key][subkey])
                                    else:
                                        stat_metric_tag_val = "%s.%s.%s" % (stat_metric_tag_val, \
                                        subkey, item[key][subkey])
                    if not stat_metric_tag_val:
                        metric = "%s.%d.%s" % (METRIC_NAME, index, item["metric"])
                    else:
                        metric = "%s.%d.%s.%s" % (METRIC_NAME, index, stat_metric_tag_val, \
                        item["metric"])
                    self.results.append(Event(TIMESTAMP_MILLIS(), \
                    "opentsdb", metric, [], item["value"]))
                return True
            response_dict = json.loads(response.text)
            LOGGER.warning("Unable to fetch stats data error message is %s", \
            response_dict["error"]["message"])
            msg.append(response_dict["error"]["message"])
            self.process_resp(msg, "STATS", "0", index)
            return False
        except requests.exceptions.ConnectionError as ex_message:
            LOGGER.warning("Unable to fetch stats data error message is %s", str(ex_message))
            self.process_resp([str(ex_message)], "STATS", "0", index)
            return False

    def create_uid(self, host, index):
        """
        Create UID for metric, tag_key and tag_value
        """
        msg = []
        operation = "WRITE"
        url = "%s%s%s" % ("http://", host, "/api/uid/assign")
        payload = {"metric": [METRIC_NAME], "tagk": [TAGK], "tagv": ["%s.%d" % (TAGV, index)]}
        headers = {"content-type": "application/json"}
        m_uuid = False
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            if response.status_code == 200:
                LOGGER.debug("UID's created for metric, tag_key and tag_value")
                m_uuid = True
            else:
                response_dict = json.loads(response.text)
                if response.status_code == 400:
                    response_keys = response_dict.keys()
                    if "tagk_errors" in response_keys:
                        if any(UID_EXISTS in ele for ele in response_dict.\
                        get("tagk_errors").values()):
                            LOGGER.debug("tagk %s value already exist", TAGK)
                            m_uuid = True
                        else:
                            LOGGER.warning("tagk has error value %s", \
                            response_dict.get("tagk_errors").get(TAGK))
                            msg.append(response_dict.get("tagk_errors").get(TAGK))
                            self.process_resp(msg, operation, "0", index)
                    if "tagv_errors" in response_keys:
                        if any(UID_EXISTS in ele for ele in response_dict.\
                        get("tagv_errors").values()):
                            LOGGER.debug("tagv %s value already exist", "%s.%d" % (TAGV, index))
                            m_uuid = True
                        else:
                            LOGGER.warning("tagv has error value %s", \
                            response_dict.get("tagv_errors").get("%s.%d" % (TAGV, index)))
                            msg.append(response_dict.get("tagv_errors").get("%s.%d" % \
                            (TAGV, index)))
                            self.process_resp(msg, operation, "0", index)
                    if "metric_errors" in response_keys:
                        if any(UID_EXISTS in ele for ele in response_dict.\
                        get("metric_errors").values()):
                            LOGGER.debug("metric %s value already exist", METRIC_NAME)
                            m_uuid = True
                        else:
                            LOGGER.warning("metric has error value %s", \
                            response_dict.get("metric_errors").get(METRIC_NAME))
                            msg.append(response_dict.get("metric_errors").get(METRIC_NAME))
                            self.process_resp(msg, operation, "0", index)
                else:
                    LOGGER.warning("Unable to create UID's for metric, tagk and tagv, \
                    error message is %s", response_dict["error"]["message"])
                    msg.append(response_dict["error"]["message"])
                    self.process_resp(msg, operation, "0", index)
        except requests.exceptions.ConnectionError as ex_message:
            LOGGER.warning("Unable to create UID's for metric, tagk and tagv, \
            error message is %s", str(ex_message))
            self.process_resp([str(ex_message)], operation, "0", index)
        return m_uuid

    def write(self, host, index):
        """
        Data will be inserted into tsdb table
        """
        msg = []
        operation = "WRITE"
        if not self.create_uid(host, index):
            return False
        url = "%s%s%s" % ("http://", host, "/api/put")
        payload = {"metric": METRIC_NAME, "timestamp": TIMESTAMP_MILLIS(), \
        "value": METRIC_VAL, "tags":{TAGK: "%s.%d" % (TAGV, index)}}
        headers = {"content-type": "application/json"}
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            if response.status_code == 204:
                LOGGER.debug("Value 1 inserted to metric %s", METRIC_NAME)
                self.process_resp([], operation, "1", index)
                return True
            response_dict = json.loads(response.text)
            msg.append(response_dict["error"]["message"])
            LOGGER.warning("Unable to write 1, error message is %s", \
            response_dict["error"]["message"])
            self.process_resp(msg, operation, "0", index)
            return False
        except requests.exceptions.ConnectionError as ex_message:
            LOGGER.warning("Unable to write 1, error message is %s", str(ex_message))
            self.process_resp([str(ex_message)], operation, "0", index)
            return False

    def read(self, host, index):
        """
        Inserted data will be read
        """
        msg = []
        operation = "READ"
        url = "%s%s%s" % ("http://", host, "/api/query")
        payload = {"start": self.test_start_timestamp, "end": TIMESTAMP_MILLIS(), "queries": [{"aggregator": "none", \
        "metric": METRIC_NAME, "tags": {TAGK: "%s.%d" % (TAGV, index)}}]}
        headers = {"content-type": "application/json"}
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            if response.status_code == 200:
                LOGGER.debug("Value read in metric %s", METRIC_NAME)
                self.process_resp([], operation, "1", index)
                return True
            response_dict = json.loads(response.text)
            LOGGER.warning("unable to read in metric %s and error message is %s", \
            METRIC_NAME, response_dict["error"]["message"])
            msg.append(response_dict["error"]["message"])
            self.process_resp(msg, operation, "0", index)
            return False
        except requests.exceptions.ConnectionError as ex_message:
            LOGGER.warning("unable to read in metric %s and error message is %s", \
            METRIC_NAME, str(ex_message))
            self.process_resp([str(ex_message)], operation, "0", index)
            return False

    def delete(self, host, index):
        """
        Delete the data
        """
        msg = []
        operation = "DELETE"
        url = "%s%s%s?%s=%s&m=none:%s{%s=%s.%d}" % ("http://", host, "/api/query", "start", \
        str(self.test_start_timestamp), METRIC_NAME, TAGK, TAGV, index)
        en_url = quote(url, safe='?=&/:')
        try:
            response = requests.delete(en_url)
            if response.status_code == 200:
                LOGGER.debug("Delete value in metric %s", METRIC_NAME)
                self.process_resp([], operation, "1", index)
                return True
            else:
                response_dict = json.loads(response.text)
                if  response.status_code == 400:
                    if DELETE_ENABLED_STATUS in response.text:
                        LOGGER.debug("Unable to delete value in metric %s and \
                        error message is %s", METRIC_NAME, response_dict["error"]["details"])
                        msg.append(response_dict["error"]["details"])
                        self.process_resp(msg, operation, "1", index)
                        return True
                LOGGER.warning("Unable to delete value in metric %s and error message is %s", \
                METRIC_NAME, response_dict["error"]["message"])
                msg.append(response_dict["error"]["message"])
                self.process_resp(msg, operation, "0", index)
                return False
        except requests.exceptions.ConnectionError as ex_message:
            LOGGER.warning("Unable to delete value in metric %s and error message is %s", \
            METRIC_NAME, str(ex_message))
            self.process_resp([str(ex_message)], operation, "0", index)
            return False

    def exec_test(self):
        """
        Starting the test
        """
        self.test_start_timestamp = TIMESTAMP_MILLIS()
        index = -1
        for host in self.hosts:
            index += 1
            LOGGER.debug("Test started in host %s", host)
            if self.api_stats(host, index):
                if self.write(host, index):
                    time.sleep(5)
                    if self.read(host, index):
                        if self.delete(host, index):
                            metric = "%s.%d.%s" % (METRIC_NAME, index, "health")
                            analyse_status = MonitorStatus["green"]
                            self.results.append(Event(TIMESTAMP_MILLIS(), "opentsdb", \
                            metric, [], analyse_status))
                            LOGGER.debug("Test finished in host %s", host)
        ok_c, ko_c = self.analyze_results(self.results)
        self.results.append(Event(self.test_start_timestamp, "opentsdb", "tsd.hosts", \
        [], self.hosts))
        self.results.append(Event(self.test_start_timestamp, "opentsdb", "tsd.hosts.ok", \
        [], ok_c))
        self.results.append(Event(self.test_start_timestamp, "opentsdb", "tsd.hosts.ko", \
        [], ko_c))
        if ko_c == 0:
            overall_status = MonitorStatus["green"]
        else:
            if ok_c == 0:
                overall_status = MonitorStatus["red"]
            else:
                overall_status = MonitorStatus["amber"]
        self.results.append(Event(self.test_start_timestamp, "opentsdb", "%s.%s" % \
        ("opentsdb", "health"), self.cause, overall_status))
        LOGGER.debug("Overall test on all host finished")
        return self.results

    def analyze_results(self, results):
        """
        Analyze ok and ko status on hosts
        """
        ok_c = 0
        ko_c = 0
        for row in results:
            if "opentsdb.health" not in row[2] and ".health" in row[2]:
                if row[4] == "ERROR":
                    ko_c += 1
                else:
                    ok_c += 1
        return ok_c, ko_c

    def do_display(self, results, hosts):
        """
        Pretty display
        """
        LOGGER.debug("do_display start")
        print("%s%s%s" % ("-"*72, " Status ", "-"*72))
        table = PrettyTable(["Timestamp", "Source", "Metric", "Cause", "Value"])
        table.align = "l"
        for row in results:
            if "tsd.hosts" in row[2]:
                table.add_row([row[0], row[1], row[2], row[3], row[4]])
        for row in results:
            if '.health' not in row[2] and "tsd.hosts" not in row[2]:
                table.add_row([row[0], row[1], row[2], row[3], row[4]])
        print(table)
        ok_c, ko_c = self.analyze_results(self.results)
        print("%s%s%s" % ("-"*72, " Summary ", "-"*72))
        row_format = "{0:>1}{1:<30}{2:<40}"
        rows = ""
        rows += row_format.format("", *["No of hosts", (ok_c + ko_c)])
        rows += "\n" + row_format.format("", *["List of hosts", hosts])
        rows += "\n" + row_format.format("", *["No of hosts(ok)", ok_c])
        rows += "\n" + row_format.format("", *["No of hosts(ko)", ko_c])
        rows += "\n" + row_format.format("", *["-"*62, ""])
        print(rows)
        print("%s%s%s" % ("-"*68, " Overall Status ", "-"*68))
        table = PrettyTable(["Timestamp", "Source", "Metric", "Cause", "Value"])
        table.align = "l"
        for row in results:
            if ".health" in row[2]and "tsd.hosts" not in row[2]:
                table.add_row([row[0], row[1], row[2], row[3], row[4]])
        print(table)

    def runner(self, args, display=True):
        """
        Main section.
        """
        plugin_args = args.split() \
            if args is not None and args.strip() \
            else ""

        options = self.read_args(plugin_args)
        self.hosts = options.hosts.split(",")
        results = self.exec_test()
        if display:
            self.do_display(results, options.hosts)
        return results

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

Purpose:    Runs a series of tests on HDFS to check health

"""

import time
import threading
import argparse
import logging
import traceback
import subprocess
import happybase
from Hbase_thrift import AlreadyExists
from pnda_plugin import PndaPlugin
from pnda_plugin import Event

LOGGER = logging.getLogger("TESTBOTPLUGIN")
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)
TESTBOTPLUGIN = lambda: HadoopBlackboxPlugin()

class HadoopBlackboxPlugin(PndaPlugin):
    '''
    For each service run PNDA blackbox tests and also query HDFS for its view of that service.
    Aggregate the results together to one result - OK, WARN or ERROR - as well as returning
    the results of the explicit tests and in the case of problems, the list of causes from
    the blackbox tests and HDFS combined
    '''
    def read_args(self, args):
        '''
        This class argument parser.
        This shall come from main runner in the extra arg
        '''
        parser = argparse.ArgumentParser(prog=self.__class__.__name__, usage='%(prog)s [options]',
                                         description='Key metrics from CDH cluster')
        parser.add_argument('--hdfshost', default='localhost', help='HDFS host e.g. localhost')
        parser.add_argument('--hdfsport', default='8020', help='HDFS thrift port e.g. 8020')
        parser.add_argument('--hbasehost', default='localhost', help='HBase host e.g. localhost')
        parser.add_argument('--hbaseport', default=20550, help='HBase port e.g. 20550')
        parser.add_argument('--hivehost', default="localhost", help='Hive host e.g. 10.0.0.2')
        parser.add_argument('--hiveport', default=10001, help='Hive port e.g. 10001')
        parser.add_argument('--impalaport', default=21050, help='Impala port e.g. 21050')

        return parser.parse_args(args)

    def runner(self, args, display=True):
        values = []
        health_values = []

        plugin_args = args.split() \
                    if args is not None and args.strip() \
                    else ""

        options = self.read_args(plugin_args)

        hbase = None

        def run_test_sequence():
            test_value = 'un1eqV4lu3'
            # pylint: disable=too-many-return-statements
            hbase = happybase.Connection(host=options.hbasehost, port=int(options.hbaseport))
            if abort_test_sequence is True:
                return
            reason = []
            try:
                start = TIMESTAMP_MILLIS()

                try:
                    hbase.create_table('blackbox_test_table', {'cf': dict()})
                    logging.debug("test table created")
                except AlreadyExists:
                    logging.debug("test table exists")

                table = hbase.table('blackbox_test_table')
                end = TIMESTAMP_MILLIS()
                create_table_ok = True
                create_table_ms = end-start
                values.append(Event(TIMESTAMP_MILLIS(),
                                    'HBASE',
                                    "hadoop.HBASE.create_table_time_ms",
                                    [],
                                    create_table_ms))
            except:
                LOGGER.error(traceback.format_exc())
                create_table_ok = False
                reason = ['Create HBase table operation failed']
            health_values.append(Event(TIMESTAMP_MILLIS(),
                                       'HBASE',
                                       "hadoop.HBASE.create_table_succeeded",
                                       reason,
                                       create_table_ok))

            #write some data to it
            if abort_test_sequence is True:
                return
            reason = []
            try:
                start = TIMESTAMP_MILLIS()
                table.put('row_key', {'cf:column': test_value})
                end = TIMESTAMP_MILLIS()
                write_hbase_ok = True
                write_hbase_ms = end-start
                values.append(Event(TIMESTAMP_MILLIS(),
                                    'HBASE',
                                    "hadoop.HBASE.write_time_ms",
                                    [],
                                    write_hbase_ms))
            except:
                LOGGER.error(traceback.format_exc())
                write_hbase_ok = False
                reason = ['Failed to insert row in HBase table']
            health_values.append(Event(TIMESTAMP_MILLIS(),
                                       'HBASE',
                                       "hadoop.HBASE.write_succeeded",
                                       reason,
                                       write_hbase_ok))

            #read some data from it
            if abort_test_sequence is True:
                return
            reason = []
            try:
                start = TIMESTAMP_MILLIS()
                row = table.row('row_key', columns=['cf:column'])
                end = TIMESTAMP_MILLIS()
                read_hbase_ms = end-start
                print(row)
                read_hbase_ok = row[b'cf:column'].decode() == test_value
                values.append(Event(TIMESTAMP_MILLIS(),
                                    'HBASE',
                                    "hadoop.HBASE.read_time_ms",
                                    [],
                                    read_hbase_ms))
            except:
                LOGGER.error(traceback.format_exc())
                # hbase_fix_output = subprocess.check_output(['sudo',
                #                                             '-u',
                #                                             'hbase',
                #                                             'hbase',
                #                                             'hbck',
                #                                             '-repair',
                #                                             'blackbox_test_table'])
                # for line in hbase_fix_output.splitlines():
                #     if 'Status:' in line or 'inconsistencies detected' in line:
                #         LOGGER.debug(line)
                # subprocess.check_output(['sudo',
                #                          '-u',
                #                          'hbase',
                #                          'hbase',
                #                          'zkcli',
                #                          'rmr',
                #                          '/hbase/table/blackbox_test_table'])
                # subprocess.check_output(['sudo',
                #                          '-u',
                #                          'hdfs',
                #                          'hadoop',
                #                          'fs',
                #                          '-rm',
                #                          '-r',
                #                          '-f',
                #                          '-skipTrash',
                #                          '/hbase/data/default/blackbox_test_table'])

                read_hbase_ok = False
                reason = ['Failed to fetch row by row key from HBase']

            health_values.append(Event(TIMESTAMP_MILLIS(),
                                       'HBASE',
                                       "hadoop.HBASE.read_succeeded",
                                       reason,
                                       read_hbase_ok))

            #create some hive metadata
            if abort_test_sequence is True:
                return
            # reason = []
            # try:
            #     start = TIMESTAMP_MILLIS()
            #     create_table = """CREATE EXTERNAL TABLE blackbox_test_table (key STRING, value STRING)
            #                       STORED BY \"org.apache.hadoop.hive.hbase.HBaseStorageHandler\"
            #                       WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf:column\") TBLPROPERTIES(\"hbase.table.name\" = \"blackbox_test_table\")"""
            #     hive_output = run_hive_query(create_table)
            #     logging.debug("hive metadata created")
            #     end = TIMESTAMP_MILLIS()
            #     create_metadata_ms = end-start
            #     create_metadata_ok = True
            #     values.append(Event(TIMESTAMP_MILLIS(),
            #                         cdh.get_name('HIVE'),
            #                         "hadoop.HIVE.create_metadata_time_ms",
            #                         [],
            #                         create_metadata_ms))
            # except:
            #     LOGGER.error(traceback.format_exc())
            #     create_metadata_ok = False
            #     reason = ['CREATE EXTERNAL TABLE statement failed on Hive Metastore']
            # health_values.append(Event(TIMESTAMP_MILLIS(),
            #                            cdh.get_name('HIVE'),
            #                            "hadoop.HIVE.create_metadata_succeeded",
            #                            reason,
            #                            create_metadata_ok))

            # #read some data via impala using it
            # if abort_test_sequence is True:
            #     return

            # if cdh.get_impala_endpoint() is not None:
            #     reason = []
            #     try:
            #         start = TIMESTAMP_MILLIS()
            #         impala = connect(host=cdh.get_impala_endpoint(), port=options.impalaport)
            #         end = TIMESTAMP_MILLIS()
            #         impala.cursor().execute("invalidate metadata")
            #         connect_to_impala_ms = end-start
            #         connect_to_impala_ok = True
            #         values.append(Event(TIMESTAMP_MILLIS(),
            #                             cdh.get_name('IMPALA'),
            #                             "hadoop.IMPALA.connection_time_ms",
            #                             [],
            #                             connect_to_impala_ms))
            #     except:
            #         LOGGER.error(traceback.format_exc())
            #         connect_to_impala_ok = False
            #         reason = ['Failed to connect to Impala']
            #     health_values.append(Event(TIMESTAMP_MILLIS(),
            #                                cdh.get_name('IMPALA'),
            #                                "hadoop.IMPALA.connection_succeeded",
            #                                reason,
            #                                connect_to_impala_ok))

            #     if abort_test_sequence is True:
            #         return
            #     reason = []
            #     try:
            #         start = TIMESTAMP_MILLIS()
            #         impala_cursor = impala.cursor()
            #         impala_cursor.execute("SELECT * FROM blackbox_test_table")
            #         table_contents = impala_cursor.fetchall()
            #         end = TIMESTAMP_MILLIS()
            #         read_impala_ms = end-start
            #         read_impala_ok = table_contents[0][1] == test_value
            #         values.append(Event(TIMESTAMP_MILLIS(),
            #                             cdh.get_name('IMPALA'),
            #                             "hadoop.IMPALA.read_time_ms",
            #                             [],
            #                             read_impala_ms))
            #     except:
            #         LOGGER.error(traceback.format_exc())
            #         read_impala_ok = False
            #         reason = ['Failed to SELECT from Impala']
            #     health_values.append(Event(TIMESTAMP_MILLIS(),
            #                                cdh.get_name('IMPALA'),
            #                                "hadoop.IMPALA.read_succeeded",
            #                                reason,
            #                                read_impala_ok))
            # else:
            #     reason = []
            #     try:
            #         start = TIMESTAMP_MILLIS()
            #         hive_output = run_hive_query("SELECT * FROM blackbox_test_table")
            #         end = TIMESTAMP_MILLIS()
            #         read_hive_ms = end-start
            #         read_hive_ok = test_value in hive_output
            #         values.append(Event(TIMESTAMP_MILLIS(),
            #                             cdh.get_name('HQUERY'),
            #                             "hadoop.HQUERY.read_time_ms",
            #                             [],
            #                             read_hive_ms))
            #     except:
            #         LOGGER.error(traceback.format_exc())
            #         read_hive_ok = False
            #         reason = ['Failed to SELECT from Hive']
            #     health_values.append(Event(TIMESTAMP_MILLIS(),
            #                                cdh.get_name('HQUERY'),
            #                                "hadoop.HQUERY.read_succeeded",
            #                                reason,
            #                                read_hive_ok))

            # #delete metadata
            # if abort_test_sequence is True:
            #     return
            # reason = []
            # try:
            #     start = TIMESTAMP_MILLIS()
            #     hive_output = run_hive_query("DROP TABLE blackbox_test_table")
            #     end = TIMESTAMP_MILLIS()
            #     drop_metadata_ms = end-start
            #     drop_metadata_ok = True
            #     values.append(Event(TIMESTAMP_MILLIS(),
            #                         cdh.get_name('HIVE'),
            #                         "hadoop.HIVE.drop_table_time_ms",
            #                         [],
            #                         drop_metadata_ms))
            # except:
            #     LOGGER.error(traceback.format_exc())
            #     drop_metadata_ok = False
            #     reason = ['Failed to DROP table in Hive Metastore']
            # health_values.append(Event(TIMESTAMP_MILLIS(),
            #                            cdh.get_name('HIVE'),
            #                            "hadoop.HIVE.drop_table_succeeded",
            #                            reason,
            #                            drop_metadata_ok))

            #delete hbase table
            if abort_test_sequence is True:
                return
            reason = []
            try:
                start = TIMESTAMP_MILLIS()
                # Disabled deleting table to work around apparent hbase bug (see VPP-17) but leaving
                # test step in so it can be easily re-enabled for testing.
                hbase.disable_table('blackbox_test_table')
                hbase.delete_table('blackbox_test_table')
                end = TIMESTAMP_MILLIS()
                drop_table_ms = end-start
                drop_table_ok = True
                values.append(Event(TIMESTAMP_MILLIS(),
                                    'HBASE',
                                    "hadoop.HBASE.drop_table_time_ms",
                                    [],
                                    drop_table_ms))
            except:
                LOGGER.error(traceback.format_exc())
                drop_table_ok = False
                reason = ['Failed to drop table in HBase']
            health_values.append(Event(TIMESTAMP_MILLIS(),
                                       'HBASE',
                                       "hadoop.HBASE.drop_table_succeeded",
                                       reason,
                                       drop_table_ok))

        def run_hive_query(query):
            beeline_output = subprocess.check_output([
                "beeline",
                "-u", "jdbc:hive2://%s:%s/;transportMode=http;httpPath=cliservice" % (options.hivehost, options.hiveport),
                "-e",
                query])
            logging.debug(beeline_output)
            return beeline_output

        def to_status(flag):
            '''
            Convert True to OK and False to ERROR
            '''
            if flag in [True, False]:
                status = 'OK' if flag is True else 'ERROR'
            else:
                status = flag

            return status

        def default_health_value(name, service, operation, failed_step):
            '''
            Check health value
            '''
            result = False
            if not ([event for event in health_values if event.metric == name]):
                if failed_step is not None:
                    message = 'Did not attempt to %s due to timeout waiting for: %s' % (operation,
                                                                                        failed_step)
                else:
                    message = 'Timed out waiting for %s to complete' % operation

                health_values.append(Event(TIMESTAMP_MILLIS(),
                                           service,
                                           name,
                                           [message],
                                           False))
                result = True
            return result

        test_thread = threading.Thread(target=run_test_sequence)
        test_thread.daemon = True
        abort_test_sequence = False
        test_thread.start()
        test_thread.join(60.0)
        abort_test_sequence = True
        if hbase is not None:
            hbase.close()
        failed_step = None
        if default_health_value("hadoop.HBASE.create_table_succeeded",
                                "HBASE",
                                "create HBase table",
                                failed_step) and failed_step is None:
            failed_step = "create HBase table"
        if default_health_value("hadoop.HBASE.write_succeeded",
                                "HBASE",
                                "write to HBase",
                                failed_step) and failed_step is None:
            failed_step = "write to HBase"
        if default_health_value("hadoop.HBASE.read_succeeded",
                                "HBASE",
                                "read from HBase",
                                failed_step) and failed_step is None:
            failed_step = "read from HBase"
        # if default_health_value("hadoop.HIVE.create_metadata_succeeded",
        #                         "HIVE",
        #                         "create Hive Metastore table", failed_step) and failed_step is None:
        #     failed_step = "create Hive Metastore table"
        # if cdh.get_impala_endpoint() is not None:
        #     if default_health_value("hadoop.IMPALA.connection_succeeded",
        #                             "IMPALA",
        #                             "connect to Impala", failed_step) and failed_step is None:
        #         failed_step = "connect to Impala"
        #     if default_health_value("hadoop.IMPALA.read_succeeded",
        #                             "IMPALA",
        #                             "SELECT from Impala", failed_step) and failed_step is None:
        #         failed_step = "SELECT from Impala"
        # else:
        #     if default_health_value("hadoop.HQUERY.read_succeeded",
        #                             "HQUERY",
        #                             "SELECT from Hive", failed_step) and failed_step is None:
        #         failed_step = "SELECT from Hive"
        # if default_health_value("hadoop.HIVE.drop_table_succeeded",
        #                         "HIVE",
        #                         "DROP table in Hive Metastore",
        #                         failed_step) and failed_step is None:
        #     failed_step = "DROP table in Hive Metastore"
        if default_health_value("hadoop.HBASE.drop_table_succeeded",
                                "HBASE",
                                "drop table in HBase", failed_step) and failed_step is None:
            failed_step = "drop table in HBase"

        # cdh_status_indicators = cdh.get_status_indicators()
        # health_values.extend(cdh_status_indicators)
        overall = {}
        for health_val in health_values:
            try:
                current = overall[health_val.source]
                current_val = to_status(current.value)
                current_causes = current.causes
            except KeyError:
                current_val = 'OK'
                current_causes = []

            update = to_status(health_val.value)

            # If current is ERROR, output is ERROR, regardless
            # If current is WARN, output is WARN if update is OK but ERROR if further WARN or ERROR
            # If update is OK, output is OK if OK, WARN if WARN and ERROR if ERROR

            out = 'ERROR'
            if current_val != "ERROR":
                if current_val == 'WARN':
                    if update == 'OK':
                        out = 'WARN'
                if current_val == 'OK':
                    out = update
            current_val = out
            current_causes.extend(health_val.causes)

            overall[health_val.source] = Event(health_val.timestamp,
                                               health_val.source,
                                               'hadoop.%s.health' % health_val.source,
                                               current_causes,
                                               current_val)

        values.extend(health_values)
        values.extend(overall.values())

        if display:
            self._do_display(values)

        return values

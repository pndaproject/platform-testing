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
Purpose:    Plugin for producer & consumer test to/from Kafka
"""

import io
import time
import datetime
import random
import logging
import avro.schema
import avro.io

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

from plugins.common.defcom import TestbotResult

LOGGER = logging.getLogger("TestbotPlugin")
TIMESTAMP_MILLIS = lambda: int(time.time() * 1000)

class Prod2Cons(object):
    '''
    Implements blackbox producer & consumer test to/from Kafka
    '''
    def __init__(self, host, port, schema_path, topic, nbmsg, consumer_timeout):
        self.topic = topic
        self.nbmsg = nbmsg
        self.sent_msg = 0
        self.host = host
        self.port = port
        self.sent = [-100] * self.nbmsg
        self.rcv = [-100] * self.nbmsg
        self.runtag = str(random.randint(10, 100000))
        try:
            self.broker = KafkaClient("%s:%d" % (self.host, self.port))
        except:
            raise ValueError(
                "KafkaClient (%s:%d) - init failed" % (self.host, self.port))
        try:
            self.producer = SimpleProducer(self.broker)
        except:
            raise ValueError(
                "SimpleProducer (%s:%d) - init failed" % (self.host, self.port))
        try:
            self.consumer = SimpleConsumer(
                self.broker, "testbot", topic, iter_timeout=consumer_timeout)
        except:
            raise ValueError(
                "SimpleConsumer (%s:%d) - init failed" % (self.host, self.port))
        try:
            self.schema = avro.schema.parse(open(schema_path).read())
        except:
            raise ValueError(
                "Prod2Cons load schema (%s) - init failed" % (schema_path))

    def add_sent(self, index):
        '''
           add a datetime now event
        '''
        self.sent[index] = datetime.datetime.now()

    def add_rcv(self, index):
        '''
           add a datetime now event
        '''
        self.rcv[index] = datetime.datetime.now()

    def average_ms(self):
        '''
           compute average between sent / rcv values
        '''
        result = 0
        for i in range(len(self.sent)):
            delta = (self.rcv[i] - self.sent[i])
            result += int(delta.total_seconds() * 1000)  # milliseconds
        return int(result / len(self.sent))

    def prod(self):
        '''
           The test producer
        '''
        LOGGER.debug("prod2cons - start producer")
        writer = avro.io.DatumWriter(self.schema)
        for i in xrange(self.nbmsg):
            rawdata = "%s|%s" % (self.runtag, str(i))
            quar_data = "%s|%s" % (self.runtag, str(i))
            # Generating unique data for dataset health check
            if i is 5:
                rawdata = "%s|%s|%s" % (self.runtag, str(i), 'testavrodata')
                quar_data = "%s|%s|%s" % (self.runtag, str(i), 'quardata')
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write({"timestamp": TIMESTAMP_MILLIS(),
                          "src": "testbot",
                          "host_ip": "localhost",
                          "rawdata": rawdata},
                         encoder)
            raw_bytes = bytes_writer.getvalue()
            self.add_sent(i)
            self.producer.send_messages(self.topic, raw_bytes)
            self.producer.send_messages(self.topic, quar_data)
            self.sent_msg += 2
        return 0

    def consumer_reset(self):
        '''
           Indicate to restart from the most recent offset
        '''
        self.consumer.seek(0, 2)

    def cons(self):
        '''
           Run the consumer and return a test result struct
        '''
        LOGGER.debug("prod2cons - start consumer")
        readcount = 0
        readvalid = 0
        readnotvalid = 0
        avg_ms = -1
        # time.sleep(2) added for a local test for checking lond delay display
        for message in self.consumer:
            readcount += 1
            try:
                newmessage = message[1][3]
                if newmessage.split('|')[0] == self.runtag:
                    readvalid +=1
                    self.add_rcv(int(newmessage.split('|')[1]))
                    continue

                bytes_reader = io.BytesIO(newmessage)
                decoder = avro.io.BinaryDecoder(bytes_reader)
                reader = avro.io.DatumReader(self.schema)
                msg = reader.read(decoder)
                rawsplit = msg['rawdata'].split('|')
                if rawsplit[0] == self.runtag:
                    readvalid += 1
                    self.add_rcv(int(rawsplit[1]))
                else:
                    readnotvalid += 1
                    LOGGER.error("consumer  reads unexpected message [%s] - runtag is [%s]",
                                 msg['rawdata'],
                                 self.runtag)

            except:
                LOGGER.error("prod2cons - consumer failed")
                raise Exception("consumer failed")

        if readcount == self.nbmsg and readvalid == self.nbmsg:
            LOGGER.debug("consumer : test run ok")
            avg_ms = self.average_ms()
        return TestbotResult(self.sent_msg, readvalid, readnotvalid, avg_ms)

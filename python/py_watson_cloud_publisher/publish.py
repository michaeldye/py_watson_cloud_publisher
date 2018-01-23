#!/usr/bin/env python3
# -*- coding: latin-1 -*-
""" Cache and publish ts data """

__author__ = 'Christopher Dye <dyec@us.ibm.com>'
__copyright__ = '2018 IBM'
__license__ = 'Apache License 2.0'

import math
import os
import sys
import logging
import multiprocessing as mp
import copy

from logging import config
from datetime import datetime

import simplejson as json

import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt

from cloudant.client import Cloudant

REDACTED_PASS_EMPTY = '<empty>'
REDACTED_PASS_SET = '<set but redacted>'

class CachePublisher(object):
    """ cache member stores [{<record>}] where record is augmented with timestamp upon ingestion by this lib. Tries to publish every 10 seconds to each destination. Clears own cache after publishing (successfully or unsuccessfully). """

    def _logger_setup(self):
        try:
            sys.stdout.write(str.format("\nAttempting to open logging.ini config file from directory: {}", os.path.join(os.path.dirname(__file__))))
            logging.config.fileConfig(os.path.join(os.path.dirname(__file__), 'logging.ini'))
            sys.stdout.write("\nLogging configuration success\n")
            self.logger = logging.getLogger(__name__)

            self.logger.info("Logging configuration success")
        except Exception as ex:
            raise SystemError(str.format("Required logging config file logging.ini could not be found. Exception {}", repr(ex)))

    def __init__(self, logger, mqtt_pub_interval, mqtt_pub_max, cloudant_pub_interval, cloudant_pub_max, mqtt_connector_opts, cloudant_connector_opts):
        #
        # {
        #   <ts>: {
        #     datum: {},
        #     mpub: False,
        #     cpub: False,
        #   },
        # }
        self.cache = {}

        # set up multiprocess queue
        self._ctx = mp.get_context('fork')
        self._queue = self._ctx.Queue()

        if not logger:
            self._logger_setup()
        else:
            self.logger = logger

        # TODO: add some error handling for incoming args

        # bookkeeping
        self._mqtt_last_pub = datetime.utcnow()
        self._mqtt_pub_interval = mqtt_pub_interval
        # the maximum number of records to publish per (mqtt_pub_interval) seconds to this destination
        self._mqtt_pub_max = mqtt_pub_max

        self._cloudant_last_pub = datetime.utcnow()
        self._cloudant_pub_interval = cloudant_pub_interval
        self._cloudant_pub_max = cloudant_pub_max

        self.mqtt_hostname = mqtt_connector_opts['hostname']
        self.mqtt_port = mqtt_connector_opts['port']
        self.mqtt_client_id = mqtt_connector_opts['client_id']
        self.mqtt_auth = mqtt_connector_opts['auth']
        self.mqtt_tls = mqtt_connector_opts['tls']
        self.mqtt_topic = mqtt_connector_opts['topic']

        self.cloudant_username = cloudant_connector_opts['username']
        self.cloudant_password = cloudant_connector_opts['password']
        self.cloudant_url = cloudant_connector_opts['url']

        def sani_auth_fn(auth):
            auth["password"] = REDACTED_PASS_EMPTY if not auth['password'] else REDACTED_PASS_SET
            return auth

        self.logger.info(str.format("CachePublisher configuration for mqtt:: mqtt_pub_interval: {}, mqtt_pub_max: {}; mqtt_hostname: {}, mqtt_port: {}, mqtt_client_id: {}, mqtt_auth: {}, mqtt_tls: {}, mqtt_topic: {}", self._mqtt_pub_interval, self._mqtt_pub_max, self.mqtt_hostname, self.mqtt_port, self.mqtt_client_id, sani_auth_fn(copy.deepcopy(self.mqtt_auth)), self.mqtt_tls, self.mqtt_topic))
        self.logger.info(str.format("CachePublisher configuration for cloudant:: cloudant_pub_interval: {}, cloudant_pub_max: {}; cloudant_username: {}, cloudant_password: {}, cloudant_url: {}", self._cloudant_pub_interval, self._cloudant_pub_max, self.cloudant_username, REDACTED_PASS_EMPTY if not self.cloudant_password else REDACTED_PASS_SET, self.cloudant_url))

    def _collect_data_and_pub(self, now, last_pub, pub_interval, pub_max, publisher):
        elapsed = (now-last_pub).total_seconds()
        if elapsed > pub_interval:
            dist_factor = math.ceil(len(self.cache) / (pub_max * (elapsed / pub_interval)))
            self.logger.debug('Dist calc from length: %d, given elapsed time: %s. Dist factor: %d', len(self.cache), elapsed, dist_factor)

            # just a list of data (the values from self.cache) where the publishing function's name is not in pub_by
            filtered_cache = dict({k:v for (k,v) in self.cache.items() if publisher.__qualname__ not in v['pub_by']})
            self.logger.debug('filtered_cache: %s', filtered_cache)

            # data in sub is proper subset of filtered_cache cache with key still ts
            sub = dict([p for idx, p in enumerate(filtered_cache.items()) if (idx % dist_factor == 0)])
            self.logger.debug('sub: %s', sub)

            #self.logger.debug('Publish interval reached. Calculated distribution factor: %f. Sublist: %s', dist, sub)
            cloned = copy.deepcopy([v["datum"] for (k,v) in  sub.items()])
            self.logger.debug('cloned: %s', cloned)

            # keys only for lookup convenience
            cloned_ts = sub.keys()
            self.logger.debug('cloned_ts: %s', cloned_ts)

            # mark all pub_by with publisher name in self.cache; if it was in cloned subset
            for k,v in self.cache.items():
                if k in cloned_ts:
                    self.cache[k]['pub_by'].append(publisher.__qualname__)
            self.logger.debug('updated cache: %s', self.cache)

            if len(cloned) == 0:
                self.logger.info('No data to publish, skipping starting publisher processes')
            else:
                process = self._ctx.Process(target=publisher, args=(cloned,))
                process.daemon = True
                process.start() # all forked processes will get joined at parent process shutdown
                self.logger.debug('Started publishing subprocess with cloned list (%d) given (%d)', len(cloned), len(self.cache))

            # return time to update
            return now
            # self.logger.debug('Complete cache (%d): %s', len(self.cache), self.cache)
        else:
            return last_pub

    def write_and_pub(self, datum):
        """ Data is an object suitable for serialization using json. Upon write the data will be stamped with the time with microsecond resolution. This is a blocking function: it will not return until cache can be safely written to again."""
        now = datetime.utcnow()
        nowStr = now.isoformat() # for inclusion in the json d.s.

        if not isinstance(datum, dict):
            tb.sys.exec_info()[2]
            raise TypeError(str.format("Invalid data input type: {}", type(data))).with_traceback(tb)

        datum["ts"] = nowStr
        self.cache[nowStr] = {'datum': datum, 'pub_by': []}

        self.logger.info('Added datum: %s. Cache size: %d', datum, len(self.cache))

        # so we know when to clear the cache
        longest_interval, longest_last_pub = (self._mqtt_pub_interval,self._mqtt_last_pub) if (self._mqtt_pub_interval > self._cloudant_pub_interval) else (self._cloudant_pub_interval,self._cloudant_last_pub)

        self._mqtt_last_pub = self._collect_data_and_pub(now, self._mqtt_last_pub, self._mqtt_pub_interval, self._mqtt_pub_max, self._publish_mqtt)

        self._cloudant_last_pub = self._collect_data_and_pub(now, self._cloudant_last_pub, self._cloudant_pub_interval, self._cloudant_pub_max, self._publish_cloudant)

        longest_elapsed = (now-longest_last_pub).total_seconds()
        if longest_elapsed > longest_interval:
            self.cache.clear()

    def _publish_cloudant(self, data):
        """ Intended to be called automatically by write_and_pub on configured interval. """

        try:
            self.logger.debug(str.format('Publishing data to cloudant: {}', data))
            client = Cloudant(self.cloudant_username, self.cloudant_password, url=self.cloudant_url, connect=True, auto_renew=True)

            testdb = client['test']
            testdb.bulk_docs(data)

            self.logger.info("Sent %d records to cloudant", len(data))
        except Exception as ex:
            self.logger.exception(ex)
        finally:
            client.disconnect()


    def _publish_mqtt(self, data):
        """ Intended to be called automatically by write_and_pub on configured interval. """

        qos = 0 # gonna send lots of data, need to not overwhelm ingest service

        try:
            # instantiate a new connector client on each invocation; we're expecting the cache delay time to be large enough that this isn't a problem
            msgs = list(map(lambda r: ({'topic': self.mqtt_topic, 'payload': json.dumps(r), 'qos': qos, 'retain': False}), data))
            self.logger.debug(str.format('Publishing data w/ common topic {}. Data: {}', self.mqtt_topic, msgs))

            publish.multiple(msgs=msgs, hostname=self.mqtt_hostname, port=self.mqtt_port, client_id=self.mqtt_client_id, keepalive=60, will=None, auth=self.mqtt_auth, tls=self.mqtt_tls, protocol=mqtt.MQTTv311, transport="tcp")

            self.logger.info("Sent %d records to mqtt broker for publishing", len(data))

        except Exception as ex:
            self.logger.exception(ex)

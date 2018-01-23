#!/usr/bin/env python3
# -*- coding: latin-1 -*-
""" Cache and publish ts data """

from py_watson_cloud_publisher import publish
import time
import sys
import traceback
import logging

def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    mqtt_pub_interval = 10

    mqtt_records_interval_max_publish = 20

    cloudant_pub_interval = 20

    cloudant_records_interval_max_publish = 30

    mqtt_connection_opts = {
        'hostname': '',
        'port': ,
        'client_id': '',
        'topic': '',
        'auth': {
            'username': '',
            'password': '',
        },
        'tls': {
            'ca_certs': '<full path to pem-encoded x509 certs file>',
        },
    }

    cloudant_connection_opts = {
        'username': '',
        'password': '',
        'url': '',
    }

    p = publish.CachePublisher(logging.getLogger('CachePublisher'), mqtt_pub_interval, mqtt_records_interval_max_publish, cloudant_pub_interval, cloudant_records_interval_max_publish, mqtt_connection_opts, cloudant_connection_opts)

    while True:
        p.write_and_pub({"f": 1, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 2, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 3, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 4, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 5, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 6, "g": 2})
        time.sleep(2)
        p.write_and_pub({"f": 7, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 8, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 9, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 10, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 11, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 12, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 13, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 14, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 15, "g": 2})
        time.sleep(1)
        p.write_and_pub({"f": 16, "g": 2})
        time.sleep(2)
        p.write_and_pub({"f": 17, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 18, "g": 2})
        time.sleep(.05)
        p.write_and_pub({"f": 19, "g": 2})


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        sys.stderr.write(str.format("\nError: {}", ex))
        traceback.print_exception()
    finally:
        # fix code so we get here only on signals like TERM, KILL
        sys.exit(1)

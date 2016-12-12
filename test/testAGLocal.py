#!/usr/bin/env python

"""
   Copyright 2016 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

"""
Plugin-Test
"""

import time
import traceback
import threading
import os
import imp
import sys
import json

from types import ModuleType

# import packages under src/
test_dirpath = os.path.dirname(os.path.abspath(__file__))
driver_root = os.path.dirname(test_dirpath)
src_root = os.path.join(driver_root, "src")
sys.path.append(src_root)

class syndicate_util_gateway(ModuleType):
    @classmethod
    def log_error(module, msg):
        print msg

    @classmethod
    def make_metadata_command(module, op, f_type, mode, size, path, write_ttl=0):
        return {
            "op": op,
            "f_type": f_type,
            "mode": mode,
            "size": size,
            "path": path,
            "write_ttl": write_ttl
        }

    @classmethod
    def crawl(module, cmd):
        print "> crawl"
        print cmd
        return 0 # success

    @classmethod
    def request_path(module, request):
        return request.path

    @classmethod
    def path_join(module, path1, path2):
        return os.path.join(path1, path2)

    @classmethod
    def request_byte_offset(module, request):
        return request.byte_offset

    @classmethod
    def request_byte_len(module, request):
        return request.byte_len

class syndicate_util(ModuleType):
    gateway = syndicate_util_gateway

class syndicate(ModuleType):
    util = syndicate_util

def load_config_secrets(cs_dir):
    if not os.path.exists(cs_dir):
        return None, None

    config_data = None
    with open(cs_dir + "/config") as conf:
        config_data = json.load(conf)

    secret_data = None
    with open(cs_dir + "/secrets") as secret:
        secret_data = json.load(secret)

    return config_data, secret_data

def findDriver():
    driver_path = os.path.abspath(src_root + "/sgfsdriver/ag_driver/driver")
    if os.path.exists(driver_path):
        return imp.load_source("driver",
                               driver_path)
    else:
        return None

def setup_test(dataset_path):
    #override syndicate.util.gateway package
    sys.modules["syndicate"] = syndicate
    sys.modules["syndicate.util"] = syndicate_util
    sys.modules["syndicate.util.gateway"] = syndicate_util_gateway

    #override sys.argv
    sys.argv[1] = "crawl"

    #create a temp directory
    if not os.path.exists(dataset_path):
        os.mkdir(dataset_path)

def main():
    if len(sys.argv) != 2:
        print "Usage: %s <config/secrets dir>" % sys.argv[0]
        return

    cs_dir = sys.argv[1]

    CONFIG, SECRETS = load_config_secrets(cs_dir)
    if CONFIG is None:
        print "cannot find config"
        return

    if SECRETS is None:
        print "cannot find secrets"
        return

    setup_test(CONFIG["DATASET_DIR"])

    driver = findDriver()

    dinit = driver.driver_init(CONFIG, SECRETS)
    if not dinit:
        print("cannot init driver")
        sys.exit(1)

    try:
        while driver.next_dataset(CONFIG, SECRETS):
            pass
    except:
        print "shutting down"

    if dinit:
        driver.driver_shutdown()


if __name__ == "__main__":
    main()

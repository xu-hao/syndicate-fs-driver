#!/usr/bin/python

"""
   Copyright 2016 The Trustees of Princeton University

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
import os
import sys
import json

# import packages under src/
test_dirpath = os.path.dirname(os.path.abspath(__file__))
driver_root = os.path.dirname(test_dirpath)
src_root = os.path.join(driver_root, "src")
sys.path.append(src_root)

from sagfsdriver.lib.pluginloader import pluginloader

dataset_dir = None
path_queue = []
fs = None

ENTRY_UPDATED = 0
ENTRY_ADDED = 1
ENTRY_REMOVED = 2

CONFIG = {
   "DATASET_DIR":       "/tmp/test_local",
   "EXEC_FMT":          "/usr/bin/python -m syndicate.ag.gateway",
   "DRIVER":            "syndicate.ag.drivers.fs",
   "DRIVER_FS_PLUGIN":  "local",
   "DRIVER_FS_PLUGIN_CONFIG": {}
}

SECRETS = {}

def _initFS( driver_config, driver_secrets, scan_dataset=False ):
    global fs
    global dataset_dir

    if fs:
        return True

    # continue only when fs is not initialized
    if not driver_config.has_key('DRIVER_FS_PLUGIN'):
        print("No DRIVER_FS_PLUGIN defined")
        return False

    if not driver_config.has_key('DRIVER_FS_PLUGIN_CONFIG'):
        print("No DRIVER_FS_PLUGIN_CONFIG defined")
        return False

    if not driver_config.has_key('DATASET_DIR'):
        print("No DATASET_DIR defined")
        return False

    dataset_dir = driver_config['DATASET_DIR']
    dataset_dir = "/" + dataset_dir.strip("/")

    plugin = driver_config['DRIVER_FS_PLUGIN']

    if isinstance(driver_config['DRIVER_FS_PLUGIN_CONFIG'], dict):
        plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
    elif isinstance(driver_config['DRIVER_FS_PLUGIN_CONFIG'], basestring):
        json_plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
        plugin_config = json.loads(json_plugin_config)

    plugin_config["secrets"] = driver_secrets
    plugin_config["dataset_root"] = dataset_dir

    try:
        loader = pluginloader()
        fs = loader.load(plugin, plugin_config)

        if not fs:
            print("No such driver plugin found: %s" % plugin )
            return False

        fs.set_notification_cb(datasets_update_cb)
        fs.connect(scan_dataset)
    except Exception as e:
        print("Unable to initialize a driver")
        print(str(e))
        traceback.print_exc()
        return False
    return True

def _shutdownFS():
    global fs
    if fs:
        try:
            fs.close()
        except Exception as e:
            pass
    fs = None

def datasets_update_cb(updated_entries, added_entries, removed_entries):
    for u in updated_entries:
        entry = {}
        entry["flag"] = ENTRY_UPDATED
        entry["stat"] = u
        path_queue.append(entry)
        print "Updated -", entry

    for a in added_entries:
        entry = {}
        entry["flag"] = ENTRY_ADDED
        entry["stat"] = a
        path_queue.append(entry)
        print "Added -", entry

    for r in removed_entries:
        entry = {}
        entry["flag"] = ENTRY_REMOVED
        entry["stat"] = r
        path_queue.append(entry)
        print "Removed -", entry

def driver_init( driver_config, driver_secrets ):
    """
    Do the one-time driver setup.
    """

    global fs
    global dataset_dir

    if not _initFS( driver_config, driver_secrets, True ):
        print("Unable to init filesystem")
        return False

    if not fs.exists( dataset_dir ):
        print("No such file or directory: %s" % dataset_dir )
        return False 

    if not fs.is_dir( dataset_dir ):
        print("Not a directory: %s" % dataset_dir )
        return False

    return True


def driver_shutdown():
    """
    Do the one-time driver shutdown
    """
    _shutdownFS()
    

def next_dataset():
    """
    Return the next dataset command for the AG to process.
    Should block until there's data.

    Must call gateway.crawl() to feed the data into the AG.
    Return True if there are more datasets to process.
    Return False if not.
    """

    global path_queue

    next_stat = None

    # find the next file or directory 
    while True:

        next_stat = None
        if len(path_queue) > 0:
            next_stat = path_queue[0]
            path_queue.pop(0)

        if next_stat is None:
            # no more data
            # stop calling this method
            return False

        flag = next_stat["flag"]
        stat = next_stat["stat"]

        if not stat.path.startswith(dataset_dir):
            print("Not belong to a dataset: %s" % stat.path )
            continue

        publish_path = stat.path[len(dataset_dir):]
        
        cmd = None

        if stat.directory:
            # directory 
            if flag == ENTRY_UPDATED or flag == ENTRY_ADDED:
                print("> put directory %s" % publish_path)
            elif flag == ENTRY_REMOVED:
                print("> delete directory %s" % publish_path)
        else:
            # file 
            if flag == ENTRY_UPDATED or flag == ENTRY_ADDED:
                print("> put file %d %s" % (stat.size, publish_path))
            elif flag == ENTRY_REMOVED:
                print("> delete file %d %s" % (stat.size, publish_path))

        continue

    # have more data
    return True

def setup_test(dataset_path):
    pass

def main():
    global CONFIG
    global SECRETS

    setup_test(CONFIG["DATASET_DIR"])

    dinit = driver_init(CONFIG, SECRETS)
    if not dinit:
        print("cannot init driver")
        sys.exit(1)

    while next_dataset():
        pass

    print("waiting 20 secs")
    # do something
    time.sleep(20)

    while next_dataset():
        pass

    if dinit:
        driver_shutdown()


if __name__ == "__main__":
    main()


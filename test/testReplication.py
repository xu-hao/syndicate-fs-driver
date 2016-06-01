#!/usr/bin/env python

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
Replication module test
"""

import time
import traceback
import threading
import errno
import os
import io
import imp
import sys
import json

# import packages under src/
test_dirpath = os.path.dirname(os.path.abspath(__file__))
driver_root = os.path.dirname(test_dirpath)
src_root = os.path.join(driver_root, "src")
sys.path.append(src_root)

import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.lib.replication as replication

from sgfsdriver.lib.pluginloader import pluginloader

storage_dir = None
fs = None

def _initFS( driver_config, driver_secrets, role ):
    global fs
    global storage_dir
    global block_replication

    if fs:
        return True

    # continue only when fs is not initialized
    if not driver_config.has_key( 'DRIVER_FS_PLUGIN' ):
        print "No DRIVER_FS_PLUGIN defined"
        return False

    if not driver_config.has_key( 'DRIVER_FS_PLUGIN_CONFIG' ):
        print "No DRIVER_FS_PLUGIN_CONFIG defined"
        return False

    if not driver_config.has_key('STORAGE_DIR'):
        print "No STORAGE_DIR defined"
        return False

    storage_dir = driver_config['STORAGE_DIR']
    storage_dir = "/" + storage_dir.strip("/")

    plugin = driver_config['DRIVER_FS_PLUGIN']

    if isinstance( driver_config['DRIVER_FS_PLUGIN_CONFIG'], dict ):
        plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
    elif isinstance( driver_config['DRIVER_FS_PLUGIN_CONFIG'], basestring ):
        json_plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
        plugin_config = json.loads( json_plugin_config )

    plugin_config["secrets"] = driver_secrets
    plugin_config["work_root"] = storage_dir

    try:
        loader = pluginloader()
        fs = loader.load( plugin, plugin_config, role )

        if not fs:
            print "No such driver plugin found: %s" % plugin
            return False

        fs.connect()
    except Exception as e:
        print "Unable to initialize a driver"
        print str( e )
        traceback.print_exc()
        return False
    return True

def _shutdownFS():
    global fs
    if fs:
        try:
            fs.close()
        except Exception:
            pass
    fs = None

def driver_init( driver_config, driver_secrets ):
    """
    Do the one-time driver setup.
    """

    global fs
    global storage_dir

    role = abstractfs.afsrole.WRITE
    if not _initFS( driver_config, driver_secrets, role ):
        print "Unable to init filesystem"
        return False

    if not fs.exists( "/" ):
        print "No such file or directory: %s" % storage_dir
        return False

    if not fs.is_dir( "/" ):
        print "Not a directory: %s" % storage_dir
        return False

    return True


def driver_shutdown():
    """
    Do the one-time driver shutdown
    """

    _shutdownFS()


def read_chunk(path, block_size, block_id, block_version, outfile):
    """
        Read a chunk of data.
        @chunk_request is a DriverRequest
        @outfile is a file to return the data read.
        @driver_config is a dict containing the driver's config
        @driver_secrets is a dict containing the driver's unencrypted secrets
    """

    global fs

    try:
        # use replication module to access a file containing the block
        replica = replication.replica( fs, path, block_size )
        replica.makeConsistent()
        buf = replica.readBlock( block_id, block_version )
        if not buf:
            print "WARN: block %d of '%s' does not exist" % ( block_id, path )
            return -errno.ENOENT

        outfile.write( buf )
    except Exception:
        print traceback.format_exc()
        return -errno.EIO
    return 0


def write_chunk(path, block_size, block_id, block_version, chunk_buf):
    global fs

    try:
        # use replication module to access a file containing the block
        replica = replication.replica( fs, path, block_size )
        replica.makeConsistent()
        replica.replicateBlock( block_id, chunk_buf, block_version )
    except Exception:
        print traceback.format_exc()
        return -errno.EIO
    return 0


def delete_chunk( path, block_size, block_id, block_version ):
    global fs

    try:
        # use replication module to access a file containing the block
        replica = replication.replica( fs, path, block_size )
        replica.makeConsistent()
        replica.deleteBlock( block_id, block_version )
    except Exception:
        print traceback.format_exc()
        return -errno.EIO
    return 0

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

def setup_test(storage_path):
    #create a temp directory
    if not os.path.exists(storage_path):
        os.mkdir(storage_path)

def replicate(in_path, out_path, block_version):
    block_id = 0
    with open(in_path, "r") as f:
        while True:
            buf = f.read(1024)
            if buf:
                print "block_id = %d" % (block_id)
                write_chunk(out_path, 1024, block_id, block_version, buf)
                block_id += 1
            else:
                break
    return block_id

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

    setup_test(CONFIG["STORAGE_DIR"])

    dinit = driver_init(CONFIG, SECRETS)
    if not dinit:
        print("cannot init driver")
        sys.exit(1)

    """
    replicate test
    """
    block_num = replicate("../LICENSE", "/REPLICA_TARGET_FILE", 1)

    """
    read test
    """
    for i in range(0, block_num):
        buf = io.BytesIO()
        read_chunk("/REPLICA_TARGET_FILE", 1024, i, 1, buf)
        print buf.getvalue()

    """
    block update test
    """
    replicate("../README.md", "/REPLICA_TARGET_FILE", 2)

    """
    block delete test
    """
    for i in range(0, block_num):
        delete_chunk("/REPLICA_TARGET_FILE", 1024, i, 1)

    if dinit:
        driver_shutdown()

if __name__ == "__main__":
    main()

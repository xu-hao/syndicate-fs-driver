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
Replication module test
"""

import traceback
import os
import imp
import sys
import json
import io

# import packages under src/
test_dirpath = os.path.dirname(os.path.abspath(__file__))
driver_root = os.path.dirname(test_dirpath)
src_root = os.path.join(driver_root, "src")
sys.path.append(src_root)
sys.path.append(test_dirpath)

import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.lib.replication as replication

from sgfsdriver.lib.pluginloader import pluginloader


class ReplicationDriverException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ReplicationDriver():
    def __init__(self, driver_config, driver_secrets):
        self.block_size = 1024
        role = abstractfs.afsrole.WRITE

        # continue only when fs is not initialized
        if not 'DRIVER_FS_PLUGIN' in driver_config:
            raise ReplicationDriverException("No DRIVER_FS_PLUGIN defined")

        if not 'DRIVER_FS_PLUGIN_CONFIG' in driver_config:
            raise ReplicationDriverException(
                "No DRIVER_FS_PLUGIN_CONFIG defined"
            )

        if not 'STORAGE_DIR' in driver_config:
            raise ReplicationDriverException("No STORAGE_DIR defined")

        self.storage_dir = driver_config['STORAGE_DIR']
        self.storage_dir = "/" + self.storage_dir.strip("/")

        plugin = driver_config['DRIVER_FS_PLUGIN']

        if isinstance(driver_config['DRIVER_FS_PLUGIN_CONFIG'], dict):
            plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
        elif isinstance(driver_config['DRIVER_FS_PLUGIN_CONFIG'], basestring):
            # JSONFY
            json_plugin_config = driver_config['DRIVER_FS_PLUGIN_CONFIG']
            plugin_config = json.loads(json_plugin_config)

        plugin_config["secrets"] = driver_secrets
        plugin_config["work_root"] = self.storage_dir

        try:
            loader = pluginloader()
            self.fs = loader.load(plugin, plugin_config, role)

            if not self.fs:
                raise ReplicationDriverException(
                    "No such driver plugin found: %s" % plugin
                )

            self.fs.connect()

            if not self.fs.exists("/"):
                raise ReplicationDriverException(
                    "No such file or directory: %s" % self.storage_dir
                )

            if not self.fs.is_dir("/"):
                raise ReplicationDriverException(
                    "Not a directory: %s" % self.storage_dir
                )

        except Exception as e:
            traceback.print_exc()
            raise ReplicationDriverException(e)

    def shutdown(self):
        """
        Do the one-time driver shutdown
        """
        if self.fs:
            try:
                self.fs.close()
            except Exception:
                pass
        self.fs = None

    def get_chunk_len(self, path):
        # use replication module to access a file containing the block
        repl = replication.replica(self.fs, path, self.block_size)
        repl.fix_consistency()

        return repl.get_data_block_len()

    def read_chunk(self, path, block_id, block_version, outfile):
        # use replication module to access a file containing the block
        repl = replication.replica(self.fs, path, self.block_size)
        repl.fix_consistency()

        requests = []
        dblock = replication.data_block(block_id, block_version, None)
        requests.append(dblock)
        responses = repl.read_data_blocks(requests)
        if not responses or not responses[0] or not responses[0].data:
            raise ReplicationDriverException(
                "WARN: block %d of '%s' does not exist" % (block_id, path)
            )

        outfile.write(responses[0].data)
        return 0

    def write_chunk(self, path, block_id, block_version, chunk_buf):
        # use replication module to access a file containing the block
        repl = replication.replica(self.fs, path, self.block_size)
        repl.fix_consistency()

        repl.begin_transaction()
        requests = []
        dblock = replication.data_block(block_id, block_version, chunk_buf)
        requests.append(dblock)
        repl.write_data_blocks(requests)

        repl.commit()
        return 0

    def delete_chunk(self, path, block_id, block_version):
        # use replication module to access a file containing the block
        repl = replication.replica(self.fs, path, self.block_size)
        repl.fix_consistency()

        repl.begin_transaction()

        requests = []
        dblock = replication.data_block(block_id, block_version, None)
        requests.append(dblock)
        repl.delete_data_blocks(requests)

        repl.commit()
        return 0

    def replicate_all(self, in_path, out_path, block_version=1):
        # use replication module to access a file containing the block
        repl = replication.replica(self.fs, out_path, self.block_size)
        repl.fix_consistency()
        repl.begin_transaction()

        requests = []
        block_id = 0
        with open(in_path, "r") as f:
            while True:
                buf = f.read(self.block_size)
                if buf:
                    dblock = replication.data_block(
                        block_id,
                        block_version,
                        buf
                    )
                    requests.append(dblock)
                    block_id += 1
                else:
                    break

        repl.write_data_blocks(requests)

        repl.commit()
        return 0

    def read_and_check(self, in_path, comp_path, block_version=1):
        block_id = 0
        file_size = 0
        size_read = 0
        chunk_len = self.get_chunk_len(in_path)

        with open(comp_path, "r") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            f.seek(0, os.SEEK_SET)

            while block_id < chunk_len:
                comp_buf = io.BytesIO()
                self.read_chunk(in_path, block_id, block_version, comp_buf)
                read_buf = f.read(self.block_size)

                comp_buf_arr = comp_buf.getvalue()
                assert len(comp_buf_arr) == len(read_buf)

                if len(comp_buf_arr) == 0:
                    assert file_size == size_read
                    break

                offset = 0
                for o in comp_buf_arr:
                    assert o == read_buf[offset]
                    offset += 1

                block_id += 1
                size_read += len(comp_buf_arr)

    def delete_all(self, path, block_version=1):
        chunk_len = self.get_chunk_len(path)
        block_id = 0
        while True:
            result = self.delete_chunk(path, block_id, block_version)
            block_id += 1

            if result != 0:
                break

            if block_id >= chunk_len:
                break


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


class TestcaseNotExist(Exception):
    """
    Cannot find the testcase
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def load_testcase(testcase_path):
    if os.path.exists(testcase_path):
        filename = os.path.basename(testcase_path)
        module_name = "testcase_%s" % filename
        testcase = imp.load_source(module_name, testcase_path)
        if testcase:
            return testcase
        else:
            raise TestcaseNotExist(
                "unable to find a testcase for %s" % testcase_path
            )
    else:
        raise TestcaseNotExist(
            "unable to find a testcase for %s" % testcase_path
        )


def main():
    if len(sys.argv) != 3:
        print "Usage: %s <config/secrets dir> <testcase path>" % sys.argv[0]
        sys.exit(1)

    cs_dir = sys.argv[1]
    testcase_path = sys.argv[2]

    CONFIG, SECRETS = load_config_secrets(cs_dir)
    if CONFIG is None:
        print "cannot find config"
        return

    if SECRETS is None:
        print "cannot find secrets"
        return

    storage_path = CONFIG["STORAGE_DIR"]
    if not os.path.exists(storage_path):
        os.mkdir(storage_path)

    try:
        driver = ReplicationDriver(CONFIG, SECRETS)

        # start test
        testcase = load_testcase(testcase_path)
        test = testcase.replication_test_impl(driver)
        print "start test (%s)!" % os.path.basename(testcase_path)
        test.start()
        print "finish test (%s)!" % os.path.basename(testcase_path)

        driver.shutdown()
    except Exception:
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

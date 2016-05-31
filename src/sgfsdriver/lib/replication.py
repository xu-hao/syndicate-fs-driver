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

import os
import sys
import time
import threading
import struct

import sgfsdriver.lib.abstractfs as abstractfs

INCOMPLETE_FILE_SUFFIX = ".part"

"""
undo-log class
"""
class undo_log_block(object):
    def __init__(self, block_id, block_data, block_version):
        self.block_id = 0
        self.block_data = None
        self.block_version = 0

    def toBinary(self):
        s = struct.Struct('=qq')
        buf = bytearray(8 + 8 + len(self.block_data))
        s.pack_into(buf, 0, self.block_id, self.block_version)
        buf[:] = self.block_data
        return buf

    @classmethod
    def fromBinary(cls, buf):
        s = struct.Struct('=qq')
        tup = s.unpack_from(buf, 0)
        block_id = tup[0]
        block_version = tup[1]
        block_data = buf[16:]
        return undo_log_block(block_id, block_data, block_version)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_log_block %d %d>" % (self.block_id, self.block_data)


class undo_log(object):
    UNDO_LOG_SUFFIX = ".ulog"

    def __init__(self, fs, path):
        self.fs = fs
        self.data_path = path
        self.log_path = self._makeLogPath(path):

        # read log data
        if not self.fs.exists(self.log_path):
            self.log = None
        else:
            buf = self.fs.read(self.log_path, 0, sys.maxsize)
            self.log = undo_log_block.fromBinary(buf)

    def _makeLogPath(self, path):
        return "%s%s" % (path, UNDO_LOG_SUFFIX)

    def clearLog(self):
        if self.log:
            if self.fs.exists(self.log_path):
                self.fs.unlink(self.log_path)
        self.log = None

    def write(self, block_id, block_data, block_version):
        self.log = undo_log_block(block_id, block_data, block_version)
        self.fs.write(self.log_path, 0, self.log.toBinary())

    def read(self):
        return self.log

    @classmethod
    def isLogPath(cls, path):
        if path.endswith(UNDO_LOG_SUFFIX):
            return True
        return False

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_log %s %s>" % (self.data_path, self.log_path, self.log)

class replica(object):
    REPLICA_INCOMPLETE_SUFFIX = ".part"
    REPLICA_BLOCK_VERSIONS_XATTR_KEY = "block_versions"

    def __init__(self, fs, path, block_size):
        self.fs = fs
        self.data_path = path
        self.block_size = block_size
        self.log = undo_log(fs, path)

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _getLock(self):
        return self.lock

    def _makeIncompletePath(self, path):
        return "%s%s" % (path, UNDO_LOG_SUFFIX)

    def _makeDirs(self, path):
        with self._getLock():
            parent_path = os.path.dirname(path)
            if not self.fs.exists(parent_path):
                self.fs.make_dirs(parent_path)

    def isInTransaction(self):
        with self._getLock():
            if self.fs.exists(self._makeIncompletePath(self.data_path)):
                return True
            else:
                return False

    def beginTransaction(self):
        with self._getLock():
            # need to check file existance because the file may not exist if there's no replicated blocks
            if self.fs.exists(self.data_path):
                self.fs.rename(self.data_path, self._makeIncompletePath(self.data_path))

    def commit(self):
        with self._getLock():
            incomplete_path = self._makeIncompletePath(self.data_path)
            self.log.clearLog()
            self.fs.rename(incomplete_path, self.data_path)

    def rollback(self):
        with self._getLock():
            incomplete_path = self._makeIncompletePath(self.data_path)
            # roll-back
            log_data = self.log.read()
            if log_data:
                # step1: copy old block back
                self._writeBlockToDataFile(incomplete_path, log_data.block_id, log_data.block_data)

                # step2: copy old version back
                block_versions = self._readBlockVersionsFromDataFile(incomplete_path)
                block_versions[log.block_id] = log_data.block_version
                self._writeBlockVersionsToDataFile(incomplete_path, block_versions)

                # step3: remove log
                self.log.clearLog()

            # step4: rename back
            self.fs.rename(incomplete_path, self.data_path)

    def replicateBlock(self, block_id, block_data, block_version):
        with self._getLock():
            self._makeDirs()

            incomplete_path = self._makeIncompletePath(self.data_path)
            old_block_versions = []

            # step1: copy an old block to log
            if self.fs.exists(self.data_path):
                old_block_data = self._readBlockFromDataFile(self.data_path, block_id)
                if old_block_data:
                    # read block version
                    old_block_versions = self._readBlockVersionsFromDataFile(self.data_path)
                    old_block_version = old_block_versions[block_id]

                    # copy to log
                    self.log.write(block_id, old_block_data, old_block_version)

            # step2: rename the target file - in transaction
            self.beginTransaction()

            # step3: set the version in the data file negative
            if len(old_block_versions) > block_id:
                old_block_versions[block_id] *= -1
            else:
                fill_count = block_id - len(self.old_block_versions) + 1
                for i in range(0, fill_count):
                    old_block_versions.append(0)

            self._writeBlockVersionsToDataFile(incomplete_path, old_block_versions)

            # step4: overwrite data block
            self._writeBlockToDataFile(incomplete_path, block_id, block_data)

            # step5: set the version in the data file new version
            old_block_versions[block_id] = block_version
            self._writeBlockVersionsToDataFile(incomplete_path, old_block_versions)

            # step7: remove undo log & rename the target file back
            self.commit()

    def makeConsistent(self):
        with self._getLock():
            if self.isInTransaction():
                # make consistent by roll-back
                self.rollback()
            else:
                # if log exists, clear log
                self.log.clearLog()

    def readBlock(self, block_id, block_version):
        with self._getLock():
            if self.fs.exists(self.data_path):
                versions = self._readBlockVersionsFromDataFile(self.data_path)
                if block_id < len(versions):
                    version = versions[block_id]
                    if version == block_version:
                        block_data = self.fs.read(self.data_path, block_id * self.block_size, self.block_size)
                        return block_data
            return None

    def deleteBlock(self, block_id, block_version):
        with self._getLock():
            if self.fs.exists(self.data_path):
                versions = self._readBlockVersionsFromDataFile(self.data_path)
                if block_id < len(versions):
                    version = versions[block_id]
                    if version == block_version:
                        # mark the block deleted
                        versions[block_id] = 0
                        all_blocks_deleted = True
                        for v in versions:
                            if v != 0:
                                all_blocks_deleted = False
                                break

                        if all_blocks_deleted:
                            self.fs.unlink(self.data_path)
                        else:
                            self._writeBlockVersionsToDataFile(self.data_path, versions)

                        return True
            return False

    def _readBlockFromDataFile(self, path, block_id):
        with self._getLock():
            block_data = self.fs.read(path, block_id * self.block_size, self.block_size)
            return block_data

    def _writeBlockToDataFile(self, path, block_id, block_data):
        with self._getLock():
            self.fs.write(path, block_id * self.block_size, block_data)

    def _readBlockVersionsFromDataFile(self, path):
        with self._getLock():
            versions_xattr = self.fs.get_xattr(path, REPLICA_BLOCK_VERSIONS_XATTR_KEY)
            versions_str_arr = versions_xattr.split(",")
            versions_int_arr = []
            for version_str in versions_str_arr:
                versions_int_arr.append(int(version_str))
            return versions_int_arr

    def _writeBlockVersionsToDataFile(self, path, versions):
        with self._getLock():
            value = ""
            for v in versions:
                if len(value) > 0:
                    value += ","
                value += str(v)

            self.fs.set_xattr(path, REPLICA_BLOCK_VERSIONS_XATTR_KEY, value)

    @classmethod
    def isLogPath(cls, path):
        return undo_log.isLogPath(path)


"""
Interface class to replication
"""
class replication(object):
    def __init__(self, fs, block_size):
        self.fs = fs
        self.block_size = block_size
        self.replicas = {}
        self.lock = threading.RLock()

        # load all replicas
        self._loadAllReplica()
        # make consistent
        self.makeConsistent()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _getLock(self):
        return self.lock

    def getReplica(self, path):
        with self._getLock():
            if path not in self.replicas:
                # create a new replica object
                self.replicas[path] = replica(self.fs, path, self.block_size)

            return self.replicas[path]

    def _loadAllReplica(self):
        with self._getLock():
            replicas = {}
            stack = ["/"]
            while len(stack) > 0:
                last_dir = stack.pop(0)
                entries = fs.list_dir(last_dir)
                for entry in entries:
                    # entry is a filename
                    entry_path = last_dir.rstrip("/") + "/" + entry

                    # ignore undo-log
                    if not replica.isLogPath(entry_path):
                        st = fs.stat(entry_path)
                        if st.directory:
                            stack.append(entry_path)
                        else:
                            replicas[path] = replica(self.fs, entry_path, self.block_size)

            self.replicas = replicas

    def makeConsistent(self):
        with self._getLock():
            for key in self.replicas:
                replica = self.replicas[key]
                replica.makeConsistent()

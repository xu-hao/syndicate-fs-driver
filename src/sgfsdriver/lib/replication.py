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

import os
import sys
import time
import threading
import struct

import sgfsdriver.lib.abstractfs as abstractfs

"""
undo-log class
"""
class undo_log_block(object):
    def __init__(self, block_id, block_data, block_version, block_size):
        self.block_id = block_id
        self.block_data = block_data
        self.block_version = block_version
        self.block_size = block_size

    def toBinary(self):
        s = struct.Struct('=qqq')
        buf = bytearray(8 + 8 + 8 + len(self.block_data))
        s.pack_into(buf, 0, self.block_id, self.block_version, self.block_size)
        buf[:] = self.block_data
        return buf

    @classmethod
    def fromBinary(cls, buf):
        s = struct.Struct('=qqq')
        tup = s.unpack_from(buf, 0)
        block_id = tup[0]
        block_version = tup[1]
        block_size = tup[2]
        block_data = buf[24:]
        return undo_log_block(block_id, block_data, block_version, block_size)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_log_block %d %d %d>" % (self.block_id, self.block_version, self.block_size)


class undo_log(object):
    UNDO_LOG_SUFFIX = ".ulog"

    def __init__(self, fs, path):
        self.fs = fs
        self.data_path = path
        self.log_path = self._makeLogPath(path)

        # read log data
        if not self.fs.exists(self.log_path):
            self.log = None
        else:
            buf = self.fs.read(self.log_path, 0, sys.maxsize)
            self.log = undo_log_block.fromBinary(buf)

    def _makeLogPath(self, path):
        return "%s%s" % (path, self.UNDO_LOG_SUFFIX)

    def clearLog(self):
        if self.log:
            if self.fs.exists(self.log_path):
                self.fs.unlink(self.log_path)
        self.log = None

    def write(self, block_id, block_data, block_version, block_size):
        self.log = undo_log_block(block_id, block_data, block_version, block_size)
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
        return "<undo_log %s %s %s>" % (self.data_path, self.log_path, self.log)

class block_meta(object):
    def __init__(self, block_refer_log, block_version, block_size):
        self.block_refer_log = 0
        self.block_version = block_version
        self.block_size = block_size

    def toString(self):
        return "%d|%d|%d" % (self.block_refer_log, self.block_version, self.block_size)

    @classmethod
    def fromString(cls, s):
        arr = s.split("|")
        return block_meta(int(arr[0]), int(arr[1]), int(arr[2]))

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<block_meta %d %d %d>" % (self.block_refer_log, self.block_version, self.block_size)

class file_meta(object):
    def __init__(self, block_meta_arr=[]):
        self.block_meta_arr = block_meta_arr

    def getSize(self):
        return len(self.block_meta_arr)

    def setSize(self, size):
        if len(self.block_meta_arr) > size:
            # trim out
            self.block_meta_arr = self.block_meta_arr[:size]
        else:
            # zero fill
            for i in range(0, size - len(self.block_meta_arr)):
                self.block_meta_arr.append(block_meta(0,0,0))

    def set(self, block_id, block_refer_log, block_version, block_size):
        if len(self.block_meta_arr) > block_id:
            self.block_meta_arr[block_id].block_refer_log = block_refer_log
            self.block_meta_arr[block_id].block_version = block_version
            self.block_meta_arr[block_id].block_size = block_size
        else:
            # zero fill
            for i in range(0, block_id - len(self.block_meta_arr) + 1):
                self.block_meta_arr.append(block_meta(0,0,0))

            self.block_meta_arr[block_id].block_refer_log = block_refer_log
            self.block_meta_arr[block_id].block_version = block_version
            self.block_meta_arr[block_id].block_size = block_size

    def get(self, block_id):
        if len(self.block_meta_arr) > block_id:
            return self.block_meta_arr[block_id]
        else:
            return block_meta(0,0,0)

    def toString(self):
        s = ""
        for meta in self.block_meta_arr:
            if len(s) > 0:
                s += ","
            s += meta.toString()
        return s

    @classmethod
    def fromString(cls, s):
        block_meta_arr = []

        if s:
            arr = s.split(",")
            for a in arr:
                m = block_meta.fromString(a)
                block_meta_arr.append(m)
        return file_meta(block_meta_arr)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<file_meta %s>" % (self.block_meta_arr)

class replica(object):
    REPLICA_INCOMPLETE_SUFFIX = ".part"
    REPLICA_BLOCK_INFO_XATTR_KEY = "user.block_info"

    def __init__(self, fs, path, chunk_size):
        self.fs = fs
        self.data_path = path
        self.chunk_size = chunk_size
        self.log = undo_log(fs, path)
        self.lock = threading.RLock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _getLock(self):
        return self.lock

    def _makeIncompletePath(self, path):
        return "%s%s" % (path, self.REPLICA_INCOMPLETE_SUFFIX)

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
                block_info = self._readBlockInfoFromDataFile(incomplete_path)
                block_info.set(log_data.block_id, 0, log_data.block_version, log_data.block_size)
                self._writeBlockInfoToDataFile(incomplete_path, block_info)

                # step3: remove log
                self.log.clearLog()

            # step4: rename back
            self.fs.rename(incomplete_path, self.data_path)

    def replicateBlock(self, block_id, block_data, block_version):
        with self._getLock():
            self._makeDirs(self.data_path)

            incomplete_path = self._makeIncompletePath(self.data_path)
            block_info = file_meta()
            fresh_file = True

            # step1: copy an old block to log
            if self.fs.exists(self.data_path):
                block_info = self._readBlockInfoFromDataFile(self.data_path)
                block_meta = block_info.get(block_id)

                if block_meta.block_version != 0:
                    old_block_data = self._readBlockFromDataFile(self.data_path, block_id, block_meta.block_size)
                    # copy to log
                    self.log.write(block_id, old_block_data, block_meta.block_version, block_meta.block_size)

                fresh_file = False

            # step2: rename the target file - in transaction
            if not fresh_file:
                self.beginTransaction()

            # step3: make the block refer log
            block_meta = block_info.get(block_id)
            block_info.set(block_id, 1, block_meta.block_version, block_meta.block_size)

            if not fresh_file:
                self._writeBlockInfoToDataFile(incomplete_path, block_info)

            # step4: overwrite data block
            self._writeBlockToDataFile(incomplete_path, block_id, block_data)

            # step5: set the version in the data file new version
            block_info.set(block_id, 0, block_version, len(block_data))
            self._writeBlockInfoToDataFile(incomplete_path, block_info)

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

    def rename(self, new_path):
        with self._getLock():
            self.makeConsistent()
            self._makeDirs(new_path)

            if self.fs.exists(new_path):
                # error
                return False
            else:
                # rename
                self.fs.rename(self.data_path, new_path)
                return True

    def readBlock(self, block_id, block_version):
        with self._getLock():
            if self.fs.exists(self.data_path):
                block_info = self._readBlockInfoFromDataFile(self.data_path)
                block_meta = block_info.get(block_id)
                if block_meta.block_version == block_version:
                    block_data = self.fs.read(self.data_path, block_id * self.chunk_size, block_meta.block_size)
                    return block_data
            return None

    def deleteBlock(self, block_id, block_version):
        with self._getLock():
            if self.fs.exists(self.data_path):
                block_info = self._readBlockInfoFromDataFile(self.data_path)
                block_meta = block_info.get(block_id)
                if block_meta.block_version == block_version:
                    # mark the block deleted
                    block_info.set(block_id, 0, 0, 0)

                    last_live_block_id = -1
                    for i in range(0, block_info.getSize()):
                        block_meta = block_info.get(i)
                        if block_meta.block_version != 0:
                            last_live_block_id = i
                            break

                    if last_live_block_id == -1:
                        self.fs.unlink(self.data_path)
                    else:
                        block_meta = block_info.get(last_live_block_id)
                        self._writeBlockInfoToDataFile(self.data_path, block_info)
                        self.fs.truncate(self.data_path, last_live_block_id * self.chunk_size + block_meta.block_size)

                    return True
            return False

    def _readBlockFromDataFile(self, path, block_id, block_size):
        with self._getLock():
            block_data = self.fs.read(path, block_id * self.chunk_size, block_size)
            return block_data

    def _writeBlockToDataFile(self, path, block_id, block_data):
        with self._getLock():
            self.fs.write(path, block_id * self.chunk_size, block_data)

    def _readBlockInfoFromDataFile(self, path):
        with self._getLock():
            block_info_xattr = self.fs.get_xattr(path, self.REPLICA_BLOCK_INFO_XATTR_KEY)
            block_info = file_meta.fromString(block_info_xattr)
            return block_info

    def _writeBlockInfoToDataFile(self, path, block_info):
        with self._getLock():
            value = block_info.toString()
            self.fs.set_xattr(path, self.REPLICA_BLOCK_INFO_XATTR_KEY, value)

    @classmethod
    def isLogPath(cls, path):
        return undo_log.isLogPath(path)

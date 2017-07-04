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
import threading
import pickle

from abc import ABCMeta, abstractmethod


class undo_event_log(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def type_string(self):
        pass


class undo_size_log(object):
    """
    undo-log size info
    """
    TYPE_STR = "size"

    def __init__(self, file_size):
        self.size = file_size

    def type_string(self):
        return undo_size_log.TYPE_STR

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_size_log size(%d)>" % self.size


class undo_block_log(object):
    """
    undo-log block info
    """
    def __init__(self, block_id, block_data, block_version, block_size):
        self.id = block_id
        self.data = block_data
        self.version = block_version
        self.size = block_size

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_block_log id(%d) ver(%s) size(%d)>" % \
            (self.id, self.version, self.size)


class undo_log(object):
    """
    undo-log file containing a list of undo_block_logs
    """
    UNDO_LOG_SUFFIX = "undo"

    def __init__(self, fs, path):
        self.fs = fs
        self.data_path = path
        self.log_path = undo_log.make_log_path(path)
        self.block_logs = []
        self.event_logs = []
        self.synced = True
        self.file_exist = False

        # read logs if exists
        if self.fs.exists(self.log_path):
            st = self.fs.stat(self.log_path)
            buf = self.fs.read(self.log_path, 0, st.size)
            self._deserialize(buf)
            self.file_exist = True

    def _serialize(self):
        data = (self.block_logs, self.event_logs)
        return pickle.dumps(data)

    def _deserialize(self, buf):
        data = pickle.loads(buf)
        b_logs, e_logs = data
        self.block_logs = b_logs
        self.event_logs = e_logs

    def rename(self, new_path):
        new_log_path = undo_log.make_log_path(new_path)
        if self.fs.exists(new_log_path):
            # error - file already exists
            return False

        if self.file_exist:
            self.fs.rename(self.log_path, new_log_path)
        self.data_path = new_path
        self.log_path = new_log_path
        return True

    def clear(self):
        if self.file_exist:
            self.fs.unlink(self.log_path)
        self.block_logs = []
        self.event_logs = []
        self.synced = True

    def sync(self):
        if not self.synced:
            ds = self._serialize()
            self.fs.write(self.log_path, 0, ds)
            self.synced = True
            self.file_exist = True

    def write_block_log(self, block_log, sync_now=True):
        self.block_logs.append(block_log)
        self.synced = False
        if sync_now:
            self.sync()

    def read_block_logs(self):
        return self.block_logs

    def write_event_log(self, size_log, sync_now=True):
        self.event_logs.append(size_log)
        self.synced = False
        if sync_now:
            self.sync()

    def read_event_logs(self):
        return self.event_logs

    @classmethod
    def make_log_path(self, path):
        return "%s.%s" % (path, undo_log.UNDO_LOG_SUFFIX)

    @classmethod
    def is_log_path(cls, path):
        if path.endswith(".%s" % undo_log.UNDO_LOG_SUFFIX):
            return True
        return False

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<undo_log data(%s) log(%s)>" % \
            (self.data_path, self.log_path)


class block_meta(object):
    META_FLAG_EMPTY = 0
    META_FLAG_DATAIN = 1
    META_FLAG_REF_LOG = 2

    def __init__(self, flag, block_version, block_size):
        self.flag = flag
        self.version = block_version
        self.size = block_size

    def make_empty(self):
        self.flag = block_meta.META_FLAG_EMPTY
        self.version = 0
        self.size = 0

    def is_empty(self):
        return self.flag == block_meta.META_FLAG_EMPTY

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<block_meta flag(%d) ver(%s) size(%d)>" % \
            (self.flag, self.version, self.size)


class meta_file(object):
    META_FILE_SUFFIX = "meta"

    def __init__(self, fs, path):
        self.fs = fs
        self.data_path = path
        self.meta_path = meta_file.make_meta_path(path)
        self.blocks = []
        self.synced = True
        self.file_exist = False

        # read meta data if exists
        if self.fs.exists(self.meta_path):
            st = self.fs.stat(self.meta_path)
            buf = self.fs.read(self.meta_path, 0, st.size)
            self._deserialize(buf)
            self.file_exist = True

    def _serialize(self):
        data = (self.blocks)
        return pickle.dumps(data)

    def _deserialize(self, buf):
        data = pickle.loads(buf)
        self.blocks = data

    def rename(self, new_path):
        new_meta_path = meta_file.make_meta_path(new_path)
        if self.fs.exists(new_meta_path):
            # error - file already exists
            return False

        if self.file_exist:
            self.fs.rename(self.meta_path, new_meta_path)
        self.data_path = new_path
        self.meta_path = new_meta_path
        return True

    def clear(self):
        if self.file_exist:
            self.fs.unlink(self.meta_path)
        self.blocks = []
        self.synced = True

    def sync(self):
        if not self.synced:
            ds = self._serialize()
            self.fs.write(self.meta_path, 0, ds)
            self.synced = True
            self.file_exist = True

    def write_block_meta(self, block_id, meta, sync_now=True):
        if len(self.blocks) > block_id:
            # overwrite
            self.blocks[block_id] = meta
            self.synced = False
        else:
            # zero fill
            for i in range(0, block_id - len(self.blocks) + 1):
                empty_block_meta = block_meta(False, 0, 0)
                self.blocks.append(empty_block_meta)
                self.synced = False

            self.blocks[block_id] = meta
            self.synced = False

        self.compact_block_meta(sync_now)

    def delete_block_meta(self, block_id, sync_now=True):
        if len(self.blocks) > block_id:
            self.blocks[block_id].make_empty()
            self.synced = False

        self.compact_block_meta(sync_now)

    def compact_block_meta(self, sync_now=True):
        # scan backward and trim out
        cut_blocks_to = 0
        for i in xrange(len(self.blocks), 0, -1):
            if not self.blocks[i - 1].is_empty():
                cut_blocks_to = i
                break

        self.blocks = self.blocks[:cut_blocks_to]
        self.synced = False

        if sync_now:
            self.sync()

    def get_block_meta_len(self):
        return len(self.blocks)

    def read_block_meta(self, block_id):
        if len(self.blocks) > block_id:
            return self.blocks[block_id]
        else:
            return block_meta(False, 0, 0)

    def get_data_file_size(self):
        # scan and sum
        data_file_size = 0
        for i in xrange(0, len(self.blocks)):
            data_file_size += self.blocks[i].size
        return data_file_size

    @classmethod
    def make_meta_path(self, path):
        return "%s.%s" % (path, meta_file.META_FILE_SUFFIX)

    @classmethod
    def is_meta_path(cls, path):
        if path.endswith(".%s" % meta_file.META_FILE_SUFFIX):
            return True
        return False

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<meta_file data(%s) meta(%s) blocks(%d)>" % \
            (self.data_path, self.meta_path, len(self.blocks))


class data_block(object):
    def __init__(self, block_id, block_version, block_data):
        self.id = block_id
        self.version = block_version
        self.data = block_data

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "<data_block id(%d) ver(%s) size(%d)>" % \
            (self.id, self.version, len(self.data))


class replica(object):
    REPLICA_INCOMPLETE_SUFFIX = "part"

    def __init__(self, fs, path, block_size):
        self.fs = fs
        self.data_path = path
        self.incomplete_path = replica.make_incomplete_path(path)
        self.block_size = block_size
        self.log = undo_log(fs, path)
        self.meta = meta_file(fs, path)
        self.lock = threading.RLock()
        self.transaction = False
        self.file_exist = False

        if self.fs.exists(self.data_path):
            self.file_exist = True
        else:
            if self.fs.exists(self.incomplete_path):
                self.file_exist = True
                self.transaction = True

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _get_lock(self):
        return self.lock

    def _make_parent_dirs(self, path):
        with self._get_lock():
            parent_path = os.path.dirname(path)
            self.fs.make_dirs(parent_path)

    def begin_transaction(self):
        with self._get_lock():
            if self.transaction:
                raise IOError("already in transaction")

            # need to check file existance
            # because the file may not exist if there's no replicated blocks
            if self.file_exist:
                st = self.fs.stat(self.data_path)
                file_size = st.size
                self.fs.rename(
                    self.data_path, self.incomplete_path
                )
            else:
                file_size = 0

            self.log.clear()
            size_log = undo_size_log(file_size)
            self.log.write_event_log(size_log, False)
            self.transaction = True

    def commit(self):
        with self._get_lock():
            if not self.transaction:
                raise IOError("not in transaction")

            self.log.clear()
            self.meta.sync()
            file_size = self.meta.get_data_file_size()
            if file_size > 0:
                self.fs.truncate(self.incomplete_path, file_size)
                self.fs.rename(self.incomplete_path, self.data_path)
                self.file_exist = True
            else:
                if self.file_exist:
                    self.fs.unlink(self.incomplete_path)
                    self.meta.clear()
                    self.file_exist = False
            self.transaction = False

    def rollback(self):
        with self._get_lock():
            if not self.transaction:
                raise IOError("not in transaction")

            # roll-back
            block_logs = self.log.read_block_logs()
            data_blocks = []
            for block_log in block_logs:
                # step1: copy old block back
                dblock = data_block(
                    block_log.id,
                    block_log.version,
                    block_log.data[:block_log.size]
                )
                data_blocks.append(dblock)

                # step2: copy old version back
                if block_log.data:
                    flag = block_meta.META_FLAG_DATAIN
                else:
                    flag = block_meta.META_FLAG_EMPTY

                bmeta = block_meta(
                    flag,
                    block_log.version,
                    block_log.size
                )
                self.meta.write_block_meta(
                    block_log.id,
                    bmeta,
                    False
                )

            if len(data_blocks) > 0:
                self._write_data_blocks(data_blocks)

            event_logs = self.log.read_event_logs()
            new_file_size = 0
            for event_log in event_logs:
                if event_log.type_string() == undo_size_log.TYPE_STR:
                    new_file_size = event_log.size

            if self.file_exist:
                if new_file_size > 0:
                    self.fs.truncate(
                        self.incomplete_path,
                        new_file_size
                    )
                else:
                    self.fs.unlink(self.incomplete_path)
                    self.meta.clear()
                    self.file_exist = False

            self.meta.sync()

            # step3: remove log
            self.log.clear()

            # step4: rename back
            if self.file_exist:
                self.fs.rename(self.incomplete_path, self.data_path)
            self.transaction = False

    def write_data_blocks(self, data_blocks):
        with self._get_lock():
            if not self.transaction:
                raise IOError("not in transaction")

            # step1: copy an old block to log
            if self.file_exist:
                bmeta_arr = []
                id_size_arr = []
                for dblock in data_blocks:
                    bmeta = self.meta.read_block_meta(dblock.id)
                    bmeta_arr.append(bmeta)
                    id_size_arr.append((dblock.id, bmeta.size))

                old_dblocks = self._read_data_blocks(id_size_arr)

                # write to log
                # step2: make the block refer log
                for i in xrange(0, len(data_blocks)):
                    dblock = data_blocks[i]
                    bmeta = bmeta_arr[i]
                    old_dblock = old_dblocks[i]

                    block_log = undo_block_log(
                        dblock.id,
                        old_dblock,
                        bmeta.version,
                        bmeta.size)
                    self.log.write_block_log(block_log, False)

                    new_block_meta = block_meta(
                        block_meta.META_FLAG_REF_LOG,
                        bmeta.version,
                        bmeta.size
                    )
                    self.meta.write_block_meta(dblock.id, new_block_meta,
                                               False)

                self.log.sync()
                self.meta.sync()
            else:
                self._make_parent_dirs(self.data_path)

            # step3: overwrite data block
            if len(data_blocks) > 0:
                self._write_data_blocks(data_blocks)

            # step4: set the version in the data file new version
            for i in xrange(0, len(data_blocks)):
                dblock = data_blocks[i]
                new_block_meta = block_meta(
                    block_meta.META_FLAG_DATAIN,
                    dblock.version,
                    len(dblock.data)
                )
                self.meta.write_block_meta(dblock.id, new_block_meta, False)

            self.meta.sync()

    def fix_consistency(self):
        with self._get_lock():
            if self.file_exist:
                if self.transaction:
                    # fix by roll-back
                    self.rollback()
                else:
                    self.meta.compact_block_meta(True)
                    expected_file_size = self.meta.get_data_file_size()
                    st = self.fs.stat(self.data_path)
                    if expected_file_size != st.size:
                        # not good
                        self.fs.truncate(self.data_path, st.size)

            # clean up
            self.log.clear()

    def rename(self, new_path):
        with self._get_lock():
            if self.transaction:
                raise IOError("in transaction")

            self._make_parent_dirs(new_path)

            if self.fs.exists(new_path):
                # error - file already exists
                return False
            else:
                # rename
                if self.file_exist:
                    self.fs.rename(self.data_path, new_path)
                self.data_path = new_path
                self.incomplete_path = replica.make_incomplete_path(new_path)
                self.meta.rename(new_path)
                self.log.rename(new_path)
                return True

    def get_data_file_size(self):
        with self._get_lock():
            if self.transaction:
                raise IOError("in transaction")

            return self.meta.get_data_file_size()

    def get_data_block_len(self):
        with self._get_lock():
            if self.transaction:
                raise IOError("in transaction")

            return self.meta.get_block_meta_len()

    def read_data_blocks(self, data_blocks):
        with self._get_lock():
            if self.transaction:
                raise IOError("in transaction")

            r_dblocks = []
            for dblock in data_blocks:
                bmeta = self.meta.read_block_meta(dblock.id)
                # empty - default
                r_dblock = data_block(
                    dblock.id,
                    dblock.version,
                    None
                )

                if bmeta.version == dblock.version:
                    # good to go
                    if self.file_exist:
                        if bmeta.size > 0:
                            block_data = self.fs.read(
                                self.data_path,
                                dblock.id * self.block_size,
                                bmeta.size)
                            r_dblock = data_block(
                                dblock.id,
                                dblock.version,
                                block_data
                            )

                r_dblocks.append(r_dblock)
            return r_dblocks

    def delete_data_blocks(self, data_blocks):
        with self._get_lock():
            if not self.transaction:
                raise IOError("not in transaction")

            if self.file_exist:
                bmeta_arr = []
                for dblock in data_blocks:
                    bmeta = self.meta.read_block_meta(dblock.id)
                    bmeta_arr.append(bmeta)

                    if bmeta.version == dblock.version:
                        # mark the block deleted
                        bmeta.make_empty()
                        self.meta.write_block_meta(dblock.id, bmeta, False)

                self.meta.sync()

    def _read_data_blocks(self, id_size_arr):
        #TODO: Need to make this funciton more efficient
        data_blocks = []
        for id_size in id_size_arr:
            block_id, data_size = id_size
            if data_size > 0:
                block_data = self.fs.read(
                    self.incomplete_path,
                    block_id * self.block_size,
                    data_size)
            else:
                block_data = None
            data_blocks.append(block_data)
        return data_blocks

    def _write_data_blocks(self, data_blocks):
        #TODO: Need to make this funciton more efficient
        for dblock in data_blocks:
            if dblock is not None and len(dblock.data) > 0:
                self.fs.write(
                    self.incomplete_path,
                    dblock.id * self.block_size,
                    dblock.data)
                self.file_exist = True

    @classmethod
    def make_incomplete_path(self, path):
        return "%s.%s" % (path, replica.REPLICA_INCOMPLETE_SUFFIX)

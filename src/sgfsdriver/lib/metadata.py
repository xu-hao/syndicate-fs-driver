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
import sqlite3

import sgfsdriver.lib.abstractfs as abstractfs


def get_current_time():
    return int(round(time.time() * 1000))

"""
Database
"""
class dbmanager(object):
    def __init__(self, dbpath="datasetmeta.db"):
        self.db_path = dbpath;
        self.db_conn = sqlite3.connect(dbpath, check_same_thread=False)
        self.lock = threading.RLock()
        self.createTable()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def createTable(self):
        self._lock()
        db_cursor = self.db_conn.cursor()
        # create a table if not exist
        db_cursor.execute("CREATE TABLE IF NOT EXISTS tbl_dataset( \
                                                         path VARCHAR NOT NULL, \
                                                         filename VARCHAR NOT NULL, \
                                                         status VARCHAR NOT NULL, \
                                                         registered_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
                                                         PRIMARY KEY (path, filename) \
                                                         );")
        self.db_conn.commit()
        self._unlock()

    def dropTable(self):
        self._lock()
        db_cursor = self.db_conn.cursor()
        db_cursor.execute("DROP TABLE tbl_dataset")
        self.db_conn.commit()
        self._unlock()

    def insertEntry(self, path, filename, status):
        self._lock()
        db_cursor = self.db_conn.cursor()
        db_cursor.execute("INSERT INTO tbl_dataset(path, filename, status) values (?, ?, ?)", (path, filename, status.toJson(),))
        self.db_conn.commit()
        self._unlock()

    def updateEntry(self, path, filename, status):
        self._lock()
        db_cursor = self.db_conn.cursor()
        db_cursor.execute("UPDATE tbl_dataset SET status=?, registered_time=CURRENT_TIMESTAMP WHERE path=? AND filename=?", (status.toJson(), path, filename,))
        self.db_conn.commit()
        self._unlock()

    def deleteEntry(self, path, filename):
        self._lock()
        db_cursor = self.db_conn.cursor()
        db_cursor.execute("DELETE FROM tbl_dataset WHERE path=? AND filename=?", (path, filename,))
        self.db_conn.commit()
        self._unlock()

    def getDirectoryEntries(self, path):
        self._lock()
        db_cursor = self.db_conn.cursor()
        entries = []
        for row_path, row_filename, row_status, row_registered_time in db_cursor.execute("SELECT * FROM tbl_dataset WHERE path=? ORDER BY filename", (path,)):
            entry = {"path": row_path, "filename": row_filename, "status": abstractfs.afsstat.fromJson(row_status),
                     "registered_time": row_registered_time}
            entries.append(entry)

        self._unlock()
        return entries
        

"""
Interface class to datasetmeta
"""
class datasetmeta(object):
    def __init__(self, root_path="/", 
                       update_event_handler=None, request_for_update_handler=None):
        self.root_path = root_path
        self.dbmanager = dbmanager()
        self.lock = threading.RLock()
        self.update_event_handler = update_event_handler
        self.request_for_update_handler = request_for_update_handler

    def __enter__(self):
        self._lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._unlock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def getRootPath(self):
        return self.root_path

    def _onRequestUpdate(self, directory):
        if self.request_for_update_handler:
            self.request_for_update_handler(directory)

    def _onDirectoryUpdate(self, updated_entries, added_entries, removed_entries):
        if (len(updated_entries) > 0) or \
           (len(added_entries) > 0) or \
           (len(removed_entries) > 0):
            # if any of these are not empty
            if self.update_event_handler:
                self.update_event_handler(updated_entries, added_entries, removed_entries)

        # for subdirectories
        if added_entries:
            for added_entry in added_entries:
                if added_entry.directory:
                    self._onRequestUpdate(added_entry)

        if updated_entries:
            for updated_entry in updated_entries:
                if updated_entry.directory:
                    self._onRequestUpdate(updated_entry)

    def updateDirectory(self, path=None, entries=None):
        if entries is None:
            entries = []

        with self.lock:
            # find removed/added/updated entries
            old_entries = {}
            for entry in self.dbmanager.getDirectoryEntries(path):
                old_entries[entry["filename"]] = entry["status"]

            new_entries = {}
            for entry in entries:
                new_entries[entry.name] = entry
                
            set_prev = set(old_entries.keys())
            set_new = set(new_entries.keys())

            set_intersection = set_prev & set_new

            unchanged_entries = []
            updated_entries = []
            removed_entries = []
            added_entries = []

            # check update and unchanged
            for key in set_intersection:
                e_old = old_entries[key]
                e_new = new_entries[key]
                if e_old == e_new:
                    # unchanged
                    unchanged_entries.append(e_old)
                else:
                    # changed
                    updated_entries.append(e_new)

            # check removed
            for key in set_prev:
                if key not in set_intersection:
                    # removed
                    e_old = old_entries[key]
                    removed_entries.append(e_old)

            # check added
            for key in set_new:
                if key not in set_intersection:
                    # added
                    e_new = new_entries[key]
                    added_entries.append(e_new)

            # if directory is removed
            for removed_entry in removed_entries:
                if removed_entry.directory:
                    removed_subdir = self._getStatsRecursive(removed_entry.path)
                    for removed_subdir_entry in removed_subdir:
                        removed_entries.append(removed_subdir_entry)

            # apply to dataset
            for entry in removed_entries:
                self.dbmanager.deleteEntry(os.path.dirname(entry.path), entry.name)

            for entry in updated_entries:
                self.dbmanager.updateEntry(os.path.dirname(entry.path), entry.name, entry)

            for entry in added_entries:
                self.dbmanager.insertEntry(os.path.dirname(entry.path), entry.name, entry)

            self._onDirectoryUpdate(updated_entries, added_entries, removed_entries)

    def getDirectory(self, path):
        with self.lock:
            entries = self.dbmanager.getDirectoryEntries(path)
        return entries

    def _getStatsRecursive(self, path):
        entries = []
        for entry in self.dbmanager.getDirectoryEntries(path):
            dirs = []
            stat = entry["status"]
            entries.append(stat)
            if stat.directory:
                dirs.append(stat.path)

            for dirpath in dirs:
                for sub_entry in self._getStatsRecursive(dirpath):
                    entries.append(sub_entry)
        return entries        

    def walk(self):
        entries = []
        with self.lock:
            for entry in self._getStatsRecursive(self.root_path):
                entries.append(entry)
        return entries


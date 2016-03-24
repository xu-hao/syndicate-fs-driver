#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

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
Local-filesystem Plugin
"""
import os
import sys
import errno
import time
import stat
import logging
import pyinotify

import sagfsdriver.lib.abstractfs as abstractfs
import sagfsdriver.lib.metadata as metadata

logger = logging.getLogger('syndicate_local_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_local_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

class InotifyEventHandler(pyinotify.ProcessEvent):
    def __init__(self, plugin):
        self.plugin = plugin

    def process_IN_CREATE(self, event):
        logger.info("Creating: %s" % event.pathname)
        self.plugin.on_update_detected("create", event.pathname)

    def process_IN_DELETE(self, event):
        logger.info("Removing: %s" % event.pathname)
        self.plugin.on_update_detected("remove", event.pathname)

    def process_IN_MODIFY(self, event):
        logger.info("Modifying: %s" % event.pathname)
        self.plugin.on_update_detected("modify", event.pathname)

    def process_IN_ATTRIB(self, event):
        logger.info("Modifying attributes: %s" % event.pathname)
        self.plugin.on_update_detected("modify", event.pathname)

    def process_IN_MOVED_FROM(self, event):
        logger.info("Moving a file from : %s" % event.pathname)
        self.plugin.on_update_detected("create", event.pathname)

    def process_IN_MOVED_TO(self, event):
        logger.info("Moving a file to : %s" % event.pathname)
        self.plugin.on_update_detected("remove", event.pathname)

    def process_default(self, event):
        logger.info("Unhandled event to a file : %s" % event.pathname)

class plugin_impl(abstractfs.afsbase):
    def __init__(self, config, role=abstractfs.afsrole.DISCOVER):
        if not config:
            raise ValueError("fs configuration is not given correctly")

        dataset_root = config.get("dataset_root")
        if not dataset_root:
            raise ValueError("dataset_root configuration is not given correctly")

        # set role
        self.role = role

        # config can have unicode strings
        dataset_root = dataset_root.encode('ascii','ignore')
        dataset_root = dataset_root.rstrip("/")

        # set inotify
        self.watch_manager = pyinotify.WatchManager()
        self.notify_handler = InotifyEventHandler(self)
        self.notifier = pyinotify.ThreadedNotifier(self.watch_manager, 
                                                   self.notify_handler)

        # init dataset tracker
        self.dataset_tracker = metadata.datasetmeta(root_path=dataset_root,
                                                    update_event_handler=self._on_dataset_update, 
                                                    request_for_update_handler=self._on_request_update)

        self.notification_cb = None

    def on_update_detected(self, operation, path):
        if operation in ["create", "remove", "modify"]:
            if path:
                parent_path = os.path.dirname(path)
                entries = os.listdir(parent_path)
                stats = []
                for entry in entries:
                    # entry is a filename
                    entry_path = os.path.join(parent_path, entry)
                    # get stat
                    sb = os.stat(entry_path)

                    st = abstractfs.afsstat(directory=stat.S_ISDIR(sb.st_mode), 
                                            path=entry_path,
                                            name=entry, 
                                            size=sb.st_size,
                                            checksum=0,
                                            create_time=sb.st_ctime,
                                            modify_time=sb.st_mtime)
                    stats.append(st)
                self.dataset_tracker.updateDirectory(path=parent_path, entries=stats)

    def _on_dataset_update(self, updated_entries, added_entries, removed_entries):
        if self.notification_cb:
            self.notification_cb(updated_entries, added_entries, removed_entries)

    def _on_request_update(self, entry):
        entries = os.listdir(entry.path)
        stats = []
        for e in entries:
                # entry is a filename
                entry_path = os.path.join(entry.path, e)
                # get stat
                sb = os.stat(entry_path)

                st = abstractfs.afsstat(directory=stat.S_ISDIR(sb.st_mode), 
                                        path=entry_path,
                                        name=e, 
                                        size=sb.st_size,
                                        checksum=0,
                                        create_time=sb.st_ctime,
                                        modify_time=sb.st_mtime)
                stats.append(st)

        self.dataset_tracker.updateDirectory(path=entry.path, entries=stats)

    def connect(self, scan_dataset=True):
        dataset_root = self.dataset_tracker.getRootPath()
        if not os.path.exists(dataset_root):
            raise IOError("dataset root does not exist")

        # start monitoring
        self.notifier.start()

        mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_MODIFY | pyinotify.IN_ATTRIB | pyinotify.IN_MOVED_FROM | pyinotify.IN_MOVED_TO | pyinotify.IN_MOVE_SELF
        self.watch_directory = self.watch_manager.add_watch(dataset_root, 
                                                            mask, 
                                                            rec=True,
                                                            auto_add=True)

        if scan_dataset:
            # add initial dataset
            entries = os.listdir(dataset_root)
            stats = []
            for entry in entries:
                # entry is a filename
                entry_path = os.path.join(dataset_root, entry)
                # get stat
                sb = os.stat(entry_path)

                st = abstractfs.afsstat(directory=stat.S_ISDIR(sb.st_mode), 
                                        path=entry_path,
                                        name=entry, 
                                        size=sb.st_size,
                                        checksum=0,
                                        create_time=sb.st_ctime,
                                        modify_time=sb.st_mtime)
                stats.append(st)
            self.dataset_tracker.updateDirectory(path=dataset_root, entries=stats)

    def close(self):
        if self.watch_manager and self.watch_directory:
            self.watch_manager.rm_watch(self.watch_directory.values())

        if self.notifier:
            self.notifier.stop()

    def exists(self, path):
        ascii_path = path.encode('ascii','ignore')
        return os.path.exists(ascii_path)

    def list_dir(self, dirpath):
        ascii_path = dirpath.encode('ascii','ignore')
        return os.listdir(ascii_path)

    def is_dir(self, dirpath):
        ascii_path = dirpath.encode('ascii','ignore')
        if os.path.exists(ascii_path):
            sb = os.stat(ascii_path)
            return stat.S_ISDIR(sb.st_mode)
        return False

    def read(self, filepath, offset, size):
        ascii_path = filepath.encode('ascii','ignore')
        buf = None
        try:
            with open(ascii_path, "r") as f:
                f.seek(offset)
                buf = f.read(size)
        except Exception, e:
            logger.error("Failed to read %s: %s" % (file_path, e))
            sys.exit(1)
        return buf

    def plugin(self):
        return self.__class__

    def role(self):
        return self.role

    def set_notification_cb(self, notification_cb):
        self.notification_cb = notification_cb



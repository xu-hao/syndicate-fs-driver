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
iPlant Data Store Plugin
"""
import os
import time
import logging
import json
import threading
import sgfsdriver.lib.abstractfs as abstractfs
import sgfsdriver.plugins.datastore.bms_client as bms_client
import sgfsdriver.plugins.datastore.irods_client as irods_client

logger = logging.getLogger('syndicate_datastore_filesystem')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('syndicate_datastore_filesystem.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

class BMSEventHandler(object):
    def __init__(self, plugin, work_root):
        self.plugin = plugin
        self.work_root = work_root

    def MessageHandler(self, message):
        # parse message and update directory
        if not message or len(message) <= 0:
            logger.info("Empty message")
            return

        msg = json.loads(message)
        if not msg:
            logger.info("Empty JSON message")
            return

        operation = msg.get("operation")
        if not operation:
            logger.info("Empty operation")
            return

        # convert to ascii
        operation = operation.encode('ascii','ignore')

        if operation in ["collection.add", "data-object.add"]:
            path = msg.get("path")
            if not path:
                logger.info("Empty path for operation %s" % operation)
                return

            path = path.encode('ascii','ignore')
            if not path.startswith(self.work_root):
                return

            logger.info("Creating: %s" % path)
            self.plugin.on_update_detected("create", path)
            return
        elif operation in ["collection.rm", "data-object.rm"]:
            path = msg.get("path")
            if not path:
                logger.info("Empty path for operation %s" % operation)
                return

            path = path.encode('ascii','ignore')
            if not path.startswith(self.work_root):
                return

            logger.info("Removing: %s" % path)
            self.plugin.on_update_detected("remove", path)
        elif operation == "data-object.mod":
            path = msg.get("entity_path")
            if not path:
                logger.info("Empty path for operation %s" % operation)
                return

            path = path.encode('ascii','ignore')
            if not path.startswith(self.work_root):
                return

            logger.info("Modifying: %s" % path)
            self.plugin.on_update_detected("modify", path)
        elif operation in ["collection.mv", "data-object.mv"]:
            old_path = msg.get("old-path")
            if not old_path:
                logger.info("Empty old-path for operation %s" % operation)
                return

            old_path = old_path.encode('ascii','ignore')
            if old_path.startswith(self.work_root):
                logger.info("Moving a file from : %s" % old_path)
                self.plugin.on_update_detected("remove", old_path)

            new_path = msg.get("new-path")
            if not new_path:
                logger.info("Empty new-path for operation %s" % operation)
                return

            new_path = new_path.encode('ascii','ignore')
            if new_path.startswith(self.work_root):
                logger.info("Moving a file to : %s" % new_path)
                self.plugin.on_update_detected("create", new_path)
        else:
            logger.info("Unhandled operation to a file : %s" % operation)
            logger.info("- %s" % msg)


class plugin_impl(abstractfs.afsbase):
    def __init__(self, config, role=abstractfs.afsrole.DISCOVER):
        if not config:
            raise ValueError("fs configuration is not given correctly")

        work_root = config.get("work_root")
        if not work_root:
            raise ValueError("work_root configuration is not given correctly")

        secrets = config.get("secrets")
        if not secrets:
            raise ValueError("secrets are not given correctly")

        user = secrets.get("user")
        user = user.encode('ascii','ignore')
        if not user:
            raise ValueError("user is not given correctly")

        password = secrets.get("password")
        password = password.encode('ascii','ignore')
        if not password:
            raise ValueError("password is not given correctly")

        irods_config = config.get("irods")
        if not irods_config:
            raise ValueError("irods configuration is not given correctly")

        bms_config = None
        if role == abstractfs.afsrole.DISCOVER:
            bms_config = config.get("bms")
            if not bms_config:
                raise ValueError("bms configuration is not given correctly")

        # set role
        self.role = role

        # config can have unicode strings
        work_root = work_root.encode('ascii','ignore')
        self.work_root = work_root.rstrip("/")

        self.irods_config = irods_config
        self.bms_config = bms_config

        # init irods client
        # we convert unicode (maybe) strings to ascii since python-irodsclient cannot accept unicode strings
        irods_host = self.irods_config["host"]
        irods_host = irods_host.encode('ascii','ignore')
        irods_zone = self.irods_config["zone"]
        irods_zone = irods_zone.encode('ascii','ignore')

        logger.info("__init__: initializing irods_client")
        self.irods = irods_client.irods_client(host=irods_host,
                                               port=self.irods_config["port"],
                                               user=user,
                                               password=password,
                                               zone=irods_zone)

        if self.role == abstractfs.afsrole.DISCOVER:
            # init bms client
            logger.info("__init__: initializing bms_client")
            path_filter = work_root.rstrip("/") + "/*"

            acceptor = bms_client.bms_message_acceptor("path",
                                                       path_filter)
            logger.info("__init__: path_filter = " + path_filter)
            self.bms = bms_client.bms_client(host=self.bms_config["host"],
                                             port=self.bms_config["port"],
                                             user=user,
                                             password=password,
                                             vhost=self.bms_config["vhost"],
                                             acceptors=[acceptor])

            self.notify_handler = BMSEventHandler(self, self.work_root)
            self.bms.setCallbacks(on_message_callback=self.notify_handler.MessageHandler)

        self.notification_cb = None
        # create a re-entrant lock (not a read lock)
        self.lock = threading.RLock()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def _get_lock(self):
        return self.lock

    def on_update_detected(self, operation, path):
        ascii_path = path.encode('ascii','ignore')
        driver_path = self._make_driver_path(ascii_path)

        self.clear_cache(driver_path)
        if operation == "remove":
            if self.notification_cb:
                entry = abstractfs.afsevent(driver_path, None)
                self.notification_cb([], [], [entry])
        elif operation in ["create", "modify"]:
            if self.notification_cb:
                st = self.stat(driver_path)
                entry = abstractfs.afsevent(driver_path, st)
                if operation == "create":
                    self.notification_cb([], [entry], [])
                elif operation == "modify":
                    self.notification_cb([entry], [], [])

    def _make_irods_path(self, path):
        if path.startswith(self.work_root):
            return path.rstrip("/")

        if path.startswith("/"):
            return self.work_root + path.rstrip("/")

        return self.work_root + "/" + path.rstrip("/")

    def _make_driver_path(self, path):
        if path.startswith(self.work_root):
            return path[len(self.work_root):].rstrip("/")
        return path.rstrip("/")

    def connect(self):
        logger.info("connect: connecting to iRODS")
        self.irods.connect()

        if self.role == abstractfs.afsrole.DISCOVER:
            if not self.irods.exists(self.work_root):
                raise IOError("work_root does not exist")

            try:
                logger.info("connect: connecting to BMS")
                self.bms.connect()
            except:
                self.close()

    def close(self):
        logger.info("close")
        if self.role == abstractfs.afsrole.DISCOVER:
            logger.info("close: closing BMS")
            if self.bms:
                self.bms.close()

        logger.info("close: closing iRODS")
        if self.irods:
            self.irods.close()

    def stat(self, path):
        with self._get_lock():
            ascii_path = path.encode('ascii','ignore')
            irods_path = self._make_irods_path(ascii_path)
            driver_path = self._make_driver_path(ascii_path)
            # get stat
            sb = self.irods.stat(irods_path)
            return abstractfs.afsstat(directory=sb.directory,
                                      path=driver_path,
                                      name=os.path.basename(driver_path),
                                      size=sb.size,
                                      checksum=sb.checksum,
                                      create_time=sb.create_time,
                                      modify_time=sb.modify_time)

    def exists(self, path):
        with self._get_lock():
            ascii_path = path.encode('ascii','ignore')
            irods_path = self._make_irods_path(ascii_path)
            exist = self.irods.exists(irods_path)
            return exist

    def list_dir(self, dirpath):
        with self._get_lock():
            ascii_path = dirpath.encode('ascii','ignore')
            irods_path = self._make_irods_path(ascii_path)
            l = self.irods.list_dir(irods_path)
            return l

    def is_dir(self, dirpath):
        with self._get_lock():
            ascii_path = dirpath.encode('ascii','ignore')
            irods_path = self._make_irods_path(ascii_path)
            d = self.irods.is_dir(irods_path)
            return d

    def make_dirs(self, dirpath):
        with self._get_lock():
            ascii_path = dirpath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            if not self.exists(irods_path):
                self.irods.make_dirs(irods_path)

    def read(self, filepath, offset, size):
        with self._get_lock():
            ascii_path = filepath.encode('ascii','ignore')
            irods_path = self._make_irods_path(ascii_path)
            buf = self.irods.read(irods_path, offset, size)
            return buf

    def write(self, filepath, offset, buf):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.write(irods_path, offset, buf)

    def truncate(self, filepath, size):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.truncate(irods_path, size)

    def clear_cache(self, path):
        with self._get_lock():
            if path:
                ascii_path = path.encode('ascii', 'ignore')
                irods_path = self._make_irods_path(ascii_path)
                self.irods.clear_stat_cache(irods_path)
            else:
                self.irods.clear_stat_cache(None)

    def unlink(self, filepath):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.unlink(irods_path)

    def rename(self, filepath1, filepath2):
        with self._get_lock():
            ascii_path1 = filepath1.encode('ascii', 'ignore')
            ascii_path2 = filepath2.encode('ascii', 'ignore')
            irods_path1 = self._make_irods_path(ascii_path1)
            irods_path2 = self._make_irods_path(ascii_path2)
            self.irods.rename(irods_path1, irods_path2)

    def set_xattr(self, filepath, key, value):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            self.irods.set_xattr(irods_path, key, value)

    def get_xattr(self, filepath, key):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            irods_path = self._make_irods_path(ascii_path)
            return self.irods.get_xattr(irods_path, key)

    def list_xattr(self, filepath):
        with self._get_lock():
            ascii_path = filepath.encode('ascii', 'ignore')
            localfs_path = self._make_irods_path(ascii_path)
            return self.irods.list_xattr(localfs_path)

    def plugin(self):
        return self.__class__

    def role(self):
        return self.role

    def set_notification_cb(self, notification_cb):
        self.notification_cb = notification_cb

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
iPlant Data Store Plugin
"""

import os
import time
import logging
import json
import sagfsdriver.lib.abstractfs as abstractfs
import sagfsdriver.lib.metadata as metadata
import sagfsdriver.plugins.datastore.bms_client as bms_client
import sagfsdriver.plugins.datastore.irods_client as irods_client

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

class plugin_impl(abstractfs.afsbase):
    def __init__(self, config, role=abstractfs.afsrole.DISCOVER):
        logger.info("__init__")

        if not config:
            logger.error("__init__: fs configuration is not given correctly")
            raise ValueError("fs configuration is not given correctly")

        dataset_root = config.get("dataset_root")
        if not dataset_root:
            logger.error("__init__: dataset_root configuration is not given correctly")
            raise ValueError("dataset_root configuration is not given correctly")

        # config can have unicode strings
        dataset_root = dataset_root.encode('ascii','ignore')
        dataset_root = dataset_root.rstrip("/")

        secrets = config.get("secrets")
        if not secrets:
            logger.error("__init__: secrets are not given correctly")
            raise ValueError("secrets are not given correctly")

        user = secrets.get("user")
        user = user.encode('ascii','ignore')
        if not user:
            logger.error("__init__: user is not given correctly")
            raise ValueError("user is not given correctly")

        password = secrets.get("password")
        password = password.encode('ascii','ignore')
        if not password:
            logger.error("__init__: password is not given correctly")
            raise ValueError("password is not given correctly")

        irods_config = config.get("irods")
        if not irods_config:
            logger.error("__init__: irods configuration is not given correctly")
            raise ValueError("irods configuration is not given correctly")

        bms_config = config.get("bms")
        if not bms_config:
            logger.error("__init__: bms configuration is not given correctly")
            raise ValueError("bms configuration is not given correctly")

        # set role
        logger.info("__init__: set role : " + str(role))
        self.role = role

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
            path_filter = dataset_root.rstrip("/") + "/*"

            acceptor = bms_client.bms_message_acceptor("path", 
                                                       path_filter)
            logger.info("__init__: path_filter = " + path_filter)
            self.bms = bms_client.bms_client(host=self.bms_config["host"], 
                                             port=self.bms_config["port"], 
                                             user=user, 
                                             password=password, 
                                             vhost=self.bms_config["vhost"],
                                             acceptors=[acceptor])

            self.bms.setCallbacks(on_message_callback=self._on_bms_message_receive)

            # init dataset tracker
            logger.info("__init__: initializing dataset tracker")
            self.dataset_tracker = metadata.datasetmeta(root_path=dataset_root,
                                                        update_event_handler=self._on_dataset_update, 
                                                        request_for_update_handler=self._on_request_update)

        self.notification_cb = None

    def _on_bms_message_receive(self, message):
        logger.info("_on_bms_message_receive : " + message)
        # parse message and update directory
        if not message or len(message) <= 0:
            return

        dataset_root = self.dataset_tracker.getRootPath()

        msg = json.loads(message)
        operation = msg.get("operation")
        if operation:
            operation = operation.encode('ascii','ignore')
            if operation in ["collection.add", "collection.rm", "data-object.add", "data-object.rm"]:
                path = msg.get("path")
                if path:
                    path = path.encode('ascii','ignore')
                    parent_path = os.path.dirname(path)
                    entries = self.irods.listStats(parent_path)
                    stats = []
                    for e in entries:
                        stat = abstractfs.afsstat(directory=e.directory, 
                                                  path=e.path,
                                                  name=e.name, 
                                                  size=e.size,
                                                  checksum=e.checksum,
                                                  create_time=e.create_time,
                                                  modify_time=e.modify_time)
                        stats.append(stat)
                    self.dataset_tracker.updateDirectory(path=parent_path, entries=stats)

            elif operation in ["data-object.mod"]:
                path = msg.get("entity_path")
                if path:
                    path = path.encode('ascii','ignore')
                    parent_path = os.path.dirname(path)
                    entries = self.irods.listStats(parent_path)
                    stats = []
                    for e in entries:
                        stat = abstractfs.afsstat(directory=e.directory, 
                                                  path=e.path,
                                                  name=e.name, 
                                                  size=e.size,
                                                  checksum=e.checksum,
                                                  create_time=e.create_time,
                                                  modify_time=e.modify_time)
                        stats.append(stat)
                    self.dataset_tracker.updateDirectory(path=parent_path, entries=stats)

            elif operation in ["collection.mv", "data-object.mv"]:
                old_path = msg.get("old-path")
                old_parent_path = None
                if old_path:
                    old_path = old_path.encode('ascii','ignore')
                    if old_path.startswith(dataset_root):
                        old_parent_path = os.path.dirname(old_path)
                        entries = self.irods.listStats(old_parent_path)
                        stats = []
                        for e in entries:
                            stat = abstractfs.afsstat(directory=e.directory, 
                                                      path=e.path,
                                                      name=e.name, 
                                                      size=e.size,
                                                      checksum=e.checksum,
                                                      create_time=e.create_time,
                                                      modify_time=e.modify_time)
                            stats.append(stat)
                        self.dataset_tracker.updateDirectory(path=old_parent_path, entries=stats)

                new_path = msg.get("new-path")
                if new_path:
                    new_path = new_path.encode('ascii','ignore')
                    if new_path.startswith(dataset_root):
                        new_parent_path = os.path.dirname(new_path)
                        if new_parent_path != old_parent_path:
                            entries = self.irods.listStats(new_parent_path)
                            stats = []
                            for e in entries:
                                stat = abstractfs.afsstat(directory=e.directory, 
                                                          path=e.path,
                                                          name=e.name, 
                                                          size=e.size,
                                                          checksum=e.checksum,
                                                          create_time=e.create_time,
                                                          modify_time=e.modify_time)
                                stats.append(stat)
                            self.dataset_tracker.updateDirectory(path=new_parent_path, entries=stats)

    def _on_dataset_update(self, updated_entries, added_entries, removed_entries):
        logger.info("_on_dataset_update")
        if self.notification_cb:
            self.notification_cb(updated_entries, added_entries, removed_entries)

    def _on_request_update(self, entry):
        logger.info("_on_request_update")
        entries = self.irods.listStats(entry.path)
        stats = []
        for e in entries:
            stat = abstractfs.afsstat(directory=e.directory, 
                                      path=e.path,
                                      name=e.name, 
                                      size=e.size,
                                      checksum=e.checksum,
                                      create_time=e.create_time,
                                      modify_time=e.modify_time)
            stats.append(stat)
        self.dataset_tracker.updateDirectory(path=entry.path, entries=stats)

    def connect(self):
        logger.info("connect")
        logger.info("connect: connecting to iRODS")
        self.irods.connect()

        if self.role == abstractfs.afsrole.DISCOVER:
            logger.info("connect: connecting to BMS")
            self.bms.connect()

            # add initial dataset
            logger.info("connect: Add initial dataset")
            dataset_root = self.dataset_tracker.getRootPath()
            entries = self.irods.listStats(dataset_root)
            stats = []
            for entry in entries:
                stat = abstractfs.afsstat(directory=entry.directory, 
                                          path=entry.path,
                                          name=entry.name, 
                                          size=entry.size,
                                          checksum=entry.checksum,
                                          create_time=entry.create_time,
                                          modify_time=entry.modify_time)
                stats.append(stat)
            self.dataset_tracker.updateDirectory(path=dataset_root, entries=stats)

    def close(self):
        logger.info("close")
        if self.role == abstractfs.afsrole.DISCOVER:
            logger.info("close: closing BMS")
            self.bms.close()

        logger.info("close: closing iRODS")
        self.irods.close()

    def exists(self, path):
        logger.info("exists : " + path)
        ascii_path = path.encode('ascii','ignore')
        return self.irods.exists(ascii_path)

    def list_dir(self, dirpath):
        logger.info("list_dir : " + dirpath)
        ascii_path = dirpath.encode('ascii','ignore')
        return self.irods.list(ascii_path)

    def is_dir(self, dirpath):
        logger.info("is_dir : " + dirpath)
        ascii_path = dirpath.encode('ascii','ignore')
        return self.irods.isDir(ascii_path)

    def read(self, filepath, offset, size):
        logger.info("read : " + filepath + ", off(" + str(offset) + "), size(" + str(size) + ")")
        ascii_path = filepath.encode('ascii','ignore')
        return self.irods.read(ascii_path, offset, size)

    def plugin(self):
        return self.__class__

    def role(self):
        return self.role

    def set_notification_cb(self, notification_cb):
        self.notification_cb = notification_cb



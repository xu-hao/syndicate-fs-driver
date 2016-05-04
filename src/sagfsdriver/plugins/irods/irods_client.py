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

import traceback
import os
import irods
import logging

from irods.session import iRODSSession
from irods.data_object import iRODSDataObject, iRODSDataObjectFileRaw
from expiringdict import ExpiringDict
#from retrying import retry
#from timeout_decorator import timeout

logger = logging.getLogger('irods_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('irods_client.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)


MAX_ATTEMPT = 3         # 3 retries
ATTEMPT_INTERVAL = 5000 # 5 sec
TIMEOUT_SECONDS = 20    # 20 sec

METADATA_CACHE_SIZE = 1000
METADATA_CACHE_TTL = 60 # 60 sec

"""
Timeout only works at a main thread.
"""

"""
Interface class to iRODS
"""
class irods_status(object):
    def __init__(self, directory=False, 
                       path=None,
                       name=None, 
                       size=0,
                       checksum=0,
                       create_time=0,
                       modify_time=0):
        self.directory = directory
        self.path = path
        self.name = name
        self.size = size
        self.checksum = checksum
        self.create_time = create_time
        self.modify_time = modify_time

    @classmethod
    def fromCollection(cls, col):
        return irods_status(directory=True, 
                            path=col.path,
                            name=col.name)

    @classmethod
    def fromDataObject(cls, obj):
        return irods_status(directory=False, 
                            path=obj.path,
                            name=obj.name, 
                            size=obj.size, 
                            checksum=obj.checksum, 
                            create_time=obj.create_time, 
                            modify_time=obj.modify_time)

    def __eq__(self, other): 
        return self.__dict__ == other.__dict__

    def __repr__(self): 
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<irods_status %s %s %d %s>" % (rep_d, self.name, self.size, self.checksum) 

class irods_client(object):
    def __init__(self, host=None,
                       port=1247,
                       user=None,
                       password=None,
                       zone=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone
        self.session = None

        # init cache
        self.meta_cache = ExpiringDict(max_len=METADATA_CACHE_SIZE,
                                       max_age_seconds=METADATA_CACHE_TTL)

    def connect(self):
        self.session = iRODSSession(host=self.host, 
                                    port=self.port, 
                                    user=self.user, 
                                    password=self.password, 
                                    zone=self.zone)

    def close(self):
        self.session.cleanup()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        coll = self.session.collections.get(path)
        stats = []
        for col in coll.subcollections:
            stats.append(irods_status.fromCollection(col))

        for obj in coll.data_objects:
            stats.append(irods_status.fromDataObject(obj))

        self.meta_cache[path] = stats
        return stats

    """
    Returns irods_status
    """
    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def stat(self, path):
        parent = os.path.dirname(path)
        stats = self._ensureDirEntryStatLoaded(parent)
        if stats:
            for stat in stats:
                if stat.path == path:
                    return stat
        return None

    """
    Returns directory entries in string
    """
    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def list_dir(self, path):
        stats = self._ensureDirEntryStatLoaded(path)
        if stats:
            entries = []
            for stat in stats:
                entries.append(stat.name)
            return entries
        return None

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def is_dir(self, path):
        stat = self.stat(path)
        if stat:
            return stat.directory
        return False

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def exists(self, path):
        stat = self.stat(path)
        if stat:
            return True
        return False

    def clear_stat_cache(self, path=None):
        if(path):
            if path in self.meta_cache:
                # directory
                del self.meta_cache[path]
            else:
                # file
                parent = os.path.dirname(path)
                if parent in self.meta_cache:
                    del self.meta_cache[parent]
        else:
            self.meta_cache.clear

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def read(self, path, offset, size):
        logger.info("read : " + path + ", off(" + str(offset) + "), size(" + str(size) + ")")
        buf = None
        try:
            logger.info("read: opening a file " + path)
            obj = self.session.data_objects.get(path)
            with obj.open('r') as f:
                if offset != 0:
                    logger.info("read: seeking at " + str(offset))
                    new_offset = f.seek(offset)
                    if new_offset != offset:
                        logger.error("read: offset mismatch - requested(" + str(offset) + "), but returned(" + new_offset + ")")
                        raise Exception("read: offset mismatch - requested(" + str(offset) + "), but returned(" + new_offset + ")")

                logger.info("read: reading size " + str(size))
                buf = f.read(size)
                logger.info("read: read done")

        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        logger.info("read: returning the buf(" + buf + ")")
        return buf

    #@retry(stop_max_attempt_number=MAX_ATTEMPT, wait_fixed=ATTEMPT_INTERVAL, wrap_exception=True)
    def download(self, path, to):
        obj = self.session.data_objects.get(path)
        with obj.open('r') as f:
            try:
                with open(to, 'w') as wf:
                    while(True):
                        buf = f.read(1024*1024)

                        if not buf:
                            break

                        wf.write(buf)
            except Exception, e:
                logger.error("download: " + traceback.format_exc())
                traceback.print_exc()
                raise e

        return to


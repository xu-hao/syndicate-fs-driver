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

import traceback
import os
import stat
import logging
import ftplib
import ftputil

logger = logging.getLogger('ftp_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('ftp_client.log')
fh.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)

METADATA_CACHE_SIZE = 10000
METADATA_CACHE_TTL = 60     # 60 sec

"""
Interface class to FTP
"""


class ftp_status(object):
    def __init__(self,
                 directory=False,
                 path=None,
                 name=None,
                 size=0,
                 create_time=0):
        self.directory = directory
        self.path = path
        self.name = name
        self.size = size
        self.create_time = create_time

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        rep_d = "F"
        if self.directory:
            rep_d = "D"

        return "<ftp_status %s %s %d>" % \
            (rep_d, self.name, self.size)


class ftp_client(object):
    def __init__(self,
                 host=None,
                 port=21,
                 user='anonymous',
                 password='anonymous@email.com'):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.session = None

    def connect(self):
        my_session_factory = ftputil.session.session_factory(
            base_class=ftplib.FTP,
            port=self.port,
            debug_level=2
        )

        self.session = ftputil.FTPHost(
            self.host,
            self.user,
            self.password,
            session_factory=my_session_factory
        )
        self.session.stat_cache.resize(METADATA_CACHE_SIZE)
        self.session.stat_cache.max_age = METADATA_CACHE_TTL
        self.session.stat_cache.enable()

    def close(self):
        self.session.close()

    def reconnect(self):
        self.close()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    """
    Returns ftp_status
    """
    def stat(self, path):
        try:
            sb = self.session.lstat(path)
            return ftp_status(
                directory=stat.S_ISDIR(sb.st_mode),
                path=path,
                name=os.path.basename(path),
                size=sb.st_size,
                create_time=sb.st_ctime
            )
        except Exception:
            return None

    """
    Returns directory entries in string
    """
    def list_dir(self, path):
        try:
            entries = self.session.listdir(path)
            return entries
        except Exception:
            return None

    def is_dir(self, path):
        sb = self.stat(path)
        if sb:
            return sb.directory
        return False

    def make_dirs(self, path):
        self.session.makedirs(path)
        self.session.stat_cache.invalidate(path)

    def exists(self, path):
        try:
            sb = self.stat(path)
            if sb:
                return True
            return False
        except Exception:
            return False

    def clear_stat_cache(self, path=None):
        if(path):
            self.session.stat_cache.invalidate(path)
        else:
            self.session.stat_cache.clear()

    def read(self, path, offset, size):
        logger.info(
            "read : %s, off(%d), size(%d)" %
            (path, offset, size)
        )
        buf = None
        try:
            logger.info("read: opening a file - %s" % path)

            with self.session.open(path, "rb", rest=offset) as f:
                logger.info("read: reading size - %d" % size)
                buf = f.read(size)
                logger.info("read: read done")
        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        #logger.info("read: returning the buf(" + buf + ")")
        return buf

    def write(self, path, offset, buf):
        logger.info(
            "write : %s, off(%d), size(%d)" %
            (path, offset, len(buf)))
        try:
            with self.session.open(path, 'wb', rest=offset) as f:
                logger.info("write: writing buffer %d" % len(buf))
                f.write(buf)
                logger.info("write: writing done")
        except Exception, e:
            logger.error("write: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def truncate(self, path, size):
        logger.info("truncate : %s" % path)
        raise IOError("truncate is not supported")

    def unlink(self, path):
        logger.info("unlink : %s" % path)
        try:
            logger.info("unlink: deleting a file - %s" % path)
            self.session.unlink(path)
            logger.info("unlink: deleting done")

        except Exception, e:
            logger.error("unlink: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path)

    def rename(self, path1, path2):
        logger.info("rename : %s -> %s" % (path1, path2))
        try:
            logger.info("rename: renaming a file - %s to %s" % (path1, path2))
            self.session.rename(path1, path2)
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)

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
import datetime
import logging
import boto3

from expiringdict import ExpiringDict

logger = logging.getLogger('s3_client')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('s3_client.log')
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
Interface class to S3
"""


class s3_status(object):
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

        return "<s3_status %s %s %d>" % \
            (rep_d, self.name, self.size)


class s3_client(object):
    def __init__(self,
                 bucket=None,
                 access_id=None,
                 access_key=None,
                 region=None):
        self.bucket = bucket
        self.access_id = access_id
        self.access_key = access_key
        self.region = None
        if region:
            self.region = region

        self.session = None

        # init cache
        self.meta_cache = ExpiringDict(max_len=METADATA_CACHE_SIZE,
                                       max_age_seconds=METADATA_CACHE_TTL)

    def connect(self):
        if self.region:
            self.session = boto3.client(
                "s3",
                self.region,
                aws_access_key_id=self.access_id,
                aws_secret_access_key=self.access_key
            )
        else:
            self.session = boto3.client(
                "s3",
                aws_access_key_id=self.access_id,
                aws_secret_access_key=self.access_key
            )

        try:
            self.session.create_bucket(Bucket=self.bucket)
        except Exception, e:
            logger.error(
                "Could not create a bucket - %s" % self.bucket
            )
            raise e

    def close(self):
        self.session = None

    def reconnect(self):
        self.close()
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _ensureDirEntryStatLoaded(self, path):
        # reuse cache
        if path in self.meta_cache:
            return self.meta_cache[path]

        stats = []
        paginator = self.session.get_paginator("list_objects")
        if path == "/":
            dir_path = ""
        else:
            dir_path = (path + "/").lstrip("/")

        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Delimiter="/",
            Prefix=dir_path
        )

        for page in page_iterator:
            contents = page['Contents']
            for obj in contents:
                key = obj["Key"]
                name = key[len(dir_path):]
                directory = False
                size = 0
                if name.endswith("/"):
                    directory = True
                if not directory:
                    size = obj["Size"]
                last_modified = obj["LastModified"]
                sb = s3_status(
                    directory=directory,
                    path=key.rstrip("/"),
                    name=name.rstrip("/"),
                    size=size,
                    create_time=last_modified
                )
                stats.append(sb)

        self.meta_cache[path] = stats
        return stats

    """
    Returns s3_status
    """
    def stat(self, path):
        try:
            if path == "/":
                response = self.session.list_buckets()
                create_time = datetime.datetime(2015, 1, 1)
                for bucket in response["Buckets"]:
                    if bucket["Name"] == self.bucket:
                        create_time = bucket["CreationDate"]
                        return s3_status(
                            directory=True,
                            path="/",
                            name="/",
                            size=0,
                            create_time=create_time
                        )

                return None
            else:
                # try bulk loading of stats
                parent = os.path.dirname(path)
                stats = self._ensureDirEntryStatLoaded(parent)
                if stats:
                    for sb in stats:
                        if sb.path == path:
                            return sb
                return None
        except Exception:
            return None

    """
    Returns directory entries in string
    """
    def list_dir(self, path):
        stats = self._ensureDirEntryStatLoaded(path)
        entries = []
        if stats:
            for sb in stats:
                entries.append(sb.name)
        return entries

    def is_dir(self, path):
        sb = self.stat(path)
        if sb:
            return sb.directory
        return False

    def make_dirs(self, path):
        if not self.exists(path):
            # make parent dir first
            self.make_dirs(os.path.dirname(path))
            self.session.put_object(
                Bucket=self.bucket,
                ContentLength=0,
                Key=path + "/"
            )
            # invalidate stat cache
            self.clear_stat_cache(os.path.dirname(path))

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
            logger.info("read: reading size - %d" % size)

            response = self.session.get_object(
                Bucket=self.bucket,
                Key=path,
                Range="bytes=%d-%d" % (offset, offset + size)
            )

            logger.info("read: read done")
        except Exception, e:
            logger.error("read: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        return response["Body"].read()

    def write(self, path, offset, buf):
        logger.info(
            "write : %s, off(%d), size(%d)" %
            (path, offset, len(buf)))
        try:
            if offset != 0:
                response = self.session.get_object(
                    Bucket=self.bucket,
                    Key=path
                )
                old_data = response["Body"].read()
                old_data[offset:len(buf)] = buf

                logger.info("write: writing buffer %d" % len(old_data))
                response = self.session.put_object(
                    Body=old_data,
                    Bucket=self.bucket,
                    Key=path
                )
                logger.info("write: writing done")

            else:
                logger.info("write: writing buffer %d" % len(buf))
                response = self.session.put_object(
                    Body=buf,
                    Bucket=self.bucket,
                    Key=path
                )
                logger.info("write: writing done")
        except Exception, e:
            logger.error("write: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(os.path.dirname(path))

    def truncate(self, path, size):
        logger.info("truncate : %s" % path)
        raise IOError("truncate is not supported")

    def unlink(self, path):
        logger.info("unlink : %s" % path)
        try:
            logger.info("unlink: deleting a file - %s" % path)
            response = self.session.delete_object(
                Bucket=self.bucket,
                Key=path
            )
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
            response = self.session.copy_object(
                Bucket=self.bucket, 
                CopySource="%s/%s" % (self.bucket, path1),
                Key=path2
            )
            response = self.session.delete_object(
                Bucket=self.bucket,
                Key=path1
            )
            logger.info("rename: renaming done")

        except Exception, e:
            logger.error("rename: " + traceback.format_exc())
            traceback.print_exc()
            raise e

        # invalidate stat cache
        self.clear_stat_cache(path1)
        self.clear_stat_cache(path2)

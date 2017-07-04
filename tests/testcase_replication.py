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

REP_TARGET_FILE = "/REPLICA_TARGET_FILE"


class replication_test_impl():
    def __init__(self, driver):
        if not driver:
            raise ValueError("driver is not given correctly")

        self.driver = driver

    def start(self):
        """
        replicate test
        """
        print "Replicate ../LICENSE"
        self.driver.replicate_all("../LICENSE", REP_TARGET_FILE, 1)

        """
        read test
        """
        print "Read test ../LICENSE"
        self.driver.read_and_check(REP_TARGET_FILE, "../LICENSE", 1)

        """
        block update test
        """
        print "Replicate ../README.md"
        self.driver.replicate_all("../README.md", REP_TARGET_FILE, 2)

        """
        block delete test
        """
        print "Delete test ../REPLICA_TARGET_FILE"
        self.driver.delete_all("/REPLICA_TARGET_FILE", 1)

        """
        read test
        """
        print "Read test ../README.md"
        self.driver.read_and_check(REP_TARGET_FILE, "../README.md", 2)

        """
        block delete test
        """
        print "Delete test ../REPLICA_TARGET_FILE"
        self.driver.delete_all("/REPLICA_TARGET_FILE", 1)

        """
        read test
        """
        print "Read test ../README.md"
        self.driver.read_and_check(REP_TARGET_FILE, "../README.md", 2)

        """
        block delete test
        """
        print "Delete test ../REPLICA_TARGET_FILE"
        self.driver.delete_all("/REPLICA_TARGET_FILE", 2)

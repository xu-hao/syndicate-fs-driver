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

import os
import imp

"""
Exceptions
"""
class PluginNotExist(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class PluginLoaderError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

"""
Find a plugin and create an instance
"""
class pluginloader(object):
    def __init__(self):
        pass

    def findModule(self, plugin_name=None):
        module_dir = os.path.dirname(os.path.abspath(__file__))
        module_path = os.path.abspath(module_dir + "/../plugins/" + plugin_name + "/" + plugin_name + ".py")
        print module_path
        if os.path.exists(module_path):
            return imp.load_source(plugin_name,
                                   module_path)
        else:
            return None

    def load(self, plugin_name=None, plugin_config=None):
        if plugin_name:
            print "load", plugin_name
            plugin = self.findModule(plugin_name)
            if plugin:
                return plugin.plugin_impl(plugin_config)
            else:
                raise PluginNotExist("unable to find a plugin for %s" % plugin_name)
        else:
            raise PluginLoaderError("a plugin name is not given")



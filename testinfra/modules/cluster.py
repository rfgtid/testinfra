# coding: utf-8
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import unicode_literals
from collections import deque
from itertools import takewhile
import re
from testinfra.modules.base import Module


# This is the resulting class tree after parsing the command command output

class _PCSItem(object):
    def __init__(self, parent=None, level=-1):
        self.parent = parent
        self.level = level
        self.name = None

    def find(self, name):
        return self if self.name == name else None

    def __str__(self):
        pname = self.parent.name if self.parent else "None"
        return "[{0}] TYPE: '{3}'' NAME: '{1}' (parent is '{2}')".format(
            self.level,
            self.name,
            pname,
            self.__class__.__name__)

    def display(self):
        """debug method"""
        print(str(self))


class _PCSNameValuePair(_PCSItem):

    def __init__(self, parent=None, level=-1):
        super(_PCSNameValuePair, self).__init__(parent, level)
        self.value = None

    def __str__(self):
        pname = self.parent.name if self.parent else "None"
        return "[{0}] NAME: '{1}' TYPE: '{4} VALUE: '{2}' \
            (parent is '{3}')".format(
            self.level,
            self.name,
            self.value,
            pname,
            self.__class__.__name__)


class _PCSResource(_PCSItem):

    def __init__(self, parent=None, level=-1):
        super(_PCSResource, self).__init__(parent, level)
        self._state = None

    @property
    def state(self):
        return self._state

    def __str__(self):
        pname = self.parent.name if self.parent else "None"
        return "[{0}] NAME: TYPE: '{4}' {1}' STATE: '{2}' \
            (parent is '{3}')".format(
            self.level,
            self.name,
            self.state,
            pname,
            self.__class__.__name__)


class _PCSMasterResource(_PCSResource):
    pass


class _PCSSlaveResource(_PCSResource):
    pass


class _PCSCloneResource(_PCSResource):
    pass


class _PCSComposite(_PCSItem):

    def __init__(self, parent=None, level=-1):
        super(_PCSComposite, self).__init__(parent, level)
        self.children = []

    def __str__(self):
        pname = self.parent.name if self.parent else "None"
        return "[{0}] NAME: '{1}' TYPE '{4}' #CHILDREN {2} \
            (parent is '{3}')".format(
            self.level,
            self.name,
            len(self.children),
            pname,
            self.__class__.__name__)

    def find(self, name):
        if self.name == name:
            return self
        for child in self.children:
            found = child.find(name)
            if found:
                return found
        return None

    def display(self):
        print(str(self))
        for child in self.children:
            child.display()


class _PCSResourceComposite(_PCSComposite):

    @property
    def state(self):
        for child in self.children:
            if child.state == 'Stopped':
                return 'Stopped'
        return 'Started'


class _PCSResourceGroup(_PCSResourceComposite):
    pass


class _PCSMasterSlaveSet(_PCSResourceComposite):
    pass


class _PCSCloneSet(_PCSResourceComposite):
    pass


# Custom resource parsers
regexp = {
    # Interesting name/value pairs
    'nodes':      re.compile(r"(\d+)\s+(Nodes configured)"),
    'online':     re.compile(r"(Online):\s+\[(.+)\]"),
    # Resource composite root
    'res_root':   re.compile(r"(Full list of resources):"),
    # ordinary resources
    'resource':   re.compile(r"\s+(\w+)\s+\((.+)\):\s+(\w+)"),
    # Master/Slave Set composite
    'res_ms':     re.compile(r"\s+(Master/Slave Set):\s+(.+)\s+\[(\w+)\]"),
    'masters':    re.compile(r"\s+(Masters):\s+\[(.+)\]"),
    'slaves':     re.compile(r"\s+(Slaves):\s+\[(.+)\]"),
    # Clone Set Composite and resources
    'clone':      re.compile(r"\s+(Clone Set):\s+(.+)\s+\[(\w+)\]"),
    'started':    re.compile(r"\s+(Started):\s+\[(.+)\]"),
    'stopped':    re.compile(r"\s+(Stopped):\s+\[(.+)\]"),
    # Resource Group composite
    'res_group':  re.compile(r"\s+(Resource Group):\s+(\w+)"),
    # Failed actions list
    'failed_act': re.compile(r"(Failed actions):")
}


def build_item(line, parent, level):
    """Buld a _PCSItem by parsing a pcs output line"""
    matched = False
    item = None
    # Try ad-hoc parsers first
    for key in regexp:
        match_obj = regexp[key].match(line)
        if match_obj and key == 'nodes':
            item = _PCSNameValuePair(parent=parent, level=level)
            item.name = match_obj.group(2)
            item.value = int(match_obj.group(1))
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'online':
            item = _PCSNameValuePair(parent=parent, level=level)
            item.name = match_obj.group(1)
            item.value = set(match_obj.group(2).strip().split(' '))
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'res_root':
            item = _PCSComposite(parent=parent, level=level)
            item.name = match_obj.group(1)
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'failed_act':
            item = _PCSComposite(parent=parent, level=level)
            item.name = match_obj.group(1)
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'res_ms':
            item = _PCSMasterSlaveSet(parent=parent, level=level)
            item.name = match_obj.group(2)
            item.childname = match_obj.group(3)
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'masters':
            n = len(match_obj.group(2).strip().split(' '))
            for i in range(0, n):
                item = _PCSMasterResource(parent=parent, level=level)
                item.name = parent.childname
                item._state = 'Started'
                parent.children.append(item)
            matched = True
            break
        elif match_obj and key == 'slaves':
            n = len(match_obj.group(2).strip().split(' '))
            for i in range(0, n):
                item = _PCSSlaveResource(parent=parent, level=level)
                item.name = parent.childname
                item._state = 'Started'
                parent.children.append(item)
            matched = True
            break
        elif match_obj and key == 'clone':
            item = _PCSCloneSet(parent=parent, level=level)
            item.name = match_obj.group(2)
            item.childname = match_obj.group(3)
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'started':
            n = len(match_obj.group(2).strip().split(' '))
            for i in range(0, n):
                item = _PCSResource(parent=parent, level=level)
                item.name = parent.childname
                item._state = 'Started'
                parent.children.append(item)
            matched = True
            break
        elif match_obj and key == 'stopped':
            n = len(match_obj.group(2).strip().split(' '))
            for i in range(0, n):
                item = _PCSResource(parent=parent, level=level)
                item.name = parent.childname
                item._state = 'Stopped'
                parent.children.append(item)
            matched = True
            break
        elif match_obj and key == 'res_group':
            item = _PCSResourceGroup(parent=parent, level=level)
            item.name = match_obj.group(2)
            matched = True
            parent.children.append(item)
            break
        elif match_obj and key == 'resource':
            item = _PCSResource(parent=parent, level=level)
            item.name = match_obj.group(1)
            item.res_type = match_obj.group(2)
            item._state = match_obj.group(3)
            matched = True
            parent.children.append(item)
            break

    if not matched:
        # Then try generic name/value pairs parsing
        nvpair = line.split(':', 1)
        if len(nvpair) == 2:
            item = _PCSNameValuePair(parent=parent, level=level)
            item.name = nvpair[0].strip()
            item.value = nvpair[1].strip()
        else:
            # Otherwise return a generic _PCSItem with name only
            item = _PCSItem(parent=parent, level=level)
            item.name = line
        try:
            # Try adding to parent
            parent.children.append(item)
        except Exception:
            pass

    return item


def build_tree(stdout):
    """Parse pcs output and builds a tree of _PCSItem nodes"""
    is_spc = ' '.__eq__
    lines = iter(stdout.splitlines())
    parent = deque()
    root = _PCSComposite()
    root.name = "ROOT"
    prev_level = root.level
    prev_node = root
    for line in lines:
        if line == '':
            continue
        level = len(list(takewhile(is_spc, line)))
        if level > prev_level:
            parent.appendleft(prev_node)
         elif level == 0:
            while len(parent) > 1:
                parent.popleft()
        elif level < prev_level:
            parent.popleft()
        child = build_item(line, parent=parent[0], level=level)
        prev_level = level
        prev_node = child
    return root


class Cluster(Module):
    """Test Clusters and their resources

    Implementations:

    - Linux: use Pacemaker

    """

    def __init__(self, cluster_output=''):
        super(Cluster, self).__init__()

    @property
    def name(self):
        """Return The cluster name

        >>> with Sudo():
        ...    Cluster().name.startswith('authdb_cluster')
        >>> True

        """
        raise NotImplementedError

    @property
    def online_nodes(self):
        """Return a set of online cluster nodes

        >>> with Sudo():
        ...    Cluster().online_nodes
        >>> set(['10.0.0.31', '10.0.0.32']

        """
        raise NotImplementedError

    def is_resource(self, name):
        """Return True if name is a primitive resource

        >>> with Sudo():
        ...    Cluster().is_resource('p_vip_authdb')
        >>> True

        """
        raise NotImplementedError

    def is_cloned_resource(self, name):
        """Return True if name is a cloned resource

        >>> with Sudo():
        ...    Cluster().is_cloned_resource('p_ping')
        >>> True
        """
        raise NotImplementedError

    def is_ms_resource(self, name):
        """Return True if name is a master/slave resource

        >>> with Sudo():
        ...    Cluster().is_ms_resource('p_drbd_authdb')
        >>> True

        """
        raise NotImplementedError

    def is_resource_group(self, name):
        """Return True if name is a resource group

        >>> with Sudo():
        ...    Cluster().is_resource_group('g_authdb')
        >>> True

        """
        raise NotImplementedError

    def is_started(self, name):
        """Return True if resource with given name is started.

        If a Master/Slave or Clone resource returns True only
        if started in *all* nodes.

        >>> with Sudo():
        ...    Cluster().is_starded('p_drbd_authdb')
        >>> True

        """
        raise NotImplementedError

    @classmethod
    def get_module_class(cls, _backend):
        Command = _backend.get_module("Command")
        SystemInfo = _backend.get_module("SystemInfo")
        if SystemInfo.type == "linux":
            if Command.exists("/usr/sbin/pcs"):
                return PacemakerCluster
            else:
                raise NotImplementedError
        else:
            raise NotImplementedError

    def __repr__(self):
        return "<Cluster %s>" % (self.name,)


class PacemakerCluster(Cluster):

    def __init__(self, cluster_output=''):
        super(PacemakerCluster, self).__init__(simulate)
        if cluster_output == '':
            cluster_output = self.check_output("pcs status")
        self.root = build_tree(cluster_output)

    def _find(self, name, klass):
        """Helper method to find resources given by name"""
        obj = self.root.find(name)
        if obj is None:
            raise RuntimeError(
                "Cannot find '%s: %s' object in pcs output"
                % (klass.__name__, name))
        if not isinstance(obj, klass):
            raise RuntimeError(
                "item %s is not a '%s' but a '%s'"
                % (name, klass.__name__, obj.__class__.__name__))
        return obj

    @property
    def name(self):
        """Return The cluster name

        >>> with Sudo():
        ...    Cluster().name.startswith('authdb_cluster')
        >>> True

        """
        nvpair = self.root.find('Cluster name')
        if nvpair is None:
            raise RuntimeError(
                "Cannot find 'Cluster name' object in pcs output")
        return nvpair.value

    @property
    def online_nodes(self):
        """Return a set of online cluster nodes

        >>> with Sudo():
        ...    Cluster().online_nodes
        >>> set(['10.0.0.31', '10.0.0.32']

        """
        nvpair = self.root.find('Online')
        if nvpair is None:
            raise RuntimeError("Cannot find 'Online' object in pcs output")
        return nvpair.value

    def is_resource(self, name):
        """Return True if name is a primitive resource

        >>> with Sudo():
        ...    Cluster().is_resource('p_vip_authdb')
        >>> True

        """
        res = self._find(name, _PCSResource)
        return res is not None

    def is_cloned_resource(self, name):
        """Return True if name is a cloned resource

        >>> with Sudo():
        ...    Cluster().is_cloned_resource('p_ping')
        >>> True

        """
        res = self._find(name, _PCSResource)
        return res.parent.__class__ == _PCSCloneSet

    def is_ms_resource(self, name):
        """Return True if name is a master/slave resource

        >>> with Sudo():
        ...    Cluster().is_ms_resource('p_drbd_authdb')
        >>> True

        """
        res = self._find(name, _PCSResource)
        return res.parent.__class__ == _PCSMasterSlaveSet

    def is_resource_group(self, name):
        """Return True if name is a resource group

        >>> with Sudo():
        ...    Cluster().is_resource_group('g_authdb')
        >>> True

        """
        rg = self._find(name, _PCSResourceGroup)
        return rg is not None

    def is_started(self, name):
        """Return True if resource with given name is started.

        If a Master/Slave or Clone resource returns True
        only if started in *all* nodes.

        >>> with Sudo():
        ...    Cluster().is_started('p_drbd_authdb')
        >>> True

        """
        obj = self.root.find(name)
        if obj is None:
            raise RuntimeError("Cannot find '%s' object in pcs output"
                               % (name,))
        if not (isinstance(obj, _PCSResource) or
                issubclass(obj.__class__, _PCSResourceComposite)):
                    raise RuntimeError(
                        "item %s is not a single or composite \
                        resource but a '%s'"
                        % (name, obj.__class__.__name__))
        if (issubclass(obj.parent.__class__, _PCSMasterSlaveSet) or
                issubclass(obj.parent.__class__, _PCSCloneSet)):
                    obj = obj.parent
        return obj.state == 'Started'

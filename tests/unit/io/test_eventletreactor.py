# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa

from tests.unit.io.utils import TimerConnectionTests
from tests.unit.io.eventlet_utils import restore_saved_module
from tests import notpypy
from tests import notmonkeypatch

import time
from eventlet import monkey_patch, kill

from cassandra.io.eventletreactor import EventletConnection


@notmonkeypatch
# There are some issues with some versions of pypy and eventlet
@notpypy
class EventletTimerTest(unittest.TestCase, TimerConnectionTests):
    @classmethod
    def setUpClass(cls):
        cls.connection_class = EventletConnection
        # We only to patch the time module
        monkey_patch(time=True)
        EventletConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        kill(EventletConnection._timeout_watcher)
        EventletConnection._timers = None
        restore_saved_module(time)

# Copyright ScyllaDB, Inc.
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

import os
import subprocess
import sys

from tests.integration import use_cluster, get_cluster, get_node, start_cluster_wait_for_up, \
    libevtest, PROTOCOL_VERSION, CASSANDRA_IP

# GH-1133: `cqlsh -e` drops its Session without shutdown(). When a GC pass
# collects it, Session.__del__ closes all its connections on whatever thread
# triggered the GC; if that's the libev loop thread inside its locked block,
# connection_destroyed() takes the same lock again. In the wild that GC
# timing is luck (10-15% of runs on 32 shards); here a thread trace hook
# forces the collection onto the exact line from the issue's stack, so
# everything after it (Session.__del__ -> pool shutdown -> close of real
# libev connections) runs for real, every time.
CHILD = r"""
import gc, inspect, os, sys, threading, weakref
gc.disable()  # only the forced collection below may collect the Session
from tests import connection_class
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevLoop

code = LibevLoop._loop_will_run.__code__
src, first_line = inspect.getsourcelines(LibevLoop._loop_will_run)
target = first_line + next(i for i, l in enumerate(src) if "self._new_conns = set()" in l)
dropped = None
collected = threading.Event()

def on_line(frame, event, arg):
    # the loop thread holds _lock on this line, as in the issue's stack
    if event == "line" and frame.f_lineno == target and dropped is not None and dropped() is not None:
        gc.collect()
        if dropped() is None:  # Session.__del__ ran and returned
            collected.set()
    return on_line

threading.settrace(lambda frame, event, arg: on_line if frame.f_code is code else None)

def connect():
    return Cluster([sys.argv[1]], protocol_version=int(sys.argv[2]),
                   connection_class=connection_class).connect()

class Shell(object):  # like cqlsh's Shell: holds the Session, sits in a cycle
    pass
shell = Shell()
shell.self, shell.session = shell, connect()
dropped = weakref.ref(shell.session)
del shell  # never shut down; only the GC can free it now
# A second session's new connections make the loop run its locked block again
kept = []
threading.Thread(target=lambda: kept.append(connect()), daemon=True).start()
ok = collected.wait(30)
print("collected" if ok else "loop thread stuck in Session.__del__", flush=True)
os._exit(0 if ok else 3)
"""

SHARDS = int(os.getenv('EXIT_HANG_SMP', '8'))  # overprovisioned, fits a 4-core CI runner


def setup_module():
    use_cluster('exit_without_shutdown', [1], start=False)
    node = get_node(1)
    if not node.is_running():
        # SCYLLA_EXT_OPTS="--smp N" is not honored by ccm, set_smp() is
        node.set_smp(SHARDS)
        node.set_mem_mb_per_cpu(512)
        start_cluster_wait_for_up(get_cluster())


@libevtest
def test_session_collected_on_loop_thread_does_not_deadlock():
    """
    A Session collected by a GC pass that runs on the libev loop thread,
    while it holds the loop lock, must not deadlock that thread.

    @jira_ticket GH-1133
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    result = subprocess.run([sys.executable, "-c", CHILD, CASSANDRA_IP, str(PROTOCOL_VERSION)],
                            cwd=repo_root, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr[-2000:]

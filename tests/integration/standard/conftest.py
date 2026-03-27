import pytest
import logging

# Cluster topology groups for test ordering.
# Tests are sorted so that modules sharing the same CCM cluster run
# together, minimising expensive cluster teardown/restart cycles.
# Lower number = runs first.  Modules not listed get a high default.
_MODULE_CLUSTER_ORDER = {
    # Group 0: default 3-node singledc (CLUSTER_NAME = 'test_cluster')
    "test_metadata": 0,
    "test_policies": 0,
    "test_control_connection": 0,
    "test_routing": 0,
    "test_prepared_statements": 0,
    "test_metrics": 0,
    "test_connection": 0,
    "test_concurrent": 0,
    "test_custom_payload": 0,
    "test_query_paging": 0,
    "test_single_interface": 0,
    "test_rate_limit_exceeded": 0,
    # Group 1: 'cluster_tests' (--smp 2, 3 nodes)
    "test_cluster": 1,
    "test_shard_aware": 1,
    # Group 2: 'shared_aware' (--smp 2 --memory 2048M, 3 nodes)
    "test_use_keyspace": 2,
    "test_client_routes": 2,
    # Group 3: single-node cluster
    "test_types": 3,
    "test_cython_protocol_handlers": 3,
    "test_custom_protocol_handler": 3,
    "test_row_factories": 3,
    "test_udts": 3,
    "test_client_warnings": 3,
    "test_application_info": 3,
    # Group 4: destructive / special clusters (run last)
    "test_ip_change": 4,
    "test_authentication": 4,
    "test_authentication_misconfiguration": 4,
    "test_custom_cluster": 4,
    "test_query": 4,
    # Group 5: tablets (destructive — decommissions a node)
    "test_tablets": 5,
    # Group 6: schema change + node kill (destructive — kills node2)
    "test_concurrent_schema_change_and_node_kill": 6,
    # Group 7: multi-dc (7 nodes — most expensive to create)
    "test_rack_aware_policy": 7,
}


def pytest_collection_modifyitems(items):
    """Sort tests so modules with the same cluster topology are adjacent.

    Uses the original collection index as tie-breaker so that the
    definition order inside each file is preserved (important for tests
    that depend on running order, e.g. destructive tablet tests).
    """
    orig_order = {id(item): idx for idx, item in enumerate(items)}

    def _sort_key(item):
        module_name = item.module.__name__.rsplit(".", 1)[-1]
        return (_MODULE_CLUSTER_ORDER.get(module_name, 99), item.fspath, orig_order[id(item)])

    items[:] = sorted(items, key=_sort_key)


# from https://github.com/streamlit/streamlit/pull/5047/files
def pytest_sessionfinish():
    # We're not waiting for scriptrunner threads to cleanly close before ending the PyTest,
    # which results in raised exception ValueError: I/O operation on closed file.
    # This is well known issue in PyTest, check out these discussions for more:
    # * https://github.com/pytest-dev/pytest/issues/5502
    # * https://github.com/pytest-dev/pytest/issues/5282
    # To prevent the exception from being raised on pytest_sessionfinish
    # we disable exception raising in logging module
    logging.raiseExceptions = False

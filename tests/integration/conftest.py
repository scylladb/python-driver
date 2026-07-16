import os
import logging

import pytest
from ccmlib.cluster_factory import ClusterFactory as CCMClusterFactory

from tests.integration import record_failed_test_report, preserve_ccm_logs_on_failure, teardown_package

from . import CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME
from . import path as ccm_path


def pytest_runtest_logreport(report):
    record_failed_test_report(report)


@pytest.fixture(scope="session", autouse=True)
def cleanup_clusters():

    yield

    preserve_ccm_logs_on_failure()

    if not os.environ.get('DISABLE_CLUSTER_CLEANUP'):
        for cluster_name in [CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME,
                             'cluster_tests', 'shared_aware', 'sni_proxy', 'test_ip_change', 'test_client_routes_replacement']:
            try:
                cluster = CCMClusterFactory.load(ccm_path, cluster_name)
                logging.debug("Using external CCM cluster {0}".format(cluster.name))
                cluster.clear()
            except FileNotFoundError:
                pass

@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_packages():
    print('setup')
    yield
    preserve_ccm_logs_on_failure()
    teardown_package()

import os
import logging

import pytest

from tests.integration import (
    CCMClusterFactory,
    record_failed_test_report, preserve_ccm_logs_on_failure,
    preserve_ccm_logs_before_cleanup, teardown_package)

from . import CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME
from . import path as ccm_path


_LOG_REPORT_PLUGIN_NAME = 'integration-log-reports'


class _IntegrationLogReports:
    def __init__(self):
        self.integration_finalized = False

    def pytest_runtest_logreport(self, report):
        try:
            if (getattr(report, '_integration_log_report', False) or
                    (report.when == 'teardown' and self.integration_finalized)):
                record_failed_test_report(report)
        finally:
            if report.when == 'teardown':
                self.integration_finalized = False


def pytest_configure(config):
    # Session finalizers can run under an item outside the integration tree.
    config.pluginmanager.register(
        _IntegrationLogReports(), name=_LOG_REPORT_PLUGIN_NAME)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    # Conftest scoping identifies reports belonging to integration items.
    outcome.get_result()._integration_log_report = True


@pytest.fixture(scope="session", autouse=True)
def cleanup_clusters(request):

    yield

    observer = request.config.pluginmanager.get_plugin(_LOG_REPORT_PLUGIN_NAME)
    observer.integration_finalized = True
    preserve_ccm_logs_on_failure()

    if not os.environ.get('DISABLE_CLUSTER_CLEANUP'):
        for cluster_name in [CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME,
                             'cluster_tests', 'shared_aware', 'sni_proxy', 'test_ip_change', 'test_client_routes_replacement']:
            try:
                cluster = CCMClusterFactory.load(ccm_path, cluster_name)
                logging.debug("Clearing CCM cluster {0}".format(cluster.name))
                with preserve_ccm_logs_before_cleanup(cluster_name):
                    cluster.clear()
            except FileNotFoundError:
                pass

@pytest.fixture(scope='session', autouse=True)
def setup_and_teardown_packages():
    print('setup')
    yield
    preserve_ccm_logs_on_failure()
    teardown_package()

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

from pathlib import Path
import shutil
from types import SimpleNamespace
from unittest import mock

import pytest


@pytest.fixture
def integration_logs(monkeypatch, tmp_path):
    # Import the helpers without selecting or starting a real cluster.
    monkeypatch.delenv('SCYLLA_VERSION', raising=False)
    monkeypatch.setenv('CASSANDRA_VERSION', '3.11.4')
    monkeypatch.delenv('MAPPED_CASSANDRA_VERSION', raising=False)
    import tests.integration as integration

    monkeypatch.setattr(integration, 'path', str(tmp_path / 'ccm'))
    monkeypatch.setattr(
        integration, '_log_artifact_dir', str(tmp_path / 'artifacts'))
    monkeypatch.setattr(integration, '_failed_reports', [])
    monkeypatch.setattr(integration, '_preserved_ccm_log_scopes', set())
    monkeypatch.setattr(integration, '_provisional_log_failure', None)
    monkeypatch.setattr(integration, '_log_snapshot_count', 0)
    monkeypatch.setattr(integration, '_current_log_failure', None)
    yield integration
    finish_test(integration)


def write_log(integration, cluster_name='test_cluster', content='failure log'):
    log = (Path(integration.path) / cluster_name / 'node1' /
           'logs' / 'system.log')
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(content)
    return log


def record_failure(integration, nodeid='test_first', when='call'):
    integration.record_failed_test_report(
        SimpleNamespace(failed=True, nodeid=nodeid, when=when))


def finish_test(integration):
    integration.record_failed_test_report(
        SimpleNamespace(failed=False, when='teardown'))


def saved_logs(integration):
    return sorted(Path(integration._log_artifact_dir).rglob('system.log'))


def removing_cluster(integration, monkeypatch, log):
    cluster = SimpleNamespace(
        name='test_cluster', remove=lambda: shutil.rmtree(log.parents[2]))
    monkeypatch.setattr(integration, 'CCM_CLUSTER', cluster)
    monkeypatch.setattr(integration, 'KEEP_TEST_CLUSTER', False)
    monkeypatch.setattr(integration, 'check_log_error', lambda: None)


@pytest.mark.parametrize('previous_failure', [False, True])
@pytest.mark.parametrize('fault', [None, 'remove'])
def test_no_failure_discards_provisional_captures(
        integration_logs, monkeypatch, caplog, previous_failure, fault):
    log = write_log(integration_logs)
    previous_logs = []
    if previous_failure:
        record_failure(integration_logs)
        integration_logs.preserve_ccm_logs_on_failure()
        finish_test(integration_logs)
        previous_logs = saved_logs(integration_logs)
        log.write_text('passing test log')
    integration_logs.preserve_ccm_logs_on_failure()
    provisional = integration_logs._provisional_log_failure
    assert list(provisional.rglob('system.log'))
    caplog.clear()
    with monkeypatch.context() as cleanup_fault:
        if fault == 'remove':
            remove = mock.Mock(side_effect=PermissionError('cleanup denied'))
            cleanup_fault.setattr(
                integration_logs.shutil, 'rmtree', remove)
        finish_test(integration_logs)
    if fault == 'remove':
        remove.assert_called_once_with(provisional)
        assert list(provisional.rglob('system.log'))
        assert 'Failed to discard temporary ccm logs' in caplog.text
        assert 'cleanup denied' in caplog.text
    else:
        assert not provisional.exists()
        assert 'Failed to discard temporary ccm logs' not in caplog.text
        if not previous_failure:
            assert not Path(integration_logs._log_artifact_dir).exists()
    assert [p.read_text() for p in previous_logs] == (
        ['failure log'] if previous_failure else [])
    assert integration_logs._provisional_log_failure is None
    assert integration_logs._current_log_failure is None


def test_repeated_cleanup_does_not_copy_logs_again(integration_logs):
    write_log(integration_logs)
    record_failure(integration_logs)
    integration_logs.preserve_ccm_logs_on_failure('test_cluster')
    with mock.patch.object(
            integration_logs.shutil, 'copy2',
            side_effect=AssertionError('unexpected repeated copy')):
        integration_logs.preserve_ccm_logs_on_failure('test_cluster')
        integration_logs.preserve_ccm_logs_on_failure()
        integration_logs.preserve_ccm_logs_on_failure()
    assert [p.read_text() for p in saved_logs(integration_logs)] == [
        'failure log']


def test_later_failure_retains_earlier_logs_and_attribution(integration_logs):
    log = write_log(integration_logs)
    record_failure(integration_logs)
    integration_logs.preserve_ccm_logs_on_failure()
    finish_test(integration_logs)
    log.write_text('second failure log')
    record_failure(integration_logs, 'test_second', 'teardown')

    saved = saved_logs(integration_logs)
    assert [p.read_text() for p in saved] == [
        'failure log', 'second failure log']
    assert saved[0].parents[5] != saved[1].parents[5]
    report_file = Path(integration_logs._log_artifact_dir) / 'failed-tests.txt'
    report = report_file.read_text()
    assert 'call test_first [failure-0001-' in report
    assert 'teardown test_second [failure-0002-' in report


def test_overlapping_cleanup_copies_each_cluster_once(integration_logs):
    first = write_log(integration_logs, 'first')
    second = write_log(integration_logs, 'second')
    record_failure(integration_logs)
    with mock.patch.object(
            integration_logs.shutil, 'copy2', wraps=shutil.copy2) as copy:
        integration_logs.preserve_ccm_logs_on_failure('first')
        integration_logs.preserve_ccm_logs_on_failure()
        integration_logs.preserve_ccm_logs_on_failure('second')
        integration_logs.preserve_ccm_logs_on_failure()
    assert [call.args[0] for call in copy.call_args_list] == [first, second]


def test_recreated_cluster_keeps_separate_incarnations(
        integration_logs, monkeypatch):
    log = write_log(integration_logs)
    obsolete = log.parent / 'old-node.log'
    obsolete.write_text('first incarnation only')
    record_failure(integration_logs)
    removing_cluster(integration_logs, monkeypatch, log)
    integration_logs.remove_cluster()
    write_log(integration_logs, content='recreated cluster log')
    integration_logs.preserve_ccm_logs_on_failure()

    saved = saved_logs(integration_logs)
    assert [p.read_text() for p in saved] == [
        'failure log', 'recreated cluster log']
    assert (saved[0].parent / obsolete.name).read_text() == (
        'first incarnation only')
    assert not (saved[1].parent / obsolete.name).exists()


def test_first_teardown_failure_keeps_pre_removal_capture_without_move(
        integration_logs, monkeypatch):
    log = write_log(integration_logs)
    removing_cluster(integration_logs, monkeypatch, log)
    with mock.patch.object(
            integration_logs.shutil, 'move',
            side_effect=AssertionError('capture must not depend on moving')):
        integration_logs.remove_cluster()
        assert not log.exists()
        provisional = integration_logs._provisional_log_failure
        captured = saved_logs(integration_logs)
        assert [p.read_text() for p in captured] == ['failure log']
        record_failure(integration_logs, when='teardown')
    assert saved_logs(integration_logs) == captured
    assert [p.read_text() for p in captured] == ['failure log']
    report = (provisional / 'failed-tests.txt').read_text()
    assert 'teardown test_first [%s]' % provisional.name in report
    assert integration_logs._provisional_log_failure is None


def test_removal_retries_reuse_pre_cleanup_capture(
        integration_logs, monkeypatch):
    log = write_log(integration_logs)
    removing_cluster(integration_logs, monkeypatch, log)
    remove = integration_logs.CCM_CLUSTER.remove
    attempts = 0

    def remove_after_retries():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError('busy')
        remove()

    integration_logs.CCM_CLUSTER.remove = remove_after_retries
    with mock.patch.object(integration_logs.time, 'sleep'), mock.patch.object(
            integration_logs.shutil, 'copy2', wraps=shutil.copy2) as copy:
        integration_logs.remove_cluster()
    assert copy.call_count == 1
    assert attempts == 3
    assert not log.exists()
    record_failure(integration_logs, when='teardown')
    assert [p.read_text() for p in saved_logs(integration_logs)] == [
        'failure log']
    assert not integration_logs._preserved_ccm_log_scopes


def test_failure_report_is_saved_without_cluster_directory(integration_logs):
    record_failure(integration_logs, when='setup')
    report = Path(integration_logs._log_artifact_dir) / 'failed-tests.txt'
    assert report.read_text().startswith('setup test_first [failure-0001-')


def test_preserves_only_logs_and_configuration(integration_logs):
    log = write_log(integration_logs)
    cluster = log.parents[2]
    files = ['node1/conf/custom', 'node1/config.yaml', 'cluster.conf',
             'node1/logs/output', 'node1/data/table.db']
    for name in files:
        source = cluster / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(name)
    record_failure(integration_logs)
    integration_logs.preserve_ccm_logs_on_failure('test_cluster')
    saved = saved_logs(integration_logs)[0].parents[2]
    saved_files = sorted(
        p.relative_to(saved).as_posix()
        for p in saved.rglob('*') if p.is_file())
    assert saved_files == sorted(
        [name for name in files if '/data/' not in name] +
        ['node1/logs/system.log'])


def test_external_targets_are_skipped_but_internal_file_links_are_copied(
        integration_logs, tmp_path):
    log = write_log(integration_logs)
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'secret.log').write_text('outside diagnostics')
    try:
        (log.parent / 'internal.log').symlink_to(log)
        (log.parent / 'external.log').symlink_to(outside / 'secret.log')
        (log.parent / 'external-directory').symlink_to(
            outside, target_is_directory=True)
        (Path(integration_logs.path) / 'external-cluster').symlink_to(
            outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip('symlinks unavailable: %s' % exc)
    record_failure(integration_logs)
    integration_logs.preserve_ccm_logs_on_failure()
    files = list(Path(integration_logs._log_artifact_dir).rglob('*.log'))
    assert sorted(p.name for p in files) == ['internal.log', 'system.log']
    assert all(p.read_text() == 'failure log' for p in files)


def test_workflow_fallback_uses_shared_collector_without_dependencies(
        tmp_path):
    import os
    import subprocess
    import sys

    repository = Path(__file__).resolve().parents[2]
    workflow = (
        repository / '.github/workflows/integration-tests.yml').read_text()
    # Run the exact workflow command with controlled runner roots and no
    # site packages: neither the driver nor CCM is available to the
    # script.
    command = next(line.strip() for line in workflow.splitlines()
                   if line.strip().startswith('python3 tests/integration/'))
    helper = repository / command.split()[1]
    first = tmp_path / 'tests/integration/ccm/first/node1/logs'
    second = tmp_path / 'runner/.ccm/second/node1/conf'
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / 'system.log').write_text('first logs')
    (second / 'custom').write_text('second configuration')
    (tmp_path / 'runner/.ccm/root.conf').write_text('root configuration')
    cached = tmp_path / 'runner/.ccm/scylla-repository/release/conf'
    cached.mkdir(parents=True)
    (cached / 'scylla.yaml').write_text('cached release configuration')
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'secret.log').write_text('outside diagnostics')
    try:
        (first / 'internal.log').symlink_to(first / 'system.log')
        (first / 'external.log').symlink_to(outside / 'secret.log')
        (second / 'external-directory').symlink_to(
            outside, target_is_directory=True)
        (tmp_path / 'runner/.ccm/external-cluster').symlink_to(
            outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip('symlinks unavailable: %s' % exc)
    args = command.split()[2:]
    args = [str(tmp_path / 'runner/.ccm') if arg == '/home/runner/.ccm'
            else arg for arg in args]
    result = subprocess.run(
        [sys.executable, '-S', str(helper), *args], cwd=tmp_path,
        env={**os.environ, 'PYTHONPATH': ''}, capture_output=True, text=True,
        timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    artifacts = tmp_path / 'integration-test-logs'
    files = [p for p in artifacts.rglob('*') if p.is_file()]
    assert sorted(p.name for p in files) == [
        'custom', 'internal.log', 'root.conf', 'system.log']
    assert sorted(p.read_text() for p in files) == [
        'first logs', 'first logs', 'root configuration',
        'second configuration']
    upload = workflow.split('- name: Upload ccm logs on failure', 1)[1]
    paths = upload.split('path:', 1)[1].split('if-no-files-found:', 1)[0]
    assert paths.strip() == 'integration-test-logs/**'
    assert not any(p.is_symlink() for p in artifacts.rglob('*'))


def test_log_directory_probe_failure_warns_and_preserves_other_files(
        integration_logs, monkeypatch, caplog):
    log = write_log(integration_logs)
    config = log.parents[2] / 'cluster.conf'
    config.write_text('cluster configuration')
    removing_cluster(integration_logs, monkeypatch, log)
    record_failure(integration_logs)
    scandir = integration_logs.os.scandir
    preserve = integration_logs._preserve_ccm_cluster_logs

    def scan_with_probe_failure(directory):
        if (isinstance(directory, (str, Path)) and
                Path(directory) == log.parent):
            raise PermissionError('unreadable logs')
        return scandir(directory)

    def preserve_with_probe_failure(*args):
        with mock.patch.object(
                integration_logs.os, 'scandir', scan_with_probe_failure):
            return preserve(*args)

    with mock.patch.object(
            integration_logs, '_preserve_ccm_cluster_logs',
            side_effect=preserve_with_probe_failure):
        integration_logs.remove_cluster()

    assert integration_logs.CCM_CLUSTER is None
    assert not log.exists()
    configs = list(
        Path(integration_logs._log_artifact_dir).rglob('cluster.conf'))
    assert [p.read_text() for p in configs] == ['cluster configuration']
    assert 'unreadable logs' in caplog.text
    assert str(log.parent) in caplog.text


@pytest.mark.parametrize(
    'target', ['file', 'directory', 'cluster', 'ccm_root'])
def test_metadata_errors_warn_and_allow_cluster_removal(
        integration_logs, monkeypatch, caplog, target):
    log = write_log(integration_logs)
    config = log.parents[2] / 'cluster.conf'
    config.write_text('cluster configuration')
    denied = {'file': log, 'directory': log.parent, 'cluster': log.parents[2],
              'ccm_root': Path(integration_logs.path)}[target]
    removing_cluster(integration_logs, monkeypatch, log)
    record_failure(integration_logs)
    stat = Path.stat

    def stat_accessible_path(source, *args, **kwargs):
        if isinstance(source, (str, Path)):
            source_path = Path(source)
            if source_path == denied or denied in source_path.parents:
                raise PermissionError(13, 'metadata denied', str(denied))
        return stat(source, *args, **kwargs)

    with mock.patch.object(Path, 'stat', stat_accessible_path):
        integration_logs.remove_cluster()

    assert integration_logs.CCM_CLUSTER is None
    assert not log.exists()
    assert not saved_logs(integration_logs)
    configs = list(
        Path(integration_logs._log_artifact_dir).rglob('cluster.conf'))
    expected = (
        ['cluster configuration'] if target in ('file', 'directory') else [])
    assert [p.read_text() for p in configs] == expected
    assert 'metadata denied' in caplog.text
    assert str(denied) in caplog.text


@pytest.mark.parametrize(
    'failure', ['copy', 'artifact_root', 'report', 'provisional'])
def test_diagnostic_errors_do_not_prevent_cluster_removal(
        integration_logs, monkeypatch, caplog, failure):
    log = write_log(integration_logs)
    removing_cluster(integration_logs, monkeypatch, log)
    with mock.patch.object(
            integration_logs.time, 'sleep',
            side_effect=AssertionError('retry')):
        if failure == 'provisional':
            Path(integration_logs._log_artifact_dir).write_text(
                'not a directory')
            integration_logs.remove_cluster()
        else:
            if failure == 'artifact_root':
                Path(integration_logs._log_artifact_dir).write_text(
                    'not a directory')
            if failure == 'report':
                with mock.patch.object(
                        Path, 'open',
                        side_effect=OSError('report unwritable')):
                    record_failure(integration_logs)
                    integration_logs.remove_cluster()
            else:
                record_failure(integration_logs)
            if failure == 'copy':
                with mock.patch.object(
                        integration_logs.shutil, 'copy2',
                        side_effect=OSError('rotated')):
                    integration_logs.remove_cluster()
            elif failure != 'report':
                integration_logs.remove_cluster()
    assert integration_logs.CCM_CLUSTER is None
    assert not log.exists()
    assert 'Failed to preserve' in caplog.text
    if failure == 'copy':
        assert 'No ccm diagnostic files copied' in caplog.text
        assert 'Copied 1 ccm diagnostic' not in caplog.text
    elif failure == 'report':
        assert 'Failed to preserve test report: report unwritable' in caplog.text
        assert [p.read_text() for p in saved_logs(integration_logs)] == [
            'failure log']


@pytest.mark.parametrize(
    'scenario,later_unit', [
        ('startup', False), ('teardown', False),
        ('call_and_teardown', False), ('passing', False),
        ('cqlengine_teardown', False), ('skip', False),
        pytest.param('passing', True, id='mixed_passing'),
        pytest.param('teardown', True, id='mixed_teardown'),
        ('unit_setup', True), ('unit_call', True)])
def test_real_pytest_cleanup_and_report_order(tmp_path, scenario, later_unit):
    import json
    import os
    import subprocess
    import sys
    import textwrap

    # Keep integration hooks in their actual pytest subtree while
    # controlling CCM.
    (tmp_path / 'conftest.py').write_text(textwrap.dedent('''
        import json
        import os
        import sys
        from pathlib import Path
        from types import SimpleNamespace
        import shutil

        # Wheel unit tests, including Windows, need no CCM install.
        sys.modules['ccmlib'] = None
        import tests.integration as integration

        scenario = os.environ['LOG_TEST_SCENARIO']
        integration.path = str(Path(__file__).parent / 'ccm')
        integration._log_artifact_dir = str(
            Path(__file__).parent / 'artifacts')
        integration.KEEP_TEST_CLUSTER = False
        integration.check_log_error = lambda: None
        integration.call = lambda *args: 0
        integration.common = SimpleNamespace(switch_cluster=lambda *args: None)
        integration.setup_keyspace = lambda **kwargs: None


        class FakeCluster:
            def __init__(self, root, name, **kwargs):
                self.name = name
                self.root = Path(root) / name
                self.nodes = {}
                output = self.root / 'node1' / 'logs' / 'system.log'
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text('server diagnostics: ' + scenario)

            def set_configuration_options(self, *args, **kwargs):
                pass

            def set_dse_configuration_options(self, *args, **kwargs):
                pass

            def set_install_dir(self, **kwargs):
                pass

            def populate(self, *args, **kwargs):
                pass

            def start(self, **kwargs):
                if scenario == 'startup':
                    raise RuntimeError('controlled startup failure')

            def remove(self):
                shutil.rmtree(self.root)

            def clear(self):
                shutil.rmtree(self.root)


        def load(root, name):
            if not (Path(root) / name).is_dir():
                raise FileNotFoundError(name)
            return FakeCluster(root, name)

        integration.CCMCluster = FakeCluster
        integration.CCMClusterFactory = SimpleNamespace(load=load)

        from tests.conftest import pytest_runtest_makereport


        def pytest_sessionfinish(session):
            state = {
                'staging_disposed': (
                    integration._provisional_log_failure is None),
                'source_removed': not (
                    Path(integration.path) / 'test_cluster').exists()}
            (Path(__file__).parent / 'state.json').write_text(
                json.dumps(state))
    '''))
    integration_dir = tmp_path / 'integration'
    integration_dir.mkdir()
    (integration_dir / 'conftest.py').write_text(textwrap.dedent('''
        import os
        import pytest
        import tests.integration as integration
        from tests.integration.conftest import *

        scenario = os.environ['LOG_TEST_SCENARIO']
        if scenario == 'cqlengine_teardown':
            from tests.integration.cqlengine.conftest import (
                setup_and_teardown_packages)


        @pytest.fixture(scope='session', autouse=True)
        def cluster(setup_and_teardown_packages, cleanup_clusters):
            integration.use_cluster('test_cluster', [1], set_keyspace=False)
            yield
            if scenario in (
                    'teardown', 'call_and_teardown', 'cqlengine_teardown'):
                raise RuntimeError('controlled session teardown failure')
    '''))
    integration_case = integration_dir / 'test_case.py'
    integration_case.write_text(textwrap.dedent('''
        import os
        import pytest


        def test_case():
            if os.environ['LOG_TEST_SCENARIO'] == 'call_and_teardown':
                pytest.fail('controlled call failure')
            if os.environ['LOG_TEST_SCENARIO'] == 'skip':
                pytest.skip('controlled skip converted to failure')
    '''))
    test_paths = [str(integration_case)]
    if later_unit:
        unit_dir = tmp_path / 'unit'
        unit_dir.mkdir()
        unit_case = unit_dir / 'test_unit.py'
        unit_case.write_text(textwrap.dedent('''
            import os
            import pytest


            @pytest.fixture
            def unit_setup():
                if os.environ['LOG_TEST_SCENARIO'] == 'unit_setup':
                    pytest.fail('controlled unrelated unit setup failure')


            def test_unit(unit_setup):
                if os.environ['LOG_TEST_SCENARIO'] == 'unit_call':
                    pytest.fail('controlled unrelated unit call failure')
        '''))
        test_paths.append(str(unit_case))
        if scenario in ('unit_setup', 'unit_call'):
            # Separate the unrelated failure from final session cleanup.
            later_case = unit_dir / 'test_later.py'
            later_case.write_text('def test_later():\n    pass\n')
            test_paths.append(str(later_case))
    env = os.environ.copy()
    for name in ('SCYLLA_VERSION', 'MAPPED_CASSANDRA_VERSION',
                 'DISABLE_CLUSTER_CLEANUP'):
        env.pop(name, None)
    env.update(CASSANDRA_VERSION='3.11.4', LOG_TEST_SCENARIO=scenario,
               PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', CASS_DRIVER_NO_SKIP='1',
               PYTHONPATH=str(Path(__file__).resolve().parents[2]))
    result = subprocess.run(
        [sys.executable, '-m', 'pytest', '-q', '--confcutdir', str(tmp_path),
         '-p', 'no:cacheprovider', *test_paths],
        cwd=tmp_path, env=env, text=True, capture_output=True, timeout=30)
    assert result.returncode == (0 if scenario == 'passing' else 1), (
        result.stdout + result.stderr)
    state = json.loads((tmp_path / 'state.json').read_text())
    assert state == {'staging_disposed': True, 'source_removed': True}
    artifacts = tmp_path / 'artifacts'
    if scenario in ('passing', 'unit_setup', 'unit_call'):
        assert not artifacts.exists()
    else:
        logs = list(artifacts.rglob('system.log'))
        assert logs, result.stdout + result.stderr
        assert all(
            p.read_text() == 'server diagnostics: ' + scenario for p in logs)
        report = (artifacts / 'failed-tests.txt').read_text()
        phases = ('setup',) if scenario == 'startup' else (
            ('call', 'teardown') if scenario == 'call_and_teardown' else (
                ('call',) if scenario == 'skip' else ('teardown',)))
        failed_nodeid = ('unit/test_unit.py::test_unit' if later_unit else
                         'integration/test_case.py::test_case')
        for phase in phases:
            assert phase + ' ' + failed_nodeid + ' [failure-' in report

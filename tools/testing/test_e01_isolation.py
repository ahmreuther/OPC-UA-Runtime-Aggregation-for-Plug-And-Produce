# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""E01 run isolation and current C16 lifecycle evidence tests. No services start."""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_and_export as runner


class IsolationTests(unittest.TestCase):
    def test_runtime_override_changes_snapshots_without_touching_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / 'runtime'
            runtime.mkdir()
            run = root / 'run'
            run.mkdir()
            (runtime / 'config.json').write_text('{"hosts": []}', encoding='utf-8')
            with patch.dict(os.environ, {'OJIES_E01_RUNTIME_DIR': str(runtime)}):
                self.assertEqual(runner.runtime_root(), runtime.resolve())
                snapshot = runner.snapshot_runtime_inputs(run)
                self.assertEqual(json.loads(snapshot['config.json'].read_text()), {'hosts': []})

    def test_occupied_port_fails_before_any_process_start(self):
        with patch.object(runner.socket, 'socket') as socket, patch.object(runner.subprocess, 'Popen') as start:
            socket.return_value.__enter__.return_value.bind.side_effect = OSError('occupied')
            with self.assertRaisesRegex(RuntimeError, 'occupied'):
                runner.ensure_local_ports_available('opc.tcp://127.0.0.1:48400', 16)
            start.assert_not_called()

    def test_windows_console_failure_uses_only_owned_pid_tree(self):
        process = Mock(pid=12345, returncode=1)
        process.poll.return_value = None
        process.send_signal.side_effect = OSError('no attached console')
        process.ojies_output_thread = None
        process.ojies_server_log = None
        with patch.object(runner.sys, 'platform', 'win32'), patch.object(
                runner.subprocess, 'run', return_value=Mock(returncode=0)) as command:
            runner.stop_server(process)
        self.assertEqual(command.call_args.args[0], ['taskkill', '/PID', '12345', '/T', '/F'])
        self.assertTrue(process.ojies_shutdown_record['forced'])
        process.kill.assert_not_called()


class LifecycleTests(unittest.TestCase):
    def event(self, event, attempt='one', source='urn:test:1'):
        return {'event': event, 'attempt_id': attempt, 'source': source, 'endpoint': 'opc.tcp://127.0.0.1:4860'}

    def report(self, onboarding, admission=None):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            admission = admission if admission is not None else [{'event': 'policy', 'integration_writers': 1}]
            for filename, records in [('onboarding_events.jsonl', onboarding), ('admission_events.jsonl', admission)]:
                (root / filename).write_text(''.join(json.dumps(r) + '\n' for r in records), encoding='utf-8')
            return runner.build_runtime_lifecycle_report(root, {'urn:test:1'})

    def test_complete_serial_attempt_passes(self):
        self.assertTrue(self.report([self.event('started'), self.event('completed')])['passed'])

    def test_recovered_attempt_retains_original_failure(self):
        report = self.report([
            self.event('started'), self.event('failed'),
            self.event('started', 'two'), self.event('completed', 'two'),
        ])
        self.assertFalse(report['passed'])
        self.assertEqual(report['onboarding_event_counts']['failed'], 1)
        self.assertEqual(report['completed_source_count'], 1)

    def test_malformed_json_is_not_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'onboarding_events.jsonl').write_text('{bad\n', encoding='utf-8')
            (root / 'admission_events.jsonl').write_text('{}\n', encoding='utf-8')
            report = runner.build_runtime_lifecycle_report(root, {'urn:test:1'})
            self.assertFalse(report['passed'])
            self.assertTrue(any('onboarding_events.jsonl:' in error for error in report['errors']))

    def test_overlap_and_unfinished_attempt_rejected(self):
        report = self.report([self.event('started'), self.event('started', 'two'), self.event('completed', 'two')])
        self.assertFalse(report['passed'])
        self.assertTrue(any('overlapping' in error for error in report['errors']))
        self.assertTrue(any('no terminal' in error for error in report['errors']))

    def test_wrong_terminal_identity_rejected(self):
        report = self.report([self.event('started'), self.event('completed', source='urn:other')])
        self.assertFalse(report['passed'])

    def test_discovery_failure_after_completion_is_rejected(self):
        report = self.report([self.event('started'), self.event('completed')], [
            {'event': 'policy', 'integration_writers': 1},
            {'event': 'discovery_failed', 'detail': 'BadTimeout'},
        ])
        self.assertFalse(report['passed'])
        self.assertIn('Admission event: discovery_failed', report['errors'])


if __name__ == '__main__':
    unittest.main()

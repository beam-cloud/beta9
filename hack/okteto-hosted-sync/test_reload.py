"""Exercise the watcher's actual shutdown functions against real child processes."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import textwrap
import unittest

SCRIPT = Path(__file__).with_name('reload.sh').read_text()
ALIVE = SCRIPT.split('alive() {', 1)[1].split('start_gateway() {', 1)[0]
STOP = SCRIPT.split('stop_gateway() {', 1)[1].split('healthy() {', 1)[0]
BUDGET = next(line for line in SCRIPT.splitlines() if line.startswith('shutdown_seconds='))
VALIDATE = next(line for line in SCRIPT.splitlines() if line.startswith('[[ "$shutdown_seconds"'))


class ShutdownTests(unittest.TestCase):
    def run_shutdown(self, *, drain, budget=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            victim = root / 'victim.py'
            victim.write_text(textwrap.dedent('''\
                import signal,sys,time
                from pathlib import Path
                root=Path(sys.argv[1])
                def stop(*_):
                    time.sleep(float(sys.argv[2]))
                    (root/'drained').write_text('complete')
                    raise SystemExit(0)
                signal.signal(signal.SIGTERM,stop)
                (root/'ready').touch()
                while True: time.sleep(0.1)
            '''))
            code = '\n'.join([
                'set -euo pipefail', BUDGET, VALIDATE,
                'log() { printf "%s\\n" "$*"; }',
                'alive() {' + ALIVE, 'stop_gateway() {' + STOP,
                'python3 "$1/victim.py" "$1" "$2" &', 'gateway_pid=$!',
                'while [[ ! -e "$1/ready" ]]; do sleep 0.05; done',
                'stop_gateway',
            ])
            env = os.environ.copy()
            env.pop('HOSTED_DEV_SHUTDOWN_SECONDS', None)
            if budget is not None:
                env['HOSTED_DEV_SHUTDOWN_SECONDS'] = str(budget)
            child = subprocess.Popen(
                ['bash', '-c', code, 'shutdown-test', str(root), str(drain)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, start_new_session=True,
            )
            try:
                output, _ = child.communicate(timeout=25)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()
                raise
            return child.returncode, output, (root/'drained').exists()

    def test_default_allows_drain_longer_than_old_ten_second_cutoff(self):
        code, output, drained = self.run_shutdown(drain=12)
        self.assertEqual(code, 0, output)
        self.assertTrue(drained, output)
        self.assertNotIn('stopping the old gateway', output)

    def test_stuck_process_is_killed_after_explicit_budget(self):
        code, output, drained = self.run_shutdown(drain=30, budget=1)
        self.assertEqual(code, 0, output)
        self.assertFalse(drained)
        self.assertIn('exceeded 1 seconds', output)

    def test_invalid_budget_fails_before_starting_a_gateway(self):
        code, output, drained = self.run_shutdown(drain=0, budget=0)
        self.assertNotEqual(code, 0)
        self.assertIn('must be a positive integer', output)
        self.assertFalse(drained)


if __name__ == '__main__':
    unittest.main()

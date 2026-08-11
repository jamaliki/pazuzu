from __future__ import annotations

import subprocess
import unittest
from pathlib import Path
from unittest import mock

from pazuzu import launchd


def result(exit_code: int, output: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], exit_code, output)


class LaunchdTests(unittest.TestCase):
    @mock.patch("pazuzu.launchd.time.sleep")
    @mock.patch("pazuzu.launchd._launchctl")
    def test_install_waits_for_unload_and_retries_bootstrap(
        self, launchctl: mock.Mock, _sleep: mock.Mock
    ) -> None:
        launchctl.side_effect = [
            result(0),  # bootout
            result(0),  # still loaded
            result(1),  # unloaded
            result(5, "Input/output error"),
            result(1),  # not loaded after failed bootstrap
            result(0),
        ]

        with mock.patch("pazuzu.launchd._write_plist", return_value=Path("agent.plist")):
            installed = launchd._install("test.service", {})

        self.assertEqual(Path("agent.plist"), installed)
        self.assertEqual(6, launchctl.call_count)


if __name__ == "__main__":
    unittest.main()

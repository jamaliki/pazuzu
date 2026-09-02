from __future__ import annotations

import asyncio
import os
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pazuzu import cli
from pazuzu.errors import CommandTimedOut
from pazuzu.process import ProcessResult
from pazuzu.transfer import copy_argv, rsync_argv, run_transfer
from pazuzu.transport import SshAttachment


def attachment() -> SshAttachment:
    return SshAttachment(
        host="example-host",
        ssh_binary="/custom/ssh with spaces",
        control_path=Path("/tmp/pazuzu state/ssh.ctl"),
        generation=3,
    )


class TransferArgumentTests(unittest.TestCase):
    def test_cp_reuses_the_master_without_direct_fallback(self) -> None:
        argv = copy_argv(
            attachment(),
            ["--", "-rp", "local file", ":/remote/file"],
            executable="/custom/scp",
        )

        self.assertEqual("/custom/scp", argv[0])
        self.assertEqual(["-S", "/custom/ssh with spaces"], argv[1:3])
        self.assertIn("ControlMaster=no", argv)
        self.assertIn("ControlPersist=no", argv)
        self.assertIn("ProxyCommand=/usr/bin/false", argv)
        self.assertIn("BatchMode=yes", argv)
        self.assertIn("ControlPath=/tmp/pazuzu state/ssh.ctl", argv)
        self.assertEqual(["-rp", "local file", "example-host:/remote/file"], argv[-3:])

    def test_rsync_uses_the_owned_ssh_binary_and_master(self) -> None:
        argv = rsync_argv(
            attachment(),
            ["--", "-av", "--delete", "local/", ":/remote/"],
            executable="/custom/rsync",
        )

        self.assertEqual(["/custom/rsync", "-e"], argv[:2])
        remote_shell = shlex.split(argv[2])
        self.assertEqual("/custom/ssh with spaces", remote_shell[0])
        self.assertIn("ProxyCommand=/usr/bin/false", remote_shell)
        self.assertIn("ControlPath=/tmp/pazuzu state/ssh.ctl", remote_shell)
        self.assertEqual(
            ["-av", "--delete", "local/", "example-host:/remote/"], argv[-4:]
        )

    def test_download_rewrites_only_the_pazuzu_endpoint(self) -> None:
        argv = copy_argv(attachment(), [":results/file", "local-file"])

        self.assertEqual(["example-host:results/file", "local-file"], argv[-2:])

    def test_requires_exactly_one_pazuzu_remote_endpoint(self) -> None:
        for arguments in (
            ["local", "other-local"],
            [":first", ":second"],
            ["local", ":"],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                copy_argv(attachment(), arguments)

    def test_rejects_other_hosts_and_transport_overrides(self) -> None:
        invalid = (
            (copy_argv, ["local", "other-host:/remote"]),
            (copy_argv, ["-o", "ProxyCommand=attacker", "local", ":remote"]),
            (copy_argv, ["-Sattacker", "local", ":remote"]),
            (copy_argv, ["-Dattacker", "local", ":remote"]),
            (rsync_argv, ["-e", "attacker", "local", ":remote"]),
            (rsync_argv, ["--rsh=attacker", "local", ":remote"]),
            (rsync_argv, ["local", "rsync://other-host/module"]),
        )
        for builder, arguments in invalid:
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                builder(attachment(), arguments)


class TransferProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_exit_code_is_returned(self) -> None:
        result = ProcessResult(23, b"out", b"err", False, False)

        with mock.patch("pazuzu.transfer.run_process", new=mock.AsyncMock(return_value=result)) as run:
            actual = await run_transfer(["/custom/scp", "source", "destination"])

        self.assertEqual(result, actual)
        run.assert_awaited_once_with(
            ["/custom/scp", "source", "destination"],
            stdin=b"",
            timeout=600.0,
            output_limit=1024 * 1024,
        )

    async def test_stalled_transfer_is_bounded_and_not_replayed(self) -> None:
        with mock.patch(
            "pazuzu.transfer.run_process", new=mock.AsyncMock(side_effect=TimeoutError)
        ) as run, self.assertRaises(CommandTimedOut):
            await run_transfer(["/custom/scp", "source", "destination"], timeout=0.25)
        run.assert_awaited_once()

    async def test_timeout_kills_the_transfer_process_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            child_pid_file = Path(directory) / "child.pid"
            script = Path(directory) / "stalled.py"
            script.write_text(
                "import os, subprocess, sys, time\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                "open(sys.argv[1], 'w').write(str(child.pid))\n"
                "time.sleep(60)\n"
            )
            with self.assertRaises(CommandTimedOut):
                await run_transfer(
                    [sys.executable, str(script), str(child_pid_file)], timeout=0.2
                )
            child_pid = int(child_pid_file.read_text())
            for _ in range(20):
                try:
                    os.kill(child_pid, 0)
                except ProcessLookupError:
                    break
                await asyncio.sleep(0.05)
            else:
                self.fail("transfer child process survived process-group cleanup")


class TransferCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_cp_gets_a_probed_attachment_and_runs_scp(self) -> None:
        arguments = cli._parser().parse_args(
            ["cp", "--scp", "/custom/scp", "--", "local", ":/remote"]
        )
        gateway = mock.AsyncMock(
            return_value={
                "exit_code": 17,
                "stdout": "",
                "stderr": "copy failed",
                "stdout_truncated": False,
                "stderr_truncated": False,
            }
        )

        with (
            mock.patch("pazuzu.cli.call_gateway", gateway),
        ):
            result = await cli._run(arguments)

        self.assertEqual(17, result)
        params = gateway.await_args.args[2]
        self.assertEqual("cp", params["tool"])
        self.assertEqual("/custom/scp", params["executable"])
        self.assertEqual(["--", "local", ":/remote"], params["arguments"])

    async def test_rsync_parser_preserves_native_arguments(self) -> None:
        arguments = cli._parser().parse_args(
            ["rsync", "--rsync", "/custom/rsync", "--", "-av", "local/", ":remote/"]
        )

        self.assertEqual("/custom/rsync", arguments.rsync)
        self.assertEqual(["--", "-av", "local/", ":remote/"], arguments.transfer_arguments)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import shlex
import unittest
from pathlib import Path
from unittest import mock

from pazuzu import cli
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
        process = mock.Mock(returncode=23)
        process.wait = mock.AsyncMock(return_value=23)
        spawn = mock.AsyncMock(return_value=process)

        with mock.patch("pazuzu.transfer.asyncio.create_subprocess_exec", spawn):
            result = await run_transfer(["/custom/scp", "source", "destination"])

        self.assertEqual(23, result)
        spawn.assert_awaited_once_with("/custom/scp", "source", "destination")

    async def test_cancellation_terminates_the_native_client(self) -> None:
        stopped = asyncio.Event()
        process = mock.Mock(returncode=None)

        async def wait() -> int:
            await stopped.wait()
            process.returncode = -15
            return -15

        process.wait = mock.AsyncMock(side_effect=wait)
        process.terminate.side_effect = stopped.set
        spawn = mock.AsyncMock(return_value=process)

        with mock.patch("pazuzu.transfer.asyncio.create_subprocess_exec", spawn):
            task = asyncio.create_task(run_transfer(["/custom/rsync"]))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        process.terminate.assert_called_once_with()


class TransferCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_cp_gets_a_probed_attachment_and_runs_scp(self) -> None:
        arguments = cli._parser().parse_args(
            ["cp", "--scp", "/custom/scp", "--", "local", ":/remote"]
        )
        descriptor = attachment().as_dict()
        gateway = mock.AsyncMock(return_value=descriptor)
        transfer = mock.AsyncMock(return_value=17)

        with (
            mock.patch("pazuzu.cli.call_gateway", gateway),
            mock.patch("pazuzu.cli.run_transfer", transfer),
        ):
            result = await cli._run(arguments)

        self.assertEqual(17, result)
        gateway.assert_awaited_once_with(
            arguments.socket, "connection_attachment", timeout=140.0
        )
        argv = transfer.await_args.args[0]
        self.assertEqual("/custom/scp", argv[0])
        self.assertEqual(["local", "example-host:/remote"], argv[-2:])

    async def test_rsync_parser_preserves_native_arguments(self) -> None:
        arguments = cli._parser().parse_args(
            ["rsync", "--rsync", "/custom/rsync", "--", "-av", "local/", ":remote/"]
        )

        self.assertEqual("/custom/rsync", arguments.rsync)
        self.assertEqual(["--", "-av", "local/", ":remote/"], arguments.transfer_arguments)


if __name__ == "__main__":
    unittest.main()

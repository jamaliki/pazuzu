from __future__ import annotations

import asyncio
import contextlib
import errno
import fcntl
import json
import os
import pty
import select
import signal
import struct
import sys
import tempfile
import termios
import textwrap
import time
import unittest
from io import StringIO
from pathlib import Path
from unittest import mock

from pazuzu import cli
from pazuzu.errors import PazuzuError
from pazuzu.shell import (
    remote_tmux_command,
    require_interactive_terminal,
    run_attached_ssh,
    run_shell,
    shell_argv,
    validate_session_name,
)
from pazuzu.transport import ShellAttachment


def attachment(generation: int = 1, *, host: str = "test-host") -> ShellAttachment:
    return ShellAttachment(
        host=host,
        ssh_binary="/usr/bin/ssh",
        control_path=Path(f"/private/ssh-{generation}.ctl"),
        generation=generation,
    )


async def connected() -> dict[str, object]:
    return {"connection": "connected"}


class ShellConstructionTests(unittest.TestCase):
    def test_session_names_use_a_narrow_ascii_grammar(self) -> None:
        for valid in ("a", "A9", "analysis-1", "work.tree_2", "z" * 64):
            with self.subTest(valid=valid):
                self.assertEqual(valid, validate_session_name(valid))

        for invalid in ("", "-starts-with-option", "_underscore", "a/b", "é", "z" * 65):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "session name"
            ):
                validate_session_name(invalid)

    def test_remote_command_checks_tmux_and_execs_the_named_session(self) -> None:
        command = remote_tmux_command("analysis-1")

        self.assertIn("command -v tmux", command)
        self.assertIn("pazuzu shell requires tmux on the remote host", command)
        self.assertTrue(command.endswith("exec tmux new-session -A -s analysis-1"))
        with self.assertRaises(ValueError):
            remote_tmux_command("safe; touch /tmp/injected")

    def test_ssh_argv_allocates_a_pty_and_fails_closed(self) -> None:
        descriptor = attachment(host="-oProxyCommand=attacker")
        arguments = shell_argv(descriptor, "analysis")

        self.assertEqual("/usr/bin/ssh", arguments[0])
        self.assertIn("-tt", arguments)
        self.assertEqual("/private/ssh-1.ctl", arguments[arguments.index("-S") + 1])
        for option in (
            "ControlMaster=no",
            "ControlPersist=no",
            "ProxyCommand=/usr/bin/false",
            "ConnectTimeout=5",
        ):
            self.assertIn(option, arguments)
        separator = arguments.index("--")
        self.assertEqual("-oProxyCommand=attacker", arguments[separator + 1])
        self.assertGreater(separator, arguments.index("ProxyCommand=/usr/bin/false"))

    def test_attachment_parser_rejects_extra_ssh_configuration(self) -> None:
        value = attachment().as_dict()
        self.assertEqual(attachment(), ShellAttachment.from_dict(value))

        value["proxy_command"] = "attacker"
        with self.assertRaisesRegex(ValueError, "descriptor fields"):
            ShellAttachment.from_dict(value)

    def test_non_terminal_use_is_rejected(self) -> None:
        terminal = StringIO()
        with self.assertRaisesRegex(ValueError, "interactive terminal"):
            require_interactive_terminal(terminal, terminal)


class ShellLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_clean_exit_does_not_reattach(self) -> None:
        attachments = 0
        reconnects = 0
        spawns: list[list[str]] = []

        async def get_attachment() -> ShellAttachment:
            nonlocal attachments
            attachments += 1
            return attachment()

        async def reconnect() -> dict[str, object]:
            nonlocal reconnects
            reconnects += 1
            return await connected()

        async def spawn(arguments: list[str], _cancellation: asyncio.Event) -> int:
            spawns.append(arguments)
            return 0

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=connected,
            spawn_ssh=spawn,
            cancellation=asyncio.Event(),
        )

        self.assertEqual(0, result)
        self.assertEqual(1, attachments)
        self.assertEqual(0, reconnects)
        self.assertEqual(1, len(spawns))

    async def test_missing_tmux_returns_127_without_retry(self) -> None:
        spawns = 0

        async def get_attachment() -> ShellAttachment:
            return attachment()

        async def spawn(_arguments: list[str], _cancellation: asyncio.Event) -> int:
            nonlocal spawns
            spawns += 1
            return 127

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=connected,
            status=connected,
            spawn_ssh=spawn,
            cancellation=asyncio.Event(),
        )

        self.assertEqual(127, result)
        self.assertEqual(1, spawns)

    async def test_transport_loss_reattaches_the_same_session_on_a_new_generation(self) -> None:
        descriptors = [attachment(1), attachment(2)]
        exits = [255, 0]
        spawned: list[list[str]] = []
        reconnects = 0
        output = StringIO()

        async def get_attachment() -> ShellAttachment:
            return descriptors.pop(0)

        async def reconnect() -> dict[str, object]:
            nonlocal reconnects
            reconnects += 1
            return await connected()

        async def spawn(arguments: list[str], _cancellation: asyncio.Event) -> int:
            spawned.append(arguments)
            return exits.pop(0)

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=connected,
            spawn_ssh=spawn,
            stderr=output,
            cancellation=asyncio.Event(),
        )

        self.assertEqual(0, result)
        self.assertEqual(1, reconnects)
        self.assertEqual(["/private/ssh-1.ctl", "/private/ssh-2.ctl"], [
            arguments[arguments.index("-S") + 1] for arguments in spawned
        ])
        self.assertTrue(all(arguments[-1].endswith("-s analysis") for arguments in spawned))
        self.assertIn("connection lost", output.getvalue())
        self.assertIn("connection restored", output.getvalue())

    async def test_repeated_gateway_states_are_reported_once_per_transition(self) -> None:
        states = iter(
            ["connected", "offline", "offline", "reconnecting", "reconnecting", "connected"]
        )
        exits = iter([255, 0])
        output = StringIO()

        async def status() -> dict[str, object]:
            return {"connection": next(states)}

        async def get_attachment() -> ShellAttachment:
            return attachment()

        async def reconnect() -> dict[str, object]:
            raise PazuzuError("still offline")

        async def spawn(_arguments: list[str], _cancellation: asyncio.Event) -> int:
            return next(exits)

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=status,
            spawn_ssh=spawn,
            sleep=lambda _delay: asyncio.sleep(0),
            stderr=output,
            cancellation=asyncio.Event(),
        )

        message = output.getvalue()
        self.assertEqual(0, result)
        self.assertEqual(1, message.count("connection is unavailable"))
        self.assertEqual(1, message.count("reconnecting SSH"))
        self.assertEqual(1, message.count("connection restored"))

    async def test_authentication_required_waits_and_then_resumes(self) -> None:
        states = iter(["connected", "authentication_required", "authentication_required", "connected"])
        exits = iter([255, 0])
        output = StringIO()

        async def status() -> dict[str, object]:
            return {"connection": next(states)}

        async def get_attachment() -> ShellAttachment:
            return attachment()

        async def reconnect() -> dict[str, object]:
            raise PazuzuError("authentication required")

        async def spawn(_arguments: list[str], _cancellation: asyncio.Event) -> int:
            return next(exits)

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=status,
            spawn_ssh=spawn,
            sleep=lambda _delay: asyncio.sleep(0),
            stderr=output,
            cancellation=asyncio.Event(),
        )

        self.assertEqual(0, result)
        self.assertEqual(1, output.getvalue().count("SSH reauthorization is required"))
        self.assertIn("`pazuzu reconnect`", output.getvalue())

    async def test_local_cancellation_stops_the_attached_child_without_reconnect(self) -> None:
        cancellation = asyncio.Event()
        child_stopped = False
        reconnects = 0

        async def get_attachment() -> ShellAttachment:
            return attachment()

        async def reconnect() -> dict[str, object]:
            nonlocal reconnects
            reconnects += 1
            return await connected()

        async def spawn(_arguments: list[str], stop: asyncio.Event) -> int:
            nonlocal child_stopped
            asyncio.get_running_loop().call_soon(stop.set)
            await stop.wait()
            child_stopped = True
            return 130

        result = await run_shell(
            "analysis",
            get_attachment=get_attachment,
            reconnect=reconnect,
            status=connected,
            spawn_ssh=spawn,
            cancellation=cancellation,
        )

        self.assertEqual(130, result)
        self.assertTrue(child_stopped)
        self.assertEqual(0, reconnects)

    async def test_two_named_shells_never_cross_attach(self) -> None:
        async def run_named(session: str) -> str:
            spawned = ""

            async def get_attachment() -> ShellAttachment:
                return attachment()

            async def spawn(arguments: list[str], _cancellation: asyncio.Event) -> int:
                nonlocal spawned
                spawned = arguments[-1]
                return 0

            await run_shell(
                session,
                get_attachment=get_attachment,
                reconnect=connected,
                status=connected,
                spawn_ssh=spawn,
                cancellation=asyncio.Event(),
            )
            return spawned

        analysis, debugging = await asyncio.gather(
            run_named("analysis"), run_named("debugging")
        )

        self.assertTrue(analysis.endswith("-s analysis"))
        self.assertTrue(debugging.endswith("-s debugging"))


class ShellCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_defaults_to_the_pazuzu_session(self) -> None:
        arguments = cli._parser().parse_args(["shell"])

        self.assertEqual("pazuzu", arguments.session)

    async def test_cli_rejects_non_tty_use_before_contacting_the_gateway(self) -> None:
        arguments = cli._parser().parse_args(["shell"])
        gateway = mock.AsyncMock()
        with (
            mock.patch(
                "pazuzu.cli.require_interactive_terminal",
                side_effect=ValueError("not a terminal"),
            ),
            mock.patch("pazuzu.cli.call_gateway", gateway),
            self.assertRaisesRegex(ValueError, "not a terminal"),
        ):
            await cli._run(arguments)

        gateway.assert_not_awaited()


class AttachedProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_terminates_the_local_child(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "marker.txt"
            script = root / "child.py"
            script.write_text(
                textwrap.dedent(
                    """
                    import signal
                    import sys
                    import time
                    from pathlib import Path

                    marker = Path(sys.argv[1])
                    marker.write_text("started")

                    def terminate(_signum, _frame):
                        marker.write_text("terminated")
                        raise SystemExit(0)

                    signal.signal(signal.SIGTERM, terminate)
                    while True:
                        time.sleep(0.05)
                    """
                )
            )
            cancellation = asyncio.Event()
            task = asyncio.create_task(
                run_attached_ssh([sys.executable, str(script), str(marker)], cancellation)
            )
            for _ in range(100):
                if marker.exists():
                    break
                await asyncio.sleep(0.01)
            cancellation.set()

            result = await asyncio.wait_for(task, 3.0)

            self.assertEqual(130, result)
            self.assertEqual("terminated", marker.read_text())


class AttachedPtyTests(unittest.TestCase):
    def test_attached_process_inherits_the_pty_resize_and_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_file = root / "ssh.pty.json"
            script = root / "pty_child.py"
            script.write_text(
                f"#!{sys.executable}\n"
                + textwrap.dedent(
                    """
                    import json
                    import os
                    import signal
                    import sys
                    import time
                    from pathlib import Path

                    control_path = Path(sys.argv[sys.argv.index("-S") + 1])
                    state_file = control_path.with_suffix(".pty.json")
                    state = {
                        "arguments": sys.argv[1:],
                        "stdin_tty": os.isatty(0),
                        "stdout_tty": os.isatty(1),
                        "stderr_tty": os.isatty(2),
                        "resized": False,
                        "interrupted": False,
                    }

                    def save():
                        state_file.write_text(json.dumps(state))

                    def resize(_signum, _frame):
                        state["resized"] = True
                        save()

                    def interrupt(_signum, _frame):
                        state["interrupted"] = True
                        save()
                        raise SystemExit(0)

                    signal.signal(signal.SIGWINCH, resize)
                    signal.signal(signal.SIGINT, interrupt)
                    save()
                    print("READY", flush=True)
                    deadline = time.monotonic() + 3
                    while not state["resized"] and time.monotonic() < deadline:
                        time.sleep(0.01)
                    if not state["resized"]:
                        raise SystemExit(4)
                    os.killpg(os.getpgrp(), signal.SIGINT)
                    time.sleep(1)
                    raise SystemExit(5)
                    """
                )
            )
            script.chmod(0o700)

            child_pid, master_fd = pty.fork()
            if child_pid == 0:
                async def child_main() -> int:
                    descriptor = ShellAttachment(
                        host="pty-host",
                        ssh_binary=str(script),
                        control_path=root / "ssh.ctl",
                        generation=1,
                    )
                    return await run_attached_ssh(
                        shell_argv(descriptor, "pty-test"), asyncio.Event()
                    )

                result = asyncio.run(child_main())
                os._exit(0 if result == 0 else result)

            output = bytearray()
            wait_status: int | None = None
            resized = False
            deadline = time.monotonic() + 8
            try:
                while time.monotonic() < deadline:
                    readable, _, _ = select.select([master_fd], [], [], 0.05)
                    if readable:
                        try:
                            output.extend(os.read(master_fd, 4096))
                        except OSError as exc:
                            if exc.errno != errno.EIO:
                                raise
                    if b"READY" in output and not resized:
                        dimensions = struct.pack("HHHH", 48, 132, 0, 0)
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, dimensions)
                        resized = True
                    waited_pid, wait_status = os.waitpid(child_pid, os.WNOHANG)
                    if waited_pid:
                        break
                if wait_status is None:
                    self.fail(f"PTY child did not exit; output={bytes(output)!r}")
            finally:
                if wait_status is None:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(child_pid, signal.SIGKILL)
                    os.waitpid(child_pid, 0)
                os.close(master_fd)

            self.assertEqual(0, os.waitstatus_to_exitcode(wait_status), bytes(output))
            state = json.loads(state_file.read_text())
            self.assertTrue(state["stdin_tty"])
            self.assertTrue(state["stdout_tty"])
            self.assertTrue(state["stderr_tty"])
            self.assertTrue(state["resized"])
            self.assertTrue(state["interrupted"])
            self.assertIn("-tt", state["arguments"])
            self.assertIn("ProxyCommand=/usr/bin/false", state["arguments"])
            self.assertTrue(state["arguments"][-1].endswith("-s pty-test"))


if __name__ == "__main__":
    unittest.main()

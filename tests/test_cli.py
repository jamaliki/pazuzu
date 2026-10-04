from __future__ import annotations

import os
import socket
import unittest
from unittest import mock

from pazuzu import cli


class StdinTests(unittest.TestCase):
    def read_from(self, descriptor: int) -> bytes:
        with os.fdopen(os.dup(descriptor), "rb", buffering=0) as raw, mock.patch.object(
            cli.sys, "stdin", mock.Mock(isatty=lambda: False, fileno=raw.fileno, buffer=raw)
        ):
            return cli._read_stdin()

    def test_pipe_and_file_input_is_read_to_eof(self) -> None:
        reader, writer = os.pipe()
        os.write(writer, b"print('remote')\n")
        os.close(writer)
        try:
            self.assertEqual(b"print('remote')\n", self.read_from(reader))
        finally:
            os.close(reader)

    def test_inherited_socket_that_never_closes_is_not_input(self) -> None:
        ours, inherited = socket.socketpair()
        try:
            self.assertEqual(b"", self.read_from(inherited.fileno()))
        finally:
            ours.close()
            inherited.close()

    def test_exec_accepts_no_stdin_like_ssh(self) -> None:
        arguments = cli._parser().parse_args(["exec", "-n", "--", "hostname"])
        self.assertTrue(arguments.no_stdin)
        self.assertFalse(cli._parser().parse_args(["exec", "--", "hostname"]).no_stdin)

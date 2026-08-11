from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ModuleNotFoundError:
    ClientSession = StdioServerParameters = stdio_client = None  # type: ignore[assignment]

from pazuzu.gateway import GatewayServer
from pazuzu.ssh import OpenSshSupervisor, SshSettings


@unittest.skipIf(ClientSession is None, "MCP extra is not installed")
class McpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.socket_path = self.root / "gateway.sock"
        self.environment = mock.patch.dict(
            os.environ, {"PAZUZU_FAKE_SSH_STATE": str(self.root / "state.json")}
        )
        self.environment.start()
        supervisor = OpenSshSupervisor(
            SshSettings(
                host="test-host",
                control_path=self.root / "ssh.ctl",
                ssh_binary=str(Path(__file__).with_name("fake_ssh.py")),
                connect_timeout=1.0,
                probe_timeout=2.0,
                connection_attempts=1,
            )
        )
        self.gateway = GatewayServer(supervisor, self.socket_path)
        self.gateway_task = asyncio.create_task(self.gateway.serve())
        for _ in range(100):
            if self.socket_path.exists():
                break
            await asyncio.sleep(0.01)

    async def asyncTearDown(self) -> None:
        self.gateway.stop_event.set()
        await self.gateway_task
        self.environment.stop()
        self.temporary.cleanup()

    async def test_stdio_protocol_lists_and_calls_tools(self) -> None:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=[
                "-m",
                "pazuzu.mcp_server",
                "--socket",
                str(self.socket_path),
                "--transport",
                "stdio",
            ],
            env=dict(os.environ),
        )
        async with (
            stdio_client(parameters) as (reader, writer),
            ClientSession(reader, writer) as session,
        ):
            await session.initialize()
            tools = await session.list_tools()
            names = {tool.name for tool in tools.tools}
            response = await session.call_tool("execute", {"command": "from-mcp"})
            submitted = await session.call_tool(
                "submit_slurm_job",
                {
                    "name": "mcp-smoke",
                    "argv": ["python3", "-m", "train"],
                    "cwd": "/remote/repo",
                    "log_dir": "/remote/logs",
                    "memory_gb_per_node": 128,
                    "time_limit": "02:00:00",
                    "cpus_per_task": 14,
                    "gpus_per_node": 1,
                    "partition": "gpu",
                },
            )
            status = await session.call_tool("slurm_job_status", {"job_id": "12345"})

        self.assertEqual(
            {
                "cancel_slurm_job",
                "connection_health",
                "execute",
                "inspect_slurm_job",
                "reconnect",
                "slurm_queue",
                "slurm_job_status",
                "submit_slurm_job",
                "tail_remote_file",
            },
            names,
        )
        self.assertFalse(response.isError)
        self.assertEqual("ran:from-mcp\n", response.structuredContent["stdout"])
        self.assertFalse(submitted.isError)
        self.assertEqual("12345", submitted.structuredContent["job_id"])
        self.assertFalse(status.isError)
        self.assertEqual("RUNNING", status.structuredContent["state"])


if __name__ == "__main__":
    unittest.main()

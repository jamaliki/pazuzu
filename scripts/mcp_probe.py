"""Initialize a live Pazuzu MCP endpoint and make two harmless calls."""

from __future__ import annotations

import asyncio
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def probe(url: str) -> None:
    async with (
        streamable_http_client(url) as (reader, writer, _),
        ClientSession(reader, writer) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        names = {tool.name for tool in tools.tools}
        expected = {"connection_health", "execute", "reconnect"}
        if not expected <= names:
            raise RuntimeError(f"missing MCP tools: {sorted(expected - names)}")
        health = await session.call_tool("connection_health", {"probe": True})
        identity = await session.call_tool("execute", {"command": "hostname"})
        if health.isError or identity.isError:
            raise RuntimeError("Pazuzu MCP smoke call failed")
        print(identity.structuredContent["stdout"].strip())


if __name__ == "__main__":
    asyncio.run(probe(sys.argv[1]))

# Pazuzu agent guide

Pazuzu is a small local supervisor for one OpenSSH connection. Keep the core
dependency-free and keep authentication inside the user's existing OpenSSH
configuration.

## Invariants

- A live ControlMaster process is not proof that it can open a session.
- Never replay an arbitrary remote command after an uncertain disconnect.
- Only callers that know an operation is idempotent may request one replay.
- Each command gets an independent SSH channel; never share an interactive shell.
- Keep stdout and stderr bounded and treat remote text as untrusted data.
- The MCP adapter is optional and must not own connection state.
- Service bridges must reuse the owned master, disable direct fallback, and
  remain unaware of the forwarded application's protocol.
- Keep Slurm support generic; workflow and scientific recipe semantics belong upstream.

## Validation

Run before committing:

```bash
uv run --extra dev ruff check .
uv run python -m unittest discover -s tests -v
uv run --extra mcp python -m unittest discover -s tests -v
```

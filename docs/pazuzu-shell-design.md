# Design: resilient interactive shells

Status: implemented

## Decision

Add a `pazuzu shell` command that opens a real interactive terminal through
Pazuzu's existing OpenSSH `ControlMaster` and attaches it to a persistent remote
`tmux` session. If the SSH connection dies, Pazuzu repairs the connection and
the command reattaches to the same `tmux` session.

Do not turn `pazuzu exec` or the Unix gateway protocol into a terminal stream.
Do not replay terminal input. The remote `tmux` session, rather than local byte
replay, preserves the shell and any command running inside it.

The intended interface is:

```bash
pazuzu shell
pazuzu shell --session analysis
```

The default session name is `pazuzu`. `--session` lets one user keep several
independent remote shells.

## Context: what Pazuzu is

Pazuzu is a local supervisor for one SSH connection to one preconfigured host.
It separates connection ownership from the clients that need remote access:

```text
CLI / MCP / dashboards
          |
          | private Unix socket
          v
  Pazuzu gateway and supervisor
          |
          | private OpenSSH ControlMaster
          v
       remote host
```

The long-running gateway owns the `ControlMaster`, probes it with real SSH
sessions, classifies authentication failures, and replaces an unusable master
under one repair lock. Clients use independent channels through that master.
The MCP adapter owns no SSH state and can restart without reconnecting SSH.

The current command API is deliberately conservative:

- `pazuzu exec` captures bounded stdout and stderr;
- arbitrary commands are never replayed after an uncertain disconnect;
- explicitly idempotent operations may opt into one replay after a proven
  repair;
- standard input is finite and bounded;
- direct SSH fallback is disabled, so clients cannot accidentally create a
  second connection outside the supervisor.

These properties make `exec` useful for agents and automation, but they also
make it unsuitable for interactive work. It has no PTY, reads piped input only,
bounds output, and returns one result after the remote process exits.

## User problem

Sometimes a person needs to do light exploratory work on the remote host:
navigate a worktree, inspect a few files, run `squeue`, or use an interactive
debugger. Opening an unrelated `ssh` connection works, but loses the benefits of
Pazuzu:

- it can create a competing connection or stale multiplexer;
- it does not share Pazuzu's health checks and repair lock;
- it has a separate failure and reauthorization path;
- a network interruption destroys the interactive shell and may terminate its
  foreground command.

The desired experience is close to ordinary SSH while retaining Pazuzu's single
connection owner and recovery behavior.

## Why `tmux` is the persistence boundary

An interactive terminal is stateful. At the moment a connection fails, Pazuzu
cannot know whether the final keystroke reached the remote shell. Replaying
terminal bytes could duplicate a command, submit a job twice, or corrupt an
editor buffer. No local reconnect algorithm can remove that ambiguity.

`tmux` solves the problem at the correct boundary. The shell, terminal state,
and foreground process remain on the remote host. A replacement SSH channel
only attaches a new terminal view to that existing state. Pazuzu never needs to
reconstruct the shell or repeat input.

This gives two distinct safety contracts:

| Interface | Recovery contract |
| --- | --- |
| `pazuzu exec` | Return one bounded result; never replay unless the caller explicitly marks the operation safe. |
| `pazuzu shell` | Never replay terminal input; reconnect the transport and reattach to remote `tmux`. |

## Goals

- Behave like a normal interactive terminal, including colours, completion,
  signals, full-screen programs, and terminal resizing.
- Reuse the gateway-owned SSH master without allowing direct fallback.
- Keep the remote shell and its foreground command alive across local network
  failures and SSH master replacement.
- Recover automatically when the gateway becomes connected again.
- Report `authentication_required` clearly while leaving provider-specific
  login steps to site documentation.
- Let intentional `tmux` detach and shell exit end the local command cleanly.
- Keep Pazuzu's base installation dependency-free. `tmux` is a remote runtime
  prerequisite, not a Python dependency.

## Non-goals

- Surviving a remote host reboot or a killed `tmux` server.
- Guaranteeing delivery of keystrokes sent during a disconnect.
- Recording, buffering, or replaying terminal traffic.
- Automating SSH-provider authentication or storing credentials.
- Providing browser terminals, MCP terminal tools, file transfer, or port
  forwarding in this change.
- Running heavy work on a shared login host. Site policy still applies.
- Supporting hosts without `tmux` in version one.

## Proposed architecture

### Keep terminal bytes out of the gateway

The gateway's newline-delimited JSON protocol is designed for bounded request
and response objects. Relaying a PTY through it would require a new streaming
protocol for raw bytes, flow control, terminal resizing, signals, cancellation,
and backpressure. It would also make the launchd-owned gateway part of every
interactive data path.

Instead, add one small gateway operation that returns an attachment descriptor
for the current connection:

```json
{
  "host": "configured-host",
  "ssh_binary": "/usr/bin/ssh",
  "control_path": "/private/local/path/ssh.ctl",
  "generation": 3
}
```

The operation must call the supervisor's existing connection check before it
returns. The descriptor contains no credentials or provider configuration. It
is available only through the mode-0600 local Unix socket and describes the
same local SSH state already usable by the caller.

The CLI then starts an OpenSSH child attached directly to its stdin, stdout,
stderr, and controlling terminal:

```text
local terminal
      |
      v
ssh -tt -S <control-path> <host> <tmux command>
      |
      v
tmux new-session -A -s <session>
```

The SSH child must use the same fail-closed options as non-interactive command
channels:

```text
-S <control-path>
-o ControlMaster=no
-o ControlPersist=no
-o ProxyCommand=/usr/bin/false
-o ConnectTimeout=5
```

`ProxyCommand=/usr/bin/false` is essential. If the advertised master disappears
between descriptor creation and SSH startup, the child must fail rather than
opening an independent connection. The CLI then asks the gateway to repair the
connection and obtains a fresh descriptor.

### Remote command

The remote command should fail clearly when `tmux` is absent, then replace the
remote shell process with `tmux`:

```sh
command -v tmux >/dev/null 2>&1 || {
    printf '%s\n' 'pazuzu shell requires tmux on the remote host' >&2
    exit 127
}
exec tmux new-session -A -s SESSION
```

Construct this command from validated data with `shlex`; do not interpolate an
unchecked session name into shell text. Accept session names matching a narrow
ASCII grammar such as `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`.

### Connection and reattachment loop

One `pazuzu shell` invocation owns the following local loop:

1. Require an actual local terminal. Refuse redirected stdin or stdout rather
   than offering a partially interactive session.
2. Request an attachment descriptor from the gateway.
3. Start the SSH PTY and attach to the selected `tmux` session.
4. If SSH exits with code 0, return 0. This covers `exit` and intentional tmux
   detach (`Ctrl-b d`).
5. If the remote command reports missing `tmux`, return a clear error without
   retrying.
6. Return any other ordinary remote exit code without retrying.
7. If SSH exits with 255, treat the terminal view as lost, not the remote shell.
   Ask the gateway to reconnect and wait for `connected` state.
8. Obtain a new descriptor and attach again to the same session.
9. Continue until a clean exit or local cancellation.

Only print connection state changes. A long outage should produce one useful
line such as `connection lost; remote tmux session is still running`, followed
by another when the state changes, rather than a message on every poll.

The gateway already retries failed connections with bounded backoff. The shell
client may request one immediate reconnect after losing SSH, then poll local
gateway status at a modest interval. It must not launch its own master.

### Authentication recovery

When the gateway reports `authentication_required`, keep the local shell
wrapper alive and print a provider-neutral message:

```text
pazuzu: SSH reauthorization is required.
Complete the configured provider login in another terminal, then run
`pazuzu reconnect`. Waiting to reattach; press Ctrl-C to stop.
```

The exact authentication command belongs in site documentation, not Pazuzu.
Once the gateway becomes connected, the wrapper reattaches automatically.

Do not launch a browser, invoke a provider CLI, retain credentials, or guess how
the target site authenticates.

### Signals and terminal behavior

The SSH child must inherit the real terminal rather than using pipes. OpenSSH
then handles PTY allocation, terminal modes, and `SIGWINCH` propagation.

The parent wrapper must distinguish terminal input from local cancellation:

- while SSH is attached, `Ctrl-C` must reach OpenSSH and the remote foreground
  program rather than stopping the wrapper;
- while Pazuzu is waiting to reconnect, `Ctrl-C` stops the wrapper and returns
  130;
- termination stops the current SSH child and does not kill remote `tmux`;
- no `start_new_session=True`, output capture, or bounded-output helper should
  be used for the interactive child.

Signal handling needs an explicit integration test because Python and OpenSSH
share the foreground process group. Implement it in one focused helper rather
than modifying the bounded subprocess runner used by `exec`.

## Public CLI

Version one should remain small:

```text
usage: pazuzu shell [--socket SOCKET] [--session NAME]
```

- `--socket` follows the other gateway client commands.
- `--session` defaults to `pazuzu`.
- There is no host argument: the selected gateway already owns one host.
- There is no command argument: use `pazuzu exec` for non-interactive commands.
- There is no `--retry-safe`: reattachment never replays terminal input.
- There is no `--no-tmux` fallback because that would silently discard the main
  reliability property.

Document `exit` to terminate the remote shell and `Ctrl-b d` to leave the remote
session running for later attachment.

## Implementation plan

### 1. Represent an attachment descriptor

In `src/pazuzu/transport.py`, add an immutable value carrying only:

- configured host;
- SSH executable;
- control socket path;
- connection generation, supplied by the supervisor.

Keep SSH argument construction in a pure function so its fail-closed options
are easy to test. Do not expose the full `SshSettings` object through the
gateway.

### 2. Add a supervisor operation

In `src/pazuzu/supervisor.py`, add a method such as
`shell_attachment()` that:

1. calls `_ensure_connected()`;
2. returns the descriptor for that generation.

It must not stop maintenance, reserve the repair lock for the lifetime of the
shell, or promise that the descriptor cannot become stale. A descriptor is a
snapshot. The CLI handles the race by failing closed and requesting another
one.

### 3. Extend the local gateway

In `src/pazuzu/gateway.py`, add a `shell_attachment` operation with no request
parameters. Return the descriptor as JSON. Validate that the parameter object
is empty so the internal API remains narrow.

Do not send terminal data, authentication material, or an open file descriptor
through the JSON protocol.

### 4. Add a focused shell module

Create `src/pazuzu/shell.py` for:

- session-name validation;
- pure remote-command and SSH-argv construction;
- the terminal-attached SSH subprocess;
- the reconnect and reattach loop;
- concise state-change reporting;
- local signal handling.

This behavior does not belong in `process.py`: that module intentionally owns
bounded, non-interactive subprocesses and cancellation by process group.

Keep the reconnect policy functional at its boundary. For example, make the
loop depend on small callables for `get_attachment`, `reconnect`, `status`, and
`spawn_ssh`; tests can then exercise state transitions without a real cluster.

### 5. Wire the CLI

In `src/pazuzu/cli.py`:

- register `shell` with `--socket` and `--session`;
- reject non-TTY use before contacting the gateway;
- call the shell module;
- preserve exit code 130 for local interruption;
- continue using the existing top-level error formatting.

Do not add an MCP shell tool. MCP calls are not interactive terminals, and
exposing one would weaken the bounded-output and injection-safety contracts.

### 6. Update documentation and agent guidance

After implementation:

- add the command to `README.md` with detach, resume, disconnect, and
  authentication examples;
- update `AGENTS.md` to state that ordinary commands still use independent
  non-interactive channels, while `pazuzu shell` is an explicit PTY exception
  backed by remote `tmux`;
- keep all hostnames and provider-specific recovery commands in site-specific
  documentation.

## Failure semantics

| Failure | Expected behavior |
| --- | --- |
| Gateway is not running | Exit with the existing gateway-unavailable error. |
| Gateway is offline at startup | Wait using gateway state, or exit on local Ctrl-C; never open direct SSH. |
| Control socket disappears before SSH starts | SSH fails closed; request repair and a new descriptor. |
| Network dies during a shell | SSH exits; remote tmux continues; Pazuzu repairs and reattaches. |
| Authentication expires | Report `authentication_required` and wait without automating login. |
| `tmux` is missing | Exit with an installation prerequisite error; do not reconnect-loop. |
| User runs `exit` | Remote tmux session ends and `pazuzu shell` returns 0. |
| User presses `Ctrl-b d` | SSH returns cleanly; tmux session remains available for the next invocation. |
| User cancels while Pazuzu is waiting, or terminates the wrapper | Stop locally and return 130; leave remote tmux untouched. |
| Remote host reboots | Reconnection succeeds, but `tmux -A` creates a new session; the previous shell state is unrecoverable. |
| Gateway restarts but preserves the master | Existing shell continues; later reattachments use the adopted master. |

## Security and operational constraints

- Treat the private Unix socket and ControlPath as local user capabilities;
  preserve their current restrictive permissions.
- Never accept arbitrary SSH options from `pazuzu shell` in version one.
- Never allow OpenSSH to fall back to a new direct connection.
- Validate and quote the tmux session name.
- Do not capture terminal output or write session transcripts.
- Do not put authentication recovery commands in Pazuzu.
- Do not add remote package installation. Detect missing `tmux` and stop.
- The shell is for light work. Pazuzu cannot enforce a site's login-node policy,
  so documentation must continue to direct computation to the scheduler.

## Tests

Add tests at the narrowest useful layers.

### Pure unit tests

- valid and invalid tmux session names;
- remote command quoting;
- SSH argv includes `-tt`, the advertised `-S` path, and every fail-closed
  option;
- no user value can become an SSH option or unquoted shell fragment.

### Gateway and supervisor tests

- `shell_attachment` returns the configured host, executable, path, and current
  generation;
- it repairs a missing master before returning;
- malformed parameters are rejected;
- a stale descriptor does not permit direct fallback.

### Reattachment-loop tests

Use a fake SSH launcher and fake gateway callbacks:

- exit 0 returns without another attach;
- missing tmux returns a useful error without another attach;
- exit 255 followed by a healthy new generation attaches twice to the same
  session;
- repeated offline states print only state transitions;
- `authentication_required` waits and resumes after `connected`;
- local cancellation stops the child, returns 130, and does not issue remote
  cleanup;
- `Ctrl-C` during an attached shell reaches the fake remote foreground process
  rather than cancelling the wrapper;
- two named sessions never cross-attach.

### PTY integration test

Run one test under a real pseudo-terminal rather than mocking `isatty()` alone.
Use a fake SSH executable that records arguments and checks that it inherited a
TTY. Verify terminal resize and interruption behavior where the platform permits
it. Keep the test local; live-cluster tests are separate.

### Live acceptance test

On a disposable remote account or allocation:

1. Start `pazuzu shell --session shell-smoke`.
2. Record a value in the shell and start a harmless timed command.
3. Interrupt the underlying SSH connection without killing remote tmux.
4. Confirm Pazuzu creates a new connection generation and reattaches.
5. Confirm the value and command survived.
6. Detach with `Ctrl-b d`, invoke the command again, and confirm it reattaches.
7. Test provider reauthorization only when it can be done without disrupting
   unrelated work.

Run the existing validation suite as well:

```bash
uv run --extra dev ruff check .
uv run python -m unittest discover -s tests -v
uv run --extra mcp python -m unittest discover -s tests -v
```

## Acceptance criteria

The feature is complete when:

- `pazuzu shell` feels like an ordinary SSH terminal during healthy operation;
- it uses only the gateway-owned ControlMaster;
- a broken master cannot trigger direct SSH fallback;
- a network interruption preserves the remote shell and foreground command;
- reconnection attaches to the same named tmux session without replaying input;
- authentication failure is actionable and provider-neutral;
- intentional detach, exit, and local cancellation have distinct behavior;
- existing `exec`, Python, MCP, Slurm, and service tests remain unchanged in
  behavior;
- the base package remains dependency-free.

## Deliberately deferred questions

Consider these only after the minimal shell is proven:

- a `pazuzu shell list` convenience wrapper around `tmux list-sessions`;
- a configurable default session name;
- remote working-directory selection;
- support for another remote terminal multiplexer;
- `mosh` for network roaming where UDP and site policy permit it.

None is required for the first implementation.

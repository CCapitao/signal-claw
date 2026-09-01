#!/usr/bin/env python3
"""
signal-claw: a Signal -> Claude bridge.

Runs one `signal-cli jsonRpc --receive-mode=on-start` child process, then
multiplexes incoming-message notifications and outgoing-send requests over
that single process. This avoids signal-cli's per-account database lock,
which prevents a second send invocation while a receive is active.

Routing rules (both homeline DMs and note-to-self share these):
  - Body normalized to 'pulse-agents' (bare OR after prefix strip)
        -> render local dashboard, reply directly (no claude spawn).
  - Body starts with '<TRIGGER_WORD>@<HOSTNAME>' (case-insensitive, optional
    ':' / ',' / '-' / whitespace separator)
        -> strip prefix, wake claude, reply to the source channel.
  - Everything else -> log and drop silently.

The prefix gate exists so one Signal account can be linked to many machines
without every machine answering every message. Each host only responds to
messages explicitly addressed to it. `pulse-agents` is the deliberate
exception: fleet-wide pings answered by every relay simultaneously.

All configuration is taken from environment variables (see config.example.env).
"""
from __future__ import annotations

import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Queue, Empty


def env(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(key, default)
    if required and not val:
        sys.stderr.write(f"signal-claw: missing required env var {key}\n")
        sys.exit(2)
    return val or ""


ACCOUNT        = env("SIGNAL_ACCOUNT", required=True)
HOMELINE       = env("SIGNAL_HOMELINE", required=True)
TRIGGER_WORD   = env("TRIGGER_WORD", "claude").lower()
HOSTNAME       = env("SIGNAL_HOSTNAME", socket.gethostname().split(".", 1)[0]).strip().lower()
SIGNAL_CLI     = env("SIGNAL_CLI", "/usr/bin/signal-cli")
CLAUDE         = env("CLAUDE", "/usr/bin/claude")
STATE_DIR      = Path(env("STATE_DIR", str(Path.home() / ".local/share/signal-claude")))
LOG_FILE       = Path(env("LOG_FILE", str(STATE_DIR / "daemon.log")))
CLAUDE_TIMEOUT = int(env("CLAUDE_TIMEOUT", "240"))
SIGNAL_RETRY   = int(env("SIGNAL_RETRY", "5"))
MAX_REPLY_LEN  = int(env("MAX_REPLY_LEN", "3800"))
SESSIONS_FILE  = Path(env("SESSIONS_FILE", str(STATE_DIR / "sessions.json")))
SASUKE_GROUP      = env("SASUKE_GROUP", "").strip()      # base64 groupId; msgs in this group bypass the prefix
CLAUDE_CONFIG_DIR = env("CLAUDE_CONFIG_DIR", "").strip()  # set → claude runs as the LifeOS DA (Sasuke)
WATCHDOG_PROBE    = int(env("WATCHDOG_PROBE", "300"))    # seconds between rpc liveness probes
WATCHDOG_STALE    = int(env("WATCHDOG_STALE", "14400"))  # recycle signal-cli after this long with no inbound traffic
HTTP_PORT         = int(env("HTTP_PORT", "0"))           # 0 = local send API disabled
HTTP_HOST         = env("HTTP_HOST", "127.0.0.1")        # loopback only by default

PREFIX_RE = re.compile(
    rf"^{re.escape(TRIGGER_WORD)}@{re.escape(HOSTNAME)}\b[\s:,\-]*",
    re.IGNORECASE,
)
PULSE_TRIGGER = "pulse-agents"

STATE_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("signal-claw")


class SignalRpc:
    """One signal-cli jsonRpc subprocess, multiplexed for send + receive."""

    def __init__(self) -> None:
        self.proc: subprocess.Popen[str] | None = None
        self.next_id = 1
        self.send_lock = threading.Lock()
        self.events: Queue[dict] = Queue()
        self.pending: dict[int, Queue[dict]] = {}
        # Inbound-traffic clock: receive events + stderr lines only. Probe
        # responses deliberately do NOT reset it, or the stale check could
        # never fire — a wedged ReceiveHelper still answers local rpc calls.
        self.last_inbound = time.time()

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [SIGNAL_CLI, "-a", ACCOUNT, "jsonRpc", "--receive-mode=on-start"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        threading.Thread(target=self._reader, daemon=True).start()
        threading.Thread(target=self._stderr_reader, daemon=True).start()
        log.info("signal-cli jsonRpc started pid=%s", self.proc.pid)

    def _reader(self) -> None:
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                log.warning("non-json stdout: %s", line[:200])
                continue
            if msg.get("method") == "receive":
                self.last_inbound = time.time()
                self.events.put(msg.get("params") or {})
            elif "id" in msg:
                waiter = self.pending.pop(msg["id"], None)
                if waiter is not None:
                    waiter.put(msg)
                elif "error" in msg:
                    log.error("signal-cli rpc error id=%s err=%s", msg.get("id"), msg["error"])

    def _stderr_reader(self) -> None:
        assert self.proc and self.proc.stderr
        for line in self.proc.stderr:
            line = line.rstrip()
            if line:
                self.last_inbound = time.time()
                log.info("[signal-cli] %s", line[-400:])

    def probe(self, timeout: float = 30.0) -> bool:
        """Round-trip a 'version' rpc call. False = signal-cli is wedged."""
        if not self.proc or not self.proc.stdin or self.proc.poll() is not None:
            return False
        waiter: Queue[dict] = Queue(maxsize=1)
        with self.send_lock:
            req_id = self.next_id
            self.next_id += 1
            self.pending[req_id] = waiter
            req = {"jsonrpc": "2.0", "method": "version", "id": req_id}
            try:
                self.proc.stdin.write(json.dumps(req) + "\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError):
                self.pending.pop(req_id, None)
                return False
        try:
            waiter.get(timeout=timeout)
            return True
        except Empty:
            self.pending.pop(req_id, None)
            return False

    def call(self, method: str, params: dict | None = None, timeout: float = 30.0) -> dict:
        """Round-trip an arbitrary rpc request. Returns the full response message.

        Raises RuntimeError on dead pipe or timeout so HTTP callers get a 5xx
        instead of a silent drop.
        """
        if not self.proc or not self.proc.stdin or self.proc.poll() is not None:
            raise RuntimeError("signal-cli not running")
        waiter: Queue[dict] = Queue(maxsize=1)
        with self.send_lock:
            req_id = self.next_id
            self.next_id += 1
            self.pending[req_id] = waiter
            req: dict = {"jsonrpc": "2.0", "method": method, "id": req_id}
            if params:
                req["params"] = params
            try:
                self.proc.stdin.write(json.dumps(req) + "\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError) as e:
                self.pending.pop(req_id, None)
                raise RuntimeError(f"signal-cli pipe dead: {e}") from e
        try:
            return waiter.get(timeout=timeout)
        except Empty:
            self.pending.pop(req_id, None)
            raise RuntimeError(f"rpc {method} timed out after {timeout}s")

    def send(self, *, recipient: str | None = None, note_to_self: bool = False,
             group_id: str | None = None, message: str) -> None:
        if not self.proc or not self.proc.stdin:
            log.error("send called before start()")
            return
        with self.send_lock:
            req_id = self.next_id
            self.next_id += 1
            params: dict = {"message": message}
            if group_id:
                params["groupId"] = group_id
            elif note_to_self:
                params["noteToSelf"] = True
            else:
                params["recipient"] = [recipient]
            req = {"jsonrpc": "2.0", "method": "send", "params": params, "id": req_id}
            try:
                self.proc.stdin.write(json.dumps(req) + "\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                log.error("send: broken pipe to signal-cli")


def extract(envelope: dict) -> tuple[str | None, str | None, str | None]:
    """Return (reply_target, body, kind) with kind 'group'|'homeline'|'nts', or (None, None, None)."""
    src = envelope.get("source") or envelope.get("sourceNumber")

    data_msg = envelope.get("dataMessage") or {}
    sync_sent = ((envelope.get("syncMessage") or {}).get("sentMessage")) or {}
    group_info = data_msg.get("groupInfo") or {}

    # Dedicated Sasuke group: any message here (from anyone) is for us; reply into the group.
    if data_msg.get("message") and SASUKE_GROUP and group_info.get("groupId") == SASUKE_GROUP:
        return SASUKE_GROUP, data_msg["message"], "group"

    # Kurama gap: a send into the Sasuke group from the account's own phone
    # (the primary device) arrives as a *sync* message, not a dataMessage.
    # Route it like a group prompt — but only when sourceDevice is 1 (the
    # phone). Syncs from other linked devices are replies/relays from sibling
    # bridge daemons; routing those would let two bridges prompt each other
    # in an infinite loop.
    sync_group = (sync_sent.get("groupInfo") or {}).get("groupId")
    if (sync_sent.get("message") and SASUKE_GROUP
            and sync_group == SASUKE_GROUP
            and envelope.get("sourceDevice") == 1):
        return SASUKE_GROUP, sync_sent["message"], "group"

    # Direct 1:1 message from the trusted home line (not a group).
    if data_msg.get("message") and src == HOMELINE and not group_info:
        return HOMELINE, data_msg["message"], "homeline"

    # Note-to-self (1:1 self sync, not a group sync).
    if sync_sent.get("message") and not sync_sent.get("groupInfo"):
        dest = sync_sent.get("destination") or sync_sent.get("destinationNumber")
        if dest == ACCOUNT:
            return ACCOUNT, sync_sent["message"], "nts"

    return None, None, None


def match_prefix(body: str) -> tuple[bool, str]:
    """Return (matched, body-with-prefix-removed). Empty residue is allowed."""
    s = body.lstrip()
    m = PREFIX_RE.match(s)
    if not m:
        return False, body
    return True, s[m.end():]


def normalize_trigger(text: str) -> str:
    """Collapse whitespace + lowercase so 'Pulse Agents' == 'pulse-agents'... almost.

    We strip whitespace entirely, so 'PULSE-AGENTS', 'pulse  agents', and 'Pulse-Agents'
    all collapse to 'pulse-agents'. Hyphens are preserved.
    """
    return "".join(text.lower().split())


def render_pulse() -> str:
    """Return a one-line dashboard for fleet-wide 'pulse-agents' pings.

    Reads /proc directly to avoid forking df/uptime/free. Falls back to '?' on
    any parse error so we never fail to reply.
    """
    host = socket.gethostname().split(".", 1)[0]

    def _read(path: str) -> str:
        try:
            return Path(path).read_text()
        except OSError:
            return ""

    try:
        up_s = float(_read("/proc/uptime").split()[0])
        d, rem = divmod(int(up_s), 86400)
        h, rem = divmod(rem, 3600)
        m, _ = divmod(rem, 60)
        uptime = f"{d}d{h:02d}h{m:02d}m" if d else f"{h}h{m:02d}m"
    except (ValueError, IndexError):
        uptime = "?"

    load_parts = _read("/proc/loadavg").split()
    load = " ".join(load_parts[:3]) if len(load_parts) >= 3 else "?"

    try:
        meminfo: dict[str, int] = {}
        for line in _read("/proc/meminfo").splitlines():
            key, _, rest = line.partition(":")
            if rest:
                meminfo[key.strip()] = int(rest.strip().split()[0])
        total_g = meminfo["MemTotal"] / 1024 / 1024
        avail_g = meminfo.get("MemAvailable", meminfo["MemTotal"]) / 1024 / 1024
        mem = f"{total_g - avail_g:.1f}/{total_g:.1f}G"
    except (KeyError, ValueError):
        mem = "?"

    try:
        st = os.statvfs("/")
        total_g = st.f_blocks * st.f_frsize / 1024**3
        free_g  = st.f_bavail * st.f_frsize / 1024**3
        used_g  = total_g - free_g
        pct = int(round(used_g / total_g * 100)) if total_g else 0
        disk = f"{used_g:.0f}/{total_g:.0f}G ({pct}%)"
    except OSError:
        disk = "?"

    return f"{host} · up {uptime} · load {load} · mem {mem} · root {disk}"


def fast_path_pulse_agents(body: str, target: str, nts: bool, rpc: "SignalRpc") -> bool:
    """If body == 'pulse-agents' (normalized), render+send and return True."""
    if normalize_trigger(body) != PULSE_TRIGGER:
        return False
    try:
        reply = render_pulse()
    except Exception as e:
        log.exception("pulse render failed")
        reply = f"({HOSTNAME}: pulse render failed: {e})"
    if nts:
        rpc.send(note_to_self=True, message=reply)
    else:
        rpc.send(recipient=target, message=reply)
    log.info("fast-path pulse-agents -> %s", "note-to-self" if nts else target)
    return True


_sessions_lock = threading.Lock()


def load_sessions() -> dict[str, str]:
    try:
        with SESSIONS_FILE.open() as f:
            data = json.load(f)
            return {k: v for k, v in data.items() if isinstance(v, str)}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as e:
        log.warning("sessions file unreadable, starting fresh: %s", e)
        return {}


def save_sessions(sessions: dict[str, str]) -> None:
    try:
        tmp = SESSIONS_FILE.with_suffix(".json.tmp")
        with tmp.open("w") as f:
            json.dump(sessions, f, indent=2, sort_keys=True)
        tmp.replace(SESSIONS_FILE)
    except OSError as e:
        log.error("failed to save sessions: %s", e)


def _invoke_claude(args: list[str], prompt: str) -> subprocess.CompletedProcess[str]:
    child_env = dict(os.environ)
    if CLAUDE_CONFIG_DIR:
        child_env["CLAUDE_CONFIG_DIR"] = CLAUDE_CONFIG_DIR   # run as the LifeOS DA (Sasuke)
    return subprocess.run(
        [CLAUDE, "-p", prompt, *args],
        capture_output=True, text=True,
        timeout=CLAUDE_TIMEOUT,
        cwd=str(Path.home()),
        env=child_env,
    )


def run_claude(prompt: str, channel: str) -> str:
    """Invoke claude with persistent per-channel session memory.

    First message in a channel: --session-id <new uuid> (creates the session).
    Subsequent messages: --resume <uuid> (continues the same conversation).
    If --resume fails (session deleted/expired), recover by minting a fresh UUID.
    """
    with _sessions_lock:
        sessions = load_sessions()
        existing = sessions.get(channel)

    try:
        if existing:
            proc = _invoke_claude(["--resume", existing], prompt)
            if proc.returncode != 0 and ("not found" in (proc.stderr or "").lower()
                                          or "no such session" in (proc.stderr or "").lower()):
                log.warning("session %s lost for channel=%s, restarting", existing, channel)
                existing = None  # fall through to creation path
        if not existing:
            new_id = str(uuid.uuid4())
            proc = _invoke_claude(["--session-id", new_id], prompt)
            with _sessions_lock:
                sessions = load_sessions()
                sessions[channel] = new_id
                save_sessions(sessions)

        out = (proc.stdout or "").strip()
        if not out:
            out = (proc.stderr or "").strip() or "(claude returned no output)"
        return out
    except subprocess.TimeoutExpired:
        return f"(claude timed out after {CLAUDE_TIMEOUT}s)"
    except Exception as e:
        return f"(claude error: {e})"


def truncate(text: str, n: int = MAX_REPLY_LEN) -> str:
    if len(text) <= n:
        return text
    return text[: n - 30] + "\n…[truncated]"


def handle(params: dict, rpc: SignalRpc) -> None:
    envelope = params.get("envelope") or {}
    target, body, kind = extract(envelope)
    if not body or not kind:
        return

    src = envelope.get("source")

    # Dedicated Sasuke group: the group IS the filter — no trigger prefix required.
    if kind == "group":
        prompt = body.strip()
        if not prompt:
            return
        channel = f"group:{target}"
        log.info("group prompt src=%s body=%r", src, prompt[:200])
        reply = truncate(run_claude(prompt, channel))
        log.info("reply len=%d -> group", len(reply))
        rpc.send(group_id=target, message=reply)
        return

    nts = (kind == "nts")

    if fast_path_pulse_agents(body, target, nts, rpc):
        return

    matched, stripped = match_prefix(body)
    if not matched:
        log.info("dropped (no %s@%s prefix) src=%s body=%r",
                 TRIGGER_WORD, HOSTNAME, src, body[:80])
        return

    if fast_path_pulse_agents(stripped, target, nts, rpc):
        return

    prompt = stripped.strip()
    if not prompt:
        log.info("dropped (empty after prefix strip) src=%s", src)
        return

    channel = "nts" if nts else f"homeline:{target}"
    log.info("prompt src=%s channel=%s body=%r", src, channel, prompt[:200])
    reply = truncate(run_claude(prompt, channel))
    log.info("reply len=%d -> %s", len(reply), "note-to-self" if nts else target)

    if nts:
        rpc.send(note_to_self=True, message=reply)
    else:
        rpc.send(recipient=target, message=reply)


# --- Local send API ---------------------------------------------------------
# A loopback HTTP door so local services (e.g. the bullshit-guard confessional)
# can send through THIS process — the account DB has one lock and we hold it.
# The rpc object recycles when signal-cli is recycled, so handlers read it from
# this holder, never from a captured reference.
_rpc_holder: dict[str, "SignalRpc | None"] = {"rpc": None}
_groups_cache: dict = {"ts": 0.0, "groups": []}


def _list_groups(rpc: "SignalRpc") -> list[dict]:
    now = time.time()
    if now - _groups_cache["ts"] < 300 and _groups_cache["groups"]:
        return _groups_cache["groups"]
    resp = rpc.call("listGroups")
    groups = resp.get("result") or []
    _groups_cache.update(ts=now, groups=groups)
    return groups


class ApiHandler(BaseHTTPRequestHandler):
    def _reply(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:  # route to our log, not stderr
        log.info("[api] " + fmt, *args)

    def do_GET(self) -> None:
        rpc = _rpc_holder["rpc"]
        if self.path.rstrip("/") == "/groups":
            if rpc is None:
                return self._reply(503, {"error": "signal-cli not ready"})
            try:
                groups = [{"id": g.get("id"), "name": g.get("name")} for g in _list_groups(rpc)]
                return self._reply(200, {"groups": groups})
            except RuntimeError as e:
                return self._reply(502, {"error": str(e)})
        return self._reply(404, {"error": "unknown path"})

    def do_POST(self) -> None:
        rpc = _rpc_holder["rpc"]
        if self.path.rstrip("/") != "/send":
            return self._reply(404, {"error": "unknown path"})
        if rpc is None:
            return self._reply(503, {"error": "signal-cli not ready"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, json.JSONDecodeError):
            return self._reply(400, {"error": "invalid json"})
        message = req.get("message")
        if not message or not isinstance(message, str):
            return self._reply(400, {"error": "message required"})
        group_id = req.get("groupId")
        group_name = req.get("groupName")
        try:
            if not group_id and group_name:
                for g in _list_groups(rpc):
                    if (g.get("name") or "").strip().lower() == group_name.strip().lower():
                        group_id = g.get("id")
                        break
                if not group_id:
                    return self._reply(404, {"error": f"group not found: {group_name}"})
            if not group_id:
                return self._reply(400, {"error": "groupId or groupName required"})
            resp = rpc.call("send", {"groupId": group_id, "message": message})
            if "error" in resp:
                return self._reply(502, {"error": resp["error"]})
            log.info("[api] sent %d chars to group %s", len(message), group_id[:12])
            return self._reply(200, {"ok": True, "result": resp.get("result")})
        except RuntimeError as e:
            return self._reply(502, {"error": str(e)})


def start_api() -> None:
    if not HTTP_PORT:
        return
    server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), ApiHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("send API listening on %s:%d", HTTP_HOST, HTTP_PORT)


def main() -> None:
    log.info("=== signal-claw starting ===")
    start_api()
    while True:
        rpc = SignalRpc()
        try:
            rpc.start()
        except Exception:
            log.exception("failed to spawn signal-cli")
            time.sleep(SIGNAL_RETRY)
            continue
        _rpc_holder["rpc"] = rpc

        try:
            last_probe = time.time()
            while True:
                try:
                    params = rpc.events.get(timeout=5)
                except Empty:
                    params = None
                if rpc.proc and rpc.proc.poll() is not None:
                    log.warning("signal-cli exited code=%s", rpc.proc.returncode)
                    break
                now = time.time()
                if now - last_probe >= WATCHDOG_PROBE:
                    last_probe = now
                    if not rpc.probe():
                        log.warning("watchdog: rpc probe failed, recycling signal-cli")
                        break
                    stale = now - rpc.last_inbound
                    if stale >= WATCHDOG_STALE:
                        log.warning("watchdog: no inbound traffic for %ds, recycling signal-cli", int(stale))
                        break
                if params is None:
                    continue
                try:
                    handle(params, rpc)
                except Exception:
                    log.exception("handler crashed")
        finally:
            try:
                if rpc.proc and rpc.proc.poll() is None:
                    rpc.proc.terminate()
                    rpc.proc.wait(timeout=5)
            except Exception:
                pass

        time.sleep(SIGNAL_RETRY)


if __name__ == "__main__":
    main()

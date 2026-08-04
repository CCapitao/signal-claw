#!/usr/bin/env python3
"""icloud_door: the iCloud-folder door for the Receipts Drop.

Runs as a systemd oneshot on a timer. Pulls new files from the Hinata iCloud
drop folder over SSH, feeds them through the same file_drop() spine as the
Signal doors, then moves ingested originals to ingested/ on Hinata.

Email leg: a Hinata Mail.app rule saves receipts@bluefenix.net attachments
into the same folder — this door doesn't know or care how files arrive.

Env comes from config.env (sourced by the systemd unit's bash -lc wrapper).
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import daemon  # noqa: E402  (shares env config, log, gates, and the spine)

HOST     = daemon.env("ICLOUD_HOST", "hinata")
REMOTE   = daemon.env("ICLOUD_DROP_DIR",
                      "Library/Mobile Documents/com~apple~CloudDocs/Blue Fenix Productions/drop")
SETTLE_S = int(daemon.env("ICLOUD_SETTLE_S", "30"))
PULL_DIR = Path(daemon.env("ICLOUD_PULL_DIR", str(Path.home() / "drop/icloud-pull")))

SSH = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", HOST]


def sh(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def dq(s: str) -> str:
    """Escape a string for embedding inside a double-quoted remote shell arg."""
    return (s.replace("\\", "\\\\").replace('"', '\\"')
             .replace("$", "\\$").replace("`", "\\`"))


def rsh(script: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    """Run a shell snippet on Hinata with $D set to the drop dir."""
    return sh(SSH + [f'D="$HOME/{REMOTE}"; {script}'], timeout=timeout)


def reply(message: str, attachments: list[str] | None = None) -> None:
    """Confirmation into the 📥 Drop group via the bridge's loopback API."""
    if not (daemon.DROP_GROUP and daemon.HTTP_PORT):
        return
    try:
        body: dict = {"groupId": daemon.DROP_GROUP, "message": message}
        if attachments:
            body["attachments"] = list(attachments)
        req = urllib.request.Request(
            f"http://{daemon.HTTP_HOST}:{daemon.HTTP_PORT}/send",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=30)
    except Exception as e:
        daemon.log.warning("icloud door: reply failed: %s", e)


def main() -> int:
    r = rsh('mkdir -p "$D/ingested"')
    if r.returncode != 0:
        daemon.log.info("icloud door: hinata unreachable (%s)", (r.stderr or "").strip()[:120])
        return 0  # transient; the timer will retry

    # Nudge dataless placeholders to materialize; they get picked up next cycle.
    rsh('cd "$D" 2>/dev/null && for p in .*.icloud; do [ -e "$p" ] || continue; '
        'o="${p#.}"; o="${o%.icloud}"; brctl download "$D/$o" 2>/dev/null; done')

    # List settled, materialized, top-level files: "mtime size name".
    lst = rsh('cd "$D" && find . -maxdepth 1 -type f ! -name ".*" '
              '-exec stat -f "%m %z %N" {} \\; 2>/dev/null')
    now = time.time()
    candidates: list[tuple[str, int]] = []
    for line in (lst.stdout or "").splitlines():
        parts = line.strip().split(" ", 2)
        if len(parts) != 3:
            continue
        mtime, size, name = parts
        name = name[2:] if name.startswith("./") else name
        try:
            if now - int(mtime) < SETTLE_S:
                continue  # still settling / mid-sync
            candidates.append((name, int(size)))
        except ValueError:
            continue

    if not candidates:
        return 0

    PULL_DIR.mkdir(parents=True, exist_ok=True)
    keep: list[tuple[Path, str]] = []
    rejected: list[str] = []
    processed: list[str] = []

    for name, size in candidates:
        safe = daemon._safe_name(name)
        if Path(safe).suffix.lower() not in daemon.DROP_EXT_ALLOW:
            rejected.append(f"{safe} (extension not allowed)")
            processed.append(name)
            continue
        local = PULL_DIR / safe
        qname = dq(name)
        with local.open("wb") as f:
            p = subprocess.run(SSH + [f'cat "$HOME/{dq(REMOTE)}/{qname}"'],
                               stdout=f, stderr=subprocess.PIPE, timeout=300)
        if p.returncode != 0 or local.stat().st_size != size:
            rejected.append(f"{safe} (pull failed or size mismatch)")
            daemon.log.error("icloud door: pull failed for %s", name)
            local.unlink(missing_ok=True)
            continue
        keep.append((local, safe))
        processed.append(name)

    if keep or rejected:
        daemon.log.info("icloud door: %d file(s) kept, %d rejected", len(keep), len(rejected))
        daemon.file_drop(keep, rejected, "", "icloud", "icloud", reply)

    # Move handled originals out of the inbox (custody now lives in the repo).
    for name in processed:
        qname = dq(name)
        mv = rsh(f'mv "$D/{qname}" "$D/ingested/{qname}"')
        if mv.returncode != 0:
            daemon.log.error("icloud door: failed to move %s to ingested/", name)

    for f in PULL_DIR.iterdir():
        f.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

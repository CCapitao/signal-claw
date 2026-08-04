#!/usr/bin/env python3
"""
linking: same-UUID auto-linking for the Receipts Drop pipeline.

A note that references `Attachments/<UUID>.png` and a payload file that
later lands as `<UUID>.jpg` are the same receipt split across two drops
(e.g. a Signal note landing before its attachment, or vice versa). This
module tracks UUID stems as pending "ref" or "file" entries with a TTL
and matches them across drops so the daemon can cross-comment both
GitHub issues instead of a human doing it by hand.

Provenance: receipts #37 / #38 (see tests/test_linking.py).
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path

log = logging.getLogger("signal-claw.linking")

# 8-4-4-4-12 hex UUID stem, case-insensitive.
_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
_UUID_STEM_RE = re.compile(rf"^{_UUID}$")
_UUID_REF_RE = re.compile(rf"({_UUID})\.[A-Za-z0-9]+")


def extract_ref_stems(text: str | None) -> set[str]:
    """UUID-format stems referenced in note text, upper-normalized.

    Matches both markdown targets (`![x](Attachments/<UUID>.png)`) and
    bare mentions (`the file <UUID>.heic never made it`) — anything
    where a UUID is immediately followed by a file extension. Non-UUID
    filenames never match.
    """
    if not text:
        return set()
    return {m.group(1).upper() for m in _UUID_REF_RE.finditer(text)}


def payload_stems(names: list[str]) -> set[str]:
    """UUID stems of payload filenames (any extension), upper-normalized."""
    stems: set[str] = set()
    for name in names:
        stem = Path(name).stem
        if _UUID_STEM_RE.match(stem):
            stems.add(stem.upper())
    return stems


def update_and_match(
    state: dict,
    *,
    drop_id: str,
    issue_url: str,
    ref_stems: set[str],
    file_stems: set[str],
    now: float,
    ttl_s: int,
) -> tuple[dict, list[dict]]:
    """Prune expired entries, match this drop's stems against pending
    entries, and pend the rest. Returns (new_state, matches) — never
    mutates the input state.

    Stems referenced AND present within the same drop are self-satisfied
    (no pend, no match). Matching is extension-agnostic — only stems are
    stored, so a `.png` ref matches a `.jpg` file. A match consumes the
    pending entry; `this_role` is the CURRENT drop's role.
    """
    new_state = {stem: entry for stem, entry in state.items() if now - entry["ts"] <= ttl_s}
    matches: list[dict] = []

    self_satisfied = ref_stems & file_stems
    effective_ref = ref_stems - self_satisfied
    effective_file = file_stems - self_satisfied

    def _process(stems: set[str], role: str, waiting_role: str) -> None:
        for stem in stems:
            pending = new_state.get(stem)
            if pending is not None and pending["role"] == waiting_role:
                matches.append({
                    "stem": stem,
                    "other_issue_url": pending["issue_url"],
                    "other_drop_id": pending["drop_id"],
                    "this_role": role,
                })
                del new_state[stem]
            else:
                new_state[stem] = {
                    "role": role,
                    "issue_url": issue_url,
                    "drop_id": drop_id,
                    "ts": now,
                }

    _process(effective_ref, "ref", "file")
    _process(effective_file, "file", "ref")

    return new_state, matches


def issue_ref(url: str) -> str:
    """Same-repo `#N` shorthand for a GitHub issue URL (rename-proof in
    comments); falls back to the URL itself if it doesn't parse."""
    m = re.search(r"/issues/(\d+)/?$", (url or "").strip())
    return f"#{m.group(1)}" if m else url


def comment_bodies(match: dict, *, this_drop_id: str, this_issue_url: str,
                   this_file_names: list[str]) -> tuple[str, str]:
    """Render the 🔗 cross-comments for one match: (on_this_issue, on_other_issue).
    Pattern provenance: the hand-written comments on receipts #37/#38."""
    stem = match["stem"]
    other = issue_ref(match["other_issue_url"])
    other_id = match["other_drop_id"]
    this = issue_ref(this_issue_url)
    trailer = "_(auto-linked by signal-claw, same UUID split across two drops)_"
    if match["this_role"] == "file":
        fname = next((n for n in this_file_names if Path(n).stem.upper() == stem), stem)
        on_this = (f"🔗 Linked: this file (`{fname}`) is the attachment referenced "
                   f"by the note in {other} ({other_id}). {trailer}")
        on_other = (f"🔗 Linked: the file `{stem}` referenced in this note arrived "
                    f"as the payload of {this} ({this_drop_id}), shipped as `{fname}`. {trailer}")
    else:  # this_role == "ref": current drop is the note; the file landed earlier
        on_this = (f"🔗 Linked: the file `{stem}` referenced in this note is the "
                   f"payload of {other} ({other_id}). {trailer}")
        on_other = (f"🔗 Linked: this file is the attachment referenced by the "
                    f"note in {this} ({this_drop_id}). {trailer}")
    return on_this, on_other


def load_state(path: str | Path) -> dict:
    """Read the pending-links state file. Missing or corrupt -> {}."""
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def save_state(path: str | Path, state: dict) -> None:
    """Write the pending-links state file, atomically (temp + rename)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(path)

"""Red spec for same-UUID auto-linking (the receipts split-drop wrinkle).

Provenance: receipts #37 (note referencing Attachments/<UUID>.png) and #38
(the same UUID arriving as a .jpg payload 27s later) had to be linked by hand.
The daemon should do it: track UUID stems as pending, match across drops,
cross-comment both issues.

Run: python3 -m unittest discover -s tests  (stdlib only — no pytest dep)
"""
import json
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import linking  # the module under test — does not exist yet (red)

UUID = "0D4E8D3B-32EC-49A9-896D-3EED7135DCD0"
NOTE_37 = (
    "# 2026.08.02 — Sunday, August 2nd   \n"
    "\U0001F948   \n"
    "Long live the Queen \U0001F478\U0001F3FB  \n"
    f"![Sun Aug](Attachments/{UUID}.png)"
)

TTL = 48 * 3600


class ExtractRefStems(unittest.TestCase):
    def test_finds_uuid_ref_in_real_note(self):
        self.assertEqual(linking.extract_ref_stems(NOTE_37), {UUID})

    def test_ignores_non_uuid_filenames(self):
        text = "see ![x](Attachments/photo.png) and [y](docs/readme.md)"
        self.assertEqual(linking.extract_ref_stems(text), set())

    def test_normalizes_case_to_upper(self):
        text = f"![x](Attachments/{UUID.lower()}.png)"
        self.assertEqual(linking.extract_ref_stems(text), {UUID})

    def test_bare_mention_without_markdown_counts(self):
        text = f"the file {UUID}.heic never made it"
        self.assertEqual(linking.extract_ref_stems(text), {UUID})

    def test_empty_and_none_safe(self):
        self.assertEqual(linking.extract_ref_stems(""), set())


class PayloadStems(unittest.TestCase):
    def test_extracts_uuid_stem_any_extension(self):
        self.assertEqual(linking.payload_stems([f"{UUID}.jpg"]), {UUID})

    def test_lowercase_filename_normalized(self):
        self.assertEqual(linking.payload_stems([f"{UUID.lower()}.png"]), {UUID})

    def test_non_uuid_names_ignored(self):
        self.assertEqual(linking.payload_stems(["note.md", "IMG_0852.jpg"]), set())


class UpdateAndMatch(unittest.TestCase):
    """State transitions. Entries: {stem: {role, issue_url, drop_id, ts}}.
    A ref pends until a file with the same stem arrives, and vice versa.
    Matching is extension-agnostic (png ref matches jpg file). Matched
    entries are consumed."""

    def test_ref_first_then_file_matches_and_consumes(self):
        state, matches = linking.update_and_match(
            {}, drop_id="R-A", issue_url="url-A",
            ref_stems={UUID}, file_stems=set(), now=1000.0, ttl_s=TTL)
        self.assertEqual(matches, [])
        self.assertIn(UUID, state)

        state, matches = linking.update_and_match(
            state, drop_id="R-B", issue_url="url-B",
            ref_stems=set(), file_stems={UUID}, now=1027.0, ttl_s=TTL)
        self.assertEqual(len(matches), 1)
        m = matches[0]
        self.assertEqual(m["stem"], UUID)
        self.assertEqual(m["other_issue_url"], "url-A")
        self.assertEqual(m["other_drop_id"], "R-A")
        self.assertEqual(m["this_role"], "file")
        self.assertNotIn(UUID, state)  # consumed

    def test_file_first_then_ref_matches(self):
        state, _ = linking.update_and_match(
            {}, drop_id="R-B", issue_url="url-B",
            ref_stems=set(), file_stems={UUID}, now=1000.0, ttl_s=TTL)
        state, matches = linking.update_and_match(
            state, drop_id="R-A", issue_url="url-A",
            ref_stems={UUID}, file_stems=set(), now=1010.0, ttl_s=TTL)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["this_role"], "ref")
        self.assertEqual(matches[0]["other_issue_url"], "url-B")

    def test_expired_pending_never_matches(self):
        state, _ = linking.update_and_match(
            {}, drop_id="R-A", issue_url="url-A",
            ref_stems={UUID}, file_stems=set(), now=1000.0, ttl_s=TTL)
        state, matches = linking.update_and_match(
            state, drop_id="R-B", issue_url="url-B",
            ref_stems=set(), file_stems={UUID}, now=1000.0 + TTL + 1, ttl_s=TTL)
        self.assertEqual(matches, [])
        # the new file pends fresh; the stale ref is gone
        self.assertEqual(state[UUID]["drop_id"], "R-B")

    def test_same_drop_ref_and_file_no_self_link(self):
        state, matches = linking.update_and_match(
            {}, drop_id="R-C", issue_url="url-C",
            ref_stems={UUID}, file_stems={UUID}, now=1000.0, ttl_s=TTL)
        self.assertEqual(matches, [])
        self.assertNotIn(UUID, state)  # self-satisfied, nothing pends


class StateIO(unittest.TestCase):
    def test_missing_file_loads_empty(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(linking.load_state(Path(d) / "nope.json"), {})

    def test_corrupt_file_loads_empty(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "pending-links.json"
            p.write_text("{not json")
            self.assertEqual(linking.load_state(p), {})

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "pending-links.json"
            state = {UUID: {"role": "ref", "issue_url": "u", "drop_id": "R-A", "ts": 1.0}}
            linking.save_state(p, state)
            self.assertEqual(linking.load_state(p), state)


class IssueRef(unittest.TestCase):
    def test_url_becomes_same_repo_shorthand(self):
        self.assertEqual(
            linking.issue_ref("https://github.com/CCapitao/receipts/issues/38"), "#38")

    def test_unparseable_url_passes_through(self):
        self.assertEqual(linking.issue_ref("issue-failed"), "issue-failed")


class CommentBodies(unittest.TestCase):
    """Both directions of the 🔗 cross-comment, mirroring the hand-written
    pattern on receipts #37/#38."""

    def _match(self, role: str) -> dict:
        return {"stem": UUID, "this_role": role,
                "other_issue_url": "https://github.com/CCapitao/receipts/issues/37",
                "other_drop_id": "R-20260802-010"}

    def test_file_role_cites_actual_filename_both_ways(self):
        on_this, on_other = linking.comment_bodies(
            self._match("file"), this_drop_id="R-20260802-011",
            this_issue_url="https://github.com/CCapitao/receipts/issues/38",
            this_file_names=[f"{UUID}.jpg", "note.md"])
        self.assertIn(f"`{UUID}.jpg`", on_this)
        self.assertIn("#37", on_this)
        self.assertIn("R-20260802-010", on_this)
        self.assertIn("#38", on_other)
        self.assertIn(f"shipped as `{UUID}.jpg`", on_other)

    def test_ref_role_points_at_the_file_issue(self):
        on_this, on_other = linking.comment_bodies(
            self._match("ref"), this_drop_id="R-20260802-011",
            this_issue_url="https://github.com/CCapitao/receipts/issues/38",
            this_file_names=["note.md"])
        self.assertIn(f"`{UUID}`", on_this)
        self.assertIn("#37", on_this)
        self.assertIn("#38", on_other)

    def test_both_bodies_carry_the_link_marker(self):
        for role in ("file", "ref"):
            for body in linking.comment_bodies(
                    self._match(role), this_drop_id="R-X",
                    this_issue_url="https://github.com/CCapitao/receipts/issues/40",
                    this_file_names=[]):
                self.assertTrue(body.startswith("🔗 Linked:"))
                self.assertIn("auto-linked by signal-claw", body)


if __name__ == "__main__":
    unittest.main()

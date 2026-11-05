"""Recover content-verified tier copies after CIFS client inode identity changes."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pytest

from viseron.components.storage.pending_journal import (
    prepare,
    publish_or_verify,
    release_indexed_source,
)
from viseron.components.storage.source_move_intent import (
    prepare_source_intent,
    set_source_digest,
)

RUN = "20261031T143215.482000Z-a7c93e12ab34"


class PublicationInodeRecoveryTests(unittest.TestCase):
    """Exercise publication replay and source release with stale saved inodes."""

    def setUp(self):
        """Create a real source hold and copied-byte publication receipt."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "source" / "camera" / RUN / "1.m4s"
        self.target = self.root / "destination" / "camera" / RUN / "1.m4s"
        self.source.parent.mkdir(parents=True)
        self.target.parent.mkdir(parents=True)
        (self.source.parent / "init.mp4").write_bytes(b"init")
        (self.target.parent / "init.mp4").write_bytes(b"init")
        self.source.write_bytes(b"video")
        self.private = self.target.parent / ".viseron-tier-move-test"
        self.private.write_bytes(self.source.read_bytes())
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.intent = prepare_source_intent(
            str(self.source), str(self.target), self.source.stat(), 42
        )
        set_source_digest(
            self.intent, digest, hashlib.sha256(b"init").hexdigest()
        )
        self.marker = prepare(
            self.target.parent,
            self.target.name,
            {"file_key": 42},
            temp_path=self.private,
            source_intent=self.intent,
            copied_digest=digest,
        )

    def stale_saved_inode(self, **updates):
        """Model the observed receipt: only its persisted inode differs."""
        record = json.loads(self.marker.read_text())
        record["identity"][1] += 1
        record.update(updates)
        self.marker.write_text(json.dumps(record))
        return record

    def release(self, committed=True):
        """Use the production source handover after a simulated DB acknowledgement."""
        return release_indexed_source(
            self.marker,
            [str(self.source.parent.parent)],
            lambda _path: True,
            lambda _record: committed,
            str(self.root / "locks"),
        )

    def assert_held(self):
        """A rejected acknowledgement retains the source and both receipts."""
        assert self.source.read_bytes() == b"video"
        assert Path(self.intent).exists()
        assert self.marker.exists()

    def test_private_copy_can_publish_after_saved_inode_changes(self):
        """A matching digest permits the original exclusive publication operation."""
        saved = self.stale_saved_inode()
        record = publish_or_verify(self.marker)
        assert self.target.read_bytes() == b"video"
        assert record["identity"][1] == self.target.stat().st_ino
        assert record["identity"] != saved["identity"]
        self.assert_held()

    def test_published_copy_replays_without_rewriting_durable_receipt(self):
        """A replay returns current verified identity, preserving original evidence."""
        publish_or_verify(self.marker)
        self.stale_saved_inode()
        original = self.marker.read_bytes()
        record = publish_or_verify(self.marker)
        assert record["identity"][1] == self.target.stat().st_ino
        assert self.marker.read_bytes() == original
        self.assert_held()

    def test_verified_copy_releases_only_after_index_acknowledgement(self):
        """Stale destination inode does not prevent verified source handover."""
        publish_or_verify(self.marker)
        self.stale_saved_inode()
        assert self.release(committed=False) == "uncommitted"
        self.assert_held()
        assert self.release() == "released"
        assert not self.source.exists()
        assert not Path(self.intent).exists()
        assert not self.marker.exists()
        assert self.target.read_bytes() == b"video"

    def test_corrupt_copy_does_not_release_source(self):
        """Same-size corruption must not be accepted as inode drift."""
        publish_or_verify(self.marker)
        self.target.write_bytes(b"wrong")
        self.stale_saved_inode()
        assert self.release() == "retry"
        self.assert_held()

    def test_corrupt_private_copy_does_not_publish(self):
        """The digest fallback cannot rename corrupted private bytes."""
        self.private.write_bytes(b"wrong")
        self.stale_saved_inode()
        with pytest.raises(FileExistsError):
            publish_or_verify(self.marker)
        assert not self.target.exists()
        self.assert_held()

    def test_missing_digest_does_not_allow_inode_recovery(self):
        """Older receipts without byte proof retain their strict identity guard."""
        publish_or_verify(self.marker)
        self.stale_saved_inode(copied_digest=None)
        assert self.release() == "retry"
        self.assert_held()

    def test_digest_without_source_receipt_does_not_allow_recovery(self):
        """Only source-backed tier copies may use the checksum fallback."""
        publish_or_verify(self.marker)
        self.stale_saved_inode(source_intent=None)
        with pytest.raises(ValueError):
            publish_or_verify(self.marker)
        self.assert_held()

    def test_matching_digest_does_not_allow_wrong_saved_size(self):
        """The checksum fallback must also match the receipt's byte count."""
        publish_or_verify(self.marker)
        record = self.stale_saved_inode()
        record["identity"][2] += 1
        self.marker.write_text(json.dumps(record))
        assert self.release() == "retry"
        self.assert_held()

    def test_private_and_public_files_remain_a_collision(self):
        """Matching bytes never authorize overwriting a competing destination."""
        self.target.write_bytes(b"video")
        self.stale_saved_inode()
        with pytest.raises(FileExistsError):
            publish_or_verify(self.marker)
        assert self.private.exists()
        self.assert_held()

    def test_symlink_is_not_adopted(self):
        """A checksum-matching symlink cannot satisfy publication verification."""
        publish_or_verify(self.marker)
        self.target.unlink()
        self.target.symlink_to(self.source)
        self.stale_saved_inode()
        assert self.release() == "retry"
        self.assert_held()

    def test_corrupt_init_still_prevents_source_release(self):
        """Recovering segment identity does not bypass run-init verification."""
        publish_or_verify(self.marker)
        self.stale_saved_inode()
        (self.target.parent / "init.mp4").write_bytes(b"nope")
        assert self.release() == "destination-unverified"
        self.assert_held()

"""Exercise the registered event clip route through Tornado's HTTP router."""

import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tornado.web
from tornado.testing import AsyncHTTPTestCase

from viseron.components.storage.tier_handler import EventClipTierHandler
from viseron.components.webserver.tiered_file_handler import TieredFileHandler


class TestEventClipRoute(AsyncHTTPTestCase):
    """A normal event clip can be fetched from its configured tier."""

    def get_app(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "tier.v1+test"
        self.clip = self.root / "event_clips" / "camera1" / "2026-11-01" / "recording-7.mp4"
        self.clip.parent.mkdir(parents=True)
        self.clip.write_bytes(b"clip bytes")
        app = tornado.web.Application()
        tier = object.__new__(EventClipTierHandler)
        tier._tier = {"path": str(self.root)}
        tier._camera = SimpleNamespace(identifier="camera1", extension="mp4")
        tier._vis = Mock()
        tier._webserver = SimpleNamespace(application=app)
        tier._category, tier._subcategory = "recorder", "event_clips"
        tier._logger = Mock()
        tier.initialize()
        return app

    def test_normal_clip_url_is_registered(self):
        """The actual route serves a date-nested .mp4, not a camera suffix."""
        def initialize(handler, path, **_kwargs):
            tornado.web.StaticFileHandler.initialize(handler, path)
            handler._failed = True
            handler._redirect = False

        async def prepare(_handler):
            return None

        with patch.object(TieredFileHandler, "initialize", initialize), patch.object(
            TieredFileHandler, "prepare", prepare
        ):
            response = self.fetch(f"/files{self.clip}")
        assert response.code == 200
        assert response.body == b"clip bytes"

    def test_other_extension_does_not_match_clip_route(self):
        """Only the camera's configured recording format matches the route."""
        unrelated = self.clip.with_suffix(".txt")
        unrelated.write_text("private")
        response = self.fetch(f"/files{unrelated}")
        assert response.code == 404

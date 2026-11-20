"""Static file handler that serves files by their persistent file key."""

from __future__ import annotations

import errno
import os
from http import HTTPStatus
from typing import TYPE_CHECKING

import tornado.web
from sqlalchemy import or_, select

from viseron.components.storage.files import resolve_file_id
from viseron.components.storage.models import FileKeyAliases, Files
from viseron.components.storage.util import is_transient_filesystem_error
from viseron.components.webserver.static_file_handler import (
    AccessTokenStaticFileHandler,
)

if TYPE_CHECKING:
    from viseron import Viseron

# Larger keys make PostgreSQL compare the BIGINT column as numeric, which cannot use
# the index and scans the whole table
MAX_FILE_KEY = 2**63 - 1


class FileKeyHandler(AccessTokenStaticFileHandler):
    """Serve a file by its file_key, which does not change when the file moves tier.

    Served at /file/<camera_identifier>/<hex file_key>. The camera is part of the
    URL so that access is validated before the database is queried.
    """

    # pylint: disable-next=arguments-differ
    def initialize(self, vis: Viseron) -> None:  # type: ignore[override]
        """Initialize the handler."""
        super().initialize("/", vis, camera_identifier="", failed=True)
        self._encoded_file_key = ""

    async def prepare(self) -> None:
        """Validate access to the camera in the URL."""
        self._camera_identifier, self._encoded_file_key = self.path_args[0].split("/")
        await super().prepare()

    def _resolve(self) -> str | None:
        """Resolve an authorized canonical or migrated key to a configured location."""
        # Convert the hexadecimal file key from the URL to an integer.
        file_key = int(self._encoded_file_key, 16)
        if file_key > MAX_FILE_KEY:
            return None

        with self._get_session() as session:
            stmt = select(Files.id).where(
                Files.camera_identifier == self._camera_identifier,
                or_(
                    Files.file_key == file_key,
                    Files.id.in_(
                        select(FileKeyAliases.file_id).where(
                            FileKeyAliases.file_key == file_key
                        )
                    ),
                ),
            )
            file_id = session.execute(stmt).scalars().first()
        if file_id is None:
            return None
        resolved = resolve_file_id(self._get_session, self._storage, file_id)
        return resolved.path if resolved else None

    async def get(
        self,
        path: str,  # noqa: ARG002
        include_body: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        """Serve the file the key resolves to."""
        try:
            resolved_path = await self.run_in_executor(self._resolve)
            if resolved_path is None:
                raise tornado.web.HTTPError(HTTPStatus.NOT_FOUND)
            self.root = os.path.dirname(resolved_path)
            await super().get(os.path.basename(resolved_path), include_body)
        except OSError as error:
            unavailable = (
                isinstance(error, FileNotFoundError)
                or error.errno in {errno.EINVAL, errno.ENOTDIR}
                or is_transient_filesystem_error(error)
            )
            if unavailable and not self._headers_written:
                raise tornado.web.HTTPError(HTTPStatus.NOT_FOUND) from error
            raise

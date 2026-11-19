"""Verify the packaged application and database can shut down cleanly."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from . import _helpers as helpers
from .conftest import _MINIMAL_CONFIG, READY_LOG_PATTERN, REQUIRED_STORAGE_DIRS

if TYPE_CHECKING:
    import docker


def test_graceful_container_shutdown(
    docker_client: docker.DockerClient,
    image: str,
    boot_timeout: float,
    docker_platform: str,
) -> None:
    """Allow supervisor cleanup to finish instead of killing the database."""
    container = docker_client.containers.create(
        image,
        platform=docker_platform or None,
        name=helpers.unique_container_name("viseron-shutdown"),
        environment={"PUID": str(os.getuid()), "PGID": str(os.getgid()), "TZ": "UTC"},
    )
    try:
        helpers.put_abs(container, "/config/config.yaml", _MINIMAL_CONFIG.read_bytes())
        helpers.create_directories(container, *REQUIRED_STORAGE_DIRS)
        container.start()
        helpers.wait_for_log(container, READY_LOG_PATTERN, timeout=boot_timeout)
        # The image allows 30 seconds for supervisor cleanup after the
        # application and PostgreSQL finish. Docker's 10-second default cuts
        # that short even when the application exits promptly.
        container.stop(timeout=90)
        container.reload()
        logs = container.logs().decode("utf-8", errors="replace")
        state = container.attrs["State"]
        assert state["ExitCode"] == 0, f"unclean container exit: {state}\n{logs}"
        assert not state["OOMKilled"]
        assert "Shutdown complete in" in logs, logs
        assert "database system is shut down" in logs, logs
    finally:
        container.remove(force=True)

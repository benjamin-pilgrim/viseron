"""Verify the rootfs s6-overlay cont-init.d scripts populated container state."""

from __future__ import annotations

import os
import shlex
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import testinfra

ENV_FILES = (
    "VISERON_CONFIG_DIR",
    "VISERON_OPENCL_SUPPORTED",
    "VISERON_VAAPI_SUPPORTED",
    "VISERON_CUDA_SUPPORTED",
    "XDG_RUNTIME_DIR",
    "XDG_CURRENT_DESKTOP",
)


@pytest.mark.parametrize("env_var", ENV_FILES)
def test_environment_files_populated(host: testinfra.host.Host, env_var: str) -> None:
    """``/var/run/environment/<var>`` should exist and be non-empty."""
    path = f"/var/run/environment/{env_var}"
    file_ = host.file(path)
    assert file_.exists, f"{path} was not created by cont-init.d"
    content = file_.content_string.strip()
    assert content, f"{path} is empty"


def test_viseron_config_dir_value(host: testinfra.host.Host) -> None:
    """``VISERON_CONFIG_DIR`` should be ``/config`` for the smoke run."""
    file_ = host.file("/var/run/environment/VISERON_CONFIG_DIR")
    assert file_.exists
    assert file_.content_string.strip() == "/config"


@pytest.mark.parametrize(
    "env_var",
    [
        "VISERON_OPENCL_SUPPORTED",
        "VISERON_VAAPI_SUPPORTED",
        "VISERON_CUDA_SUPPORTED",
    ],
)
def test_capability_env_files_are_boolean(
    host: testinfra.host.Host, env_var: str
) -> None:
    """Capability detection scripts must write ``true`` or ``false``."""
    file_ = host.file(f"/var/run/environment/{env_var}")
    assert file_.exists
    value = file_.content_string.strip()
    assert value in {"true", "false"}, f"{env_var}={value!r} is not a boolean string"


def test_user_abc_present(host: testinfra.host.Host) -> None:
    """The runtime user must have the requested uid and usable render devices."""
    user = host.user("abc")
    assert user.exists, "user 'abc' does not exist"
    assert user.uid == os.getuid(), (
        f"user 'abc' has uid {user.uid}, expected {os.getuid()}"
    )
    if host.file("/dev/dri").exists:
        devices = host.run("find /dev/dri -name 'renderD*' -type c")
        assert devices.rc == 0, devices.stderr
        for device in devices.stdout.splitlines():
            # Device groups vary between hosts and rootless UID/GID mappings.
            # Check the permission the recorder needs, rather than a group name.
            path = shlex.quote(device)
            access = host.run(
                "su - abc -c %s", shlex.quote(f"test -r {path} && test -w {path}")
            )
            assert access.rc == 0, f"abc cannot read/write {device}: {access.stderr}"


def test_ffmpeg_wrapper_resolves(host: testinfra.host.Host) -> None:
    """``which ffmpeg`` (as the abc user) should resolve to the wrapper."""
    cmd = host.run("su - abc -c 'which ffmpeg'")
    assert cmd.rc == 0, cmd.stderr
    assert cmd.stdout.strip() == "/home/abc/bin/ffmpeg"


def test_gstreamer_inspect_runs(host: testinfra.host.Host) -> None:
    """``gst-inspect-1.0 --version`` should run and print a version."""
    cmd = host.run("gst-inspect-1.0 --version")
    output = cmd.stdout + cmd.stderr
    assert cmd.rc == 0, output
    assert "gst-inspect" in output.lower() or "gstreamer" in output.lower()


def test_boot_logs_confirm_chown_enabled_by_default(boot_logs: str) -> None:
    """Boot logs should confirm volume path chown is enabled by default."""
    assert "VISERON_DISABLE_CHOWN is not enabled" in boot_logs, (
        "Expected a log line confirming volume path chown is enabled by default. "
        f"First 2KB:\n{boot_logs[:2048]}"
    )

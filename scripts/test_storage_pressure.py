#!/usr/bin/env python3
"""Disposable camera/RAM/CIFS/browser acceptance. Stops only the named test Samba."""
# Assertions are the acceptance verdict; constants are deliberately small fixtures.
# ruff: noqa: S101, PLR2004

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request


def command(*args, timeout=30, check=True):
    result = subprocess.run(
        args, check=False, text=True, capture_output=True, timeout=timeout
    )
    if check and result.returncode:
        raise RuntimeError(
            f"{args[0]} failed: {result.stderr[-2000:]} {result.stdout[-2000:]}"
        )
    return result.stdout.strip()


def wait_for(description, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                print(f"PASS: {description}", flush=True)
                return result
        except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
            last_error = error
        time.sleep(2)
    raise RuntimeError(f"Timed out: {description}; last error: {last_error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--image", required=True)
    parser.add_argument("--frontend", required=True, type=Path)
    parser.add_argument("--cifs-mount", required=True, type=Path)
    parser.add_argument(
        "--test-samba",
        required=True,
        help="Dedicated disposable server; WILL be stopped",
    )
    parser.add_argument("--http-port", type=int, default=29775)
    parser.add_argument("--rtsp-port", type=int, default=29774)
    parser.add_argument(
        "--policy",
        choices=("ordinary_first", "preserve_events"),
        default="ordinary_first",
    )
    args = parser.parse_args()
    args.repo = args.repo.resolve()
    assert (
        command("findmnt", "-n", "-o", "FSTYPE", "-T", str(args.cifs_mount)) == "cifs"
    ), "Real CIFS mount required"
    assert (args.frontend / "index.html").is_file()
    root = Path(tempfile.mkdtemp(prefix="viseron-pressure-e2e-"))
    name = root.name
    remote = args.cifs_mount / name
    remote.mkdir()
    remote.chmod(0o777)
    (remote / ".viseron-test-tier").touch()
    for path in [
        root,
        *[root / part for part in ("config", "snapshots", "recordings")],
    ]:
        path.mkdir(exist_ok=True)
        path.chmod(0o777)
    (root / "config/config.yaml").write_text(f"""logger:
  default_level: info
storage:
  tier_markers:
    /cifs: .viseron-test-tier
  recorder:
    tiers:
      - path: /buffer
        check_interval:
          seconds: 3
        retention:
          max_size: 2MiB
        on_full:
          policy: {args.policy}
      - path: /cifs
        poll: true
        retention:
          max_age: 1d
        on_full:
          policy: preserve_events
ffmpeg:
  camera:
    vertical_camera:
      name: Vertical Camera
      host: 127.0.0.1
      port: 8554
      path: /test
      recorder:
        continuous_recording: true
""")
    pod, app, mtx = name + "-pod", name + "-app", name + "-mtx"
    origin = f"http://127.0.0.1:{args.http_port}"
    publisher = None
    evidence = {
        "commit": command("git", "-C", str(args.repo), "rev-parse", "HEAD"),
        "policy": args.policy,
        "root": str(root),
        "remote": str(remote),
    }
    print(f"Artifacts: {root}; remote namespace: {remote}", flush=True)

    def sql(query):
        return command(
            "podman",
            "exec",
            app,
            "psql",
            "-U",
            "postgres",
            "-d",
            "viseron",
            "-Atc",
            query,
            timeout=8,
        )

    def logs():
        result = subprocess.run(
            ["podman", "logs", app],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
        return result.stdout + result.stderr

    def local_rows():
        return sql(
            "select path from files where tier_path='/buffer/' "
            "and subcategory='segments' order by orig_ctime"
        ).splitlines()

    def digest(path):
        return command("podman", "exec", app, "sha256sum", path).split()[0]

    def api(action):
        request = urllib.request.Request(
            origin + "/api/v1/camera/vertical_camera/manual_recording",
            data=json.dumps({"action": action}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 200
            return json.load(response)

    def public_init(segment):
        run = Path(segment).parent.name
        init_path = str(Path(segment).parent / "init.mp4")
        with urllib.request.urlopen(
            origin + "/run-init/vertical_camera/" + run, timeout=10
        ) as response:
            actual = hashlib.sha256(response.read()).hexdigest()
        assert actual == digest(init_path), "Public run-init returned wrong bytes"
        return True

    def browser(label):
        env = dict(
            os.environ,
            VISERON_TEST_BROWSER_DIAGNOSTICS=str(root / ("browser-" + label)),
            VISERON_TEST_CHROMIUM=os.environ.get(
                "VISERON_TEST_CHROMIUM", "/usr/bin/chromium"
            ),
        )
        result = subprocess.run(
            [
                "node",
                str(args.repo / "scripts/test_recording_browser.cjs"),
                str(args.repo),
                origin,
            ],
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=150,
        )
        (root / f"browser-{label}.log").write_text(result.stdout + result.stderr)
        assert result.returncode == 0, result.stdout + result.stderr
        print(result.stdout.strip(), flush=True)

    def promoted(destination, expected_hash):
        query = "select count(*) from files where path='" + destination + "'"  # noqa: S608
        if sql(query) != "1":
            return False
        assert digest(destination) == expected_hash
        command(
            "podman",
            "exec",
            app,
            "test",
            "-f",
            str(Path(destination).parent / "init.mp4"),
        )
        return True

    try:
        command(
            "podman",
            "pod",
            "create",
            "--name",
            pod,
            "-p",
            f"127.0.0.1:{args.http_port}:8888/tcp",
            "-p",
            f"127.0.0.1:{args.rtsp_port}:8554/tcp",
        )
        command(
            "podman",
            "run",
            "-d",
            "--pod",
            pod,
            "--name",
            mtx,
            "-e",
            "MTX_HLS=no",
            "docker.io/bluenviron/mediamtx:latest",
        )
        time.sleep(2)
        publisher_log = (root / "publisher.log").open("w")
        publisher = subprocess.Popen(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-re",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=320x240:rate=15",
                "-re",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=48000",
                "-c:a",
                "aac",
                "-b:a",
                "64k",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-g",
                "30",
                "-pix_fmt",
                "yuv420p",
                "-f",
                "rtsp",
                "-rtsp_transport",
                "tcp",
                f"rtsp://127.0.0.1:{args.rtsp_port}/test",
            ],
            stdout=publisher_log,
            stderr=subprocess.STDOUT,
        )
        command(
            "podman",
            "run",
            "-d",
            "--pod",
            pod,
            "--name",
            app,
            "-e",
            "S6_KILL_GRACETIME=3000",
            "--tmpfs",
            "/buffer:size=16m,mode=1777",
            "-v",
            f"{args.repo}/viseron:/src/viseron:ro",
            "-v",
            f"{args.repo}/rootfs/usr/local/nginx/conf/nginx.conf:"
            "/usr/local/nginx/conf/nginx.conf:ro",
            "-v",
            f"{args.frontend}:/src/viseron/frontend:ro",
            "-v",
            f"{remote}:/cifs",
            "-v",
            f"{root}/config:/config",
            "-v",
            f"{root}/snapshots:/snapshots",
            "-v",
            f"{root}/recordings:/recordings",
            args.image,
        )
        wait_for(
            "synthetic camera produces indexed RAM fragments",
            lambda: len(local_rows()) >= 3,
        )
        if args.policy == "preserve_events":
            print("Manual event start:", api("start"), flush=True)
            wait_for(
                "real manual event is active",
                lambda: (
                    sql("select count(*) from recordings where end_time is null") == "1"
                ),
            )
        command("podman", "stop", "-t", "3", args.test_samba)
        print("Dedicated Samba server stopped; real CIFS outage begins.", flush=True)
        before = wait_for("RAM contains pressure candidates", local_rows)
        tracked, tracked_hash = before[0], digest(before[0])
        evidence.update(tracked=tracked, tracked_hash=tracked_hash)
        if args.policy == "ordinary_first":
            wait_for(
                "old RAM footage reclaimed while new footage continues "
                "during CIFS outage",
                lambda: tracked not in local_rows() and len(local_rows()) >= 2,
                timeout=150,
            )
            wait_for(
                "public run-init returns exact MP4 init bytes",
                lambda: public_init(local_rows()[-1]),
            )
            browser("outage")
        else:
            wait_for(
                "event preservation blocks first-tier admission "
                "at its shared size limit",
                lambda: "Recording storage /buffer/: blocked" in logs(),
                timeout=150,
            )
            preserved = {path: digest(path) for path in local_rows()}
            assert preserved, "No retained event footage"
            command(
                "podman",
                "exec",
                app,
                "python3",
                "-c",
                "from pathlib import Path; p=Path('/buffer/pressure-filler'); "
                "f=p.open('wb',buffering=0); block=b'x'*65536\n"
                "while True:\n try: f.write(block)\n except OSError as e:\n"
                "  assert e.errno==28; break\nf.close()",
            )
            print(
                "Filled dedicated 16 MiB RAM filesystem to native ENOSPC.", flush=True
            )
            deadline = time.monotonic() + 16
            while time.monotonic() < deadline:
                assert all(
                    digest(path) == value for path, value in preserved.items()
                ), "Protected buffer changed"
                time.sleep(2)
            evidence["protected_fragments"] = len(preserved)
            command("podman", "exec", app, "rm", "/buffer/pressure-filler")
            print("Manual event stop:", api("stop"), flush=True)
            print("PASS: active event buffer survived full RAM filesystem.", flush=True)
        survivors = {path: digest(path) for path in local_rows()}
        assert survivors, "No local recovery candidates"
        latest_before_recovery = max(survivors)
        command("podman", "start", args.test_samba)
        print("Samba restarted; awaiting promotion, indexing and playback.", flush=True)

        def recovered_candidate():
            paths = sql(
                "select path from files where tier_path='/cifs/' "
                "and subcategory='segments'"
            ).splitlines()
            for destination in paths:
                source = "/buffer/" + destination.removeprefix("/cifs/")
                if source in survivors and promoted(destination, survivors[source]):
                    return destination, survivors[source]
            return None

        destination, survivor_hash = wait_for(
            "retained outage fragment promotes with matching bytes and run init",
            recovered_candidate,
            timeout=180,
        )
        wait_for(
            "new recording admission resumes after promotion",
            lambda: any(path > latest_before_recovery for path in local_rows()),
            timeout=90,
        )
        wait_for(
            "public run-init serves the recovered run correctly",
            lambda: public_init(destination),
        )
        browser("recovered")
        run = remote / Path(destination).relative_to("/cifs").parent
        playlist = root / "recovered.m3u8"
        command(
            "python3",
            str(args.repo / "scripts/recover_hls.py"),
            "--output",
            str(playlist),
            str(run),
        )
        command(
            "ffmpeg",
            "-v",
            "error",
            "-xerror",
            "-protocol_whitelist",
            "file,crypto,data",
            "-i",
            str(playlist),
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-fps_mode:v",
            "passthrough",
            "-enc_time_base:v",
            "demux",
            "-f",
            "null",
            "-",
            timeout=60,
        )
        print(
            "PASS: CIFS run recovered and audio/video decoded without database access.",
            flush=True,
        )
        evidence.update(
            recovered_segment=destination, recovered_hash=survivor_hash, result="passed"
        )
        print(
            f"PASS: {args.policy} combined camera/RAM/CIFS/browser acceptance.",
            flush=True,
        )
    except BaseException as error:
        evidence.update(result="failed", error=str(error))
        raise
    finally:
        try:
            (root / "app.log").write_text(logs())
            (root / "files.csv").write_text(
                sql(
                    "select tier_path,path,size,orig_ctime from files "
                    "order by orig_ctime"
                )
            )
        except Exception:
            pass
        (root / "result.json").write_text(json.dumps(evidence, indent=2))
        command("podman", "start", args.test_samba, check=False)
        if publisher is not None:
            publisher.terminate()
            try:
                publisher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                publisher.kill()
        command("podman", "pod", "rm", "-f", pod, check=False, timeout=60)
        print(f"Artifacts retained: {root}", flush=True)


if __name__ == "__main__":
    main()

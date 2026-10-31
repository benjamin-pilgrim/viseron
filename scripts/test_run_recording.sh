#!/usr/bin/env bash
# Disposable single-camera, local-storage recovery smoke test.
# Usage: VISERON_TEST_IMAGE=your-image [VISERON_RTSP_IMAGE=your-rtsp-image] \
#          scripts/test_run_recording.sh /absolute/path/to/checkout
# See docs/testing/run-recording.md before running.
set -euo pipefail
repo=${1:?Pass the absolute path to the candidate checkout}
[[ "$repo" = /* && -f "$repo/scripts/recover_hls.py" && -d "$repo/viseron" ]] || {
  echo "Checkout must be absolute and contain viseron/ and scripts/recover_hls.py" >&2
  exit 2
}
: "${VISERON_TEST_IMAGE:?Set VISERON_TEST_IMAGE to a Viseron image compatible with the checkout}"
rtsp_image=${VISERON_RTSP_IMAGE:-docker.io/bluenviron/mediamtx:latest}
recovery_script="$repo/scripts/recover_hls.py"
for dependency in podman ffmpeg python3 sha256sum; do
  command -v "$dependency" >/dev/null || { echo "Missing required command: $dependency" >&2; exit 2; }
done
root=$(mktemp -d /tmp/viseron-vertical-XXXXXX)
suffix=${root##*/}
app="${suffix}-app"
mtx="${suffix}-mtx"
pod="${suffix}-pod"
port=${VISERON_TEST_RTSP_PORT:-29472}
cleanup() {
  [[ -z ${publisher:-} ]] || pkill -P "$publisher" 2>/dev/null || true
  [[ -z ${publisher:-} ]] || kill "$publisher" 2>/dev/null || true
  podman stop -t 60 "$app" >/dev/null 2>&1 || true
  podman rm -f "$app" "$mtx" >/dev/null 2>&1 || true
  podman pod rm -f "$pod" >/dev/null 2>&1 || true
  echo "Artifacts: $root"
}
trap cleanup EXIT
mkdir -p "$root"/{config,segments,recordings,snapshots,thumbnails,event_clips,recovered}
chmod 777 "$root" "$root"/{config,segments,recordings,snapshots,thumbnails,event_clips}
cat > "$root/config/config.yaml" <<CONFIG
logger:
  default_level: info
storage:
  recorder:
    tiers:
      - path: /
        continuous:
          max_age:
            days: 1
ffmpeg:
  camera:
    vertical_camera:
      name: Vertical Camera
      host: 127.0.0.1
      port: 8554
      path: /test
      recorder:
        continuous_recording: true
CONFIG
podman pod create --name "$pod" -p "127.0.0.1:$port:8554/tcp" >/dev/null
podman run -d --pod "$pod" --name "$mtx" -e MTX_HLS=no "$rtsp_image" >/dev/null
# Retry publisher startup because MediaMTX needs time to listen.
(
  while :; do
    ffmpeg -hide_banner -loglevel warning -re -f lavfi -i testsrc2=size=320x240:rate=15 -c:v libx264 -preset ultrafast -tune zerolatency -g 30 -pix_fmt yuv420p -f rtsp -rtsp_transport tcp "rtsp://127.0.0.1:$port/test" || true
    sleep 1
  done
) > "$root/publisher.log" 2>&1 &
publisher=$!
podman run -d --pod "$pod" --name "$app" \
  -v "$repo/viseron:/src/viseron:ro" \
  -v "$root/config:/config" -v "$root/segments:/segments" \
  -v "$root/recordings:/recordings" -v "$root/snapshots:/snapshots" \
  -v "$root/thumbnails:/thumbnails" -v "$root/event_clips:/event_clips" \
  "$VISERON_TEST_IMAGE" >/dev/null
echo "Started $app; fixtures at $root; candidate $repo"
# A retained run is ready only when it has both init and a completed fragment.
list_runs() {
  while IFS= read -r -d '' init; do
    dir=${init%/init.mp4}
    if find "$dir" -maxdepth 1 -type f -name '*.m4s' -print -quit | grep -q .; then printf '%s\n' "$dir"; fi
  done < <(find "$root/segments/vertical_camera" -type f -name init.mp4 -print0 2>/dev/null)
}
wait_runs() {
  local want=$1 count
  for ((n=1; n<=${VISERON_TEST_WAIT_SECONDS:-120}; n++)); do
    count=$(list_runs | wc -l)
    if (( count >= want )); then return; fi
    if ! podman inspect -f '{{.State.Running}}' "$app" | grep -q true; then echo 'FAIL: app exited'; return 1; fi
    sleep 1
  done
  echo "FAIL: expected $want retained runs; found $count"
  find "$root/segments" -type f -printf '%P\n' | sort
  return 1
}
wait_runs 1
run1=$(list_runs | head -1)
# Exercise the same nginx route used by the browser, not just the backend.
# Files can precede database indexing, so allow the route to become ready.
podman exec -i "$app" python3 - "${run1##*/}" <<'PYTHON'
import hashlib
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

run = sys.argv[1]
expected = Path('/segments/vertical_camera') / run / 'init.mp4'
url = f'http://127.0.0.1:8888/run-init/vertical_camera/{run}'
last_error = None
for _ in range(30):
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            body = response.read()
            if response.status != 200:
                raise RuntimeError(f'unexpected HTTP {response.status}')
        if hashlib.sha256(body).digest() != hashlib.sha256(expected.read_bytes()).digest():
            raise SystemExit('FAIL: nginx returned the wrong run init bytes')
        print(f'Public nginx run-init route returned HTTP 200 with matching bytes: {run}')
        break
    except (urllib.error.URLError, FileNotFoundError, TimeoutError) as error:
        last_error = error
        time.sleep(1)
else:
    raise SystemExit(f'FAIL: nginx run-init route did not become ready: {last_error}')
PYTHON
find "$run1" -maxdepth 1 -type f \( -name init.mp4 -o -name '*.m4s' \) -print0 | sort -z | xargs -0 sha256sum > "$root/pre-restart.sha256"
echo "First run: $run1"
if [[ -n ${VISERON_TEST_DEBUG_PAUSE:-} ]]; then sleep "$VISERON_TEST_DEBUG_PAUSE"; fi
# Kill only the HLS-writing ffmpeg process in this disposable container.
podman exec "$app" python3 -c '
import os, signal
for item in os.listdir("/proc"):
    if not item.isdigit(): continue
    try:
        cmd = open("/proc/" + item + "/cmdline", "rb").read().replace(b"\0", b" ")
        if os.path.basename(cmd.split(b" ", 1)[0]).startswith(b"ffmpeg") and b"-hls_segment_filename" in cmd and b"vertical_camera" in cmd:
            os.kill(int(item), signal.SIGKILL)
            print("Killed HLS writer PID", item)
            break
    except (FileNotFoundError, ProcessLookupError, PermissionError): pass
else: raise SystemExit("FAIL: no HLS-writing FFmpeg process found")
'
wait_runs 2
mapfile -t runs < <(list_runs | sort)
run2=${runs[-1]}
[[ "$run1" < "$run2" ]] || { echo 'FAIL: restarted run is not ordered after the first run'; exit 1; }
find "$run2" -maxdepth 1 -type f \( -name init.mp4 -o -name '*.m4s' \) -print0 | sort -z | xargs -0 sha256sum > "$root/post-ffmpeg-restart.sha256"
echo "Runs after FFmpeg restart:"; printf '%s\n' "${runs[@]}"
sha256sum -c "$root/pre-restart.sha256"
# Restart Viseron against the same persisted storage and database, then stop it.
podman stop -t 60 "$app" >/dev/null
podman start "$app" >/dev/null
wait_runs 3
mapfile -t runs < <(list_runs | sort)
echo "Runs after container restart:"; printf '%s\n' "${runs[@]}"
[[ "$run2" < "${runs[-1]}" ]] || { echo 'FAIL: container restart did not create a later run'; exit 1; }
sha256sum -c "$root/pre-restart.sha256"
sha256sum -c "$root/post-ffmpeg-restart.sha256"
podman stop -t 60 "$app" >/dev/null
# PostgreSQL is now unavailable; recover solely from retained run files.
for run in "${runs[@]}"; do
  name=$(basename "$run")
  python3 "$recovery_script" --output "$root/recovered/$name.m3u8" "$run" 2>&1 | tee "$root/recovered/$name.log"
  # The smoke camera is video-only H.264. Preserve its demuxer time base:
  # the null encoder's default frame-rate time base can round distinct VFR
  # timestamps to the same tick and report a misleading non-monotonic DTS.
  ffmpeg -v error -xerror -protocol_whitelist file,crypto,data \
    -i "$root/recovered/$name.m3u8" -map 0:v:0 \
    -fps_mode:v passthrough -enc_time_base:v demux -f null - 2>&1 \
    | tee "$root/recovered/$name.decode.log"
  echo "Recovered and decoded $run"
done
echo 'PASS: distinct runs retained after FFmpeg restart and Viseron restart; offline recovery decoded with PostgreSQL stopped.'

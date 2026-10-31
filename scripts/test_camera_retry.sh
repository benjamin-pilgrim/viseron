#!/usr/bin/env bash
# Exercise nonblocking startup and recovery with two disposable RTSP cameras.
# VISERON_TEST_IMAGE=<image> scripts/test_camera_retry.sh /absolute/checkout
set -euo pipefail
repo=${1:?Pass the absolute path to the candidate checkout}
: "${VISERON_TEST_IMAGE:?Set VISERON_TEST_IMAGE}"
rtsp_image=${VISERON_RTSP_IMAGE:-docker.io/bluenviron/mediamtx:latest}
port=${VISERON_TEST_RTSP_PORT:-29473}
root=$(mktemp -d /tmp/viseron-camera-retry-XXXXXX)
suffix=${root##*/}
app=$suffix-app
mtx=$suffix-mtx
pod=$suffix-pod
cleanup() {
  for pid in ${healthy_publisher:-} ${offline_publisher:-} ${removed_publisher:-}; do
    pkill -P "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
  done
  podman stop -t 60 "$app" >/dev/null 2>&1 || true
  podman rm -f "$app" "$mtx" >/dev/null 2>&1 || true
  podman pod rm -f "$pod" >/dev/null 2>&1 || true
  echo "Artifacts: $root"
}
trap cleanup EXIT
mkdir -p "$root"/{config,segments,recordings,snapshots,thumbnails,event_clips}
chmod 777 "$root" "$root"/{config,segments,recordings,snapshots,thumbnails,event_clips}
cat > "$root/config/config.yaml" <<'CONFIG'
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
    healthy:
      name: Healthy
      host: 127.0.0.1
      port: 8554
      path: /healthy
      recorder:
        continuous_recording: true
    offline:
      name: Offline
      host: 127.0.0.1
      port: 8554
      path: /offline
      recorder:
        continuous_recording: true
    removed:
      name: Removed
      host: 127.0.0.1
      port: 8554
      path: /removed
      recorder:
        continuous_recording: true
CONFIG
podman pod create --name "$pod" -p "127.0.0.1:$port:8554/tcp" >/dev/null
podman run -d --pod "$pod" --name "$mtx" -e MTX_HLS=no "$rtsp_image" >/dev/null
publish() {
  while :; do
    ffmpeg -hide_banner -loglevel warning -re -f lavfi -i testsrc2=size=320x240:rate=15 -c:v libx264 -preset ultrafast -tune zerolatency -g 30 -pix_fmt yuv420p -f rtsp -rtsp_transport tcp "rtsp://127.0.0.1:$port/$1" || true
    sleep 1
  done
}
publish healthy >"$root/healthy.log" 2>&1 &
healthy_publisher=$!
podman run -d --pod "$pod" --name "$app" -e S6_KILL_GRACETIME=3000 \
  -v "$repo/viseron:/src/viseron:ro" \
  -v "$root/config:/config" -v "$root/segments:/segments" \
  -v "$root/recordings:/recordings" -v "$root/snapshots:/snapshots" \
  -v "$root/thumbnails:/thumbnails" -v "$root/event_clips:/event_clips" \
  "$VISERON_TEST_IMAGE" >/dev/null
wait_recording() {
  local camera=$1
  for ((n=0; n<${VISERON_TEST_WAIT_SECONDS:-180}; n++)); do
    if find "$root/segments/$camera" -name '*.m4s' -print -quit 2>/dev/null | grep -q .; then return; fi
    if [[ $(podman inspect -f '{{.State.Running}}' "$app") != true ]]; then echo "App exited" >&2; return 1; fi
    sleep 1
  done
  echo "No recording for $camera" >&2
  podman logs "$app" > "$root/app.log" 2>&1
  return 1
}
wait_recording healthy
podman logs "$app" > "$root/app-initial.log" 2>&1
grep -q 'Domain camera with identifier offline .* is not ready' "$root/app-initial.log"
grep -q 'Domain camera with identifier removed .* is not ready' "$root/app-initial.log"
grep -q 'NVR for camera Healthy initialized' "$root/app-initial.log"
echo 'Healthy camera recorded while the second camera was unavailable.'
publish offline >"$root/offline.log" 2>&1 &
offline_publisher=$!
wait_recording offline
echo 'Unavailable camera recovered and recorded without an app restart.'
podman logs "$app" > "$root/app.log" 2>&1
grep -q 'NVR for camera Offline initialized' "$root/app.log"
# Remove a still-offline camera, reload through the public websocket API, then
# bring the stream online. A cancelled retry must not recreate its NVR.
python3 - "$root/config/config.yaml" <<'PYTHON'
from pathlib import Path
import sys
path = Path(sys.argv[1])
text = path.read_text()
assert '    removed:\n' in text
path.write_text(text.split('    removed:\n', 1)[0])
PYTHON
podman exec -i "$app" python3 - <<'PYTHON'
import asyncio
import json
from tornado.websocket import websocket_connect

async def main():
    conn = await websocket_connect('ws://127.0.0.1:8888/websocket')
    greeting = json.loads(await asyncio.wait_for(conn.read_message(), 10))
    assert greeting['type'] == 'auth_not_required', greeting
    await conn.write_message(json.dumps({'command_id': 1, 'type': 'reload_config'}))
    response = json.loads(await asyncio.wait_for(conn.read_message(), 60))
    assert response['success'] and response['result']['success'], response
    assert not response['result']['restart_required'], response
    conn.close()
    print('Configuration reload succeeded.')

asyncio.run(main())
PYTHON
publish removed >"$root/removed.log" 2>&1 &
removed_publisher=$!
# Existing backoff has reached at most 30 seconds by this point. Wait long
# enough for the obsolete timer to fire if cancellation did not take effect.
sleep 40
podman logs "$app" > "$root/app-after-reload.log" 2>&1
podman logs "$mtx" > "$root/rtsp.log" 2>&1
grep -q "is publishing to path 'removed'" "$root/rtsp.log"
if grep -q 'NVR for camera Removed initialized' "$root/app-after-reload.log" ||
   find "$root/segments/removed" -name '*.m4s' -print -quit 2>/dev/null | grep -q .; then
  echo 'Removed camera restarted after reload' >&2
  exit 1
fi
echo 'Removed offline camera did not restart after reload.'

# Disconnect both active streams immediately before requesting shutdown.
for pid in "$healthy_publisher" "$offline_publisher"; do
  pkill -P "$pid" 2>/dev/null || true
  kill "$pid" 2>/dev/null || true
done
healthy_publisher=
offline_publisher=
podman stop -t 60 "$app" >/dev/null
test "$(podman inspect -f '{{.State.ExitCode}}' "$app")" = 0
podman logs "$app" > "$root/app-shutdown.log" 2>&1
grep -q 'Shutdown complete' "$root/app-shutdown.log"
if sed -n '/Initiating shutdown/,$p' "$root/app-shutdown.log" | grep -q 'Restarting frame pipe'; then
  echo 'Frame reader restarted during shutdown' >&2
  exit 1
fi
echo 'Viseron stopped cleanly during camera disconnect.'

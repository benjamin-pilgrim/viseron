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
  for pid in ${healthy_publisher:-} ${offline_publisher:-}; do
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
grep -q 'NVR for camera Healthy initialized' "$root/app-initial.log"
echo 'Healthy camera recorded while the second camera was unavailable.'
publish offline >"$root/offline.log" 2>&1 &
offline_publisher=$!
wait_recording offline
echo 'Unavailable camera recovered and recorded without an app restart.'
podman logs "$app" > "$root/app.log" 2>&1
grep -q 'NVR for camera Offline initialized' "$root/app.log"

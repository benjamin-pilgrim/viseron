#!/usr/bin/env bash
# Disposable synthetic audio/video test of recorder timestamp modes.
# Usage: VISERON_TEST_IMAGE=your-image [VISERON_TEST_TIMESTAMP_MODE=source] \
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
port=${VISERON_TEST_RTSP_PORT:-29474}
mode=${VISERON_TEST_TIMESTAMP_MODE:-wallclock}
[[ "$mode" == wallclock || "$mode" == source ]] || { echo "Invalid timestamp mode" >&2; exit 2; }
cleanup() {
  [[ -z ${publisher:-} ]] || kill -CONT "$publisher" 2>/dev/null || true
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
mode_config=""
[[ "$mode" != source ]] || mode_config="        timestamp_mode: source"
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
${mode_config}
CONFIG
podman pod create --name "$pod" -p "127.0.0.1:$port:8554/tcp" >/dev/null
podman run -d --pod "$pod" --name "$mtx" -e MTX_HLS=no "$rtsp_image" >/dev/null
# Retry publisher startup because MediaMTX needs time to listen.
(
  while :; do
    ffmpeg -hide_banner -loglevel warning -re -f lavfi -i testsrc2=size=320x240:rate=15 -re -f lavfi -i sine=frequency=1000:sample_rate=48000 -c:a aac -b:a 64k -c:v libx264 -preset ultrafast -tune zerolatency -g 30 -pix_fmt yuv420p -f rtsp -rtsp_transport tcp "rtsp://127.0.0.1:$port/test" || true
    sleep 1
  done
) > "$root/publisher.log" 2>&1 &
publisher=$!
podman run -d --pod "$pod" --name "$app" -e S6_KILL_GRACETIME=3000 \
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
# Optional destructive actions affect only this disposable test container. Pause
# its consumer until recorded media leaves the playlist, then kill/restart it.
if [[ ${VISERON_TEST_BACKLOG_RESTART:-0} == 1 ]]; then
podman exec "$app" python3 -c '
import os, signal
for p in os.listdir("/proc"):
 if not p.isdigit(): continue
 try: cmd=open("/proc/"+p+"/cmdline","rb").read()
 except OSError: continue
 if cmd.startswith(b"viseron.child_process.fragmenter.vertical_camera.process"):
  os.kill(int(p),signal.SIGSTOP); print("Stopped fragment consumer",p); break
else: raise SystemExit("No fragment consumer found")
'
python3 - "$root" "${VISERON_TEST_BACKLOG_IDENTITY_DRIFT:-0}" <<'PYCHECK'
from pathlib import Path
import json, sys, time
root=Path(sys.argv[1])
stage=root/'segments/.viseron-spool/vertical_camera'
for _ in range(100):
    runs=list(stage.iterdir()) if stage.exists() else []
    if runs:
        run=runs[0]; files=sorted(run.glob('*.m4s'))
        playlist=(run/'index.m3u8').read_text() if (run/'index.m3u8').exists() else ''
        eligible=[p for p in files if p.name not in playlist and (run/('.viseron-timing-'+p.name+'.json')).exists()]
        if eligible:
            file=eligible[0]
            rec=json.loads((run/('.viseron-timing-'+file.name+'.json')).read_text())
            if sys.argv[2] == '1':
                rec['identity'][0] += 1
                (run/('.viseron-timing-'+file.name+'.json')).write_text(json.dumps(rec))
            (root/'backlog-test.json').write_text(json.dumps({'run':run.name,'file':file.name,'time':rec['program_date_time']}))
            print('Pending outside playlist before container kill:',file.name,'timestamp',rec['program_date_time'])
            break
    time.sleep(1)
else: raise SystemExit('No captured backlog outside playlist')
PYCHECK
podman kill "$app" >/dev/null
podman start "$app" >/dev/null
python3 - "$root" "$app" <<'PYCHECK'
import datetime, json, subprocess, sys, time
from pathlib import Path
root=Path(sys.argv[1]); app=sys.argv[2]
rec=json.loads((root/'backlog-test.json').read_text())
path=f"/segments/vertical_camera/{rec['run']}/{rec['file']}"
sql="select orig_ctime from files where path = '%s'" % path
for _ in range(120):
    result=subprocess.run(['podman','exec',app,'psql','-U','postgres','-d','viseron','-Atc',sql],capture_output=True,text=True)
    if result.returncode == 0 and result.stdout.strip():
        actual=datetime.datetime.fromisoformat(result.stdout.strip()).replace(tzinfo=datetime.timezone.utc)
        expected=datetime.datetime.fromisoformat(rec['time'])
        if actual != expected: raise SystemExit(f'Wrong timestamp: {actual} != {expected}')
        if not (root/'segments/vertical_camera'/rec['run']/rec['file']).is_file(): raise SystemExit('Indexed file missing')
        print('PASS: pending fragment recovered after container kill with original timestamp:',actual)
        break
    time.sleep(1)
else: raise SystemExit('Backlog fragment not indexed after container restart')
PYCHECK
fi
# Check the actual running recorder arguments, as opposed to just the config.
podman exec -i "$app" python3 - "$mode" <<'PYTHON'
import os, sys
mode = sys.argv[1]
for pid in os.listdir('/proc'):
    if not pid.isdigit(): continue
    try:
        args = open('/proc/' + pid + '/cmdline', 'rb').read().split(b'\0')
        if args and os.path.basename(args[0]).startswith(b'ffmpeg') and b'-hls_segment_filename' in args and any(b'vertical_camera' in arg for arg in args):
            has_wallclock = b'-use_wallclock_as_timestamps' in args
            assert has_wallclock == (mode == 'wallclock'), args
            if has_wallclock:
                assert args[args.index(b'-use_wallclock_as_timestamps') + 1] == b'1', args
            print(f'Recorder timestamp mode {mode} verified')
            break
    except (FileNotFoundError, ProcessLookupError, PermissionError): pass
else: raise SystemExit('FAIL: no recorder process found')
PYTHON
mapfile -t initial_runs < <(list_runs | sort)
run1=${initial_runs[0]}
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
runs_before_ffmpeg_restart=$(list_runs | wc -l)
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
wait_runs "$((runs_before_ffmpeg_restart + 1))"
mapfile -t runs < <(list_runs | sort)
run2=${runs[-1]}
[[ "$run1" < "$run2" ]] || { echo 'FAIL: restarted run is not ordered after the first run'; exit 1; }
find "$run2" -maxdepth 1 -type f \( -name init.mp4 -o -name '*.m4s' \) -print0 | sort -z | xargs -0 sha256sum > "$root/post-ffmpeg-restart.sha256"
echo "Runs after FFmpeg restart:"; printf '%s\n' "${runs[@]}"
sha256sum -c "$root/pre-restart.sha256"
# Pause the camera publisher and terminate its FFmpeg child, so Viseron sees
# an actual RTSP outage rather than only a killed recorder process.
runs_before_outage=$(list_runs | wc -l)
kill -STOP "$publisher"
pkill -P "$publisher" 2>/dev/null || true
sleep 10
kill -CONT "$publisher"
wait_runs "$((runs_before_outage + 1))"
echo "Camera publisher reconnected and recording resumed."
# Restart Viseron against the same persisted storage and database, then stop it.
runs_before_container_restart=$(list_runs | wc -l)
podman stop -t 60 "$app" >/dev/null
podman start "$app" >/dev/null
wait_runs "$((runs_before_container_restart + 1))"
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
  # Preserve the video demuxer time base:
  # the null encoder's default frame-rate time base can round distinct VFR
  # timestamps to the same tick and report a misleading non-monotonic DTS.
  ffmpeg -v error -xerror -protocol_whitelist file,crypto,data \
    -i "$root/recovered/$name.m3u8" -map 0:v:0 -map 0:a:0 \
    -fps_mode:v passthrough -enc_time_base:v demux -f null - 2>&1 \
    | tee "$root/recovered/$name.decode.log"
  echo "Recovered and decoded $run"
done
echo "PASS: $mode audio/video runs decoded after camera reconnect, FFmpeg and container restarts with PostgreSQL stopped."

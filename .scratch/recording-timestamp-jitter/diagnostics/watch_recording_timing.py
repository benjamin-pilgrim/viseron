#!/usr/bin/env python3
"""Temporary host-side observer; no media payloads or RTSP text are persisted.

Run as root on the Podman test host. tcpdump runs in the container network
namespace; its pcap stream stays in memory. Output is bounded rotating JSONL.
This observer never restarts a recorder or changes recording files/config.
"""

import argparse
from collections import OrderedDict
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import threading
import time


class Flow:
    """Reassemble one bounded TCP direction, including overlap/retransmission."""

    def __init__(self):
        self.next_seq = None
        self.buffer = bytearray()

    def feed(self, seq, data):
        gap = False
        if self.next_seq is not None:
            delta = (seq - self.next_seq + 2**31) % 2**32 - 2**31
            if delta < 0:
                data = data[-delta:]
                seq = (seq - delta) % 2**32
            elif delta > 0:
                self.buffer.clear()
                gap = True
        if not data:
            return [], gap
        self.next_seq = (seq + len(data)) % 2**32
        self.buffer.extend(data)
        frames = []
        while self.buffer:
            start = self.buffer.find(b"$")
            if start < 0:
                self.buffer.clear()
                break
            if start:
                del self.buffer[:start]
            if len(self.buffer) < 4:
                break
            length = int.from_bytes(self.buffer[2:4], "big")
            if length < 8 or (len(self.buffer) > 4 and self.buffer[4] >> 6 != 2):
                del self.buffer[0]
                continue
            if len(self.buffer) < length + 4:
                break
            # Keep only fixed RTP/RTCP headers, discard media bytes immediately.
            frames.append((self.buffer[1], bytes(self.buffer[4:4 + min(length, 28)])))
            del self.buffer[:length + 4]
        return frames, gap


class Headers:
    def __init__(self, emit):
        self.emit = emit
        self.flows = OrderedDict()
        self.stats = OrderedDict()

    def packet(self, ethernet, when):
        if len(ethernet) < 54:
            return
        ip = ethernet[14:]
        if ethernet[12:14] == b"\x08\x00":
            ihl = (ip[0] & 15) * 4
            protocol, addresses = ip[9], ip[12:20]
        elif ethernet[12:14] == b"\x86\xdd" and len(ip) >= 60:
            ihl = 40
            protocol, addresses = ip[6], ip[8:40]
        else:
            return
        # Extension headers/fragments are intentionally unsupported and skipped.
        if protocol != 6 or len(ip) < ihl + 20:
            return
        tcp = ip[ihl:]
        src, dst, seq = struct.unpack("!HHI", tcp[:8])
        if src != 8554:
            return
        offset = (tcp[12] >> 4) * 4
        key = (addresses, src, dst)
        if tcp[13] & 2:  # New connection may reuse an old destination port.
            self.flows.pop(key, None)
        payload = tcp[offset:]
        if not payload:
            return
        flow = self.flows.setdefault(key, Flow())
        self.flows.move_to_end(key)
        if len(self.flows) > 64:
            self.flows.popitem(last=False)
        frames, gap = flow.feed(seq, payload)
        if gap:
            self.emit({"event": "tcp_capture_gap", "port": dst, "time": when})
        for channel, header in frames:
            if channel % 2:  # Standard negotiated RTP/RTCP channel pairs.
                if len(header) >= 28 and header[1] == 200:
                    ssrc, ntp_hi, ntp_lo, rtp = struct.unpack("!IIII", header[4:20])
                    self.emit({"event": "sender_report", "time": when,
                               "port": dst, "channel": channel, "ssrc": ssrc,
                               "ntp": [ntp_hi, ntp_lo], "rtp": rtp})
                continue
            if len(header) < 12:
                continue
            _, pt, sequence, timestamp, ssrc = struct.unpack("!BBHII", header[:12])
            track = (dst, channel, pt & 127, ssrc)
            bucket = self.stats.get(track)
            if bucket is None or int(when) != bucket["second"]:
                if bucket:
                    self.emit(bucket)
                bucket = {"event": "rtp_second", "second": int(when),
                          "port": dst, "channel": channel, "pt": pt & 127,
                          "ssrc": ssrc, "first_ts": timestamp, "last_ts": timestamp,
                          "first_seq": sequence, "last_seq": sequence,
                          "packets": 0, "min_step": 0, "max_step": 0}
                self.stats[track] = bucket
            step = (timestamp - bucket["last_ts"] + 2**31) % 2**32 - 2**31
            bucket["min_step"] = min(bucket["min_step"], step)
            bucket["max_step"] = max(bucket["max_step"], step)
            bucket["last_ts"] = timestamp
            bucket["last_seq"] = sequence
            bucket["packets"] += 1
            self.stats.move_to_end(track)
            if len(self.stats) > 256:
                self.emit(self.stats.popitem(last=False)[1])


# Executed within the existing container; no camera URL is returned.
MEDIA_PROBE = r'''
import json,pathlib,psutil,subprocess,tempfile,os,urllib.request
try:
 try:
 with urllib.request.urlopen('http://127.0.0.1:1984/api/diagnostics/flv-timing',timeout=2) as response: snapshot=json.load(response)
 for record in snapshot['records']:
  print(json.dumps({'event':'flv_timing','instance':snapshot['instance'],'record':record}),flush=True)
except Exception as exc:
 print(json.dumps({'event':'flv_diagnostics_unavailable','error_type':type(exc).__name__}),flush=True)
with urllib.request.urlopen('http://127.0.0.1:1984/api/streams',timeout=2) as response: streams=json.load(response)
 for name,stream in streams.items():
  if name not in ('front_door_camera','driveway_camera'): continue
  producers=[]
  for producer in (stream.get('producers') or []):
   producers.append({k:v for k,v in producer.items() if k in ('id','bytes_recv','bytes_send') and isinstance(v,int)})
  print(json.dumps({'event':'relay_state','camera':name,'producers':producers}),flush=True)
except Exception as exc:
 print(json.dumps({'event':'relay_probe_unavailable','error_type':type(exc).__name__}),flush=True)
for p in psutil.process_iter(['name','cmdline']):
 args=p.info['cmdline'] or []
 if p.info['name']!='ffmpeg' or '-i' not in args: continue
 outputs=[x for x in args if '/segments/' in x and '.m4s' in x]
 if not outputs: continue
 root=pathlib.Path(outputs[-1]).parent
 result={'event':'media_sample','pid':p.pid,'camera':root.parent.name,'run':root.name}
 try:
  result['ports']=[c.laddr.port for c in p.net_connections('tcp') if c.raddr and c.raddr.port==8554]
  # The fragmenter publishes closed spool fragments into the canonical run.
  published=pathlib.Path(str(root).replace('/segments/.viseron-spool/','/segments/'))
  paths=sorted(published.glob('*.m4s'))[-3:] if published!=root else sorted(root.glob('*.m4s'))[-4:-1]
  if len(paths)<3:
   result['state']='waiting_for_closed_segments'
  else:
   result['segments']=[x.name for x in paths]
   result['latest_mtime']=paths[-1].stat().st_mtime
   result['sizes']=[x.stat().st_size for x in paths]
   with tempfile.NamedTemporaryFile(suffix='.mp4') as sample:
    init=published/'init.mp4' if (published/'init.mp4').exists() else root/'init.mp4'
    total=init.stat().st_size+sum(result['sizes'])
    if total>32*1024*1024: raise ValueError('sample_size_limit')
    with open(init,'rb') as src: sample.write(src.read())
    for x in paths:
     with open(x,'rb') as src: sample.write(src.read())
    sample.flush()
    r=subprocess.run(['ffprobe','-v','error','-show_streams','-show_packets','-show_entries','stream=index,codec_type:packet=stream_index,pts_time,duration_time','-of','json',sample.name],capture_output=True,timeout=12)
   if r.returncode: raise ValueError('ffprobe_failed')
   data=json.loads(r.stdout); ranges={}
   for s in data.get('streams',[]):
    packets=[x for x in data.get('packets',[]) if x['stream_index']==s['index'] and 'pts_time' in x]
    if s['codec_type'] not in ('audio','video') or not packets: continue
    ranges[s['codec_type']]={'start':min(float(x['pts_time']) for x in packets),'end':max(float(x['pts_time'])+float(x.get('duration_time',0)) for x in packets),'packets':len(packets),'packet_duration':sum(float(x.get('duration_time',0)) for x in packets)}
   result['ranges']=ranges
   result['state']='sampled'
   if 'audio' in ranges and 'video' in ranges:
    result['audio_minus_video']=ranges['audio']['start']-ranges['video']['start']
 except Exception as exc:
  # Do not log exception text: subprocess/config errors can contain secrets.
  result['state']='probe_unavailable'; result['error_type']=type(exc).__name__
 print(json.dumps(result),flush=True)
'''


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', default='systemd-viseron')
    parser.add_argument('--output', required=True)
    parser.add_argument('--hours', type=float, default=72)
    parser.add_argument('--expect-audio', action='append', default=[])
    args = parser.parse_args()
    logger = logging.getLogger('timing-observer')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(args.output, maxBytes=1024*1024, backupCount=11)
    logger.addHandler(handler)

    def emit(value):
        value.setdefault('observed_at', time.time())
        logger.info(json.dumps(value, separators=(',', ':')))

    stopped = threading.Event()

    def preserve_incident(row):
        if row.get('event') != 'media_sample' or row.get('state') != 'sampled':
            return
        ranges = row.get('ranges', {})
        missing = row.get('camera') in args.expect_audio and 'audio' not in ranges
        bad = missing or abs(row.get('audio_minus_video', 0)) > 2
        if not bad:
            return
        destination = Path(args.output).parent / 'first-incident'
        if destination.exists():
            return
        destination.mkdir(mode=0o700)
        emit({'event':'incident_snapshot','camera':row['camera'],'run':row['run']})
        # Freeze pre-onset history once; continuing logs remain independently bounded.
        handler.acquire()
        try:
            handler.flush()
            for source in Path(args.output).parent.glob(Path(args.output).name+'*'):
                if source.is_file():
                    shutil.copyfile(source, destination/source.name)
        finally:
            handler.release()

    def media():
        flv_instance, flv_seq = None, 0
        while not stopped.is_set():
            try:
                r = subprocess.run(['podman','exec',args.container,'python3','-c',MEDIA_PROBE],
                                   capture_output=True, text=True, timeout=45)
                for line in r.stdout.splitlines():
                    row = json.loads(line)
                    if row.get('event') == 'flv_timing':
                        if row['instance'] != flv_instance:
                            flv_instance, flv_seq = row['instance'], 0
                        seq = row['record']['seq']
                        if seq <= flv_seq:
                            continue
                        if flv_seq and seq != flv_seq + 1:
                            emit({'event':'flv_history_gap','previous_seq':flv_seq,'next_seq':seq})
                        flv_seq = seq
                    emit(row)
                    preserve_incident(row)
                if r.returncode:
                    emit({'event':'media_probe_failed','returncode':r.returncode})
            except Exception as exc:
                emit({'event':'media_probe_failed','error_type':type(exc).__name__})
            stopped.wait(30)

    pid = subprocess.check_output(['podman','inspect','--format','{{.State.Pid}}',args.container],text=True).strip()
    capture = subprocess.Popen(['nsenter','-t',pid,'-n','tcpdump','-i','lo','-Q','in','-s','0','-U','-w','-',
                                'tcp','src','port','8554'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    signal.signal(signal.SIGTERM, lambda *_: capture.terminate())
    signal.signal(signal.SIGINT, lambda *_: capture.terminate())
    timer = threading.Timer(args.hours * 3600, capture.terminate)
    timer.daemon = True
    timer.start()
    thread = threading.Thread(target=media, daemon=True)
    thread.start()
    emit({'event':'observer_started','container_pid':int(pid),'hours':args.hours})
    observer = Headers(emit)
    try:
        header = capture.stdout.read(24)
        endian = {b'\xd4\xc3\xb2\xa1':'<', b'\xa1\xb2\xc3\xd4':'>'}.get(header[:4])
        if endian is None or struct.unpack(endian+'I',header[20:24])[0] != 1:
            raise RuntimeError('unsupported_capture_format')
        while True:
            header = capture.stdout.read(16)
            if len(header) != 16:
                break
            seconds, micros, length, _ = struct.unpack(endian+'IIII',header)
            if length > 1024*1024:
                raise RuntimeError('capture_record_too_large')
            packet = capture.stdout.read(length)
            if len(packet) != length:
                break
            observer.packet(packet, seconds+micros/1e6)
    finally:
        stopped.set()
        timer.cancel()
        capture.terminate()
        capture.wait(timeout=5)
        for bucket in observer.stats.values():
            emit(bucket)
        emit({'event':'observer_stopped'})


if __name__ == '__main__':
    main()

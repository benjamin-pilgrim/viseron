#!/usr/bin/env python3
"""Temporary, bounded camera TCP observer. Never persists packet payloads or URLs."""
import argparse
from collections import OrderedDict
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import struct
import subprocess
import threading
import time


def delta32(a, b):
    return (a - b + 2**31) % 2**32 - 2**31


def tcp_header(packet, linktype):
    """Decode only IP/TCP headers; infer data length from the original IP length."""
    if linktype == 1:
        if len(packet) < 14:
            return None
        protocol = int.from_bytes(packet[12:14], 'big')
        offset = 14
        if protocol == 0x8100 and len(packet) >= 18:
            protocol = int.from_bytes(packet[16:18], 'big')
            offset = 18
    elif linktype == 113:
        if len(packet) < 16:
            return None
        protocol, offset = int.from_bytes(packet[14:16], 'big'), 16
    elif linktype == 276:
        if len(packet) < 20:
            return None
        protocol, offset = int.from_bytes(packet[:2], 'big'), 20
    else:
        return None
    ip = packet[offset:]
    if protocol != 0x0800 or len(ip) < 20 or ip[0] >> 4 != 4:
        return None
    ihl = (ip[0] & 15) * 4
    total = int.from_bytes(ip[2:4], 'big')
    # Cameras are IPv4. Skip fragments rather than interpret them as TCP.
    if ihl < 20 or ip[9] != 6 or int.from_bytes(ip[6:8], 'big') & 0x3fff or len(ip) < ihl + 20:
        return None
    tcp = ip[ihl:]
    thl = (tcp[12] >> 4) * 4
    if thl < 20 or total < ihl + thl or len(tcp) < thl:
        return None
    src, dst, seq, ack = struct.unpack('!HHII', tcp[:12])
    sack = []
    tsval = None
    options = tcp[20:thl]
    while options:
        kind = options[0]
        if kind == 0:
            break
        if kind == 1:
            options = options[1:]
            continue
        if len(options) < 2 or options[1] < 2 or options[1] > len(options):
            break
        n = options[1]
        if kind == 5 and (n - 2) % 8 == 0:
            sack = [list(struct.unpack('!II', options[i:i+8])) for i in range(2, n, 8)][:4]
        elif kind == 8 and n == 10:
            tsval = int.from_bytes(options[2:6], 'big')
        options = options[n:]
    return {'src': socket.inet_ntoa(ip[12:16]), 'dst': socket.inet_ntoa(ip[16:20]),
            'sport': src, 'dport': dst, 'seq': seq, 'ack': ack, 'flags': tcp[13],
            'window_raw': int.from_bytes(tcp[14:16], 'big'),
            'data_bytes': total - ihl - thl, 'sack': sack, 'tsval': tsval}


def socket_rows(output, camera, address):
    """Whitelist ss fields. Never emit raw ss output, process names or addresses."""
    counters = ('bytes_received', 'bytes_sent', 'bytes_acked', 'bytes_retrans',
                'segs_in', 'segs_out', 'data_segs_in', 'data_segs_out',
                'rcv_ooopack', 'dsack_dups', 'unacked', 'lost', 'reordering',
                'lastsnd', 'lastrcv', 'lastack', 'rcv_space', 'rcv_wnd', 'snd_wnd')
    rows = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 5:
            continue
        try:
            peer, port = fields[4].rsplit(':', 1)
            peer = peer.strip('[]').removeprefix('::ffff:')
            if peer != address or int(port) != 443:
                continue
            row = {'event': 'tcp_socket', 'camera': camera,
                   'local_port': int(fields[3].rsplit(':', 1)[1]),
                   'recv_queue_bytes': int(fields[1]), 'send_queue_bytes': int(fields[2])}
        except (ValueError, IndexError):
            continue
        for key in counters:
            match = re.search(r'\b' + key + r':(\d+)\b', line)
            if match:
                row[key] = int(match[1])
        for key in ('rtt', 'rcv_rtt', 'rto', 'minrtt'):
            match = re.search(r'\b' + key + r':([\d.]+)(?:/([\d.]+))?', line)
            if match:
                row[key + '_ms'] = float(match[1])
                if key == 'rtt' and match[2]:
                    row['rtt_variation_ms'] = float(match[2])
        match = re.search(r'\bretrans:(\d+)/(\d+)\b', line)
        if match:
            row['local_tx_retrans_current'], row['local_tx_retrans_total'] = map(int, match.groups())
        match = re.search(r'\bino:(\d+)\b', line)
        if match:
            row['socket_inode'] = int(match[1])
        rows.append(row)
    return rows


class Headers:
    def __init__(self, targets, emit):
        self.targets = {address: camera for camera, address in targets.items()}
        self.emit = emit
        self.flows = OrderedDict()

    def flush(self):
        for flow in self.flows.values():
            if flow.get('bucket'):
                self.emit(flow.pop('bucket'))

    def packet(self, packet, linktype, when):
        p = tcp_header(packet, linktype)
        if p is None:
            return
        incoming = p['src'] in self.targets and p['sport'] == 443
        if incoming:
            camera, port = self.targets[p['src']], p['dport']
        elif p['dst'] in self.targets and p['dport'] == 443:
            camera, port = self.targets[p['dst']], p['sport']
        else:
            return
        key = (camera, port)
        if p['flags'] & 2:
            old = self.flows.pop(key, None)
            if old and old.get('bucket'):
                self.emit(old['bucket'])
        flow = self.flows.setdefault(key, {'seen': OrderedDict()})
        self.flows.move_to_end(key)
        if len(self.flows) > 64:
            _, old = self.flows.popitem(last=False)
            if old.get('bucket'):
                self.emit(old['bucket'])
        bucket = flow.get('bucket')
        if bucket is None or bucket['second'] != int(when):
            if bucket:
                self.emit(bucket)
            bucket = {'event': 'tcp_second', 'second': int(when), 'camera': camera,
                      'local_port': port, 'in_packets': 0, 'out_packets': 0,
                      'in_data_bytes': 0, 'out_data_bytes': 0, 'max_in_gap_ms': 0,
                      'forward_seq_gap_observations': 0, 'overlap_or_reorder_segments': 0,
                      'repeated_range_segments': 0, 'ack_repeats': 0,
                      'sack_packets': 0, 'sack_above_ack_packets': 0, 'dsack_packets': 0,
                      'zero_window_packets': 0}
            flow['bucket'] = bucket
        bucket['in_packets' if incoming else 'out_packets'] += 1
        bucket['in_data_bytes' if incoming else 'out_data_bytes'] += p['data_bytes']
        if incoming and p['data_bytes']:
            if 'arrival' in flow:
                gap = (when - flow['arrival']) * 1000
                bucket['max_in_gap_ms'] = max(bucket['max_in_gap_ms'], round(gap, 3))
                if gap >= 1000:
                    self.emit({'event': 'tcp_receive_gap', 'camera': camera, 'local_port': port,
                               'gap_ms': round(gap, 3), 'gap_ended_at': when})
            flow['arrival'] = when
            if 'next_seq' in flow:
                diff = delta32(p['seq'], flow['next_seq'])
                if diff > 0:
                    bucket['forward_seq_gap_observations'] += 1
                elif diff < 0:
                    bucket['overlap_or_reorder_segments'] += 1
            end = (p['seq'] + p['data_bytes']) % 2**32
            if 'next_seq' not in flow or delta32(end, flow['next_seq']) > 0:
                flow['next_seq'] = end
            signature = (p['seq'], p['data_bytes'])
            if signature in flow['seen']:
                bucket['repeated_range_segments'] += 1
            flow['seen'][signature] = None
            if len(flow['seen']) > 1024:
                flow['seen'].popitem(last=False)
            bucket.setdefault('first_in_seq', p['seq'])
            bucket['last_in_seq'] = p['seq']
            if p['tsval'] is not None:
                bucket.setdefault('first_in_tsval', p['tsval'])
                bucket['last_in_tsval'] = p['tsval']
        if not incoming and p['flags'] & 16:
            signature = (p['ack'], p['window_raw'])
            if p['data_bytes'] == 0 and signature == flow.get('ack'):
                bucket['ack_repeats'] += 1
            flow['ack'] = signature
            if p['sack']:
                bucket['sack_packets'] += 1
                bucket['last_sack_blocks'] = p['sack']
                bucket['last_sack_ack'] = p['ack']
                if delta32(p['sack'][0][1], p['ack']) <= 0:
                    bucket['dsack_packets'] += 1
                else:
                    bucket['sack_above_ack_packets'] += 1
            if p['window_raw'] == 0:
                bucket['zero_window_packets'] += 1
            bucket['last_ack'] = p['ack']
            bucket['advertised_window_raw'] = p['window_raw']


def capture_counters(line):
    # SIGUSR1 uses one prefixed comma-separated line; exit uses three lines.
    return [{'event': 'capture_counter',
             'counter': match[2].decode().replace(' ', '_'), 'value': int(match[1])}
            for match in re.finditer(rb'(\d+) packets (captured|received by filter|dropped by kernel)', line)]


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--targets', required=True, help='Private JSON mapping of camera labels to IPv4 addresses')
    parser.add_argument('--container', default='systemd-viseron')
    parser.add_argument('--output', required=True)
    parser.add_argument('--timing-directory')
    parser.add_argument('--hours', type=float, default=72)
    args = parser.parse_args()
    targets = json.loads(Path(args.targets).read_text())
    if not targets or len(targets) > 8:
        raise ValueError('target_count')
    targets = {str(k): str(ipaddress.IPv4Address(v)) for k, v in targets.items()}
    root = Path(args.output).parent
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    logger = logging.getLogger('camera-network')
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(args.output, maxBytes=1024*1024, backupCount=7)
    logger.addHandler(handler)
    frozen = False

    def emit(row):
        nonlocal frozen
        row.setdefault('observed_at', time.time())
        logger.info(json.dumps(row, separators=(',', ':')))
        if row.get('event') != 'tcp_receive_gap' or frozen:
            return
        destination = root / 'first-stall'
        if destination.exists():
            frozen = True
            return
        # This callback runs on the capture thread; logging remains thread safe.
        frozen = True
        destination.mkdir(mode=0o700)
        def snapshot(phase):
            try:
                with handler.lock:
                    copy_snapshot(destination)
            except OSError as exc:
                logger.info(json.dumps({'event': 'snapshot_failed', 'phase': phase, 'error_type': type(exc).__name__, 'observed_at': time.time()}))
                return
            logger.info(json.dumps({'event': 'first_stall_preserved', 'phase': phase, 'observed_at': time.time()}))

        snapshot('initial')
        followup = threading.Timer(60, snapshot, args=('post_stall',))
        followup.daemon = True
        followup.start()

    def copy_snapshot(destination):
        handler.flush()
        for source in root.glob(Path(args.output).name + '*'):
            if source.is_file():
                shutil.copyfile(source, destination / source.name)
        if args.timing_directory:
            timing = destination / 'recording-timing'
            timing.mkdir(mode=0o700, exist_ok=True)
            for source in Path(args.timing_directory).glob('timing.jsonl*'):
                if source.is_file():
                    try:
                        shutil.copyfile(source, timing / source.name)
                    except FileNotFoundError:
                        pass  # Timing observer may rotate concurrently.

    pid = subprocess.check_output(['podman', 'inspect', '--format', '{{.State.Pid}}', args.container], text=True).strip()
    interfaces = set()
    for address in targets.values():
        route = json.loads(subprocess.check_output(['nsenter', '-t', pid, '-n', 'ip', '-j', 'route', 'get', address], text=True))
        interfaces.add(route[0]['dev'])
    if len(interfaces) != 1:
        raise RuntimeError('targets_require_one_capture_interface')
    interface = interfaces.pop()
    address_filter = '(' + ' or '.join('host ' + x for x in targets.values()) + ')'
    capture = subprocess.Popen(['nsenter', '-t', pid, '-n', 'tcpdump', '-i', interface,
                                '-s', '128', '-B', '4096', '-U', '-w', '-',
                                'tcp and port 443 and ' + address_filter],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: capture.terminate())
    signal.signal(signal.SIGINT, lambda *_: capture.terminate())
    timer = threading.Timer(args.hours * 3600, capture.terminate)
    timer.daemon = True
    timer.start()

    def capture_status():
        for raw in capture.stderr:
            for row in capture_counters(raw):
                emit(row)

    def sockets():
        previous = {}
        tick = 0
        while not stop.is_set():
            began = time.monotonic()
            for camera, address in targets.items():
                try:
                    result = subprocess.run(['nsenter', '-t', pid, '-n', 'ss', '-tinHeO', 'dst', address, 'dport', '=', ':443'],
                                            capture_output=True, text=True, timeout=2)
                    if result.returncode:
                        emit({'event': 'socket_probe_failed', 'camera': camera, 'returncode': result.returncode})
                        continue
                    rows = socket_rows(result.stdout, camera, address)
                    if not rows:
                        emit({'event': 'socket_absent', 'camera': camera})
                    for row in rows[:8]:
                        key = (camera, row['local_port'], row.get('socket_inode'))
                        old = previous.get(key)
                        if old:
                            for field in ('bytes_received', 'bytes_sent', 'rcv_ooopack', 'dsack_dups', 'local_tx_retrans_total'):
                                if field in old and field in row and row[field] >= old[field]:
                                    row[field + '_delta'] = row[field] - old[field]
                            row['sample_interval_ms'] = round((time.monotonic() - old['_monotonic']) * 1000, 3)
                        previous[key] = {**row, '_monotonic': time.monotonic()}
                        emit(row)
                    if len(previous) > 64:
                        previous.clear()
                except Exception as exc:
                    emit({'event': 'socket_probe_failed', 'camera': camera, 'error_type': type(exc).__name__})
            tick += 1
            if tick % 60 == 0 and capture.poll() is None:
                capture.send_signal(signal.SIGUSR1)
            stop.wait(max(0, 1 - (time.monotonic() - began)))

    threading.Thread(target=capture_status, daemon=True).start()
    threading.Thread(target=sockets, daemon=True).start()
    emit({'event': 'network_observer_started', 'container_pid': int(pid), 'hours': args.hours,
          'cameras': list(targets), 'snaplen': 128, 'ring_max_bytes': 8*1024*1024})
    observer = Headers(targets, emit)
    try:
        header = capture.stdout.read(24)
        endian = {b'\xd4\xc3\xb2\xa1': '<', b'\xa1\xb2\xc3\xd4': '>'}.get(header[:4])
        if endian is None or len(header) != 24:
            raise RuntimeError('invalid_pcap_header')
        linktype = struct.unpack(endian + 'I', header[20:24])[0]
        if linktype not in (1, 113, 276):
            raise RuntimeError('unsupported_capture_linktype')
        emit({'event': 'capture_ready', 'linktype': linktype})
        while True:
            header = capture.stdout.read(16)
            if len(header) != 16:
                break
            sec, micros, length, _ = struct.unpack(endian + 'IIII', header)
            if length > 128:
                raise RuntimeError('capture_exceeds_snaplen')
            packet = capture.stdout.read(length)
            if len(packet) != length:
                break
            observer.packet(packet, linktype, sec + micros/1e6)
    finally:
        stop.set()
        timer.cancel()
        capture.terminate()
        capture.wait(timeout=5)
        observer.flush()
        emit({'event': 'network_observer_stopped', 'capture_returncode': capture.returncode})


if __name__ == '__main__':
    main()

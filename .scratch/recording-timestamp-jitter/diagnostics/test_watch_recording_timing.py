"""Run with python3 -m unittest discover -s THIS_DIRECTORY -p 'test_watch*'."""

import json
import struct
import unittest

from watch_recording_timing import Flow, Headers


def frame(ts=16000, sequence=1, ssrc=7):
    rtp = struct.pack('!BBHII', 128, 97, sequence, ts, ssrc) + b'PRIVATE_CAMERA_PAYLOAD'
    return b'$\x02' + len(rtp).to_bytes(2, 'big') + rtp


def packet(payload, seq=100):
    ethernet = bytes(12) + b'\x08\x00'
    ip = bytearray(20)
    ip[0], ip[9] = 0x45, 6
    tcp = bytearray(20)
    struct.pack_into('!HHI', tcp, 0, 8554, 12345, seq)
    tcp[12] = 0x50
    return ethernet + ip + tcp + payload


class ObserverTests(unittest.TestCase):
    def test_fragmentation_and_retransmission(self):
        flow = Flow()
        data = frame()
        self.assertEqual(flow.feed(100, data[:8]), ([], False))
        frames, gap = flow.feed(104, data[4:])
        self.assertFalse(gap)
        self.assertEqual(len(frames), 1)
        self.assertEqual(flow.feed(100, data), ([], False))

    def test_jump_signs_and_no_payload_or_rtsp_text_in_logs(self):
        output = []
        observer = Headers(output.append)
        data = (b'RTSP/1.0 200 OK\r\nSECRET_AUTH\r\n\r\n' + frame(1000000, 1)
                + frame(1320000, 2) + frame(680000, 3))
        observer.packet(packet(data), 10.1)
        observer.packet(packet(frame(681024, 4), 100+len(data)), 11.1)
        self.assertEqual(output[0]['max_step'], 320000)
        self.assertEqual(output[0]['min_step'], -640000)
        self.assertEqual(output[0]['packets'], 3)
        logged = json.dumps(output)
        self.assertNotIn('PRIVATE', logged)
        self.assertNotIn('SECRET', logged)

    def test_tcp_capture_gap_is_explicit(self):
        observer = Headers(lambda value: events.append(value))
        events = []
        observer.packet(packet(frame()), 10.1)
        observer.packet(packet(frame(17024, 2), 500), 10.2)
        self.assertEqual(events[0]['event'], 'tcp_capture_gap')

    def test_rtp_timestamp_wrap_is_not_large_negative_jump(self):
        events = []
        observer = Headers(events.append)
        data = frame(2**32-512, 1) + frame(512, 2)
        observer.packet(packet(data), 10.1)
        bucket = next(iter(observer.stats.values()))
        self.assertEqual(bucket['max_step'], 1024)
        self.assertEqual(bucket['min_step'], 0)

    def test_ipv6_loopback(self):
        events = []
        observer = Headers(events.append)
        original = packet(frame())
        ip = bytearray(40)
        ip[0], ip[6] = 0x60, 6
        ipv6 = bytes(12) + b'\x86\xdd' + ip + original[34:]
        observer.packet(ipv6, 10.1)
        self.assertEqual(next(iter(observer.stats.values()))['packets'], 1)


if __name__ == '__main__':
    unittest.main()

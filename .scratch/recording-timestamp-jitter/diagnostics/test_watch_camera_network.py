import importlib.util
import json
import socket
import struct
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile
import io
import types

spec=importlib.util.spec_from_file_location('network',Path(__file__).with_name('watch_camera_network.py'))
network=importlib.util.module_from_spec(spec);spec.loader.exec_module(network)

def packet(seq=100, length=100, incoming=True, ack=0, options=b'', flags=16, window=4096, payload=b''):
    src,dst=('192.0.2.40','192.0.2.1') if incoming else ('192.0.2.1','192.0.2.40')
    sport,dport=(443,50000) if incoming else (50000,443)
    thl=20+len(options)
    tcp=struct.pack('!HHIIBBHHH',sport,dport,seq,ack,(thl//4)<<4,flags,window,0,0)+options
    ip=struct.pack('!BBHHHBBH4s4s',0x45,0,20+thl+length,0,0,64,6,0,socket.inet_aton(src),socket.inet_aton(dst))
    return b'\x00'*12+b'\x08\x00'+ip+tcp+payload

class Tests(unittest.TestCase):
    def test_snaplen_does_not_change_wire_data_length(self):
        p=network.tcp_header(packet(length=64000),1)
        self.assertEqual(p['data_bytes'],64000)
        self.assertNotIn('payload',p)

    def test_gap_across_seconds_and_no_payload_retained(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        h.packet(packet(payload=b'CANARY_SECRET'),1,10.9)
        h.packet(packet(seq=200),1,12.1)
        h.flush()
        gaps=[x for x in out if x['event']=='tcp_receive_gap']
        self.assertEqual(len(gaps),1)
        self.assertEqual(gaps[0]['gap_ms'],1200)
        self.assertNotIn('CANARY',json.dumps(out))
        self.assertNotIn('192.0.2.',json.dumps(out))

    def test_sequence_hole_late_fill_and_repeated_range(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        for index,seq in enumerate([100,300,200,200]):h.packet(packet(seq=seq),1,10+index*.01)
        h.flush();row=out[-1]
        self.assertEqual(row['forward_seq_gap_observations'],1)
        self.assertEqual(row['overlap_or_reorder_segments'],2)
        self.assertEqual(row['repeated_range_segments'],1)

    def test_wrap_is_not_a_gap(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        h.packet(packet(seq=0xffffffce,length=50),1,10)
        h.packet(packet(seq=0,length=100),1,10.01)
        h.flush()
        self.assertEqual(out[-1]['forward_seq_gap_observations'],0)
        self.assertEqual(out[-1]['overlap_or_reorder_segments'],0)

    def test_sack_and_receiver_backpressure(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        options=b'\x01\x01\x05\x0a'+struct.pack('!II',300,400)
        p=packet(incoming=False,length=0,ack=200,options=options,window=0)
        h.packet(p,1,10);h.packet(p,1,10.1);h.flush()
        self.assertEqual(out[-1]['sack_packets'],2)
        self.assertEqual(out[-1]['sack_above_ack_packets'],2)
        self.assertEqual(out[-1]['dsack_packets'],0)
        self.assertEqual(out[-1]['zero_window_packets'],2)
        self.assertEqual(out[-1]['ack_repeats'],1)
        self.assertEqual(out[-1]['last_sack_blocks'],[[300,400]])

    def test_duplicate_sack_is_separate_from_missing_data_sack(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        options=b'\x01\x01\x05\x0a'+struct.pack('!II',100,200)
        h.packet(packet(incoming=False,length=0,ack=300,options=options),1,10)
        h.flush()
        self.assertEqual(out[-1]['dsack_packets'],1)
        self.assertEqual(out[-1]['sack_above_ack_packets'],0)
        self.assertEqual(out[-1]['last_sack_ack'],300)

    def test_linux_cooked_links_and_bad_packets(self):
        eth=packet()
        for kind,head in [(113,b'\x00'*14+b'\x08\x00'),(276,b'\x08\x00'+b'\x00'*18)]:
            self.assertEqual(network.tcp_header(head+eth[14:],kind)['seq'],100)
        for truncated in [b'',eth[:20],eth[:40]]:self.assertIsNone(network.tcp_header(truncated,1))

    def test_ss_whitelist_counters_and_missing_values(self):
        text='ESTAB 12 0 192.0.2.1:50000 192.0.2.40:443 ino:42 sk:SECRET cgroup:SECRET rtt:2.5/1.2 rto:201 bytes_received:12345 rcv_ooopack:4 retrans:0/2 https://user:SECRET@camera/'
        rows=network.socket_rows(text,'camera','192.0.2.40')
        self.assertEqual(rows[0]['recv_queue_bytes'],12)
        self.assertEqual(rows[0]['rtt_ms'],2.5)
        self.assertEqual(rows[0]['local_tx_retrans_total'],2)
        self.assertEqual(rows[0]['rcv_ooopack'],4)
        self.assertNotIn('SECRET',json.dumps(rows))
        self.assertNotIn('bytes_retrans',rows[0])
        self.assertEqual(network.socket_rows(text,'other','192.0.2.41'),[])

    def test_periodic_and_exit_capture_statistics(self):
        periodic=b'tcpdump: 100 packets captured, 101 packets received by filter, 0 packets dropped by kernel\n'
        rows=network.capture_counters(periodic)
        self.assertEqual([r['value'] for r in rows],[100,101,0])
        self.assertEqual(rows[-1]['counter'],'dropped_by_kernel')
        self.assertEqual(network.capture_counters(b'7 packets dropped by kernel\n')[0]['value'],7)
        self.assertEqual(network.capture_counters(b'filter description SECRET'),[])

    def test_syn_clears_old_connection_state(self):
        out=[];h=network.Headers({'camera':'192.0.2.40'},out.append)
        h.packet(packet(seq=100),1,10)
        h.packet(packet(seq=500,flags=2,length=0),1,100)
        h.packet(packet(seq=501),1,100.1);h.flush()
        self.assertFalse(any(x['event']=='tcp_receive_gap' for x in out))

class MainTest(unittest.TestCase):
    def test_real_capture_loop_freezes_initial_and_followup_without_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);targets=root/'targets.json'
            targets.write_text(json.dumps({'camera':'192.0.2.40'}))
            timing=root/'timing';timing.mkdir();(timing/'timing.jsonl').write_text('{"event":"source"}\n')
            pcap=struct.pack('<IHHIIII',0xa1b2c3d4,2,4,0,0,128,1)
            for sec,micro,seq in [(10,900000,100),(12,100000,200)]:
                data=packet(seq=seq,payload=b'CANARY_SECRET')
                pcap+=struct.pack('<IIII',sec,micro,len(data),len(data))+data
            capture=types.SimpleNamespace(stdout=io.BytesIO(pcap),stderr=io.BytesIO(),returncode=0,terminate=lambda:None,wait=lambda timeout:0)
            class Timer:
                def __init__(self,seconds,fn,args=()):self.seconds,self.fn,self.args=seconds,fn,args
                def start(self):
                    if self.seconds==60:self.fn(*self.args)
                def cancel(self):pass
            def output(command,**kwargs):
                return '123' if command[0]=='podman' else '[{"dev":"eth0"}]'
            with patch.object(network.subprocess,'check_output',side_effect=output),patch.object(network.subprocess,'Popen',return_value=capture),patch.object(network.threading,'Timer',Timer),patch.object(network.threading.Thread,'start'),patch.object(network.signal,'signal'),patch('sys.argv',['observer','--targets',str(targets),'--output',str(root/'network.jsonl'),'--timing-directory',str(timing)]):
                network.main()
            rows=[json.loads(line) for line in (root/'network.jsonl').read_text().splitlines()]
            phases=[r['phase'] for r in rows if r['event']=='first_stall_preserved']
            self.assertEqual(phases,['initial','post_stall'])
            self.assertTrue((root/'first-stall/recording-timing/timing.jsonl').exists())
            self.assertNotIn('CANARY', (root/'network.jsonl').read_text())
            for handler in list(network.logging.getLogger('camera-network').handlers):
                handler.close();network.logging.getLogger('camera-network').removeHandler(handler)

if __name__=='__main__':unittest.main()

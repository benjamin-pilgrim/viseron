#!/usr/bin/env python3
"""Real HTTP FLV -> patched go2rtc -> FFmpeg; synthetic picture/tone only."""
import http.server,threading,subprocess,tempfile,pathlib,time,json,urllib.request,os,sys,signal
root=pathlib.Path(tempfile.mkdtemp(prefix='viseron-flv-shadow-'))
fixture=root/'source.flv'
subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','testsrc2=size=160x120:rate=20','-f','lavfi','-i','sine=frequency=440:sample_rate=16000','-t','5','-c:v','libx264','-preset','ultrafast','-bf','0','-g','20','-c:a','aac','-ar','16000','-f','flv',str(fixture)],check=True,timeout=20)
b=fixture.read_bytes();tags=[];offset=13
while offset+11<=len(b):
 header=bytearray(b[offset:offset+11]);n=int.from_bytes(header[1:4],'big');body=b[offset+11:offset+11+n];previous=b[offset+11+n:offset+15+n]
 if len(previous)<4:break
 timestamp=int.from_bytes(header[4:7],'big')+(header[7]<<24)
 if header[0] in (8,9) and timestamp>=2000:
  shifted=timestamp+10000;header[4:7]=(shifted&0xffffff).to_bytes(3,'big');header[7]=(shifted>>24)&255
 tags.append((timestamp,bytes(header)+body+previous));offset+=15+n
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*_):pass
 def do_GET(self):
  self.send_response(200);self.send_header('Content-Type','video/x-flv');self.end_headers()
  try:
   self.wfile.write(b[:13]);self.wfile.flush();start=time.monotonic()
   for timestamp,data in tags:
    target=timestamp/1000+(1 if timestamp>=2000 else 0)
    delay=target-(time.monotonic()-start)
    if delay>0:time.sleep(delay)
    self.wfile.write(data);self.wfile.flush()
  except (BrokenPipeError,ConnectionResetError):pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
config=root/'go2rtc.yaml';config.write_text('api:\n  listen: "127.0.0.1:19844"\nrtsp:\n  listen: "127.0.0.1:18554"\nwebrtc:\n  listen: ""\nstreams:\n  synthetic: http://127.0.0.1:'+str(server.server_port)+'/source.flv\n')
env={**os.environ,'VISERON_FLV_TIMING_DIAGNOSTICS':'1'}
with (root/'go2rtc.log').open('w') as logs:
 process=subprocess.Popen([sys.argv[1] if len(sys.argv)>1 else '/tmp/viseron-go2rtc-flv-diagnostics','-config',str(config)],env=env,stdout=logs,stderr=logs)
 try:
  for _ in range(40):
   try:
    urllib.request.urlopen('http://127.0.0.1:19844/api/diagnostics/flv-timing',timeout=1).close();break
   except OSError:time.sleep(.1)
  recorder=subprocess.Popen(['ffmpeg','-v','error','-rtsp_transport','tcp','-analyzeduration','500000','-probesize','1000000','-i','rtsp://127.0.0.1:18554/synthetic','-map','0:v','-map','0:a','-c','copy','-movflags','+frag_keyframe+empty_moov','-y',str(root/'recording.mp4')],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
  try:recorder.communicate(timeout=8)
  except subprocess.TimeoutExpired:
   recorder.send_signal(signal.SIGINT)
   try:recorder.communicate(timeout=5)
   except subprocess.TimeoutExpired:
    recorder.kill();recorder.communicate();raise

  with urllib.request.urlopen('http://127.0.0.1:19844/api/diagnostics/flv-timing',timeout=2) as response:data=json.load(response)
  records=data['records'] if isinstance(data,dict) else data
  source=[x for x in records if x['event']=='flv_second']
  assert any(x['max_flv_step_ms']>=10000 for x in source),source
  assert any(x['max_arrival_gap_ms']>=800 for x in source),source
  probe=subprocess.run(['ffprobe','-v','error','-show_packets','-show_entries','packet=stream_index,pts_time,duration_time','-of','json',str(root/'recording.mp4')],capture_output=True,text=True,check=True,timeout=10)
  packets=json.loads(probe.stdout)['packets'];summary={}
  for index in (0,1):
   p=[x for x in packets if x['stream_index']==index and 'pts_time' in x]
   summary[str(index)]={'packets':len(p),'start':min(float(x['pts_time']) for x in p),'end':max(float(x['pts_time'])+float(x.get('duration_time',0)) for x in p),'duration_sum':sum(float(x.get('duration_time',0)) for x in p)}
  assert summary['0']['end']-summary['1']['end']>5,summary
  (root/'timing.json').write_text(json.dumps(data,indent=2))
  print(json.dumps({'passed':True,'root':str(root),'ffmpeg_returncode':recorder.returncode,'media':summary,'source_max_step_ms':max(x['max_flv_step_ms'] for x in source),'source_max_arrival_gap_ms':max(x['max_arrival_gap_ms'] for x in source)},indent=2))
 finally:
  process.terminate();process.wait(timeout=5);server.shutdown()

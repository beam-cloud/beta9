import sys,pathlib,subprocess,time,json
import argparse
parser=argparse.ArgumentParser(description="Capture native startup/pause profiles on the dedicated VM test node")
parser.add_argument('--port',required=True);parser.add_argument('--phase',choices=('startup','pause'),required=True);parser.add_argument('--seconds',type=int,required=True);parser.add_argument('--output',type=pathlib.Path,required=True);parser.add_argument('--worker-container',required=True);parser.add_argument('--network-interface',default='enp1s0')
args=parser.parse_args();port,label,seconds=args.port,args.phase,args.seconds;root=args.output;root.mkdir(parents=True,exist_ok=False);base='http://127.0.0.1:'+port+'/debug/pprof/';jobs=[];helpers=pathlib.Path(__file__).resolve().parent

def log_queue_snapshot(name):
 output=subprocess.check_output(['curl','-fsS','--max-time','3','http://127.0.0.1:'+port+'/debug/log-queue'],text=True)
 data=json.loads(output);assert data['byte_limit']==8<<20 and data['chunk_limit']==4096 and not data['sink_error'];(root/name).write_text(json.dumps(data,indent=2))
log_queue_snapshot('log-queue-before.json')
def start_record(command, stdout, stderr):
 import os,errno
 stem='cpu' if any(str(arg).endswith('perf.data')for arg in command)else 'sync'
 control,ack=root/(stem+'.control'),root/(stem+'.ack')
 os.mkfifo(control);os.mkfifo(ack)
 ackfd=os.open(ack,os.O_RDONLY|os.O_NONBLOCK)
 command[2:2]=['--clockid','mono','--delay=-1','--control=fifo:'+str(control)+','+str(ack)]
 proc=subprocess.Popen(command,stdout=stdout,stderr=stderr)
 deadline=time.monotonic()+15;controlfd=None
 try:
  while controlfd is None:
   if proc.poll()is not None:raise RuntimeError(stem+' profiler exited before readiness')
   try:controlfd=os.open(control,os.O_WRONLY|os.O_NONBLOCK)
   except OSError as error:
    if error.errno!=errno.ENXIO:raise
    if time.monotonic()>deadline:raise TimeoutError(stem+' profiler control readiness')
    time.sleep(.01)
  os.write(controlfd,b'enable\n');received=b''
  while b'ack'not in received:
   try:received+=os.read(ackfd,1024)
   except BlockingIOError:pass
   if proc.poll()is not None:raise RuntimeError(stem+' profiler ended before enable acknowledgement')
   if time.monotonic()>deadline:raise TimeoutError(stem+' profiler enable acknowledgement')
   time.sleep(.01)
 finally:
  os.close(ackfd)
  if controlfd is not None:os.close(controlfd)
 return proc
bpf=None;fuse_bpf=None
if label=='startup':
 bpf_out=(root/'hypervisor-boot.jsonl').open('w');bpf=subprocess.Popen(['bpftrace','-q','-B','none',str(helpers/'microvm-hypervisor-boot.bt')],stdout=bpf_out,stderr=(root/'hypervisor-boot.stderr').open('w'));deadline=time.monotonic()+15
 while True:
  if bpf.poll()is not None:raise RuntimeError('Hypervisor boot trace failed to attach')
  if (root/'hypervisor-boot.jsonl').stat().st_size:break
  if time.monotonic()>deadline:bpf.terminate();raise TimeoutError('Hypervisor tracing readiness')
  time.sleep(.05)
 assert json.loads((root/'hypervisor-boot.jsonl').read_text().splitlines()[0]).get('ready')
 fuse_out=(root/'fuse-response.jsonl').open('w');fuse_bpf=subprocess.Popen(['bpftrace','-q','-f','json','-B','none',str(helpers/'microvm-fuse-ops.bt')],stdout=fuse_out,stderr=(root/'fuse-response.stderr').open('w'));deadline=time.monotonic()+15
 while True:
  if fuse_bpf.poll()is not None:raise RuntimeError('FUSE response profiler failed to attach')
  if (root/'fuse-response.jsonl').stat().st_size:break
  if time.monotonic()>deadline:fuse_bpf.terminate();raise TimeoutError('FUSE profiler readiness')
  time.sleep(.05)
 message=json.loads((root/'fuse-response.jsonl').read_text().splitlines()[0]);assert json.loads(message['data']).get('ready')

pid=subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',args.worker_container],text=True).strip()
for name,endpoint in [('cpu.pprof','profile'),('block.pprof','block'),('mutex.pprof','mutex')]:
 log=(root/(name+'.stderr')).open('w');jobs.append(subprocess.Popen(['curl','-fsS','--max-time',str(seconds+12),base+endpoint+'?seconds='+str(seconds),'-o',str(root/name)],stdout=subprocess.DEVNULL,stderr=log))
if label=='startup':
 trace_log=(root/'runtime.trace.stderr').open('w');jobs.append(subprocess.Popen(['curl','-fsS','--max-time',str(seconds+12),base+'trace?seconds='+str(seconds),'-o',str(root/'runtime.trace')],stdout=subprocess.DEVNULL,stderr=trace_log))
wire=(root/'readiness-syn-headers.log').open('w');capture=subprocess.Popen(['tcpdump','-nn','-tt','-l','-i','any','-s','96','tcp port 7111 and tcp[tcpflags] & (tcp-syn|tcp-rst) != 0'],stdout=wire,stderr=(root/'readiness-syn-headers.stderr').open('w'))
sync_log=(root/'sync-trace.stderr').open('w');jobs.append(start_record(['perf','record','--quiet','-a','-e','syscalls:sys_enter_fsync,syscalls:sys_exit_fsync,syscalls:sys_enter_fdatasync,syscalls:sys_exit_fdatasync,ext4:ext4_sync_file_enter,ext4:ext4_sync_file_exit,jbd2:jbd2_start_commit','-o',str(root/'sync.data'),'--','sleep',str(seconds)],stdout=sync_log,stderr=sync_log))
stat_log=(root/'kvm-events.log').open('w');jobs.append(subprocess.Popen(['perf','stat','-a','-e','kvm:kvm_pio,kvm:kvm_exit','--','sleep',str(seconds)],stdout=stat_log,stderr=stat_log))
perf_log=(root/'perf.log').open('w');jobs.append(start_record(['perf','record','--quiet','-a','-F','99','-g','-o',str(root/'perf.data'),'--','sleep',str(seconds)],stdout=perf_log,stderr=perf_log));time.sleep(.15);(root/'clock.json').write_text(json.dumps({'perf_clock':'monotonic','ready_time_ns':time.time_ns(),'ready_monotonic_ns':time.monotonic_ns()}));print(json.dumps({'ready':True,'label':label,'seconds':seconds}),flush=True)
pid=subprocess.check_output(['docker','inspect','--format','{{.State.Pid}}',args.worker_container],text=True).strip()
worker_group=pathlib.Path('/sys/fs/cgroup'+pathlib.Path('/proc/'+pid+'/cgroup').read_text().strip().split('::',1)[1])
def fuse_mounts():
 rows=[]
 for line in pathlib.Path('/proc/'+pid+'/mountinfo').read_text().splitlines():
  fields,suffix=line.split(' - ',1);fields=fields.split();kind=suffix.split()[0]
  if kind.startswith('fuse'):rows.append({'device':fields[2],'mount':fields[4],'type':kind})
 return rows
(root/'fuse-mounts-before.json').write_text(json.dumps(fuse_mounts(),indent=2))

def worker_cpu():
 return {name:worker_group.joinpath(name).read_text().strip() for name in ('cpu.stat','cpu.pressure','cpu.weight')}
def tcpstats():
 result={}
 for filename in ('/proc/net/snmp','/proc/net/netstat'):
  lines=pathlib.Path(filename).read_text().splitlines()
  for i in range(0,len(lines)-1,2):
   names=lines[i].split();values=lines[i+1].split()
   if names[0] not in ('Tcp:','TcpExt:'):continue
   wanted={'ActiveOpens','CurrEstab','RetransSegs','TCPTimeouts','TCPLossProbes','TCPBacklogDrop','TCPRcvQDrop','TCPAbortOnMemory','TCPAbortOnTimeout','TCPFastRetrans','TCPMemoryPressures'}
   result.update({names[0]+name:int(value)for name,value in zip(names[1:],values[1:])if name in wanted})
 return result
tcp_log=(root/'tcp-sockets.jsonl').open('w');last_tcp_sample=0
keys={'pgfault','pgmajfault','pgpgin','pgpgout','compact_stall','compact_fail','compact_success','allocstall_normal','allocstall_movable','pgscan_kswapd','pgscan_direct'};end=time.monotonic()+seconds
with (root/'sysstat.jsonl').open('w') as out:
 while time.monotonic()<end:
  if label=='pause' and time.monotonic()-last_tcp_sample>.4:
   sample=subprocess.run(['ss','-tniH','state','established','( dport = :443 )'],text=True,capture_output=True,timeout=2);tcp_log.write(json.dumps({'time_ns':time.time_ns(),'sockets':sample.stdout})+'\n');tcp_log.flush();last_tcp_sample=time.monotonic()
  v={k:int(n)for k,n in (x.split()for x in open('/proc/vmstat')) if k in keys};network={k:int(pathlib.Path('/sys/class/net/'+args.network_interface+'/statistics/'+k).read_text())for k in ('rx_bytes','tx_bytes')};disks={p.name:p.joinpath('stat').read_text().strip()for p in pathlib.Path('/sys/block').iterdir()if p.name.startswith(('nvme','md','dm-'))};out.write(json.dumps({'time_ns':time.time_ns(),'monotonic_ns':time.monotonic_ns(),'cpu':open('/proc/stat').readline().strip(),'worker_cpu':worker_cpu(),'io_pressure':pathlib.Path('/proc/pressure/io').read_text().strip(),'vmstat':v,'network':network,'tcp':tcpstats(),'disks':disks})+'\n');out.flush();time.sleep(.1)

if bpf is not None:
 identities=[]
 for event in (json.loads(line)for line in (root/'hypervisor-boot.jsonl').read_text().splitlines()if line.strip().startswith('{')):
  if event.get('event')!='exec':continue
  proc=pathlib.Path('/proc')/str(event['pid'])
  try:
   status=proc.joinpath('status').read_text();nspid=next(line.split(':',1)[1].split()for line in status.splitlines()if line.startswith('NSpid:'));cgroup=proc.joinpath('cgroup').read_text().strip();cgpath=pathlib.Path('/sys/fs/cgroup'+cgroup.split('::',1)[1]);limits={name:cgpath.joinpath(name).read_text().strip()for name in ('cpu.max','cpu.weight','memory.max')};identities.append({'host_pid':event['pid'],'namespace_pids':[int(p)for p in nspid],'cgroup':cgroup,'post_ready_limits':limits})
  except (FileNotFoundError,StopIteration):identities.append({'host_pid':event['pid'],'gone':True})
 (root/'vm-process-identities.json').write_text(json.dumps(identities,indent=2))
 import signal
 bpf.send_signal(signal.SIGINT);assert bpf.wait(timeout=8)==0;bpf_out.close()
 fuse_bpf.send_signal(signal.SIGINT);assert fuse_bpf.wait(timeout=8)==0;fuse_out.close();(root/'fuse-mounts-after.json').write_text(json.dumps(fuse_mounts(),indent=2))
capture.terminate();capture.wait(timeout=5);wire.close();results=[p.wait(timeout=15) for p in jobs];print(json.dumps({'complete':True,'label':label,'exit_codes':results}),flush=True);assert not any(results),results

log_queue_snapshot('log-queue-after.json')

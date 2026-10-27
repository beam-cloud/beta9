/* Optical input probe for an ordinary streamed app, installed before its client.
 * Samples decoded pixels; no application adapter or server timestamps required.
 */
(() => {
  const samples = [], pending = [], events=[], unexpectedInputs=[];
  let armedAction=null;
  const transport={samples:[],channel:null,pending:new Map(),inputs:[],inputPending:new Map(),trace:false,seq:0};
  if(window.RTCPeerConnection) {
    const remote=RTCPeerConnection.prototype.setRemoteDescription, seen=new WeakSet();
    RTCPeerConnection.prototype.setRemoteDescription=function(...args){
      if(!seen.has(this)) {
        seen.add(this);
        this.addEventListener('datachannel',({channel})=>{
          if(channel.label!=='input')return;
          transport.channel=channel;
          channel.addEventListener('message',event=>{
            if(typeof event.data!=='string')return;
            try {
              const m=JSON.parse(event.data);
              if(m.type==='beam_cpu_input') {
                const item=transport.inputPending.get(m.data.seq);
                if(item)transport.inputs.push({...item,...m.data,ack_ms:performance.now()});
                transport.inputPending.delete(m.data.seq);return;
              }
              const start=transport.pending.get(m.data?.nonce);
              if(m.type!=='beam_cpu_bench'||start===undefined)return;
              const end=performance.now();
              transport.samples.push({start_ms:start,end_ms:end,rtt_ms:end-start,server_ms:m.data.server_ms,wire:m.data.wire});
              transport.pending.delete(m.data.nonce);
            } catch(_) {}
          });
        });
      }
      return remote.apply(this,args);
    };
  }
  if(window.RTCDataChannel) {
    const send=RTCDataChannel.prototype.send;
    RTCDataChannel.prototype.send=function(data){
      if(transport.trace && this.label==='input' && typeof data==='string' && /^(kd|ku|m),/.test(data)) {
        const seq=String(++transport.seq);
        transport.inputPending.set(seq,{sent_ms:performance.now(),kind:data.split(',',1)[0]});
        send.call(this,'BEAM_CPU_MARK,'+seq);
      }
      return send.call(this,data);
    };
  }
  transport.ping=()=>{
    const nonce=String(performance.now());transport.pending.set(nonce,performance.now());
    transport.channel.send('BEAM_CPU_BENCH,'+nonce);
  };
  window.beamInteractionTransport=transport;
  let video, callback, observer, lastChange=0, expired=0;
  const frames=[], comparisons={}, probeCosts=[];
  const primary={rect:[0,0,1,1],previous:null,pending,samples};
  const regions=()=>[primary,...Object.values(comparisons)];
  const canvas = document.createElement('canvas');
  canvas.width = 64; canvas.height = 36;
  const context = canvas.getContext('2d', {willReadFrequently:true});
  const pixels = rect => {
    const width=video.videoWidth||video.width,height=video.videoHeight||video.height;
    context.drawImage(video,width*rect[0],height*rect[1],width*rect[2],height*rect[3],0,0,64,36);
    return context.getImageData(0,0,64,36).data;
  };
  const distance = (a,b) => {
    let difference=0;
    for (let i=0;i<a.length;i+=4) difference+=Math.abs(a[i]-b[i])+Math.abs(a[i+1]-b[i+1])+Math.abs(a[i+2]-b[i+2]);
    return difference/(64*36*3);
  };
  const input = event => {
    if (event.type === 'keydown' && ['Shift','Control','Alt','Meta'].includes(event.key)) return;
    const at=performance.now();
    const expected=armedAction;
    if(!expected || expected.type!==event.type || expected.key!==null && expected.key.toLowerCase()!==event.key?.toLowerCase()) {
      unexpectedInputs.push({at,type:event.type,trusted:event.isTrusted});
      return;
    }
    armedAction=null;
    events.push({at,type:event.type,key:event.key,repeat:event.repeat});
    if(events.length>100)events.shift();
    for(const r of regions()) {
      if(!r.previous)continue;
      r.pending.push({at,image:r.previous,type:event.type,key:event.key});
      if(r.pending.length>100 && !r.pending.shift().answered && r===primary)expired++;
    }
  };
  for (const name of ['keydown','mousedown','wheel']) window.addEventListener(name,input,{capture:true,passive:true});
  const schedule=()=>video.requestVideoFrameCallback?
    video.requestVideoFrameCallback(frame):requestAnimationFrame(time=>frame(time,{}));
  const cancel=()=>video.requestVideoFrameCallback?
    video.cancelVideoFrameCallback(callback):cancelAnimationFrame(callback);
  const available=()=>video && (video.videoWidth||video.width)>0 &&
    (video.tagName!=='VIDEO'||video.readyState>=2);
  const frame = (_, metadata) => {
    if(!available()){callback=schedule();return;}
    const now = performance.now();
    if(frames.length<20000)frames.push(now);
    const sampleStart=performance.now();
    for(const r of regions()) {
      const current=pixels(r.rect);
      if(r.previous && distance(current,r.previous)>.7)lastChange=now;
      for(const item of r.pending) {
        if(item.answered || distance(current,item.image)<=.7)continue;
        if(item.firstChange===undefined)item.firstChange=now;
        // Capture-time clock mapping can move during RTCP synchronization.
        // Use observed pixels for latency; retain that estimate as telemetry.
        // Actions are serialized and the ROI settles before the next input.
        item.answered=true;
        r.samples.push({type:item.type,key:item.key,input_at_ms:item.at,latency_ms:now-item.at,
          first_visible_ms:item.firstChange-item.at,
          receive_to_present_ms:metadata.receiveTime===undefined?null:metadata.presentationTime-metadata.receiveTime,
          capture_to_present_ms:metadata.captureTime===undefined?null:metadata.presentationTime-metadata.captureTime,
          decode_ms:metadata.processingDuration===undefined?null:metadata.processingDuration*1000});
      }
      while(r.pending.length && (r.pending[0].answered || now-r.pending[0].at>5000)) {
        if(!r.pending.shift().answered && r===primary)expired++;
      }
      r.previous=current;
    }
    if(probeCosts.length<20000)probeCosts.push(performance.now()-sampleStart);
    callback=schedule();
  };
  const attach = () => {
    const current=[...document.querySelectorAll('video#stream,video#videoStream,canvas#videoCanvas')].find(s=>s.getBoundingClientRect().width>0);
    if(!current || current===video)return;
    if(video && callback)cancel();
    video=current;
    for(const r of regions())r.previous=null;
    callback=schedule();
  };
  observer=new MutationObserver(attach);
  observer.observe(document,{childList:true,subtree:true});attach();
  window.beamInteractionProbe={samples,pending,events,frames,comparisons,probeCosts,unexpectedInputs,
    arm(type,key=null){armedAction={type,key};},
    get unanswered(){return expired+pending.filter(item=>!item.answered).length;},
    get settledForMs(){return performance.now()-lastChange;},
    setRegion(rect){primary.rect=rect;primary.previous=available()?pixels(rect):null;},
    addRegion(name,rect){comparisons[name]={rect,previous:available()?pixels(rect):null,pending:[],samples:[]};},
    expire(){
      armedAction=null;
      for(const r of regions()) {
        if(r===primary)for(const item of r.pending)if(!item.answered)expired++;
        r.pending.length=0;
      }
    },
    reset(){
      for(const r of regions()){r.samples.length=0;r.pending.length=0;}
      events.length=0;frames.length=0;probeCosts.length=0;unexpectedInputs.length=0;armedAction=null;expired=0;
    },
    stop(){
      observer.disconnect();
      if(video && callback)cancel();
      for(const name of ['keydown','mousedown','wheel'])window.removeEventListener(name,input,true);
      delete window.beamInteractionProbe;
    }
  };
})();

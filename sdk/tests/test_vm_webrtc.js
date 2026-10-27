const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
async function check(path) {
  const calls = [], messages = [];
  const storage = new Map([['selkies_tab_id', 'copied-from-another-tab']]);
  class Peer {
    constructor() { this.events = {}; this.connectionState = 'new'; }
    addEventListener(name, callback) { this.events[name] = callback; }
    async setRemoteDescription(value) { calls.push(['remote', value]); }
    async addIceCandidate(value) { calls.push(['candidate', value]); }
    close() { this.connectionState = 'closed'; }
  }
  const origin = 'https://application.example';
  let status = 200;
  const timers=new Map();
  const window = {RTCPeerConnection: Peer, devicePixelRatio: 1.5,
    sessionStorage: {setItem: (key, value) => storage.set(key, value)},
    crypto: {randomUUID: () => `page-${path}`}};
  vm.runInNewContext(source, {
    window, RTCPeerConnection: Peer,
    URL, AbortSignal, WeakSet,
    location: { href: origin + path + 'stream/', origin },
    parent: { postMessage(value, target) { messages.push([value, target]); } },
    setTimeout:callback=>{timers.set(1,callback);return 1;},clearTimeout:id=>timers.delete(id),
    fetch: async (url, options) => {
      assert.equal(url.pathname, path + '_beam/kiosk/webrtc/session');
      assert.equal(options.credentials, 'same-origin');
      assert.equal(options.headers['X-Beam-Kiosk-Action'], 'webrtc');
      assert.equal(JSON.parse(options.body).sdp, 'guest-description');
      return {ok: status === 200, status, json: async () => ({sdp: 'gateway-description'})};
    },
  });
  const viewer=window.beamKiosk;
  vm.runInNewContext(source,{window});
  assert.equal(window.beamKiosk,viewer,'duplicate bootstrap tags must not install another client');
  assert.equal(storage.get('selkies_tab_id'), `page-${path}`, 'copied session storage cannot share a controller identity');
  const peer = new Peer();
  const applying = peer.setRemoteDescription({type: 'offer', sdp: 'guest-description'});
  await peer.addIceCandidate({candidate: 'private-guest-address'});
  await applying;
  assert.equal(calls.length, 1);
  assert.equal(calls[0][1].sdp, 'gateway-description');
  assert.equal(calls[0][1].type, 'offer');
  const receiver = {jitterBufferTarget: 200, playoutDelayHint: 1};
  peer.events.track({receiver});
  assert.equal(receiver.jitterBufferTarget, 0);
  assert.equal(receiver.playoutDelayHint, 0);
  await peer.addIceCandidate(null);
  assert.equal(calls[1][0], 'candidate');
  const second = new Peer();
  await second.addIceCandidate({candidate:'unrouted'});
  assert.equal(calls[2][1].candidate, 'unrouted');
  status = 403;
  await assert.rejects(second.setRemoteDescription({type:'offer',sdp:'guest-description'}));
  const errorMessage=messages.find(item=>item[0].type==='beam-kiosk-transport-error');
  assert.equal(errorMessage[1], origin);
  // A superseded peer closing must not cover the healthy replacement viewer.
  const beforeClose=messages.length;
  peer.connectionState = 'closed'; peer.events.connectionstatechange();
  assert.equal(messages.length,beforeClose);
  second.connectionState = 'disconnected'; second.events.connectionstatechange();
  assert.equal(messages.at(-1)[0].state, 'disconnected');
  assert.equal(timers.size,1);
  second.connectionState='connected';second.events.connectionstatechange();
  assert.equal(timers.size,0,'a recovered connection cancels the document reload');
  // Native close() does not dispatch connectionstatechange. Polling must catch it.
  second.close();window.beamKiosk.syncConnection();
  assert.equal(timers.size,1,'a silently closed peer must schedule recovery');
  const recovery=timers.get(1);
  window.beamKiosk.syncConnection();
  assert.equal(timers.get(1),recovery,'polling cannot postpone recovery');
  status=200;
  await new Peer().setRemoteDescription({type:'offer',sdp:'guest-description'});
  assert.equal(timers.size,0,'a replacement peer cancels the previous recovery timer');
}
async function checkDisplay() {
  const messages=[],events={},videoEvents={},windowEvents={},timers=new Map();
  let focused=true;
  class Media {
    constructor() { Object.assign(this,{style:{objectFit:'fill'},id:'stream',width:1920,height:1080,videoWidth:0,videoHeight:0,paused:true,readyState:0,muted:false}); }
    play() {
      this.attempts=(this.attempts||0)+1;
      if(this.reject) { const name=this.reject;this.reject=null;return Promise.reject(Object.assign(Error(name),{name})); }
      this.paused=false;return Promise.resolve();
    }
    addEventListener(name,callback) { videoEvents[name]=callback; }
  }
  const video=new Media();
  class Observer { constructor(callback) { this.callback=callback; } observe(target,options) { if(options.attributes) videoEvents.dimensions=this.callback; } }
  const attributes=new Set();
  const document={documentElement:{hasAttribute:()=>false,toggleAttribute:(name,on)=>on?attributes.add(name):attributes.delete(name)},hasFocus:()=>focused,hidden:false,readyState:'loading',querySelector:()=>video,addEventListener:(name,fn)=>{events[name]=fn;},removeEventListener:(name)=>{delete events[name];}};
  const displayWindow={HTMLMediaElement:Media,innerWidth:960,innerHeight:540,addEventListener(name,fn){windowEvents[name]=fn;}};
  vm.runInNewContext(source,{
    window:displayWindow, HTMLMediaElement:Media, document, MutationObserver:Observer,URL,
    parent:{postMessage:value=>messages.push(value)},location:{origin:'https://application.example',href:'https://application.example/stream/'},
    setTimeout:callback=>{timers.set(1,callback);return 1;}, clearTimeout:id=>timers.delete(id),
  });
  video.play(); assert.equal(video.muted,true,'picture autoplays without requesting audio');
  video.width=1280;video.height=720;video.videoWidth=1280;video.videoHeight=720;video.readyState=2;videoEvents.playing();
  assert.equal(timers.size,0,'old-sized frames cannot reveal the application');
  video.width=1920;video.height=1080;video.videoWidth=1920;video.videoHeight=1080;
  displayWindow.innerWidth=displayWindow.innerHeight=0;videoEvents.resize();
  assert.equal(timers.size,0,'a zero-sized embedded viewer must wait for its viewport');
  displayWindow.innerWidth=960;displayWindow.innerHeight=540;videoEvents.resize();timers.get(1)();
  assert.equal(messages.at(-1).state,'ready');
  assert(attributes.has('data-beam-kiosk-playing'),'live video suppresses the stale native play overlay');
  video.paused=true;videoEvents.pause();
  assert(!attributes.has('data-beam-kiosk-playing'),'a genuine playback failure retains its recovery control');
  video.paused=false;videoEvents.playing();
  video.width=2560;videoEvents.dimensions();assert.equal(messages.at(-1).state,'ready','resize keeps the last picture visible');
  delete displayWindow.streamResolutionDiverged;videoEvents.resize();timers.get(1)();
  assert.equal(displayWindow.streamResolutionDiverged,true,'frame dimensions reconcile a viewer even before upstream bookkeeping arrives');
  assert.equal(displayWindow.beamKiosk.fitted(),true,'a second tab can display the owner\'s framebuffer');
  assert.equal(video.style.objectFit,'contain','shared frames keep their aspect ratio and native input mapping');
  displayWindow.innerWidth=0;assert.equal(displayWindow.beamKiosk.fitted(),false,'hidden shared viewers still wait');
  displayWindow.innerWidth=960;displayWindow.streamResolutionDiverged=false;
  video.id='videoStream';video.width=1920;
  displayWindow.beamKiosk.activePeer={connectionState:'failed'};
  assert.equal(displayWindow.beamKiosk.fitted(),true,'a healthy WebCodecs fallback is independent of the failed peer');
  events.pointerdown({isTrusted:false});assert.equal(video.muted,true);
  events.pointerdown({isTrusted:true});assert.equal(video.muted,false,'a real gesture enables application audio');
  const attempts=video.attempts;
  video.reject='NotAllowedError';await video.play();
  assert.equal(video.muted,true,'blocked audio retries playback muted');
  assert.equal(video.attempts,attempts+2);
  video.reject='AbortError';await assert.rejects(video.play(),{name:'AbortError'});
  const viewer=displayWindow.beamKiosk, sent=[];
  viewer.input={input:{inputAttached:true,send:message=>sent.push(message)}};
  viewer.activePeer.connectionState='connected';
  viewer.syncViewport();
  assert.equal(sent.at(-1),'KIOSK_VIEWPORT,1920x1080');
  viewer.syncViewport();assert.equal(sent.length,1,'unchanged frames do not repeat resize requests');
  focused=false;windowEvents.blur();
  displayWindow.innerWidth=1728;displayWindow.innerHeight=846;
  windowEvents.resize();timers.get(1)();
  assert.equal(sent.length,1,'background resize cannot take over the shared desktop');
  focused=true;windowEvents.focus();timers.get(1)();
  assert.equal(sent.at(-1),'KIOSK_VIEWPORT,3456x1692','the newly focused wide tab must own its native framebuffer');
  document.hidden=true;events.visibilitychange();viewer.syncViewport();
  assert.equal(sent.length,2,'hidden documents never resize');
  document.hidden=false;events.visibilitychange();timers.get(1)();
  assert.equal(sent.length,3,'returning tabs reclaim their viewport even when its dimensions are unchanged');
  displayWindow.innerWidth=0;viewer.syncViewport();assert.equal(sent.length,3);
  displayWindow.innerWidth=5000;displayWindow.innerHeight=10000;viewer.syncViewport();
  assert.equal(sent.at(-1),'KIOSK_VIEWPORT,4080x4080','viewport requests retain the pinned encoder limit');
}
(async()=>{for(const path of ['/', '/vm/app/8080/']) await check(path);await checkDisplay();})()
 .catch(error=>{console.error(error);process.exitCode=1;});

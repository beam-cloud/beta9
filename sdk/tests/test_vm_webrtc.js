const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
async function check(path) {
  const calls = [], messages = [];
  class Peer {
    constructor() { this.events = {}; }
    addEventListener(name, callback) { this.events[name] = callback; }
    async setRemoteDescription(value) { calls.push(['remote', value]); }
    async addIceCandidate(value) { calls.push(['candidate', value]); }
  }
  const origin = 'https://application.example';
  let status = 200;
  const timers=new Map();
  vm.runInNewContext(source, {
    window: { RTCPeerConnection: Peer,devicePixelRatio:1.5 }, RTCPeerConnection: Peer,
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
}
function checkDisplay() {
  const messages=[],events={},videoEvents={},timers=new Map();
  class Media {
    constructor() { Object.assign(this,{id:'stream',width:1920,height:1080,videoWidth:0,videoHeight:0,paused:true,readyState:0,muted:false}); }
    play() { this.paused=false; return Promise.resolve(); }
    addEventListener(name,callback) { videoEvents[name]=callback; }
  }
  const video=new Media();
  class Observer { constructor(callback) { this.callback=callback; } observe(target,options) { if(options.attributes) videoEvents.dimensions=this.callback; } }
  const document={readyState:'loading',querySelector:()=>video,addEventListener:(name,fn)=>{events[name]=fn;},removeEventListener:(name)=>{delete events[name];}};
  vm.runInNewContext(source,{
    window:{HTMLMediaElement:Media,innerWidth:960,innerHeight:540,addEventListener(){}}, HTMLMediaElement:Media, document, MutationObserver:Observer,URL,
    parent:{postMessage:value=>messages.push(value)},location:{origin:'https://application.example',href:'https://application.example/stream/'},
    setTimeout:callback=>{timers.set(1,callback);return 1;}, clearTimeout:id=>timers.delete(id),
  });
  video.play(); assert.equal(video.muted,true,'picture autoplays without requesting audio');
  video.width=1280;video.height=720;video.videoWidth=1280;video.videoHeight=720;video.readyState=2;videoEvents.playing();
  assert.equal(timers.size,0,'old-sized frames cannot reveal the application');
  video.width=1920;video.height=1080;video.videoWidth=1920;video.videoHeight=1080;videoEvents.resize();timers.get(1)();
  assert.equal(messages.at(-1).state,'ready');
  video.width=2560;videoEvents.dimensions();assert.equal(messages.at(-1).state,'ready','resize keeps the last picture visible');
  events.pointerdown({isTrusted:false});assert.equal(video.muted,true);
  events.pointerdown({isTrusted:true});assert.equal(video.muted,false,'a real gesture enables application audio');
}
(async()=>{for(const path of ['/', '/vm/app/8080/']) await check(path);checkDisplay();})()
 .catch(error=>{console.error(error);process.exitCode=1;});

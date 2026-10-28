const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const flush = () => new Promise(resolve => setImmediate(resolve));

async function desktop(sourcePath) {
  const source = fs.readFileSync(sourcePath, 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];

  async function check(pathname) {
    const timers = new Map();
    let sequence = 0;
    let onMessage;
    let onClick;
    const desktop = { style: {}, contentWindow: {}, src: '' };
    const loading = {};
    const status = {};
    const button = { addEventListener(_, callback) { onClick = callback; } };
    const origin = 'https://desktop.example.com';
    let ready = false;
    const elements = { iframe: desktop, '#loading': loading, '#status': status, '#retry': button };
    const context = vm.createContext({
      URL, URLSearchParams, AbortSignal,
      fetch: async (url, options) => {
        assert.equal(new URL(url).pathname, pathname + 'vnc.html');
        assert.equal(options.cache, 'no-store');
        return { ok: ready, status: ready ? 200 : 503 };
      },
      location: { href: origin + pathname, origin },
      document: { querySelector(selector) { return elements[selector]; } },
      window: { addEventListener(_, callback) { onMessage = callback; } },
      setTimeout(callback) { timers.set(++sequence, callback); return sequence; },
      clearTimeout(id) { timers.delete(id); },
    });
    vm.runInContext(source, context);
    await flush();
    assert.equal(desktop.src, '', 'Do not load the VNC client before its backend is ready');
    assert.equal(status.textContent, 'Starting desktop…');
    ready = true;
    const advance = async () => {
      const [id, callback] = timers.entries().next().value;
      timers.delete(id);
      callback();
      await flush();
    };
    await advance();
    const state = value => onMessage({
      origin, source: desktop.contentWindow, data: { action: 'connection_state', value },
    });

    assert.equal(new URL(desktop.src).searchParams.get('path'), pathname.slice(1) + 'websockify');
    assert.equal(new URL(desktop.src).searchParams.get('kasmvnc_mode_preference'), 'image', 'Keep interactive desktops off CPU video encoders');
    state('connected');
    assert.equal(loading.hidden, true);
    assert.equal(timers.size, 0);
    assert.equal(desktop.style.visibility, 'visible');
    for (const sender of [
      { origin: 'https://other.example.com', source: desktop.contentWindow },
      { origin, source: {} },
    ]) {
      onMessage({ ...sender, data: { action: 'connection_state', value: 'disconnected' } });
      assert.equal(loading.hidden, true);
    }

    state('disconnected');
    assert.equal(loading.hidden, false);
    for (let attempt = 0; attempt < 4; attempt++) {
      await advance();
    }
    assert.equal(button.hidden, false);
    assert.equal(timers.size, 0);
    onClick();
    await flush();
    assert.equal(button.hidden, true);
    state('connected');
    assert.equal(timers.size, 0);
    console.log('PASS', pathname, 'startup readiness, routing, connection, origin checks, bounded reconnect and manual retry');
  }

  for (const pathname of ['/vm/dev-handle/8080/', '/']) await check(pathname);
}

async function kiosk(sourcePath) {
  const html = fs.readFileSync(sourcePath, 'utf8');
  assert.match(html, /data-beam-kiosk-entry/);
  assert.match(html, /src="_beam\/kiosk\/webrtc.js"/);
  const source = fs.readFileSync(path.join(path.dirname(sourcePath), 'kiosk-webrtc.js'), 'utf8');
  async function check(prefix, entry) {
    let application = 'starting', responseStatus = 200, health = true, reloads = 0;
    const requests = [], navigations = [], timers = new Map();
    const elements = {'#status': {}, '#retry': {}, '#restart': {}};
    const overlay = {setAttribute() {}, querySelectorAll: () => Object.values(elements)};
    const origin = 'https://application.example';
    const window = {addEventListener() {}};
    vm.runInNewContext(source, {
      window, URL, AbortSignal, Date, parent: {postMessage() {}},
      location: {href: origin + prefix + (entry ? '' : 'stream/'), origin, replace: url => navigations.push(url), reload: () => reloads++},
      document: {
        documentElement: {hasAttribute: () => entry}, body: {append() {}}, readyState: 'complete',
        createElement: () => overlay,
        querySelector: () => null,
      },
      setTimeout: fn => {timers.set(1, fn); return 1;}, clearTimeout: id => timers.delete(id),
      fetch: async (url, options = {}) => {
        const pathname = new URL(url).pathname;
        requests.push({path: pathname, ...options});
        if (pathname.endsWith('/restart')) return {ok: true};
        if (pathname.endsWith('/health')) return {ok: health};
        return {ok: responseStatus === 200, status: responseStatus,
          json: async () => ({state: application, application, error: 'Application closed'})};
      },
    });
    const advance = async () => {const fn = timers.get(1); timers.delete(1); fn(); await flush();};
    await flush();
    assert.match(elements['#status'].textContent, /Opening application/);
    assert.equal(navigations.length, 0);
    assert.equal(requests[0].path, prefix + '_beam/kiosk/status');
    application = 'closed'; await advance();
    assert.equal(elements['#restart'].hidden, false);
    await elements['#restart'].onclick();
    assert.equal(requests.find(r => r.method === 'POST').headers['X-Beam-Kiosk-Action'], 'restart');
    assert.equal(reloads, 1);
    responseStatus = 403; await advance();
    assert.equal(elements['#retry'].hidden, false);
    assert.match(elements['#status'].textContent, /access is unavailable/);
    elements['#retry'].onclick(); assert.equal(reloads, 2);
    responseStatus = 200; application = 'ready'; health = false;
    await window.beamKiosk.poll();
    assert.equal(navigations.length, 0, 'wait for native stream health');
    health = true; await advance();
    if (entry) {
      assert.equal(navigations.at(-1), origin + prefix + 'stream/');
      assert.equal(requests.at(-1).path, prefix + 'stream/api/health');
    } else {
      window.beamKiosk.notify('ready');
      assert.equal(overlay.hidden, true, 'the shared shell must reveal the application');
      window.beamKiosk.notify('error', 'Connection unavailable');
      assert.equal(overlay.hidden, false);
      assert.equal(elements['#status'].textContent, 'Connection unavailable');
    }
  }
  for (const prefix of ['/', '/vm/app/8080/']) for (const entry of [true, false]) await check(prefix, entry);
}

async function webrtc(sourcePath) {
  const source = fs.readFileSync(sourcePath, 'utf8');
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
    const listeners={};
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
    const document={documentElement:{hasAttribute:()=>false,toggleAttribute:(name,on)=>on?attributes.add(name):attributes.delete(name)},hasFocus:()=>focused,hidden:false,readyState:'loading',querySelector:()=>video,addEventListener:(name,fn)=>{(listeners[name]||=new Set()).add(fn);events[name]=event=>[...listeners[name]].forEach(fn=>fn(event));},removeEventListener:(name,fn)=>{listeners[name]?.delete(fn);}};
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
    const requests=sent.length;
    events.pointerdown({isTrusted:false});assert.equal(sent.length,requests);
    events.pointerdown({isTrusted:true});
    assert.equal(sent.length,requests+1,'real input can reclaim a viewport when embedded-browser focus is retained');
    video.videoWidth=video.videoHeight=4080;
    events.pointerdown({isTrusted:true});
    assert.equal(sent.length,requests+1,'input on a fitted viewer does not send redundant resizes');
  }
  for (const path of ['/', '/vm/app/8080/']) await check(path);
  await checkDisplay();
}

async function input(sourcePath) {
  const window = {};
  vm.runInNewContext(fs.readFileSync(sourcePath, 'utf8'), {window, AbortController});
  window.beamKiosk.installInput();
  const packets = [], normalized = [], zoom = [];
  const element = new EventTarget();
  let position;
  const input = {
    element, inputAttached: true, send: value => packets.push(value),
    sendMotion: () => {throw Error('unordered pointer channel used');},
    _isStreamLocked: () => false,
    _calculateTouchCoordinates: e => {input.x = e.clientX; input.y = e.clientY; position = [e.clientX, e.clientY];},
    _flushCoalescedMouseMove: () => {},
    _sendMouseState: () => packets.push(['position', position]),
    _keysymHeld: () => false,
    _resetWheelLearning: () => packets.push('reset'),
    _isFingerScroll: () => false,
    _fingerScroll: () => {},
    _addWheelNotches: (y, x) => normalized.push([y, x]),
    _sendZoomNotches: n => zoom.push(['gesture', n]),
    getWindowResolution: () => [1440, 900],
  };
  window.webrtcInput = input;
  function event(type, values = {}) {
    const e = new Event(type, {cancelable: true});
    Object.assign(e, {deltaX: 0, deltaY: 0, deltaMode: 0, clientX: 350, clientY: 240, ...values});
    element.dispatchEvent(e);
    assert(e.defaultPrevented, `${type} must stay inside the application`);
  }
  input.sendMotion('hover'); input.send('press'); input.sendMotion('drag'); input.send('release');
  assert.deepEqual(packets.slice(-4), ['hover', 'press', 'drag', 'release']);
  event('wheel', {deltaY: 1});
  assert.deepEqual(normalized.at(-1), [0, 0], 'integer trackpad deltas are not detent ticks');
  event('wheel', {deltaY: 0.25, deltaX: -0.5});
  assert.deepEqual(normalized.at(-1), [0, 0], 'fractional motion must survive');
  for (let i=0; i<395; i++) event('wheel', {deltaY: 0.25});
  assert.deepEqual(normalized.at(-1), [1, 0], 'fractional deltas must produce a complete notch');
  event('wheel', {deltaMode: 1, deltaY: 3});
  assert.deepEqual(normalized.at(-1), [1, 0]);
  event('wheel', {deltaMode: 2, deltaY: 1});
  assert.deepEqual(normalized.at(-1), [9, 0]);
  event('wheel', {deltaY: 100000});
  assert.deepEqual(normalized.at(-1), [10, 0], 'a large event must have bounded cost');
  const anchors = () => packets.filter(value => Array.isArray(value) && value[0] === 'position').length;
  const before = anchors();
  event('wheel', {deltaY: 100});
  assert.equal(anchors(), before + 1, 'stationary wheel input must re-anchor a guest-warped cursor');
  event('wheel', {ctrlKey: true, deltaY: -10});
  assert.equal(zoom.length, 0);
  event('wheel', {ctrlKey: true, deltaY: -10});
  assert.deepEqual(zoom.at(-1), ['gesture', 1]);
  assert.deepEqual(packets.find(value => Array.isArray(value) && value[0] === 'position'), ['position', [350, 240]], 'wheel anchors use current coordinates');
  input._keysymHeld = () => true;
  event('wheel', {ctrlKey: true, deltaY: 100});
  assert.deepEqual(normalized.at(-1), [1, 0], 'physical Control retains ordinary app wheel behavior');
  event('gesturestart', {scale: 1});
  event('gesturechange', {scale: 1.3});
  assert.deepEqual(zoom.at(-1), ['gesture', 2]);
  event('gestureend', {scale: 1.3});
  window.webrtcInput = null;
  assert.throws(() => input.sendMotion('old'), /unordered/);
  const detached = new Event('wheel', {cancelable: true});
  element.dispatchEvent(detached);
  assert.equal(detached.defaultPrevented, false, 'reconnect must remove old listeners');
  window.webrtcInput = input;
  window.webrtcInput = input;
  event('wheel', {deltaY: -100});
  assert.deepEqual(normalized.at(-1), [-1, 0]);

  window.webrtcInput = null;
  const unsupported = {...input, _sendZoomNotches: undefined};
  window.webrtcInput = unsupported;
  assert.match(window.beamKiosk.input.error, /_sendZoomNotches/);
  const fallback = new Event('wheel', {cancelable: true});
  element.dispatchEvent(fallback);
  assert.equal(fallback.defaultPrevented, false, 'an incompatible client must retain native scrolling');
  assert.equal(unsupported.sendMotion, input.sendMotion, 'unsupported clients keep their native channel');
}

const cases = {desktop, kiosk, webrtc, input};
cases[process.argv[2]](process.argv[3]).catch(error => {
  console.error(error);
  process.exitCode = 1;
});

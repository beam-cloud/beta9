/* Platform transport bootstrap. The application and Selkies client are unchanged. */
(() => {
  // The remote desktop has a stable 2x render density. The native client's
  // framebuffer and pointer transforms must use that density too, even on a
  // 1x monitor. Browser CSS still uses its real viewport; the final image is
  // sampled by the browser at the physical display's density. This document
  // is isolated from the embedding page.
  const nativePixelRatio=window.devicePixelRatio||1;
  Object.defineProperty(window,'devicePixelRatio',{configurable:true,get:()=>2});
  let displayState = 'starting', displayError = '', application, overlay, message, retry, restart;
  let activePeer, recoveryTimer, refreshDisplay = () => {};
  const statusEndpoint = new URL('../_beam/kiosk/status', location.href);
  window.beamKiosk = {status:null,stats:null,peers:[],pixelRatio:2,devicePixelRatio:nativePixelRatio};
  function viewportMatches(surface) {
    if(!window.innerWidth)return true;
    const width=surface.videoWidth||surface.width,height=surface.videoHeight||surface.height;
    return Math.abs(width-window.innerWidth*2)<8 && Math.abs(height-window.innerHeight*2)<8;
  }
  const render = () => {
    if (!overlay) return;
    const appReady = application?.application === 'ready';
    overlay.hidden = displayState === 'ready' && appReady && !displayError;
    overlay.style.display = overlay.hidden ? 'none' : 'grid';
    const closed = ['closed','error'].includes(application?.application);
    message.textContent = displayError || (closed ? application.error || 'Application closed.' :
      displayState === 'disconnected' ? 'Reconnecting to application…' : 'Opening application…');
    retry.hidden = !displayError && displayState !== 'disconnected';
    restart.hidden = !closed;
  };
  const notify = (state, error = '') => {
    displayState = state; displayError = error; render();
    parent.postMessage({type:'beam-kiosk-stream-state',state},location.origin);
  };
  if (typeof document !== 'undefined') {
    const mount = () => {
      overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:#111113;color:#ededee;display:grid;place-content:center;justify-items:center;gap:18px;font:14px system-ui,sans-serif';
      overlay.setAttribute('data-beam-kiosk','');
      message = document.createElement('p'); message.setAttribute('role','status');
      retry = document.createElement('button'); retry.textContent='Reconnect';
      retry.onclick = () => location.reload();
      restart = document.createElement('button'); restart.textContent='Reopen application';
      restart.onclick = async () => {
        restart.disabled=true;
        try {
          const response=await fetch(new URL('../_beam/kiosk/restart',location.href),{method:'POST',headers:{'X-Beam-Kiosk-Action':'restart'},signal:AbortSignal.timeout(5000)});
          if (!response.ok) throw new Error('Unable to reopen application.');
          location.reload();
        } catch(error) {notify('error',error.message);}
        finally {restart.disabled=false;}
      };
      for (const button of [retry,restart]) button.style.cssText='font:inherit;padding:8px 16px;color:inherit;background:#242428;border:1px solid #3a3a40;border-radius:6px;cursor:pointer';
      overlay.append(message,retry,restart); document.body.append(overlay);
      render();
      const poll = async () => {
        try {
          const response=await fetch(statusEndpoint,{cache:'no-store',signal:AbortSignal.timeout(5000)});
          if ([401,403,404].includes(response.status)) {notify('error','Application access expired. Reopen its link.');return;}
          if (!response.ok) throw new Error('Unavailable');
          application=await response.json(); window.beamKiosk.status=application;
          if (application.transport === 'websockets') {
            const video=document.querySelector('video#videoStream');
            const canvas=document.querySelector('canvas#videoCanvas');
            if (video?.videoWidth > 0 && !video.paused && video.readyState>=2 && viewportMatches(video) ||
                canvas?.width > 0 && canvas.getBoundingClientRect().width > 0 && viewportMatches(canvas)) displayState='ready';
          }
          render();
        } catch (_) {
          // A status request may fail while media and input remain healthy.
          // Do not cover the live application with a stale recovery overlay.
          if (activePeer?.connectionState !== 'connected') notify('disconnected');
        }
        setTimeout(poll,1000);
      };
      poll();
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded',mount,{once:true});
    else mount();
    window.addEventListener('message',event=>{
      if (event.origin !== location.origin || event.source !== window) return;
      if (event.data?.type === 'stats') window.beamKiosk.stats=event.data.data;
    });
  }
  // Browsers permit muted autoplay. Start the picture immediately and enable
  // sound on the first real interaction, instead of exposing a black player.
  if (typeof document !== 'undefined' && window.HTMLMediaElement) {
    let activated = false, video, ready = false, settle;
    const play = HTMLMediaElement.prototype.play;
    HTMLMediaElement.prototype.play = function(...args) {
      if (['stream','videoStream'].includes(this.id) && !activated) this.muted = true;
      return play.apply(this, args);
    };
    const activate = event => {
      if (!event.isTrusted) return;
      activated = true;
      if (video) {
        video.muted = false;
        video.play().catch(() => {});
      }
      document.removeEventListener('pointerdown', activate, true);
      document.removeEventListener('keydown', activate, true);
    };
    document.addEventListener('pointerdown', activate, true);
    document.addEventListener('keydown', activate, true);
    const fitted = () => (!activePeer || activePeer.connectionState === 'connected') &&
      video && !video.paused && video.readyState >= 2 && video.videoWidth > 0 && viewportMatches(video) &&
      (video.id==='videoStream' || video.videoWidth === video.width && video.videoHeight === video.height);
    const check = () => {
      clearTimeout(settle);
      if (!fitted()) {
        ready = false;
        return;
      }
      // A resize can deliver an old frame before the new capture is ready.
      // Reveal only after the decoded dimensions match the requested display.
      settle = setTimeout(() => {
        if (fitted() && !ready) { ready = true; notify('ready'); }
      }, 150);
    };
    const observe = () => {
      const current = document.querySelector('video#stream,video#videoStream');
      if (!current || current === video) return;
      video = current;
      notify('starting');
      for (const event of ['playing', 'loadeddata', 'resize']) video.addEventListener(event, check);
      const dimensions = new MutationObserver(check);
      dimensions.observe(video, {attributes: true, attributeFilter: ['width', 'height']});
      check();
    };
    refreshDisplay = () => {
      ready = false;
      clearTimeout(settle);
      if (video?.requestVideoFrameCallback) video.requestVideoFrameCallback(check);
      else check();
    };
    new MutationObserver(observe).observe(document, {childList: true, subtree: true});
    observe();
  }
  const proto = window.RTCPeerConnection && RTCPeerConnection.prototype;
  if (!proto) return;
  const remote = proto.setRemoteDescription;
  const candidate = proto.addIceCandidate;
  const routed = new WeakSet();
  const endpoint = new URL('../_beam/kiosk/webrtc/session', location.href);
  proto.setRemoteDescription = async function(description) {
    if (!description || !description.sdp) return remote.call(this, description);
    if (!routed.has(this) && this.addEventListener) {
      activePeer = this;
      window.beamKiosk.peers.push(this);
      this.addEventListener('track', ({receiver}) => {
        // Prefer fresh desktop frames to a playback buffer on browsers that
        // expose these receive-side latency controls.
        try { if ('jitterBufferTarget' in receiver) receiver.jitterBufferTarget = 0; } catch (_) {}
        try { if ('playoutDelayHint' in receiver) receiver.playoutDelayHint = 0; } catch (_) {}
      });
      this.addEventListener('connectionstatechange', () => {
        if (this !== activePeer) return;
        clearTimeout(recoveryTimer);
        if (this.connectionState === 'connected') {
          refreshDisplay();
          return;
        }
        if (['disconnected', 'failed', 'closed'].includes(this.connectionState)) {
          notify('disconnected');
          refreshDisplay();
          recoveryTimer = setTimeout(() => location.reload(), 8000);
        }
      });
    }
    routed.add(this);
    try {
      const response = await fetch(endpoint, {
        method: 'POST', credentials: 'same-origin', cache: 'no-store',
        headers: {'Content-Type': 'application/json', 'X-Beam-Kiosk-Action': 'webrtc'},
        body: JSON.stringify({sdp: description.sdp}), signal: AbortSignal.timeout(10000)
      });
      if (!response.ok) throw new Error(`WebRTC gateway registration failed (${response.status}); reconnect or check the VM gateway`);
      const result = await response.json();
      await remote.call(this, {type: description.type, sdp: result.sdp});
    } catch (error) {
      notify('error',error.message);
      parent.postMessage({type: 'beam-kiosk-transport-error', error: error.message}, location.origin);
      throw error;
    }
  };
  proto.addIceCandidate = function(value) {
    // Registration supplies the complete, routable candidate set. Original
    // guest addresses must not escape as later trickled candidates.
    if (routed.has(this) && value && value.candidate) return Promise.resolve();
    return candidate.call(this, value);
  };
})();

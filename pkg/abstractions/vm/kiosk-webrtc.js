/* Browser shell and authenticated transport bootstrap for an ordinary GUI app. */
(() => {
  if (window.beamKiosk) return;
  const policy = Object.freeze({
    pixelRatio: 2,
    maxPixelRatio: 4,
    statusFailures: 3,
    alignment: 8,
    maxDimension: 4080, // Pinned Selkies client's encoder limit.
    pollMs: 1000,
    requestMs: 5000,
    registrationMs: 10000,
    startupMs: 180000,
    recoveryMs: 8000,
    settleMs: 150,
    wheelPixels: 100,
    wheelLines: 3,
    wheelIdleMs: 1000,
    maxWheelNotches: 10,
    wheelEpsilon: 1e-6,
    pinchScale: 1.14,
    controlLeft: 0xffe3,
    controlRight: 0xffe4,
  });

  class KioskInput {
    constructor(input) {
      this.input = input;
      this.carry = {x: 0, y: 0, zoom: 0};
      this.events = new AbortController();
      const methods = ['send', '_isStreamLocked', '_calculateTouchCoordinates',
        '_sendMouseState', '_flushCoalescedMouseMove', '_keysymHeld',
        '_resetWheelLearning', '_isFingerScroll', '_fingerScroll',
        'getWindowResolution', '_addWheelNotches', '_sendZoomNotches'];
      const missing = methods.filter(name => typeof input[name] !== 'function');
      if (!input.element?.addEventListener || missing.length) {
        this.error = `Unsupported Selkies input client: ${missing.join(', ') || 'missing input surface'}. Rebuild the kiosk image with its pinned client.`;
        console.warn(this.error);
        return; // Keep the client's native input handlers and channel intact.
      }
      // Desktop hover, press and release must share one ordered channel. An
      // unordered hover carrying a newer sequence can otherwise erase a click.
      this.motion = input.sendMotion;
      this.sendMotion = message => input.send(message);
      input.sendMotion = this.sendMotion;
      const options = {capture: true, passive: false, signal: this.events.signal};
      input.element.addEventListener('wheel', event => this.wheel(event), options);
      for (const type of ['gesturestart', 'gesturechange', 'gestureend']) {
        input.element.addEventListener(type, event => this.gesture(event), options);
      }
    }

    close() {
      this.events.abort();
      if (this.input.sendMotion === this.sendMotion) this.input.sendMotion = this.motion;
    }

    position(event, force = false) {
      if (!this.input._trackpadMode && !this.input._isStreamLocked()) {
        const {x, y} = this.input;
        this.input._calculateTouchCoordinates(event);
        if (force || x !== this.input.x || y !== this.input.y) this.input._sendMouseState();
        else this.input._flushCoalescedMouseMove();
      }
    }

    notches(axis, delta, unit = policy.wheelPixels) {
      if (!Number.isFinite(delta)) return 0;
      if (delta * this.carry[axis] < 0) this.carry[axis] = 0;
      const limit = policy.maxWheelNotches * unit;
      this.carry[axis] += Math.max(-limit, Math.min(limit, delta));
      const count = Math.floor((Math.abs(this.carry[axis]) + policy.wheelEpsilon) / unit);
      const sign = Math.sign(this.carry[axis]);
      this.carry[axis] = sign * Math.max(0, Math.abs(this.carry[axis]) - count * unit);
      return count ? sign * count : 0;
    }

    zoom(delta, event) {
      const count = this.notches('zoom', delta, Math.log(policy.pinchScale));
      if (count) {
        this.position(event, true);
        this.input._sendZoomNotches(count);
      }
    }

    wheel(event) {
      if (!this.input.inputAttached) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      const input = this.input;
      const pinch = event.ctrlKey && !input._keysymHeld(policy.controlLeft, policy.controlRight);
      if (event.timeStamp - this.lastWheel > policy.wheelIdleMs || pinch !== this.pinch) {
        input._resetWheelLearning();
        this.carry = {x: 0, y: 0, zoom: 0};
      }
      this.lastWheel = event.timeStamp;
      this.pinch = pinch;
      this.position(event);
      if (pinch) {
        this.zoom(event.deltaMode === 0 ? -event.deltaY / policy.wheelPixels :
          -event.deltaY * Math.log(policy.pinchScale) / policy.wheelLines, event);
      } else if (input._isFingerScroll(event)) {
        input._fingerScroll(event);
      } else {
        // Pixel deltas include fractional trackpad motion. Their numeric
        // precision cannot identify a detent wheel. Never learn a changing scale
        // from the smallest event in a gesture.
        const unit = event.deltaMode === 1 ? policy.wheelPixels / policy.wheelLines : 1;
        const page = event.deltaMode === 2 ? input.getWindowResolution() : [1, 1];
        const y = this.notches('y', event.deltaY * page[1] * unit);
        const x = this.notches('x', event.deltaX * page[0] * unit);
        // Applications may warp the guest cursor while zooming. Re-anchor
        // every emitted wheel event, even if the browser pointer did not move.
        if (x || y) this.position(event, true);
        input._addWheelNotches(y, x);
      }
    }

    gesture(event) {
      if (!this.input.inputAttached) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      if (event.type === 'gesturestart') {
        this.gestureScale = 1;
        this.carry.zoom = 0;
        this.position(event);
      } else if (event.type === 'gesturechange' && event.scale > 0 && this.gestureScale) {
        this.zoom(Math.log(event.scale / this.gestureScale), event);
        this.gestureScale = event.scale;
      } else if (event.type === 'gestureend') {
        this.gestureScale = null;
      }
    }
  }

  class KioskViewer {
    constructor() {
      const ratio = typeof document !== 'undefined' && Number(document.currentScript?.dataset.pixelRatio);
      this.pixelRatio = Number.isInteger(ratio) && ratio > 0 && ratio <= policy.maxPixelRatio ? ratio : policy.pixelRatio;
      this.status = null;
      this.displayState = 'starting';
      this.displayError = '';
      this.deadline = Date.now() + policy.startupMs;
      this.failures = 0;
      this.entry = typeof document !== 'undefined' &&
        document.documentElement?.hasAttribute('data-beam-kiosk-entry');
      // The isolated client document uses the guest's stable render density.
      // CSS retains the browser viewport, including when embedded by a parent.
      Object.defineProperty(window, 'devicePixelRatio', {
        configurable: true, get: () => this.pixelRatio,
      });
    }

    endpoint(path) {
      return new URL(`${this.entry ? '' : '../'}_beam/kiosk/${path}`, location.href);
    }

    notify(state, error = '') {
      this.displayState = state;
      this.displayError = error;
      this.render();
      parent.postMessage({type: 'beam-kiosk-stream-state', state}, location.origin);
    }

    render() {
      if (!this.overlay) return;
      const ready = this.displayState === 'ready' && this.status?.application === 'ready';
      this.overlay.hidden = ready && !this.displayError;
      const closed = ['closed', 'error'].includes(this.status?.application);
      this.message.textContent = this.displayError || (closed ? this.status.error || 'Application closed.' :
        this.displayState === 'disconnected' ? 'Reconnecting to application…' : 'Opening application…');
      this.retry.hidden = !this.displayError && this.displayState !== 'disconnected';
      this.restart.hidden = !closed;
    }

    async reopen() {
      this.restart.disabled = true;
      try {
        const response = await fetch(this.endpoint('restart'), {
          method: 'POST', headers: {'X-Beam-Kiosk-Action': 'restart'},
          signal: AbortSignal.timeout(policy.requestMs),
        });
        if (!response.ok) throw new Error('Unable to reopen application. Check your access or VM logs.');
        location.reload();
      } catch (error) {
        this.notify('error', error.message);
      } finally {
        this.restart.disabled = false;
      }
    }

    mount() {
      this.overlay = document.createElement('div');
      this.overlay.setAttribute('data-beam-kiosk', '');
      this.overlay.innerHTML = `<style>
        [data-beam-kiosk] { position:fixed;inset:0;z-index:2147483647;background:#111113;color:#ededee;display:grid;place-content:center;justify-items:center;gap:18px;font:14px system-ui,sans-serif;color-scheme:dark; }
        [data-beam-kiosk][hidden], [data-beam-kiosk] [hidden] { display:none !important; }
        [data-beam-kiosk] p { max-width:38em;padding:0 24px;text-align:center; }
        [data-beam-kiosk] button { font:inherit;padding:8px 16px;color:inherit;background:#242428;border:1px solid #3a3a40;border-radius:6px;cursor:pointer; }
        [data-beam-kiosk-playing] #playButton { display:none !important; }
      </style>
      <p role="status" aria-live="polite"></p>
      <button>Reconnect</button><button>Reopen application</button>`;
      [this.message, this.retry, this.restart] = this.overlay.querySelectorAll('p,button');
      document.body.append(this.overlay);
      this.retry.onclick = () => location.reload();
      this.restart.onclick = () => this.reopen();
      this.render();
      this.poll();
    }

    viewportMatches(surface) {
      const width = surface.videoWidth || surface.width;
      const height = surface.videoHeight || surface.height;
      return window.innerWidth > 0 && window.innerHeight > 0 &&
        (window.streamResolutionDiverged || Math.abs(width - window.innerWidth * this.pixelRatio) < policy.alignment &&
          Math.abs(height - window.innerHeight * this.pixelRatio) < policy.alignment);
    }

    playing(video = this.video) {
      return video && !video.paused && video.readyState >= 2;
    }

    get webSocketLive() {
      return this.video?.id === 'videoStream' && this.playing();
    }

    async poll() {
      // Native close() changes peer state without emitting a connection event.
      this.syncConnection();
      try {
        const response = await fetch(this.endpoint('status'), {
          cache: 'no-store', signal: AbortSignal.timeout(policy.requestMs),
        });
        if ([401, 403, 404].includes(response.status)) {
          this.notify('error', 'Application access is unavailable. Reopen its link to check your access.');
          return;
        }
        if (!response.ok) throw new Error('Unavailable');
        this.status = await response.json();
        this.failures = 0;
        if (this.entry && this.status.application === 'ready') {
          const client = new URL('stream/', location.href);
          const health = await fetch(new URL('api/health', client), {
            cache: 'no-store', signal: AbortSignal.timeout(policy.requestMs),
          });
          if (!health.ok) throw new Error('Unavailable');
          location.replace(client.href);
          return;
        }
        if (this.status.transport === 'websockets') {
          const video = document.querySelector('video#videoStream');
          const canvas = document.querySelector('canvas#videoCanvas');
          if ((video?.videoWidth > 0 && this.playing(video) && this.viewportMatches(video)) ||
              (canvas?.width > 0 && canvas.getBoundingClientRect().width > 0 && this.viewportMatches(canvas))) {
            this.displayState = 'ready';
          }
        }
        this.render();
      } catch (_) {
        // Status polling must not cover a healthy, connected application.
        if (++this.failures >= policy.statusFailures && this.activePeer?.connectionState !== 'connected' && !this.webSocketLive) {
          this.notify('disconnected');
        }
      }
      if (this.entry && Date.now() > this.deadline && !['closed', 'error'].includes(this.status?.application)) {
        this.notify('error', 'The application is taking longer to start. Inspect its VM logs or reconnect.');
        return;
      }
      setTimeout(() => this.poll(), policy.pollMs);
    }

    fitted() {
      const video = this.video;
      return (this.webSocketLive || !this.activePeer || this.activePeer.connectionState === 'connected') &&
        this.playing(video) && video.videoWidth > 0 && this.viewportMatches(video) &&
        (window.streamResolutionDiverged || video.id === 'videoStream' ||
          video.videoWidth === video.width && video.videoHeight === video.height);
    }

    checkDisplay() {
      clearTimeout(this.settle);
      this.syncViewport();
      // Video can arrive before the client's first resolution bookkeeping.
      if (this.video?.id === 'stream' && this.video.videoWidth > 0 && this.video.width > 0) {
        window.streamResolutionDiverged = this.video.videoWidth !== this.video.width ||
          this.video.videoHeight !== this.video.height;
      }
      if (this.video) document.documentElement.toggleAttribute('data-beam-kiosk-playing',
        this.playing());
      // Another tab can own the shared framebuffer size. Selkies maps input
      // against the realized stream; preserve its aspect ratio in this tab.
      if (this.video && window.streamResolutionDiverged) this.video.style.objectFit = 'contain';
      if (!this.fitted()) {
        this.ready = false;
        return; // Resizing keeps the last live image visible.
      }
      this.settle = setTimeout(() => {
        if (this.fitted() && !this.ready) {
          this.ready = true;
          this.notify('ready');
        }
      }, policy.settleMs);
    }

    refreshDisplay() {
      this.ready = false;
      clearTimeout(this.settle);
      if (this.video?.requestVideoFrameCallback) this.video.requestVideoFrameCallback(() => this.checkDisplay());
      else this.checkDisplay();
    }

    observeVideo() {
      const current = document.querySelector('video#stream,video#videoStream');
      if (!current || current === this.video) return;
      this.video = current;
      this.ready = false;
      this.notify('starting');
      for (const event of ['playing', 'loadeddata', 'resize', 'pause', 'emptied']) {
        current.addEventListener(event, () => this.checkDisplay());
      }
      new MutationObserver(() => this.checkDisplay()).observe(current, {
        attributes: true, attributeFilter: ['width', 'height'],
      });
      this.checkDisplay();
    }

    installDisplay() {
      const viewer = this;
      const play = HTMLMediaElement.prototype.play;
      HTMLMediaElement.prototype.play = function(...args) {
        if (!['stream', 'videoStream'].includes(this.id)) return play.apply(this, args);
        if (!viewer.activated) this.muted = true;
        return play.apply(this, args).catch(error => {
          if (error.name !== 'NotAllowedError' || this.muted) throw error;
          this.muted = true;
          return play.apply(this, args);
        });
      };
      const activate = event => {
        if (!event.isTrusted) return;
        this.activated = true;
        if (this.video) {
          this.video.muted = false;
          this.video.play().catch(() => {});
        }
        document.removeEventListener('pointerdown', activate, true);
        document.removeEventListener('keydown', activate, true);
      };
      document.addEventListener('pointerdown', activate, true);
      document.addEventListener('keydown', activate, true);
      const resize = () => {
        clearTimeout(this.viewportTimer);
        this.viewportTimer = setTimeout(() => this.syncViewport(), policy.settleMs);
      };
      for (const event of ['focus', 'resize']) window.addEventListener(event, resize);
      const release = () => { this.viewport = null; };
      window.addEventListener('blur', release);
      document.addEventListener('visibilitychange', () => {
        if (document.hidden) release();
        else resize();
      });
      for (const type of ['pointerdown', 'keydown']) document.addEventListener(type, event => {
        if (event.isTrusted) this.syncViewport(true);
      }, true);
      if (typeof ResizeObserver !== 'undefined') new ResizeObserver(resize).observe(document.documentElement);
      new MutationObserver(() => this.observeVideo()).observe(document, {childList: true, subtree: true});
      this.observeVideo();
    }

    syncViewport(activate = false) {
      const input = this.input?.input;
      if (!input?.inputAttached || !document.hasFocus() || document.hidden ||
          !(window.innerWidth > 0 && window.innerHeight > 0) ||
          this.activePeer?.connectionState !== 'connected') return;
      const size = [window.innerWidth, window.innerHeight].map(value =>
        Math.min(policy.maxDimension, Math.floor(value * this.pixelRatio / 2) * 2)).join('x');
      const realized = `${this.video?.videoWidth}x${this.video?.videoHeight}`;
      if (size === this.viewport && (!activate || size === realized)) return;
      input.send(`KIOSK_VIEWPORT,${size}`);
      this.viewport = size;
    }

    syncConnection() {
      const state = this.webSocketLive ? 'connected' : this.activePeer?.connectionState;
      if (state === this.peerState) return;
      this.peerState = state;
      clearTimeout(this.recoveryTimer);
      this.recoveryTimer = null;
      if (state === 'connected') {
        this.viewport = null;
        this.refreshDisplay();
      } else if (['disconnected', 'failed', 'closed'].includes(state)) {
        this.notify('disconnected');
        this.refreshDisplay();
        this.recoveryTimer = setTimeout(() => location.reload(), policy.recoveryMs);
      }
    }

    trackPeer(peer) {
      this.activePeer = peer;
      this.peerState = undefined;
      this.syncConnection();
      peer.addEventListener('track', ({receiver}) => {
        // Receive-side hints favor fresh desktop frames over playback buffering.
        try { if ('jitterBufferTarget' in receiver) receiver.jitterBufferTarget = 0; } catch (_) {}
        try { if ('playoutDelayHint' in receiver) receiver.playoutDelayHint = 0; } catch (_) {}
      });
      peer.addEventListener('connectionstatechange', () => {
        if (peer === this.activePeer) this.syncConnection();
      });
    }

    installTransport() {
      // Duplicating a browser tab copies sessionStorage; give each document its
      // own controller identity before Selkies sends its signaling handshake.
      try { window.sessionStorage.setItem('selkies_tab_id', window.crypto.randomUUID()); } catch (_) {}
      const proto = window.RTCPeerConnection && RTCPeerConnection.prototype;
      if (!proto) return;
      const viewer = this, routed = new WeakSet();
      const remote = proto.setRemoteDescription, candidate = proto.addIceCandidate;
      proto.setRemoteDescription = async function(description) {
        if (!description?.sdp) return remote.call(this, description);
        if (!routed.has(this)) viewer.trackPeer(this);
        routed.add(this);
        try {
          const response = await fetch(viewer.endpoint('webrtc/session'), {
            method: 'POST', credentials: 'same-origin', cache: 'no-store',
            headers: {'Content-Type': 'application/json', 'X-Beam-Kiosk-Action': 'webrtc'},
            body: JSON.stringify({sdp: description.sdp}), signal: AbortSignal.timeout(policy.registrationMs),
          });
          if (!response.ok) throw new Error(`WebRTC gateway registration failed (${response.status}); reconnect or check the VM gateway`);
          const result = await response.json();
          await remote.call(this, {type: description.type, sdp: result.sdp});
        } catch (error) {
          viewer.notify('error', error.message);
          parent.postMessage({type: 'beam-kiosk-transport-error', error: error.message}, location.origin);
          throw error;
        }
      };
      proto.addIceCandidate = function(value) {
        // Registration replaces private guest candidates with the routable set.
        if (routed.has(this) && value?.candidate) return Promise.resolve();
        return candidate.call(this, value);
      };
    }

    installInput() {
      let input;
      Object.defineProperty(window, 'webrtcInput', {
        configurable: true,
        get: () => input,
        set: value => {
          if (input === value) return;
          this.input?.close();
          input = value;
          this.input = value ? new KioskInput(value) : null;
        },
      });
    }
  }

  const viewer = window.beamKiosk = new KioskViewer();
  viewer.installTransport();
  if (typeof document === 'undefined') return;
  viewer.installInput();
  const mount = () => viewer.mount();
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, {once: true});
  else mount();
  if (window.HTMLMediaElement) viewer.installDisplay();
})();

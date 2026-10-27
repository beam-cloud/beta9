// Installed before page scripts in every frame. Count changes in pixels that
// actually arrive, rather than requestAnimationFrame ticks or configured FPS.
(() => {
  // Diagnostic peers are local to this benchmark injection, never the product UI.
  window.beamProbePeers = [];
  if (window.RTCPeerConnection) {
    const original = RTCPeerConnection.prototype.setRemoteDescription;
    RTCPeerConnection.prototype.setRemoteDescription = function(value) {
      window.beamProbePeers = window.beamProbePeers.filter(peer => peer.connectionState !== 'closed');
      if (!window.beamProbePeers.includes(this)) window.beamProbePeers.push(this);
      return original.call(this, value);
    };
  }
  const state = { valid: false, frames: [], inputs: [], pending: [], samples: 0, lastCounter: null, lastInput: null };
  window.beamStreamProbe = state;
  const sample = document.createElement('canvas');
  sample.width = 16; sample.height = 3;
  const context = sample.getContext('2d', { willReadFrequently: true });
  context.imageSmoothingEnabled = false;
  let lastInput = null;
  let lastAlign = 0;
  window.addEventListener('keydown', event => {
    if (event.key === 'a' && !event.repeat && lastInput !== null) {
      const previous = state.pending.length ? state.pending[state.pending.length - 1].target : lastInput;
      state.pending.push({ target: (previous + 1) % 65536, time: performance.now() });
    }
  }, true);
  const counter = (pixels, row) => {
    let value = 0;
    for (let bit = 0; bit < 16; bit++) {
      if (pixels[(row * 16 + bit) * 4] > 127) value |= 1 << bit;
    }
    return value;
  };
  function tick(now) {
    // A WebGL canvas may discard its buffer after compositing. Join the end
    // of the renderer's callback order so pixels are read after its draw,
    // before that discard; never sample an invisible decoder as presentation.
    if (!state.valid && now - lastAlign > 1000) {
      lastAlign = now;
      setTimeout(() => requestAnimationFrame(tick), 0);
      return;
    }
    try {
      for (const source of document.querySelectorAll('video, canvas')) {
        const bounds = source.getBoundingClientRect();
        const style = getComputedStyle(source);
        if (!bounds.width || !bounds.height || style.display === 'none' || style.visibility === 'hidden') continue;
        const width = source.videoWidth || source.width;
        const height = source.videoHeight || source.height;
        if (width < 320 || height < 200 || source === sample) continue;
        for (const [row, y] of [[0, .0625], [1, .1125], [2, .1625]]) {
          context.drawImage(source, width * .25, height * y, width * .5, 1, 0, row, 16, 1);
        }
        const pixels = context.getImageData(0, 0, 16, 3).data;
        if (counter(pixels, 2) !== 0xA55A) continue;
        state.valid = true;
        state.samples++;
        const frame = counter(pixels, 0);
        if (frame !== state.lastCounter) {
          state.frames.push(now);
          state.lastCounter = frame;
        }
        lastInput = counter(pixels, 1);
        state.lastInput = lastInput;
        while (state.pending.length && (lastInput - state.pending[0].target + 65536) % 65536 < 32768) {
          state.inputs.push(now - state.pending.shift().time);
        }
        break;
      }
    } catch (_) { /* A decoder may replace its canvas during a resize. */ }
    requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
})();

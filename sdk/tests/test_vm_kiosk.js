const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(process.argv[2], 'utf8');
assert.match(html, /data-beam-kiosk-entry/);
assert.match(html, /src="_beam\/kiosk\/webrtc.js"/);
const source = fs.readFileSync(path.join(path.dirname(process.argv[2]), 'kiosk-webrtc.js'), 'utf8');
const flush = () => new Promise(resolve => setImmediate(resolve));
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
(async () => {for (const prefix of ['/', '/vm/app/8080/']) for (const entry of [true, false]) await check(prefix, entry);})()
  .catch(error => {console.error(error); process.exitCode = 1;});

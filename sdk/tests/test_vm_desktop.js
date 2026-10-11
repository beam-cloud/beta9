const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];

for (const pathname of ['/vm/dev-handle/8080/', '/']) {
  const timers = new Map();
  let sequence = 0;
  let onMessage;
  let onClick;
  const desktop = { style: {}, contentWindow: {}, src: '' };
  const loading = {};
  const status = {};
  const button = { addEventListener(_, callback) { onClick = callback; } };
  const origin = 'https://desktop.example.com';
  const elements = { iframe: desktop, '#loading': loading, '#status': status, '#retry': button };
  const context = vm.createContext({
    URL, URLSearchParams,
    location: { href: origin + pathname, origin },
    document: { querySelector(selector) { return elements[selector]; } },
    window: { addEventListener(_, callback) { onMessage = callback; } },
    setTimeout(callback) { timers.set(++sequence, callback); return sequence; },
    clearTimeout(id) { timers.delete(id); },
  });
  vm.runInContext(source, context);
  const state = value => onMessage({
    origin, source: desktop.contentWindow, data: { action: 'connection_state', value },
  });

  assert.equal(new URL(desktop.src).searchParams.get('path'), pathname.slice(1) + 'websockify');
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
    const [id, callback] = timers.entries().next().value;
    timers.delete(id);
    callback();
  }
  assert.equal(button.hidden, false);
  assert.equal(timers.size, 0);
  onClick();
  assert.equal(button.hidden, true);
  state('connected');
  assert.equal(timers.size, 0);
  console.log('PASS', pathname, 'routing, connection, origin checks, bounded reconnect and manual retry');
}

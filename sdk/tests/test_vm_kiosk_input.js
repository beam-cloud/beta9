const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const window = {};
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), {window, AbortController});
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

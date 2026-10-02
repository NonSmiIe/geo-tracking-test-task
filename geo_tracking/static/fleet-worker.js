const EPISODE_GAP_US = 30_000_000, FEED_LIMIT = 80, FRESH_US = 60_000_000;
const MAX_LATITUDE = 85.05112878;

let ids = [], capacity = 0, size = 0;
let latitude, longitude, mercatorX, mercatorY, stamp, dirty;
let changed = [];
const index = new Map();
let selected = -1, trail = [];
let user = null, view = null, socket = null, generation = 0, snapshotController = null;
let received = 0;
const feed = new Map();
let alertTotal = 0, alertsDirty = false, freshEpisodes = 0;
const pulses = new Set();

function grow(minimum) {
  if (minimum <= capacity) return;
  const next = Math.max(1024, capacity * 2, minimum);
  const copy = (Type, old) => { const array = new Type(next); if (old) array.set(old.subarray(0, size)); return array; };
  latitude = copy(Float64Array, latitude); longitude = copy(Float64Array, longitude);
  mercatorX = copy(Float64Array, mercatorX); mercatorY = copy(Float64Array, mercatorY);
  stamp = copy(Float64Array, stamp); dirty = copy(Uint8Array, dirty);
  capacity = next;
}
function reset() {
  ids = []; capacity = 0; size = 0; changed = []; index.clear(); selected = -1; trail = [];
  latitude = longitude = mercatorX = mercatorY = stamp = dirty = undefined;
  grow(1024);
  postMessage({ type: 'reset' });
}
function micros(value) {
  if (typeof value === 'number') return value;
  const fraction = /\.(\d+)/.exec(value)?.[1] || '';
  return Date.parse(value) * 1000 + Number(fraction.padEnd(6, '0').slice(3, 6) || '0');
}
function place(id, lat, lng, at) {
  let slot = index.get(id);
  if (slot === undefined) {
    grow(size + 1); slot = size++; ids[slot] = id; index.set(id, slot); stamp[slot] = -1;
  } else if (stamp[slot] >= at) return;
  latitude[slot] = lat; longitude[slot] = lng; stamp[slot] = at;
  const phi = Math.max(-MAX_LATITUDE, Math.min(MAX_LATITUDE, lat)) * Math.PI / 180;
  mercatorX[slot] = (lng + 180) / 360;
  mercatorY[slot] = (1 - Math.log(Math.tan(phi) + 1 / Math.cos(phi)) / Math.PI) / 2;
  if (!dirty[slot]) { dirty[slot] = 1; changed.push(slot); }
  if (slot === selected) { trail.push([lat, lng]); if (trail.length > 30) trail.shift(); }
}
function alerts(items) {
  alertTotal += items.length;
  for (const item of items) {
    const key = `${item.device_id}|${item.zone_id}`, at = item.timestamp, episode = feed.get(key);
    pulses.add(item.zone_id);
    if (episode && at - episode.last < EPISODE_GAP_US) {
      episode.last = Math.max(episode.last, at); episode.reports++; feed.delete(key); feed.set(key, episode);
      continue;
    }
    feed.delete(key);
    feed.set(key, { device_id: item.device_id, zone_id: item.zone_id, first: at, last: at, reports: 1, fresh: true });
    freshEpisodes++;
  }
  while (feed.size > FEED_LIMIT) feed.delete(feed.keys().next().value);
  alertsDirty = true;
}
function details(slot) {
  return { device_id: ids[slot], latitude: latitude[slot], longitude: longitude[slot], timestamp: stamp[slot] };
}

function flushFrame() {
  if (changed.length) {
    const slots = Int32Array.from(changed), points = new Float64Array(slots.length * 2);
    slots.forEach((slot, position) => { points[position * 2] = mercatorX[slot]; points[position * 2 + 1] = mercatorY[slot]; dirty[slot] = 0; });
    changed = [];
    postMessage({ type: 'frame', size, slots, points, selected: selected >= 0 ? { ...details(selected), trail } : null }, [slots.buffer, points.buffer]);
  }
  if (alertsDirty) {
    alertsDirty = false;
    postMessage({ type: 'alerts', total: alertTotal, fresh: freshEpisodes, feed: [...feed.values()].reverse(), pulses: [...pulses] });
    for (const episode of feed.values()) episode.fresh = false;
    freshEpisodes = 0; pulses.clear();
  }
}
setInterval(flushFrame, 250);
setInterval(() => { postMessage({ type: 'rate', received }); received = 0; }, 1000);

async function snapshot(ws, current) {
  snapshotController?.abort(); snapshotController = new AbortController();
  const signal = snapshotController.signal;
  const area = `south=${view.south}&west=${view.west}&north=${view.north}&east=${view.east}`;
  try {
    let cursor;
    do {
      const response = await fetch(`/devices/latest?limit=1000&${area}${cursor ? `&after=${encodeURIComponent(cursor)}` : ''}`, { signal, headers: { 'X-User-ID': user } });
      if (!response.ok) throw new Error(`Snapshot failed (${response.status}).`);
      const page = await response.json();
      if (socket !== ws || generation !== current) return;
      for (const item of page.items) place(item.device_id, item.latitude, item.longitude, micros(item.timestamp));
      cursor = page.next_cursor;
    } while (cursor);
  } catch (cause) {
    if (cause.name !== 'AbortError' && socket === ws) postMessage({ type: 'error', message: cause.message });
  }
}
function connect() {
  const current = ++generation;
  socket?.close();
  const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?user_id=${encodeURIComponent(user)}`);
  socket = ws;
  postMessage({ type: 'status', state: 'connecting' });
  ws.onmessage = ({ data }) => {
    if (socket !== ws) return;
    const message = JSON.parse(data);
    if (message.type === 'positions') {
      received += message.items.length;
      for (const [id, lat, lng, at] of message.items) place(id, lat, lng, at);
    } else if (message.type === 'inside_report') alerts(message.items);
    else if (message.type === 'ready') {
      postMessage({ type: 'status', state: 'live' });
      ws.send(JSON.stringify({ type: 'viewport', ...view }));
      postMessage({ type: 'zones_changed' });
    } else if (message.type === 'subscribed') snapshot(ws, current);
    else if (message.type === 'resync') { snapshot(ws, current); postMessage({ type: 'zones_changed' }); }
    else if (message.type === 'zones_changed') postMessage({ type: 'zones_changed' });
  };
  ws.onclose = () => {
    if (socket !== ws) return;
    snapshotController?.abort();
    postMessage({ type: 'status', state: 'reconnecting' });
    setTimeout(() => { if (socket === ws) connect(); }, 1500);
  };
  ws.onerror = () => { if (socket === ws) postMessage({ type: 'status', state: 'reconnecting' }); };
}

const queries = {
  list({ search, filter, limit }) {
    const needle = search.toLowerCase(), now = Date.now() * 1000, rows = [];
    let matches = 0;
    for (let slot = 0; slot < size; slot++) {
      const fresh = now - stamp[slot] < FRESH_US;
      if (filter === 'fresh' && !fresh || filter === 'stale' && fresh) continue;
      if (needle && !ids[slot].toLowerCase().includes(needle)) continue;
      matches++;
      if (rows.length < limit) rows.push({ ...details(slot), slot });
    }
    return { rows, matches };
  },
  bounds() {
    if (!size) return null;
    let south = 90, west = 180, north = -90, east = -180;
    for (let slot = 0; slot < size; slot++) {
      south = Math.min(south, latitude[slot]); north = Math.max(north, latitude[slot]);
      west = Math.min(west, longitude[slot]); east = Math.max(east, longitude[slot]);
    }
    return { south, west, north, east };
  },
};

onmessage = ({ data }) => {
  if (data.type === 'connect') {
    user = data.user; view = data.view; feed.clear(); alertTotal = 0; alertsDirty = true;
    reset(); connect();
  } else if (data.type === 'viewport') {
    view = data.view;
    if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'viewport', ...view }));
  } else if (data.type === 'select') {
    selected = data.slot ?? (data.id !== undefined ? index.get(data.id) ?? -1 : -1);
    trail = selected >= 0 ? [[latitude[selected], longitude[selected]]] : [];
    if (selected >= 0 && !dirty[selected]) { dirty[selected] = 1; changed.push(selected); }
    flushFrame();
  } else if (data.type === 'clear-feed') { feed.clear(); alertsDirty = true; }
  else if (data.type === 'query') postMessage({ type: 'reply', request: data.request, data: queries[data.name](data) });
};

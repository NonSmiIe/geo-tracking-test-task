const $ = (id) => document.getElementById(id);
const icon = (name) => { const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); const use = document.createElementNS(svg.namespaceURI, 'use'); use.setAttribute('href', `#i-${name}`); svg.append(use); return svg; };
const element = (tag, text, className) => { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (className) node.className = className; return node; };
const count = (value) => Number(value || 0).toLocaleString();
const requestedUser = new URLSearchParams(location.search).get('user');
let user = requestedUser && /^[\w.-]{1,96}$/.test(requestedUser) ? requestedUser : 'alice';
let epoch = 0, editId = null, selected = null, following = false, currentView = 'fleet', zonesVisible = true;
let alertTotal = 0, unread = 0, insightLoadId = 0, zoneLoadId = 0, errorTimer, feed = [];
const zones = new Map();
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
const map = L.map('map', { preferCanvas: true, zoomControl: false, attributionControl: false }).setView([56.9496, 24.1052], 13);
L.control.zoom({ position: 'bottomright' }).addTo(map);
L.control.scale({ position: 'bottomleft', imperial: false }).addTo(map);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);
const zoneRenderer = L.canvas();
const worker = new Worker('/static/fleet-worker.js');
let draftCircle, requestId = 0;
const pending = new Map();
const ask = (name, options = {}) => new Promise((resolve) => { const request = ++requestId; pending.set(request, resolve); worker.postMessage({ type: 'query', name, request, ...options }); });

const isoTime = (micros) => new Date(Math.floor(micros / 1000)).toISOString();
function viewport() {
  const bounds = map.getBounds(), wrap = (value) => ((value + 180) % 360 + 360) % 360 - 180;
  const south = Math.max(-90, bounds.getSouth()), north = Math.min(90, bounds.getNorth());
  if (bounds.getEast() - bounds.getWest() >= 360) return { south, west: -180, north, east: 180 };
  return { south, west: wrap(bounds.getWest()), north, east: wrap(bounds.getEast()) };
}
function error(message) {
  $('error-message').textContent = message;
  $('error').hidden = false;
  clearTimeout(errorTimer);
  errorTimer = setTimeout(() => { $('error').hidden = true; }, 9000);
}
$('dismiss-error').onclick = () => { $('error').hidden = true; };
async function api(path, options = {}, who = user) {
  const response = await fetch(path, { ...options, headers: { 'X-User-ID': who, 'Content-Type': 'application/json', ...options.headers } });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(typeof body.detail === 'string' ? body.detail.replaceAll('_', ' ') : `Request failed (${response.status}). Please try again.`);
  }
  return response.status === 204 ? null : response.json();
}
function current(version, who) { return version === epoch && who === user; }
function age(micros) {
  const seconds = Math.max(0, Math.floor((Date.now() - micros / 1000) / 1000));
  return seconds < 2 ? 'just now' : seconds < 60 ? `${seconds}s ago` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ago` : `${Math.floor(seconds / 3600)}h ago`;
}

const CELL = 34, POINT_LIMIT = 3000;
const FleetLayer = L.Layer.extend({
  onAdd() {
    this.canvas = L.DomUtil.create('canvas', 'fleet-canvas leaflet-zoom-hide');
    map.getPane('overlayPane').append(this.canvas);
    this.size = 0; this.x = new Float64Array(1024); this.y = new Float64Array(1024);
    this.visible = new Int32Array(POINT_LIMIT); this.visibleCount = 0; this.dense = false; this.clusters = [];
    this.frame = null; this.redraw = this.redraw.bind(this);
    map.on('moveend zoomend resize', this.redraw);
  },
  clear() { this.size = 0; this.redraw(); },
  update(size, slots, points) {
    if (size > this.x.length) {
      const capacity = Math.max(size, this.x.length * 2), x = new Float64Array(capacity), y = new Float64Array(capacity);
      x.set(this.x); y.set(this.y); this.x = x; this.y = y;
    }
    for (let position = 0; position < slots.length; position++) { this.x[slots[position]] = points[position * 2]; this.y[slots[position]] = points[position * 2 + 1]; }
    this.size = size; this.redraw();
  },
  screen() {
    const world = 256 * 2 ** map.getZoom(), origin = map.getPixelBounds().min;
    return { world, left: origin.x, top: origin.y };
  },
  redraw() {
    if (this.frame !== null) return;
    this.frame = requestAnimationFrame(() => { this.frame = null; this.draw(); });
  },
  draw() {
    const size = map.getSize(), ratio = Math.min(devicePixelRatio || 1, 2), { world, left, top } = this.screen();
    L.DomUtil.setPosition(this.canvas, map.containerPointToLayerPoint([0, 0]));
    this.canvas.width = size.x * ratio; this.canvas.height = size.y * ratio;
    this.canvas.style.width = `${size.x}px`; this.canvas.style.height = `${size.y}px`;
    const context = this.canvas.getContext('2d'), dark = document.documentElement.dataset.theme === 'dark';
    context.scale(ratio, ratio);
    const columns = Math.ceil(size.x / CELL) + 1, rows = Math.ceil(size.y / CELL) + 1;
    const cellCount = new Uint32Array(columns * rows), cellX = new Float64Array(columns * rows), cellY = new Float64Array(columns * rows);
    let visible = 0;
    for (let slot = 0; slot < this.size; slot++) {
      const x = this.x[slot] * world - left, y = this.y[slot] * world - top;
      if (x < -10 || y < -10 || x > size.x + 10 || y > size.y + 10) continue;
      if (visible < POINT_LIMIT) this.visible[visible] = slot;
      visible++;
      const cell = Math.floor(Math.max(0, y) / CELL) * columns + Math.floor(Math.max(0, x) / CELL);
      cellCount[cell]++; cellX[cell] += x; cellY[cell] += y;
    }
    this.visibleCount = Math.min(visible, POINT_LIMIT); this.dense = visible > POINT_LIMIT && map.getZoom() < 17;
    const ink = dark ? '#c2dea9' : '#255b40';
    context.fillStyle = ink;
    this.clusters = [];
    if (this.dense) {
      context.font = '700 11px Manrope'; context.textAlign = 'center'; context.textBaseline = 'middle';
      for (let cell = 0; cell < cellCount.length; cell++) {
        const total = cellCount[cell]; if (!total) continue;
        const x = cellX[cell] / total, y = cellY[cell] / total, radius = Math.min(17, 6 + Math.log2(total));
        context.fillStyle = ink; context.beginPath(); context.arc(x, y, radius, 0, Math.PI * 2); context.fill();
        if (total > 3) { context.fillStyle = dark ? '#18231e' : '#fffefa'; context.fillText(total > 999 ? `${(total / 1000).toFixed(total > 99999 ? 0 : 1)}k` : String(total), x, y); }
        this.clusters.push({ x, y, total, radius });
      }
    } else {
      context.beginPath();
      for (let position = 0; position < this.visibleCount; position++) {
        const slot = this.visible[position], x = this.x[slot] * world - left, y = this.y[slot] * world - top;
        context.moveTo(x + 3, y); context.arc(x, y, 3, 0, Math.PI * 2);
      }
      context.fill();
    }
    if (selected) {
      const point = map.latLngToContainerPoint([selected.latitude, selected.longitude]);
      context.strokeStyle = '#bb713a'; context.lineWidth = 2;
      context.beginPath(); context.arc(point.x, point.y, 9, 0, Math.PI * 2); context.stroke();
      if (selected.trail?.length > 1) {
        context.beginPath();
        selected.trail.forEach(([lat, lng], step) => { const p = map.latLngToContainerPoint([lat, lng]); if (step) context.lineTo(p.x, p.y); else context.moveTo(p.x, p.y); });
        context.stroke();
      }
    }
    $('device-legend').textContent = this.dense ? 'Fleet density' : 'Device';
    $('visible-count').textContent = `${count(visible)} on map`;
  },
  nearest(point) {
    const { world, left, top } = this.screen();
    let best = -1, distance = 13;
    for (let position = 0; position < this.visibleCount; position++) {
      const slot = this.visible[position], d = Math.hypot(this.x[slot] * world - left - point.x, this.y[slot] * world - top - point.y);
      if (d < distance) { best = slot; distance = d; }
    }
    return best;
  },
});
const fleet = new FleetLayer().addTo(map);

let fleetTimer;
async function renderFleet() {
  if (currentView !== 'fleet' || $('fleet-list').contains(document.activeElement)) return;
  const search = $('fleet-search').value.trim(), filter = $('fleet-filter').value, version = epoch;
  const { rows, matches } = await ask('list', { search, filter, limit: 60 });
  if (version !== epoch) return;
  const now = Date.now();
  const buttons = rows.map((item) => {
    const button = element('button', undefined, `fleet-row${selected?.device_id === item.device_id ? ' selected' : ''}`);
    button.type = 'button';
    const marker = element('span', undefined, 'fleet-icon'); marker.append(icon('fleet'));
    const text = element('span'); text.append(element('strong', item.device_id), element('small', `${item.latitude.toFixed(4)}, ${item.longitude.toFixed(4)}`));
    const when = element('span', age(item.timestamp), `fleet-age${now - item.timestamp / 1000 >= 60000 ? ' stale' : ''}`);
    button.append(marker, text, when); button.onclick = () => selectDevice({ slot: item.slot });
    return button;
  });
  if (!buttons.length) {
    const empty = element('div', undefined, 'empty-state'); empty.append(icon('fleet'), element('h3', fleet.size ? 'No matching devices' : 'Waiting for the first signal'), element('p', fleet.size ? 'Try another device name or change the filter.' : 'Connect your devices or start the load generator. Live positions will appear on the map.'));
    buttons.push(empty);
  }
  $('fleet-list').replaceChildren(...buttons);
  $('fleet-list-caption').textContent = matches ? `${count(matches)} matching devices${matches > 60 ? ' · showing the first 60, search to narrow' : ''}` : '';
}
function scheduleFleet() { clearTimeout(fleetTimer); fleetTimer = setTimeout(renderFleet, 120); }
$('fleet-search').oninput = scheduleFleet;
$('fleet-filter').onchange = scheduleFleet;
setInterval(() => { renderFleet(); updateInspector(); }, 1500);
function selectDevice(target) {
  following = false; $('follow-device').textContent = 'Follow on map';
  worker.postMessage({ type: 'select', ...target });
  $('inspector').hidden = false;
}
function updateInspector() {
  if (!selected) return;
  $('inspector-id').textContent = selected.device_id;
  $('inspector-lat').textContent = selected.latitude.toFixed(6);
  $('inspector-lng').textContent = selected.longitude.toFixed(6);
  $('inspector-time').textContent = new Date(selected.timestamp / 1000).toLocaleTimeString();
  $('inspector-age').textContent = age(selected.timestamp);
}
let centred = false;
function receiveSelected(item) {
  const first = !selected || selected.device_id !== item.device_id;
  selected = item; updateInspector();
  if (first && !centred) { centred = true; map.setView([item.latitude, item.longitude], Math.max(map.getZoom(), 14)); }
  else if (following) map.panTo([item.latitude, item.longitude], { animate: false });
}
function closeInspector() { selected = null; centred = false; following = false; $('inspector').hidden = true; worker.postMessage({ type: 'select', slot: -1 }); fleet.redraw(); }
$('close-inspector').onclick = closeInspector;
$('follow-device').onclick = () => { following = !following; $('follow-device').textContent = following ? 'Stop following' : 'Follow on map'; };
map.on('click', ({ latlng, containerPoint }) => {
  if (!$('zone-editor').hidden) {
    $('latitude').value = latlng.lat.toFixed(6); $('longitude').value = latlng.lng.toFixed(6); updateDraft(); return;
  }
  const cluster = fleet.dense && fleet.clusters.find((entry) => entry.total > 3 && Math.hypot(entry.x - containerPoint.x, entry.y - containerPoint.y) < entry.radius + 3);
  if (cluster) { map.setView(latlng, Math.min(map.getZoom() + 2, 18)); return; }
  if (fleet.dense) return;
  const slot = fleet.nearest(containerPoint);
  if (slot >= 0) { centred = true; selectDevice({ slot }); }
});
map.on('moveend', () => {
  const centre = map.getCenter();
  $('map-location').textContent = Math.abs(centre.lat - 56.9496) < .15 && Math.abs(centre.lng - 24.1052) < .3 ? 'Riga, Latvia' : `${centre.lat.toFixed(3)}°, ${centre.lng.toFixed(3)}°`;
});
$('fit-fleet').onclick = async () => {
  const bounds = await ask('bounds');
  if (!bounds) { error('No device positions yet. Start the load generator or connect devices to see your fleet.'); return; }
  map.fitBounds([[bounds.south, bounds.west], [bounds.north, bounds.east]], { padding: [60, 60], maxZoom: 15 });
};

function switchView(view) {
  currentView = view;
  document.querySelectorAll('[data-view]').forEach((button) => { const active = button.dataset.view === view; button.classList.toggle('selected', active); button.setAttribute('aria-pressed', String(active)); });
  ['fleet', 'zones', 'activity', 'brief'].forEach((name) => { $(`${name}-view`).hidden = name !== view; });
  if (view === 'fleet') renderFleet();
  if (view === 'activity') { unread = 0; $('activity-badge').hidden = true; renderAlerts(); }
  if (view === 'brief') loadInsights();
}
document.querySelectorAll('[data-view]').forEach((button) => { button.onclick = () => switchView(button.dataset.view); });
function pulseZone(zoneId) {
  const entry = zones.get(zoneId);
  if (!entry || reducedMotion.matches || !zonesVisible) return;
  entry.circle.setStyle({ weight: 4, fillOpacity: .22 });
  setTimeout(() => entry.circle.setStyle({ weight: 1.5, fillOpacity: entry.zone.active ? .09 : .025 }), 220);
}
function receiveAlerts({ total, fresh, feed: episodes, pulses }) {
  alertTotal = total; feed = episodes;
  if (currentView !== 'activity') unread += fresh;
  pulses.forEach(pulseZone);
  $('alert-count').textContent = count(alertTotal);
  $('activity-badge').textContent = unread > 99 ? '99+' : count(unread); $('activity-badge').hidden = !unread;
  if (currentView === 'activity') renderAlerts();
}
const clock = (micros) => new Date(micros / 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
function alertText(group) {
  const zoneName = zones.get(group.zone_id)?.zone.name || 'geofence';
  return [`${group.device_id} entered ${zoneName} · ${clock(group.first)}`, `inside since ${clock(group.first)} · ${count(group.reports)} report${group.reports === 1 ? '' : 's'}`];
}
function renderAlerts() {
  const rows = feed.map((group) => {
    const row = element('div', undefined, `alert-row${group.fresh ? ' fresh' : ''}`), symbol = element('span', undefined, 'alert-symbol'), content = element('div');
    const [title, detail] = alertText(group);
    symbol.append(icon('zone')); content.append(element('strong', title), element('p', detail));
    const time = element('time', `last ${new Date(group.last / 1000).toLocaleTimeString()}`); time.dateTime = isoTime(group.last); content.append(time); row.append(symbol, content);
    return row;
  });
  if (!rows.length) { const empty = element('div', undefined, 'empty-state'); empty.append(icon('activity'), element('h3', 'Nothing has crossed your radar yet'), element('p', 'When a device reports from inside one of your geofences, it appears here once and keeps counting while it stays inside.')); rows.push(empty); }
  $('alerts').replaceChildren(...rows);
}
$('clear-alerts').onclick = () => { worker.postMessage({ type: 'clear-feed' }); feed = []; unread = 0; $('activity-badge').hidden = true; renderAlerts(); };

async function loadZones(version = epoch, who = user) {
  const loadId = ++zoneLoadId, result = [];
  let cursor;
  do {
    const page = await api(`/geozones?limit=1000${cursor ? `&after=${encodeURIComponent(cursor)}` : ''}`, {}, who);
    if (!current(version, who) || loadId !== zoneLoadId) return;
    result.push(...page.items); cursor = page.next_cursor;
  } while (cursor);
  for (const entry of zones.values()) map.removeLayer(entry.circle);
  zones.clear();
  for (const zone of result) {
    const circle = L.circle([zone.latitude, zone.longitude], { renderer: zoneRenderer, radius: zone.radius_m, color: zone.active ? '#255b40' : '#77836f', weight: 1.5, fillOpacity: zone.active ? .09 : .025, dashArray: zone.active ? null : '5 5' });
    if (zonesVisible) circle.addTo(map);
    zones.set(zone.id, { circle, zone });
  }
  if (editId && !zones.has(editId)) resetEdit();
  renderZones();
  $('zone-count').textContent = count(result.filter((zone) => zone.active).length);
}
function renderZones() {
  const rows = [];
  for (const { zone, circle } of zones.values()) {
    const row = element('div', undefined, 'zone-row'), title = element('div', undefined, 'zone-title'), actions = element('div', undefined, 'zone-actions');
    title.append(element('strong', zone.name), element('span', zone.active ? 'Active' : 'Paused', `zone-state${zone.active ? '' : ' paused'}`));
    const action = (label, callback, className) => { const button = element('button', label, className); button.type = 'button'; button.onclick = async () => { const who = user, version = epoch; button.disabled = true; try { await callback(button); } catch (cause) { if (current(version, who)) error(cause.message); } finally { button.disabled = false; } }; actions.append(button); };
    action('Locate', async () => map.fitBounds(circle.getBounds(), { padding: [55, 55] }));
    action('Edit', async () => startEdit(zone));
    action(zone.active ? 'Pause' : 'Resume', async () => { const who = user, version = epoch; await api(`/geozones/${zone.id}`, { method: 'PATCH', body: JSON.stringify({ active: !zone.active }) }, who); if (current(version, who)) await loadZones(version, who); });
    action('Delete', async (button) => {
      if (button.dataset.confirm !== 'yes') { button.dataset.confirm = 'yes'; button.textContent = 'Confirm?'; setTimeout(() => { button.dataset.confirm = ''; button.textContent = 'Delete'; }, 4000); return; }
      const who = user, version = epoch;
      await api(`/geozones/${zone.id}`, { method: 'DELETE' }, who);
      if (!current(version, who)) return;
      if (editId === zone.id) resetEdit(); await loadZones(version, who);
    }, 'delete-button');
    row.append(title, element('p', `${count(zone.radius_m)} m radius · version ${zone.version}`), actions); rows.push(row);
  }
  if (!rows.length) { const empty = element('div', undefined, 'empty-state'); empty.append(icon('zone'), element('h3', 'Mark a place that matters'), element('p', 'Add a circular geofence around a depot, delivery area or location you want to watch.')); rows.push(empty); }
  $('zones').replaceChildren(...rows);
}
function updateDraft() {
  const lat = Number($('latitude').value), lng = Number($('longitude').value), radius = Number($('radius').value);
  if (!Number.isFinite(lat) || !Number.isFinite(lng) || !Number.isFinite(radius) || Math.abs(lat) > 90 || Math.abs(lng) > 180 || radius <= 0) return;
  if (!draftCircle) draftCircle = L.circle([lat, lng], { renderer: zoneRenderer, radius, color: '#bb713a', weight: 2, fillOpacity: .1, dashArray: '5 5' }).addTo(map);
  else draftCircle.setLatLng([lat, lng]).setRadius(radius);
}
function startEdit(zone = null) {
  switchView('zones'); editId = zone?.id || null;
  $('zone-editor').hidden = false; $('editor-title').textContent = zone ? 'Edit geofence' : 'New geofence'; $('save-zone').replaceChildren(document.createTextNode(zone ? 'Save changes' : 'Create geofence'), icon('arrow'));
  if (zone) {
    $('name').value = zone.name; $('latitude').value = zone.latitude; $('longitude').value = zone.longitude; $('radius').value = zone.radius_m;
  } else { const centre = map.getCenter(); $('name').value = 'New geofence'; $('latitude').value = centre.lat.toFixed(6); $('longitude').value = centre.lng.toFixed(6); $('radius').value = 500; }
  $('map-mode').textContent = 'Click to choose a centre'; updateDraft();
  if (zone) map.fitBounds(draftCircle.getBounds(), { padding: [65, 65], maxZoom: 16 });
  $('name').focus();
}
function resetEdit() { if ($('zone-editor').contains(document.activeElement)) document.activeElement.blur(); editId = null; $('zone-editor').hidden = true; $('map-mode').textContent = 'Live positions'; if (draftCircle) { map.removeLayer(draftCircle); draftCircle = null; } }
$('new-zone').onclick = () => startEdit(); $('empty-create').onclick = () => startEdit(); $('cancel-edit').onclick = resetEdit;
['latitude', 'longitude', 'radius'].forEach((id) => { $(id).oninput = updateDraft; });
document.querySelectorAll('[data-radius]').forEach((button) => { button.onclick = () => { $('radius').value = button.dataset.radius; updateDraft(); }; });
$('zone-form').onsubmit = async (event) => {
  event.preventDefault(); const who = user, version = epoch, id = editId;
  const payload = { name: $('name').value.trim(), latitude: Number($('latitude').value), longitude: Number($('longitude').value), radius_m: Number($('radius').value) };
  if (!payload.name) { error('Give your geofence a name before saving.'); return; }
  $('save-zone').disabled = true;
  try {
    await api(id ? `/geozones/${id}` : '/geozones', { method: id ? 'PATCH' : 'POST', body: JSON.stringify(payload) }, who);
    if (!current(version, who)) return; resetEdit(); await loadZones(version, who);
  } catch (cause) { if (current(version, who)) error(cause.message); }
  finally { if (current(version, who)) $('save-zone').disabled = false; }
};
$('zone-visibility').onclick = () => { zonesVisible = !zonesVisible; $('zone-visibility').setAttribute('aria-pressed', String(zonesVisible)); for (const { circle } of zones.values()) { if (zonesVisible) circle.addTo(map); else map.removeLayer(circle); } };

async function loadInsights() {
  const who = user, version = epoch, loadId = ++insightLoadId;
  $('refresh-insights').disabled = true;
  try {
    const data = await api('/insights', {}, who); if (!current(version, who) || loadId !== insightLoadId) return;
    const rows = [];
    for (const [label, value] of [['Reporting in the last minute', data.fleet.active_devices], ['Older than one minute', data.fleet.stale_devices], ['Active geofences', data.active_zones]]) { const row = element('div', undefined, 'insight-row'); row.append(element('span', label), element('strong', count(value))); rows.push(row); }
    rows.push(element('h3', 'Devices inside your zones', 'insight-section-title'));
    for (const zone of data.zones) { const row = element('div', undefined, 'occupancy-row'); row.append(element('span', zone.name), element('strong', zone.active ? `${count(zone.devices_inside)} inside` : 'Paused')); rows.push(row); }
    if (!data.zones.length) rows.push(element('p', 'Create a geofence to see its current occupancy.', 'view-description'));
    if (data.zones_truncated) rows.push(element('p', `Showing the first ${data.zones.length} zones.`, 'list-caption'));
    $('insights').replaceChildren(...rows);
  } catch (cause) { if (current(version, who) && loadId === insightLoadId) $('insights').replaceChildren(element('p', 'Could not refresh the brief. Try Refresh again.', 'view-description')); }
  finally { if (current(version, who) && loadId === insightLoadId) $('refresh-insights').disabled = false; }
}
$('refresh-insights').onclick = loadInsights;


function status(state) {
  const text = { live: 'Connected', connecting: 'Connecting', reconnecting: 'Reconnecting' }[state];
  $('status').className = `connection ${state}`; $('status-label').textContent = text;
  $('system-status').textContent = state === 'live' ? `Live stream connected · ${user}` : state === 'connecting' ? 'Connecting to the live stream' : 'Stream interrupted · reconnecting';
}
worker.onmessage = async ({ data }) => {
  if (data.type === 'frame') {
    fleet.update(data.size, data.slots, data.points);
    $('device-count').textContent = count(data.size); $('map-empty').hidden = data.size > 0;
    if (data.selected) receiveSelected(data.selected);
  } else if (data.type === 'reset') { fleet.clear(); $('device-count').textContent = '0'; $('map-empty').hidden = false; }
  else if (data.type === 'alerts') receiveAlerts(data);
  else if (data.type === 'rate') $('update-rate').textContent = `${count(data.received)} updates / sec`;
  else if (data.type === 'status') status(data.state);
  else if (data.type === 'zones_changed') {
    const who = user, version = epoch;
    try { await loadZones(version, who); if (current(version, who) && currentView === 'brief') loadInsights(); }
    catch (cause) { if (current(version, who)) error(cause.message); }
  } else if (data.type === 'reply') { pending.get(data.request)?.(data.data); pending.delete(data.request); }
  else if (data.type === 'error') error(data.message);
};
let viewportTimer;
map.on('moveend', () => {
  clearTimeout(viewportTimer);
  viewportTimer = setTimeout(() => worker.postMessage({ type: 'viewport', view: viewport() }), 200);
});
function connect() { worker.postMessage({ type: 'connect', user, view: viewport() }); }
function setIdentity() {
  $('user').value = user; $('identity-toggle').textContent = user.slice(0, 1).toUpperCase();
  $('workspace-label').textContent = `${user.slice(0, 1).toUpperCase()}${user.slice(1)}’s workspace`;
}
$('identity-toggle').onclick = () => { $('identity-panel').hidden = !$('identity-panel').hidden; if (!$('identity-panel').hidden) $('user').focus(); };
$('identity').onsubmit = (event) => {
  event.preventDefault(); const next = $('user').value.trim(); if (!next || next === user) { $('identity-panel').hidden = true; return; }
  user = next; epoch++; resetEdit(); closeInspector();
  zoneLoadId++; alertTotal = 0; unread = 0; feed = [];
  $('alert-count').textContent = '0'; $('activity-badge').hidden = true; renderAlerts();
  for (const entry of zones.values()) map.removeLayer(entry.circle); zones.clear(); renderZones(); $('zone-count').textContent = '0';
  $('insights').replaceChildren(element('p', 'Loading the latest picture…', 'view-description')); $('save-zone').disabled = false;
  $('identity-panel').hidden = true; setIdentity();
  const url = new URL(location.href); url.searchParams.set('user', user); history.replaceState({}, '', url);
  connect(); if (currentView === 'brief') loadInsights();
};
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  $('theme-toggle').setAttribute('aria-label', `Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`);
  localStorage.setItem('fleetline-theme', theme); fleet.redraw();
}
$('theme-toggle').onclick = () => applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');
applyTheme(localStorage.getItem('fleetline-theme') || 'light');
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') { resetEdit(); $('identity-panel').hidden = true; }
  if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) { event.preventDefault(); switchView('fleet'); $('fleet-search').focus(); }
});
setInterval(() => { $('clock').textContent = new Date().toLocaleTimeString(); }, 1000);
let acceptedRate = null;
async function metrics() {
  try {
    const data = await api('/stats'), fresh = data.freshness_p95_seconds; acceptedRate = data.reports_per_second;
    $('latency').textContent = fresh === null ? 'Freshness —' : `Fresh p95 ${fresh < 1 ? `${Math.round(fresh * 1000)} ms` : `${fresh.toFixed(1)} s`}`;
  } catch { $('latency').textContent = 'Freshness unavailable'; }
}
setInterval(metrics, 5000); metrics(); setIdentity(); renderZones(); renderAlerts(); renderFleet(); connect();

const minutes = (seconds) => `${Math.floor(seconds / 60)}:${String(Math.floor(seconds % 60)).padStart(2, '0')}`;
let loadRunning = false;
function loadState(state) {
  loadRunning = state.running;
  $('load-toggle').textContent = state.running ? '■ Stop load' : '▶ Start load';
  for (const id of ['load-devices', 'load-interval', 'load-duration', 'load-spread']) $(id).disabled = state.running;
  if (state.running) {
    const { load } = state, accepted = acceptedRate === null ? '—' : count(Math.round(acceptedRate));
    $('load-status').textContent = `Running ${minutes(state.elapsed_seconds)} of ${minutes(load.duration_seconds)} · ${count(load.devices)} devices · offered ${count(Math.round(state.offered_reports_per_second))}/s · accepted ${accepted}/s`;
  } else if (state.result) {
    $('load-status').textContent = `Finished: ${count(state.result.acked)} of ${count(state.result.scheduled)} reports acknowledged, ${count(Math.round(state.result.acked_reports_per_second))}/s.`;
  } else if (state.stopped_early) $('load-status').textContent = `Stopped after ${minutes(state.elapsed_seconds)}.`;
  else $('load-status').textContent = 'Idle. Devices start around the map centre.';
}
document.querySelectorAll('[data-devices]').forEach((button) => { button.onclick = () => { $('load-devices').value = button.dataset.devices; }; });
$('load-toggle').onclick = async () => {
  $('load-toggle').disabled = true;
  try {
    const centre = map.getCenter();
    const body = { devices: Number($('load-devices').value), interval_seconds: Number($('load-interval').value), duration_seconds: Number($('load-duration').value) * 60, spread_km: Number($('load-spread').value), latitude: centre.lat, longitude: ((centre.lng + 180) % 360 + 360) % 360 - 180 };
    loadState(await api(loadRunning ? '/loadgen/stop' : '/loadgen/start', loadRunning ? { method: 'POST' } : { method: 'POST', body: JSON.stringify(body) }));
  } catch (cause) { error(cause.message); }
  finally { $('load-toggle').disabled = false; }
};
async function refreshLoad() {
  try { loadState(await api('/loadgen')); } catch { $('load-status').textContent = 'The load generator is not reachable.'; }
}
setInterval(refreshLoad, 2000); refreshLoad();

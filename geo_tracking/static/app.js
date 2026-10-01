const $ = (id) => document.getElementById(id);
const icon = (name) => { const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg'); const use = document.createElementNS(svg.namespaceURI, 'use'); use.setAttribute('href', `#i-${name}`); svg.append(use); return svg; };
const element = (tag, text, className) => { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (className) node.className = className; return node; };
const count = (value) => Number(value || 0).toLocaleString();
const requestedUser = new URLSearchParams(location.search).get('user');
let user = requestedUser && /^[\w.-]{1,96}$/.test(requestedUser) ? requestedUser : 'alice';
let epoch = 0, socket, connectionVersion = 0, editId = null, selectedId = null, following = false;
let currentView = 'fleet', zonesVisible = true, alertTotal = 0, unread = 0, received = 0, rateReceived = 0;
let fleetDirty = true, alertsDirty = false, insightLoadId = 0, zoneLoadId = 0, snapshotController, errorTimer;
let demoPrefix = null, demoRunning = false;
const deviceName = (id) => demoPrefix && id.startsWith(demoPrefix) ? `Машина ${id.slice(demoPrefix.length)}` : id;
const positions = new Map(), zones = new Map(), alertFeed = [], pendingPositions = new Map();
const map = L.map('map', { preferCanvas: true, zoomControl: false, attributionControl: false }).setView([56.9496, 24.1052], 13);
L.control.zoom({ position: 'bottomright' }).addTo(map);
L.control.scale({ position: 'bottomleft', imperial: false }).addTo(map);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19 }).addTo(map);
const zoneRenderer = L.canvas();
let draftCircle;

function timestampKey(value) {
  if (typeof value === 'number') return BigInt(value);
  const fraction = /\.(\d+)/.exec(value)?.[1] || '';
  return BigInt(Date.parse(value)) * 1000n + BigInt(fraction.padEnd(6, '0').slice(3, 6) || '0');
}
const isoTime = (value) => typeof value === 'number' ? new Date(Math.floor(value / 1000)).toISOString() : value;
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
function connectionCurrent(ws, version, generation) { return version === epoch && socket === ws && generation === connectionVersion; }
function age(timestamp) {
  const seconds = Math.max(0, Math.floor((Date.now() - Date.parse(timestamp)) / 1000));
  return seconds < 2 ? 'just now' : seconds < 60 ? `${seconds}s ago` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ago` : `${Math.floor(seconds / 3600)}h ago`;
}

const FleetCanvas = L.Layer.extend({
  onAdd() {
    this.canvas = L.DomUtil.create('canvas', 'fleet-canvas leaflet-zoom-hide');
    map.getPane('overlayPane').append(this.canvas);
    this.frame = null;
    this.redraw = this.redraw.bind(this);
    map.on('moveend zoomend resize', this.redraw);
    this.redraw();
  },
  redraw() {
    if (this.frame !== null) return;
    this.frame = requestAnimationFrame(() => {
      this.frame = null;
      const size = map.getSize(), ratio = Math.min(devicePixelRatio || 1, 2);
      const topLeft = map.containerPointToLayerPoint([0, 0]);
      L.DomUtil.setPosition(this.canvas, topLeft);
      this.canvas.width = size.x * ratio;
      this.canvas.height = size.y * ratio;
      this.canvas.style.width = `${size.x}px`;
      this.canvas.style.height = `${size.y}px`;
      const context = this.canvas.getContext('2d');
      context.scale(ratio, ratio);
      context.fillStyle = document.documentElement.dataset.theme === 'dark' ? '#c2dea9' : '#255b40';
      context.beginPath();
      let visible = 0;
      const cells = new Map(), dense = positions.size > 2500 && map.getZoom() < 16;
      this.clusters = [];
      for (const entry of positions.values()) {
        const point = map.latLngToContainerPoint([entry.latitude, entry.longitude]);
        entry.point = point;
        if (point.x < -10 || point.y < -10 || point.x > size.x + 10 || point.y > size.y + 10) continue;
        visible++;
        if (dense) {
          const key = `${Math.floor(point.x / 34)},${Math.floor(point.y / 34)}`, cell = cells.get(key) || { x: 0, y: 0, count: 0 };
          cell.x += point.x; cell.y += point.y; cell.count++; cells.set(key, cell); continue;
        }
        context.moveTo(point.x + 3, point.y);
        context.arc(point.x, point.y, 3, 0, Math.PI * 2);
      }
      context.fill();
      if (dense) {
        for (const cell of cells.values()) {
          const x = cell.x / cell.count, y = cell.y / cell.count, radius = Math.min(17, 6 + Math.log2(cell.count));
          context.fillStyle = document.documentElement.dataset.theme === 'dark' ? '#c2dea9' : '#255b40';
          context.beginPath(); context.arc(x, y, radius, 0, Math.PI * 2); context.fill();
          if (cell.count > 3) { context.fillStyle = '#fffefa'; if (document.documentElement.dataset.theme === 'dark') context.fillStyle = '#18231e'; context.font = '700 11px Manrope'; context.textAlign = 'center'; context.textBaseline = 'middle'; context.fillText(cell.count > 999 ? `${(cell.count / 1000).toFixed(1)}k` : String(cell.count), x, y); }
          this.clusters.push({ x, y, count: cell.count, radius });
        }
      }
      $('device-legend').textContent = dense ? 'Fleet density' : 'Device';
      if (selectedId && positions.has(selectedId)) {
        const selected = positions.get(selectedId), point = selected.point;
        context.strokeStyle = '#bb713a'; context.lineWidth = 2;
        context.beginPath(); context.arc(point.x, point.y, 9, 0, Math.PI * 2); context.stroke();
        if (selected.trail?.length > 1) {
          context.beginPath();
          selected.trail.forEach(([lat, lng], index) => { const p = map.latLngToContainerPoint([lat, lng]); if (index) context.lineTo(p.x, p.y); else context.moveTo(p.x, p.y); });
          context.stroke();
        }
      }
      $('visible-count').textContent = `${count(visible)} on map`;
    });
  },
});
const fleetCanvas = new FleetCanvas().addTo(map);

function queuePosition(item) {
  if (demoPrefix && !item.device_id.startsWith(demoPrefix)) return;
  const key = timestampKey(item.timestamp), previous = pendingPositions.get(item.device_id) || positions.get(item.device_id);
  if (previous && previous.key >= key) return;
  pendingPositions.set(item.device_id, { ...item, timestamp: isoTime(item.timestamp), key });
}
const positionItem = ([device_id, latitude, longitude, timestamp]) => ({ device_id, latitude, longitude, timestamp });
function flushPositions() {
  if (!pendingPositions.size) return;
  for (const [id, item] of pendingPositions) {
    const previous = positions.get(id);
    if (id === selectedId) item.trail = [...(previous?.trail || []), [item.latitude, item.longitude]].slice(-30);
    positions.set(id, item);
  }
  pendingPositions.clear();
  fleetDirty = true;
  $('device-count').textContent = count(positions.size);
  $('map-empty').hidden = positions.size > 0;
  updateInspector();
  if (following && selectedId && positions.has(selectedId)) {
    const item = positions.get(selectedId);
    map.panTo([item.latitude, item.longitude], { animate: false });
  }
  fleetCanvas.redraw();
}
setInterval(flushPositions, 100);

function renderFleet() {
  if (currentView !== 'fleet' || !fleetDirty || $('fleet-list').contains(document.activeElement)) return;
  fleetDirty = false;
  const search = $('fleet-search').value.trim().toLowerCase(), filter = $('fleet-filter').value, now = Date.now();
  const matches = [];
  for (const item of positions.values()) {
    const fresh = now - Date.parse(item.timestamp) < 60000;
    if (search && !item.device_id.toLowerCase().includes(search)) continue;
    if (filter === 'fresh' && !fresh || filter === 'stale' && fresh) continue;
    matches.push(item);
  }
  const rows = matches.slice(0, 60).map((item) => {
    const button = element('button', undefined, `fleet-row${selectedId === item.device_id ? ' selected' : ''}`);
    button.type = 'button';
    const marker = element('span', undefined, 'fleet-icon'); marker.append(icon('fleet'));
    const text = element('span'); text.append(element('strong', deviceName(item.device_id)), element('small', `${item.latitude.toFixed(4)}, ${item.longitude.toFixed(4)}`));
    const when = element('span', age(item.timestamp), `fleet-age${now - Date.parse(item.timestamp) >= 60000 ? ' stale' : ''}`);
    button.append(marker, text, when); button.onclick = () => selectDevice(item.device_id);
    return button;
  });
  if (!rows.length) {
    const empty = element('div', undefined, 'empty-state'); empty.append(icon('fleet'), element('h3', positions.size ? 'No matching devices' : 'Waiting for the first signal'), element('p', positions.size ? 'Try another device name or change the filter.' : 'Connect your devices or start the load generator. Live positions will appear on the map.'));
    rows.push(empty);
  }
  $('fleet-list').replaceChildren(...rows);
  $('fleet-list-caption').textContent = matches.length ? `${count(matches.length)} matching devices${matches.length > 60 ? ' · showing the first 60, search to narrow' : ''}` : '';
}
$('fleet-search').oninput = () => { fleetDirty = true; renderFleet(); };
$('fleet-filter').onchange = () => { fleetDirty = true; renderFleet(); };
setInterval(() => { fleetDirty = true; renderFleet(); updateInspector(); }, 1500);
function selectDevice(id) {
  const item = positions.get(id); if (!item) return;
  selectedId = id; following = false; item.trail = [[item.latitude, item.longitude]];
  $('inspector').hidden = false; $('follow-device').textContent = 'Follow on map';
  map.setView([item.latitude, item.longitude], Math.max(map.getZoom(), 14));
  fleetDirty = true; renderFleet(); updateInspector(); fleetCanvas.redraw();
}
function updateInspector() {
  if (!selectedId || !positions.has(selectedId)) return;
  const item = positions.get(selectedId);
  $('inspector-id').textContent = item.device_id;
  $('inspector-lat').textContent = item.latitude.toFixed(6);
  $('inspector-lng').textContent = item.longitude.toFixed(6);
  $('inspector-time').textContent = new Date(item.timestamp).toLocaleTimeString();
  $('inspector-age').textContent = age(item.timestamp);
}
$('close-inspector').onclick = () => { selectedId = null; following = false; $('inspector').hidden = true; fleetDirty = true; renderFleet(); fleetCanvas.redraw(); };
$('follow-device').onclick = () => { following = !following; $('follow-device').textContent = following ? 'Stop following' : 'Follow on map'; };
map.on('click', ({ latlng, containerPoint }) => {
  if (!$('zone-editor').hidden) {
    $('latitude').value = latlng.lat.toFixed(6); $('longitude').value = latlng.lng.toFixed(6); updateDraft(); return;
  }
  const cluster = fleetCanvas.clusters?.find((entry) => entry.count > 3 && Math.hypot(entry.x - containerPoint.x, entry.y - containerPoint.y) < entry.radius + 3);
  if (cluster) { map.setView(latlng, Math.min(map.getZoom() + 2, 18)); return; }
  let nearest, distance = 13;
  for (const item of positions.values()) {
    if (!item.point) continue;
    const d = item.point.distanceTo(containerPoint);
    if (d < distance) { nearest = item.device_id; distance = d; }
  }
  if (nearest) selectDevice(nearest);
});
map.on('moveend', () => {
  const centre = map.getCenter();
  $('map-location').textContent = Math.abs(centre.lat - 56.9496) < .15 && Math.abs(centre.lng - 24.1052) < .3 ? 'Riga, Latvia' : `${centre.lat.toFixed(3)}°, ${centre.lng.toFixed(3)}°`;
});
$('fit-fleet').onclick = () => {
  if (!positions.size) { error('No device positions yet. Start reporting locations to see your fleet.'); return; }
  map.fitBounds(L.latLngBounds([...positions.values()].map((item) => [item.latitude, item.longitude])), { padding: [60, 60], maxZoom: 15 });
};

function switchView(view) {
  currentView = view;
  document.querySelectorAll('[data-view]').forEach((button) => { const active = button.dataset.view === view; button.classList.toggle('selected', active); button.setAttribute('aria-pressed', String(active)); });
  ['fleet', 'zones', 'activity', 'brief'].forEach((name) => { $(`${name}-view`).hidden = name !== view; });
  if (view === 'fleet') { fleetDirty = true; renderFleet(); }
  if (view === 'activity') { unread = 0; $('activity-badge').hidden = true; renderAlerts(); }
  if (view === 'brief') loadInsights();
}
document.querySelectorAll('[data-view]').forEach((button) => { button.onclick = () => switchView(button.dataset.view); });
function receiveAlerts(items) {
  items = items.map((item) => ({ ...item, timestamp: isoTime(item.timestamp) }));
  if (demoPrefix) items = items.filter((item) => item.device_id.startsWith(demoPrefix));
  alertTotal += items.length;
  if (currentView !== 'activity') unread += items.length;
  alertFeed.unshift(...items.slice(-80).reverse()); alertFeed.length = Math.min(alertFeed.length, 80);
  $('alert-count').textContent = count(alertTotal);
  $('activity-badge').textContent = unread > 99 ? '99+' : count(unread); $('activity-badge').hidden = !unread;
  alertsDirty = true;
}
setInterval(() => { if (alertsDirty && currentView === 'activity') renderAlerts(); }, 350);
function renderAlerts() {
  alertsDirty = false;
  const rows = alertFeed.map((item) => {
    const row = element('div', undefined, 'alert-row'), symbol = element('span', undefined, 'alert-symbol'), content = element('div');
    symbol.append(icon('zone')); content.append(element('strong', deviceName(item.device_id)), element('p', demoPrefix ? 'Машина сейчас внутри круга склада' : `Inside ${zones.get(item.zone_id)?.zone.name || 'geofence'} · version ${item.zone_version}`));
    const time = element('time', new Date(item.timestamp).toLocaleTimeString()); time.dateTime = item.timestamp; content.append(time); row.append(symbol, content); return row;
  });
  if (!rows.length) { const empty = element('div', undefined, 'empty-state'); empty.append(icon('activity'), element('h3', 'Nothing has crossed your radar yet'), element('p', 'Fresh reports inside your active geofences appear here, in every connected session.')); rows.push(empty); }
  $('alerts').replaceChildren(...rows);
}
$('clear-alerts').onclick = () => { alertFeed.length = 0; unread = 0; $('activity-badge').hidden = true; renderAlerts(); };

async function loadZones(version = epoch, who = user, ws = socket) {
  const loadId = ++zoneLoadId, result = [];
  let cursor;
  do {
    const page = await api(`/geozones?limit=1000${cursor ? `&after=${encodeURIComponent(cursor)}` : ''}`, {}, who);
    if (!current(version, who) || socket !== ws || loadId !== zoneLoadId) return;
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
    action(zone.active ? 'Pause' : 'Resume', async () => { const who = user, version = epoch, ws = socket; await api(`/geozones/${zone.id}`, { method: 'PATCH', body: JSON.stringify({ active: !zone.active }) }, who); if (current(version, who)) await loadZones(version, who, ws); });
    action('Delete', async (button) => {
      if (button.dataset.confirm !== 'yes') { button.dataset.confirm = 'yes'; button.textContent = 'Confirm?'; setTimeout(() => { button.dataset.confirm = ''; button.textContent = 'Delete'; }, 4000); return; }
      const who = user, version = epoch, ws = socket;
      await api(`/geozones/${zone.id}`, { method: 'DELETE' }, who);
      if (!current(version, who)) return;
      if (editId === zone.id) resetEdit(); await loadZones(version, who, ws);
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
  event.preventDefault(); const who = user, version = epoch, ws = socket, id = editId;
  const payload = { name: $('name').value.trim(), latitude: Number($('latitude').value), longitude: Number($('longitude').value), radius_m: Number($('radius').value) };
  if (!payload.name) { error('Give your geofence a name before saving.'); return; }
  $('save-zone').disabled = true;
  try {
    await api(id ? `/geozones/${id}` : '/geozones', { method: id ? 'PATCH' : 'POST', body: JSON.stringify(payload) }, who);
    if (!current(version, who)) return; resetEdit(); await loadZones(version, who, ws);
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
    if (data.zones_truncated) rows.push(element('p', 'Showing the first 50 zones.', 'list-caption'));
    $('insights').replaceChildren(...rows);
  } catch (cause) { if (current(version, who) && loadId === insightLoadId) $('insights').replaceChildren(element('p', 'Could not refresh the brief. Try Refresh again.', 'view-description')); }
  finally { if (current(version, who) && loadId === insightLoadId) $('refresh-insights').disabled = false; }
}
$('refresh-insights').onclick = loadInsights;

function status(state, text) {
  $('status').className = `connection ${state}`; $('status-label').textContent = text;
  $('system-status').textContent = state === 'live' ? `Live stream connected · ${user}` : state === 'connecting' ? 'Connecting to the live stream' : 'Stream interrupted · reconnecting';
}
async function loadSnapshot(who, version, ws, generation) {
  snapshotController?.abort(); snapshotController = new AbortController();
  const controller = snapshotController, view = viewport();
  const area = `south=${view.south}&west=${view.west}&north=${view.north}&east=${view.east}`;
  try {
    let cursor;
    do {
      const page = await api(`/devices/latest?limit=1000&${area}${cursor ? `&after=${encodeURIComponent(cursor)}` : ''}`, { signal: controller.signal }, who);
      if (!connectionCurrent(ws, version, generation)) return;
      page.items.forEach(queuePosition); cursor = page.next_cursor;
    } while (cursor);
  } catch (cause) { if (cause.name !== 'AbortError' && connectionCurrent(ws, version, generation)) error(cause.message); }
}
let viewportTimer;
function sendViewport() {
  clearTimeout(viewportTimer);
  viewportTimer = setTimeout(() => { if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ type: 'viewport', ...viewport() })); }, 200);
}
map.on('moveend', sendViewport);
function connect(who, version) {
  const generation = ++connectionVersion;
  const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws?user_id=${encodeURIComponent(who)}`);
  socket = ws; status('connecting', 'Connecting');
  ws.onmessage = async ({ data }) => {
    if (!connectionCurrent(ws, version, generation)) return;
    const message = JSON.parse(data);
    if (message.type === 'ready') {
      status('live', 'Connected');
      ws.send(JSON.stringify({ type: 'viewport', ...viewport() }));
      try { await loadZones(version, who, ws); } catch (cause) { if (connectionCurrent(ws, version, generation)) error(cause.message); }
    } else if (message.type === 'subscribed') loadSnapshot(who, version, ws, generation);
    else if (message.type === 'positions') { received += message.items.length; message.items.forEach((item) => queuePosition(positionItem(item))); }
    else if (message.type === 'inside_report') receiveAlerts(message.items);
    else if (message.type === 'zones_changed') {
      try { await loadZones(version, who, ws); if (connectionCurrent(ws, version, generation) && currentView === 'brief') loadInsights(); }
      catch (cause) { if (connectionCurrent(ws, version, generation)) error(cause.message); }
    }
  };
  ws.onclose = () => {
    if (!connectionCurrent(ws, version, generation)) return;
    snapshotController?.abort(); status('reconnecting', 'Reconnecting');
    setTimeout(() => { if (connectionCurrent(ws, version, generation)) connect(who, version); }, 1500);
  };
  ws.onerror = () => { if (connectionCurrent(ws, version, generation)) status('reconnecting', 'Reconnecting'); };
}
function setIdentity() {
  $('user').value = user; $('identity-toggle').textContent = user.slice(0, 1).toUpperCase();
  $('workspace-label').textContent = `${user.slice(0, 1).toUpperCase()}${user.slice(1)}’s workspace`;
}
$('identity-toggle').onclick = () => { $('identity-panel').hidden = !$('identity-panel').hidden; if (!$('identity-panel').hidden) $('user').focus(); };
$('identity').onsubmit = (event) => {
  event.preventDefault(); const next = $('user').value.trim(); if (!next || next === user) { $('identity-panel').hidden = true; return; }
  demoPrefix = null; demoRunning = false; $('demo-all').hidden = true;
  user = next; epoch++; socket?.close(); snapshotController?.abort(); resetEdit();
  zoneLoadId++; alertTotal = 0; unread = 0; alertFeed.length = 0; pendingPositions.clear();
  $('alert-count').textContent = '0'; $('activity-badge').hidden = true; renderAlerts();
  for (const entry of zones.values()) map.removeLayer(entry.circle); zones.clear(); renderZones(); $('zone-count').textContent = '0';
  $('insights').replaceChildren(element('p', 'Loading the latest picture…', 'view-description')); $('save-zone').disabled = false;
  $('identity-panel').hidden = true; setIdentity();
  const url = new URL(location.href); url.searchParams.set('user', user); history.replaceState({}, '', url);
  connect(user, epoch); if (currentView === 'brief') loadInsights();
};
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  $('theme-toggle').setAttribute('aria-label', `Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`);
  localStorage.setItem('fleetline-theme', theme); fleetCanvas.redraw();
}
$('theme-toggle').onclick = () => applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');
applyTheme(localStorage.getItem('fleetline-theme') || 'light');
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') { resetEdit(); $('identity-panel').hidden = true; }
  if (event.key === '/' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(document.activeElement.tagName)) { event.preventDefault(); switchView('fleet'); $('fleet-search').focus(); }
});
setInterval(() => { $('update-rate').textContent = `${count(received - rateReceived)} updates / sec`; rateReceived = received; $('clock').textContent = new Date().toLocaleTimeString(); }, 1000);
async function metrics() {
  try {
    const data = await api('/metrics'), p95 = data.instances.filter((item) => item.role === 'processor' && item.processing_ms.p95 !== undefined).map((item) => item.processing_ms.p95);
    $('latency').textContent = p95.length ? `Processing p95 ${Math.round(Math.max(...p95))} ms` : 'Processing —';
  } catch { $('latency').textContent = 'Processing unavailable'; }
}
setInterval(metrics, 5000); metrics(); setIdentity(); renderZones(); renderAlerts(); renderFleet(); connect(user, epoch);

function enterDemo(state) {
  demoPrefix = state.device_prefix;
  positions.clear(); pendingPositions.clear(); selectedId = null; following = false;
  $('inspector').hidden = true; $('device-count').textContent = '0';
  alertFeed.length = 0; alertTotal = 0; unread = 0; $('alert-count').textContent = '0';
  $('activity-badge').hidden = true; $('fleet-search').value = ''; $('fleet-filter').value = 'all';
  fleetDirty = true; renderAlerts(); fleetCanvas.redraw();
  $('demo-all').hidden = false;
  map.setView([state.latitude, state.longitude], 16);
  switchView('activity');
}
function demoState(state) {
  demoRunning = state.running;
  $('demo-toggle').textContent = state.running ? '■ Остановить демо' : '▶ Запустить демо';
  $('demo-status').textContent = state.running
    ? 'Демо идёт: машины обновляются каждую секунду. Уведомления — в списке ниже. Автостоп через 2 минуты.'
    : demoPrefix ? 'Демо остановлено. Машины замерли. Нажми запуск, чтобы повторить.' : 'Нажми кнопку — всё начнётся автоматически.';
}
$('demo-toggle').onclick = async () => {
  const who = user, version = epoch;
  $('demo-toggle').disabled = true;
  try {
    const state = await api(demoRunning ? '/demo/stop' : '/demo/start', {method: 'POST'}, who);
    if (!current(version, who)) return;
    if (state.running) { enterDemo(state); await loadZones(version, who); }
    demoState(state);
  } catch (cause) { if (current(version, who)) error(cause.message); }
  finally { if (current(version, who)) $('demo-toggle').disabled = false; }
};
$('demo-all').onclick = () => {
  demoPrefix = null; positions.clear(); pendingPositions.clear();
  $('demo-all').hidden = true; $('device-count').textContent = '0'; fleetDirty = true;
  switchView('fleet'); connect(user, epoch); refreshDemo();
};
async function refreshDemo() {
  const who = user, version = epoch;
  try {
    const state = await api('/demo', {}, who);
    if (!current(version, who)) return;
    demoState(state);
  } catch (cause) { if (current(version, who)) $('demo-status').textContent = 'Не удалось проверить демо. Попробуй кнопку запуска.'; }
}
setInterval(refreshDemo, 2000); refreshDemo();

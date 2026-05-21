// Route-level map investigation page.
// URL params: ?week_end=YYYY-MM-DD&route_id=<id>&dir=0|1
//             &pct=p95|p50&mode=adherence|volatility           (schedule-adherence category)
//             &category=adherence|speed&speed_metric=mean|p50|stddev  (new: route-speed category)
//             &stats=open|closed&speed=open|closed

const params = new URLSearchParams(window.location.search);
const weekEnd = params.get("week_end") || "";
const routeID = params.get("route_id") || "";
let activeDir = parseInt(params.get("dir") || "0", 10);
let activePct = params.get("pct") || "p95"; // "p50" | "p95"
let activeMode = params.get("mode") || "adherence"; // "adherence" | "volatility"
let activeCategory = params.get("category") || "adherence"; // "adherence" | "speed"
let activeSpeedMetric = params.get("speed_metric") || "mean"; // "mean" | "p50" | "stddev"
let statsOpen = (params.get("stats") || "open") !== "closed";
let speedOpen = (params.get("speed") || "open") !== "closed";

// ── colour + size helpers ──────────────────────────────────────────────────

// adherenceColor: maps a signed delay value (seconds) onto green→yellow→red.
// Thresholds: [-60, 180] = green zone; [-120, 420] = yellow zone; beyond = red.
function adherenceColor(delaySec) {
  if (delaySec === null || delaySec === undefined) return [180, 180, 180];
  const GREEN  = [46, 204, 113];
  const YELLOW = [243, 156, 18];
  const RED    = [231, 76, 60];
  const blend = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t));
  if (delaySec <= 180) {
    // green zone: clamp anything early/very-on-time fully green, gradient toward yellow at 180s
    const t = Math.max(0, (delaySec - (-60)) / (180 - (-60)));
    return blend(GREEN, YELLOW, t * 0.3); // keep mostly green until edge
  }
  if (delaySec <= 420) {
    // yellow zone: 180s → 420s
    const t = (delaySec - 180) / (420 - 180);
    return blend(YELLOW, RED, t * 0.5);
  }
  // red zone: 420s+, saturate to full red
  const t = Math.min(1, (delaySec - 420) / 300);
  return blend(YELLOW, RED, 0.5 + t * 0.5);
}

// volatilityColor: maps stddev (seconds) onto green→yellow→red.
// 0–60s = green, 60–180s = yellow, 180s+ = red
function volatilityColor(stddevSec) {
  if (stddevSec === null || stddevSec === undefined) return [180, 180, 180];
  const GREEN  = [46, 204, 113];
  const YELLOW = [243, 156, 18];
  const RED    = [231, 76, 60];
  const blend = (a, b, t) => a.map((v, i) => Math.round(v + (b[i] - v) * t));
  if (stddevSec <= 60) return blend(GREEN, YELLOW, (stddevSec / 60) * 0.2);
  if (stddevSec <= 180) return blend(YELLOW, RED, (stddevSec - 60) / 120 * 0.5);
  return blend(YELLOW, RED, 0.5 + Math.min(1, (stddevSec - 180) / 180) * 0.5);
}

// Multi-stop linear interpolation between RGB anchors keyed on a numeric
// value. Values outside the range clamp to the nearest anchor. Shared
// by the speed and speed-stddev gradients below.
function interpRGB(v, stops) {
  if (v === null || v === undefined || isNaN(v)) return [180, 180, 180];
  if (v <= stops[0].v) return stops[0].rgb;
  if (v >= stops[stops.length - 1].v) return stops[stops.length - 1].rgb;
  for (let i = 0; i < stops.length - 1; i++) {
    const lo = stops[i], hi = stops[i + 1];
    if (v <= hi.v) {
      const t = (v - lo.v) / (hi.v - lo.v);
      return lo.rgb.map((c, j) => Math.round(c + (hi.rgb[j] - c) * t));
    }
  }
  return stops[stops.length - 1].rgb;
}

// speedColor: per-leg average speed in mph mapped onto a slow→fast
// gradient. 5 mph = dark red, 8 mph = red, 12 mph = yellow,
// 15 mph = light green, 19+ mph = solid green. Used for both the mean
// and p50 speed views — same scale, different value source.
const SPEED_GRADIENT = [
  { v: 5,  rgb: [120,  10,  10] }, // dark red
  { v: 8,  rgb: [231,  76,  60] }, // red
  { v: 12, rgb: [243, 200,  30] }, // yellow
  { v: 15, rgb: [150, 210, 120] }, // light green
  { v: 19, rgb: [ 39, 174,  96] }, // solid green
];
function speedColor(mph) { return interpRGB(mph, SPEED_GRADIENT); }

// speedStddevColor: per-leg speed dispersion in mph. Low stddev =
// predictable trips = good (green); high stddev = bus speeds swing
// wildly leg-to-leg = bad (red). 1 mph or less green, 7 mph or more red.
const SPEED_STDDEV_GRADIENT = [
  { v: 1, rgb: [ 39, 174,  96] }, // green
  { v: 4, rgb: [243, 200,  30] }, // yellow midpoint
  { v: 7, rgb: [120,  10,  10] }, // dark red
];
function speedStddevColor(mph) { return interpRGB(mph, SPEED_STDDEV_GRADIENT); }

function toHex(arr) {
  return "#" + arr.map(v => v.toString(16).padStart(2, "0")).join("");
}

// stopRadius: returns px radius in [6, 20] range based on absolute value
function stopRadius(val, maxVal) {
  if (!val || !maxVal || maxVal === 0) return 8;
  return 6 + Math.min(1, Math.abs(val) / maxVal) * 14;
}

// ── per-stop metric extractors ─────────────────────────────────────────────

function getStopValue(stat) {
  if (!stat) return null;
  if (activeMode === "volatility") return stat.stddev_delay_s ?? null;
  return activePct === "p50" ? (stat.p50_delay_s ?? null) : (stat.p95_delay_s ?? null);
}

function getStopColor(val) {
  if (val === null) return [180, 180, 180];
  return activeMode === "volatility" ? volatilityColor(val) : adherenceColor(val);
}

// ── explanation text ───────────────────────────────────────────────────────

const INFO_TEXT = {
  "adherence-p95":   "Circle color shows how late the 95th-percentile bus is at each stop — the worst ~1-in-20 arrivals. Green = within −1 to +3 min of schedule; yellow = up to 7 min late; red = beyond that. Larger circles mean bigger delay at that percentile.",
  "adherence-p50":   "Circle color shows the median (p50) delay at each stop — the typical bus experience. Green = within −1 to +3 min of schedule; yellow = up to 7 min late; red = beyond. Larger circles mean higher typical delay.",
  "volatility":      "Circle color and size show the standard deviation of arrival delay across all observed trips at each stop for the week. Low stddev (green, small) = consistent timing even if somewhat late. High stddev (red, large) = unpredictable — some trips arrive on time, others very late.",
  "speed-mean":      "Line color shows the mean per-leg bus speed (mph) between consecutive stops, averaged across the week. Each leg's speed is computed as (distance / time) per observed trip; the line color is the mean of those. Red ≤ 5 mph (very slow), yellow ≈ 12 mph, green ≥ 19 mph (free-flowing).",
  "speed-p50":       "Line color shows the median (p50) per-leg bus speed between consecutive stops over the week. Same colorscale as the mean view: red ≤ 5 mph, yellow ≈ 12 mph, green ≥ 19 mph.",
  "speed-stddev":    "Line color shows the variability in per-leg bus speed (stddev across observed trips). Low stddev (green, ≤ 1 mph) = consistent leg times; high stddev (red, ≥ 7 mph) = unpredictable — some buses sail through, others crawl.",
};

const LEGEND_LABELS = {
  "adherence":     ["−1 min (early)", "+3 min", "+7 min+"],
  "volatility":    ["low variation", "moderate", "high variation"],
  "speed":         ["≤5 mph (slow)", "12 mph", "≥19 mph (fast)"],
  "speed-stddev":  ["≤1 mph (steady)", "4 mph", "≥7 mph (volatile)"],
};

const LEGEND_BARS = {
  "adherence":    "linear-gradient(to right, #2ecc71, #f39c12, #e74c3c)",
  "volatility":   "linear-gradient(to right, #2ecc71, #f39c12, #e74c3c)",
  "speed":        "linear-gradient(to right, rgb(120,10,10), rgb(231,76,60), rgb(243,200,30), rgb(150,210,120), rgb(39,174,96))",
  "speed-stddev": "linear-gradient(to right, rgb(39,174,96), rgb(243,200,30), rgb(120,10,10))",
};

// legendKeyFor returns the key into LEGEND_LABELS / LEGEND_BARS that
// matches the currently active view. Routes p50 + mean to the same
// "speed" scale (slow→fast); stddev gets its own (steady→volatile)
// because the polarity is flipped.
function legendKeyFor() {
  if (activeCategory === "speed") {
    return activeSpeedMetric === "stddev" ? "speed-stddev" : "speed";
  }
  return activeMode === "volatility" ? "volatility" : "adherence";
}

function infoKeyFor() {
  if (activeCategory === "speed") return `speed-${activeSpeedMetric}`;
  return activeMode === "volatility" ? "volatility" : `adherence-${activePct}`;
}

function updateInfoPanel() {
  document.getElementById("info-text").textContent = INFO_TEXT[infoKeyFor()] || "";

  const lkey = legendKeyFor();
  const labels = LEGEND_LABELS[lkey] || [];
  const spans = document.querySelectorAll("#legend-labels span");
  if (spans.length === 3 && labels.length === 3) {
    spans[0].textContent = labels[0];
    spans[1].textContent = labels[1];
    spans[2].textContent = labels[2];
  }
  const bar = document.getElementById("legend-bar");
  if (bar && LEGEND_BARS[lkey]) bar.style.background = LEGEND_BARS[lkey];

  let title;
  if (activeCategory === "speed") {
    title = activeSpeedMetric === "stddev" ? "Speed variability (mph)" : "Average bus speed (mph)";
  } else {
    title = activeMode === "volatility" ? "Volatility (stddev)" : "Schedule adherence";
  }
  document.getElementById("legend-title").textContent = title;

  // Size legend (small/large circles) only meaningful for adherence stops.
  const sizeRow = document.getElementById("legend-size-row");
  if (sizeRow) sizeRow.style.display = activeCategory === "speed" ? "none" : "";
}

// ── URL sync ───────────────────────────────────────────────────────────────

function syncURL() {
  const url = new URL(window.location.href);
  url.searchParams.set("dir", String(activeDir));
  if (activeCategory === "speed") {
    url.searchParams.set("category", "speed");
    url.searchParams.set("speed_metric", activeSpeedMetric);
    // Keep adherence params reachable in the URL so toggling back is
    // a one-click op, but don't bother including them if they're at
    // the default.
    url.searchParams.delete("pct");
    url.searchParams.delete("mode");
  } else {
    url.searchParams.delete("category");
    url.searchParams.delete("speed_metric");
    url.searchParams.set("pct", activePct);
    url.searchParams.set("mode", activeMode);
  }
  if (statsOpen) url.searchParams.delete("stats");
  else url.searchParams.set("stats", "closed");
  if (speedOpen) url.searchParams.delete("speed");
  else url.searchParams.set("speed", "closed");
  window.history.replaceState(null, "", url.toString());
}

// ── data loading ───────────────────────────────────────────────────────────

function sanitizeRouteID(rid) {
  return rid.replace(/[^a-zA-Z0-9\-_]/g, "_");
}

async function loadData() {
  if (!routeID) {
    document.getElementById("loading").textContent = "No route_id specified. Return to the weekly view and click a route's map icon.";
    return null;
  }

  const safe = sanitizeRouteID(routeID);
  const gtfsURL  = `${GCS_BASE}/gtfs/processed/route_${safe}.json`;
  const statsURL = weekEnd
    ? `${GCS_BASE}/stats/weekly/route_stops/${weekEnd}/${safe}.json`
    : null;
  const waitURL = weekEnd
    ? `${GCS_BASE}/stats/weekly/route_wait/${weekEnd}/${safe}.json`
    : null;
  const speedURL = weekEnd
    ? `${GCS_BASE}/stats/weekly/route_speed/${weekEnd}/${safe}.json`
    : null;

  try {
    const [gtfs, stopStats, waitStats, speedStats] = await Promise.all([
      fetchJSON(gtfsURL),
      statsURL  ? fetchJSON(statsURL).catch(() => null)  : Promise.resolve(null),
      waitURL   ? fetchJSON(waitURL).catch(() => null)   : Promise.resolve(null),
      speedURL  ? fetchJSON(speedURL).catch(() => null)  : Promise.resolve(null),
    ]);
    return { gtfs, stopStats, waitStats, speedStats };
  } catch (e) {
    document.getElementById("loading").textContent = `Failed to load data: ${e.message}`;
    return null;
  }
}

// ── GTFS direction helpers ─────────────────────────────────────────────────

// Returns { 0: { shapeID, stopIDs, representativeTrip }, 1: {...} }
function buildDirections(gtfs) {
  const dirs = {};
  const shapeCounts = {}; // dir → { shapeID → count }
  const tripsByDir = {};

  for (const trip of Object.values(gtfs.trips)) {
    const d = trip.direction_id;
    if (!dirs[d]) { dirs[d] = { stopIDs: new Set(), shapeIDs: {} }; }
    if (!shapeCounts[d]) shapeCounts[d] = {};
    if (!tripsByDir[d]) tripsByDir[d] = [];
    shapeCounts[d][trip.shape_id] = (shapeCounts[d][trip.shape_id] || 0) + 1;
    for (const st of trip.stop_times) {
      dirs[d].stopIDs.add(st.stop_id);
    }
    tripsByDir[d].push(trip);
  }

  // For each direction: dominant shape_id by trip count, plus the trip
  // riding that shape that has the most stops (used as the representative
  // stop-time sequence for slicing the route shape into per-pair segments).
  for (const d of Object.keys(dirs)) {
    const counts = shapeCounts[d] || {};
    let best = null, bestN = 0;
    for (const [sid, n] of Object.entries(counts)) {
      if (n > bestN) { best = sid; bestN = n; }
    }
    dirs[d].shapeID = best;
    dirs[d].stopIDs = [...dirs[d].stopIDs];

    let rep = null, repN = 0;
    for (const t of (tripsByDir[d] || [])) {
      if (t.shape_id !== best) continue;
      if (t.stop_times.length > repN) { rep = t; repN = t.stop_times.length; }
    }
    dirs[d].representativeTrip = rep;
  }
  return dirs;
}

// sliceShape extracts the polyline coordinates between two cumulative
// distances along a route shape. shape entries are [lat, lon, dist_m].
// Returns [[lon, lat], ...] (GeoJSON order) including interpolated
// endpoints. Returns null if the slice would have fewer than 2 points
// (zero-length or out-of-range request).
function sliceShape(shape, distStart, distEnd) {
  if (!Array.isArray(shape) || shape.length < 2) return null;
  if (distEnd <= distStart) return null;
  const out = [];
  for (let i = 0; i < shape.length - 1; i++) {
    const [lat1, lon1, d1] = shape[i];
    const [lat2, lon2, d2] = shape[i + 1];
    if (d2 < distStart) continue;
    if (d1 > distEnd) break;
    if (out.length === 0) {
      if (d1 >= distStart) {
        out.push([lon1, lat1]);
      } else {
        const t = (distStart - d1) / (d2 - d1);
        out.push([lon1 + (lon2 - lon1) * t, lat1 + (lat2 - lat1) * t]);
      }
    }
    if (d2 <= distEnd) {
      out.push([lon2, lat2]);
    } else {
      const t = (distEnd - d1) / (d2 - d1);
      out.push([lon1 + (lon2 - lon1) * t, lat1 + (lat2 - lat1) * t]);
      break;
    }
  }
  return out.length >= 2 ? out : null;
}

// GeoJSON LineString from shape array [[lat, lon, dist_m], …]
function shapeToGeoJSON(shapeArr) {
  return {
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: shapeArr.map(([lat, lon]) => [lon, lat]),
    },
    properties: {},
  };
}

// ── map rendering ──────────────────────────────────────────────────────────

let map = null;
let popup = null;
let gtfsData = null;
let stopStatsData = null;
let speedData = null;
let directions = null;

const ROUTE_SOURCE     = "route-shape";
const ROUTE_LAYER      = "route-line";
const STOPS_SOURCE     = "stop-circles";
const STOPS_OUTER      = "stops-outer";
const STOPS_INNER      = "stops-inner";
const ENDPOINTS_SOURCE = "route-endpoints";
const ENDPOINTS_DOT    = "endpoints-dot";
const ENDPOINTS_LABEL  = "endpoints-label";
const SPEED_SEG_SOURCE = "speed-segments";
const SPEED_SEG_LAYER  = "speed-segments-line";

// segmentLookupFor returns a map keyed on "from_stop_id__to_stop_id"
// → segment summary for the given direction. Returns {} when speed
// data hasn't loaded or the direction is missing.
function segmentLookupFor(dirID) {
  const dirs = speedData && speedData.directions;
  if (!dirs) return {};
  const block = dirs[String(dirID)] || dirs[dirID];
  if (!block || !Array.isArray(block.segments)) return {};
  const out = {};
  for (const s of block.segments) {
    out[`${s.from_stop_id}__${s.to_stop_id}`] = s;
  }
  return out;
}

function segmentMetricValue(seg) {
  if (!seg) return null;
  if (activeSpeedMetric === "p50")    return seg.p50_mph ?? null;
  if (activeSpeedMetric === "stddev") return seg.stddev_mph ?? null;
  return seg.mean_mph ?? null; // default: avg/mean
}

function segmentColor(seg) {
  const v = segmentMetricValue(seg);
  return activeSpeedMetric === "stddev" ? speedStddevColor(v) : speedColor(v);
}

function buildSpeedSegmentFeatures() {
  if (!gtfsData || !directions) return { type: "FeatureCollection", features: [] };
  const dirData = directions[activeDir] || directions[Object.keys(directions)[0]];
  if (!dirData || !dirData.representativeTrip) return { type: "FeatureCollection", features: [] };

  const shape = gtfsData.shapes[dirData.shapeID];
  const stops = gtfsData.stops;
  const trip = dirData.representativeTrip;
  const lookup = segmentLookupFor(activeDir);

  const features = [];
  const st = trip.stop_times;
  for (let i = 1; i < st.length; i++) {
    const prev = st[i - 1], curr = st[i];
    const coords = sliceShape(shape, prev.dist_along_route_m, curr.dist_along_route_m);
    if (!coords) continue;
    const seg = lookup[`${prev.stop_id}__${curr.stop_id}`] || null;
    const rgb = segmentColor(seg);
    features.push({
      type: "Feature",
      geometry: { type: "LineString", coordinates: coords },
      properties: {
        from_stop_id:   prev.stop_id,
        to_stop_id:     curr.stop_id,
        from_stop_name: (stops[prev.stop_id] || {}).stop_name || prev.stop_id,
        to_stop_name:   (stops[curr.stop_id] || {}).stop_name || curr.stop_id,
        mean_mph:       seg ? (seg.mean_mph ?? null)   : null,
        p50_mph:        seg ? (seg.p50_mph ?? null)    : null,
        stddev_mph:     seg ? (seg.stddev_mph ?? null) : null,
        n:              seg ? (seg.n ?? null)          : null,
        has_data:       !!seg,
        color:          toHex(rgb),
      },
    });
  }
  return { type: "FeatureCollection", features };
}

function initMap() {
  mapboxgl.accessToken = MAPBOX_TOKEN;
  map = new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/mapbox/light-v11",
    center: [-122.2, 37.78],
    zoom: 11,
    attributionControl: true,
  });
  map.addControl(new mapboxgl.NavigationControl(), "bottom-right");

  popup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false, maxWidth: "240px" });

  map.on("load", () => renderDirection());

  // Mapbox GL serializes null JSON properties as the string "null".
  const numProp = (v) => (v === null || v === "null" || v === undefined) ? null : Number(v);
  const delayMin = (v) => { const n = numProp(v); return n !== null && !isNaN(n) ? (n / 60).toFixed(1) + " min" : "—"; };
  const mphFmt   = (v) => { const n = numProp(v); return n !== null && !isNaN(n) ? n.toFixed(1) + " mph" : "—"; };

  map.on("mouseenter", STOPS_OUTER, (e) => {
    map.getCanvas().style.cursor = "pointer";
    const props = e.features[0].properties;
    popup.setLngLat(e.lngLat)
      .setHTML(`
        <strong>${props.stop_name}</strong><br>
        <span style="color:#888;font-size:11px">${props.stop_id}</span><br>
        <table style="margin-top:4px;font-size:12px;border-collapse:collapse">
          <tr><td style="padding:1px 6px 1px 0;color:#666">p50 delay</td><td>${delayMin(props.p50_delay_s)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">p95 delay</td><td>${delayMin(props.p95_delay_s)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">std dev</td><td>${delayMin(props.stddev_delay_s)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">observations</td><td>${numProp(props.n) !== null ? Number(props.n).toLocaleString() : "—"}</td></tr>
        </table>
      `)
      .addTo(map);
  });

  map.on("mouseleave", STOPS_OUTER, () => {
    map.getCanvas().style.cursor = "";
    popup.remove();
  });

  map.on("mouseenter", SPEED_SEG_LAYER, (e) => {
    map.getCanvas().style.cursor = "pointer";
    const props = e.features[0].properties;
    popup.setLngLat(e.lngLat)
      .setHTML(`
        <strong>${props.from_stop_name} → ${props.to_stop_name}</strong><br>
        <span style="color:#888;font-size:11px">${props.from_stop_id} → ${props.to_stop_id}</span><br>
        <table style="margin-top:4px;font-size:12px;border-collapse:collapse">
          <tr><td style="padding:1px 6px 1px 0;color:#666">mean</td><td>${mphFmt(props.mean_mph)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">p50</td><td>${mphFmt(props.p50_mph)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">std dev</td><td>${mphFmt(props.stddev_mph)}</td></tr>
          <tr><td style="padding:1px 6px 1px 0;color:#666">legs observed</td><td>${numProp(props.n) !== null ? Number(props.n).toLocaleString() : "—"}</td></tr>
        </table>
      `)
      .addTo(map);
  });

  map.on("mouseleave", SPEED_SEG_LAYER, () => {
    map.getCanvas().style.cursor = "";
    popup.remove();
  });
}

// applyCategoryVisibility flips layer visibility so the right
// rendering is shown for the active category. Speed mode hides the
// solid black route line and the colored adherence circles (the
// inner subway dot stays for landmarks) and shows colored segments.
// Adherence mode does the inverse.
function applyCategoryVisibility() {
  if (!map) return;
  const isSpeed = activeCategory === "speed";
  const set = (id, vis) => {
    if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", vis ? "visible" : "none");
  };
  set(ROUTE_LAYER,     !isSpeed);
  set(STOPS_OUTER,     !isSpeed);
  set(SPEED_SEG_LAYER, isSpeed);
}

function renderDirection() {
  if (!map || !gtfsData || !directions) return;

  const dirData = directions[activeDir] || directions[Object.keys(directions)[0]];
  if (!dirData) return;

  // ── shape layer ──────────────────────────────────────────────────────────
  const shapeArr = gtfsData.shapes[dirData.shapeID];
  const shapeGeoJSON = shapeArr
    ? shapeToGeoJSON(shapeArr)
    : { type: "Feature", geometry: { type: "LineString", coordinates: [] }, properties: {} };

  if (map.getSource(ROUTE_SOURCE)) {
    map.getSource(ROUTE_SOURCE).setData(shapeGeoJSON);
  } else {
    map.addSource(ROUTE_SOURCE, { type: "geojson", data: shapeGeoJSON });
    map.addLayer({
      id: ROUTE_LAYER,
      type: "line",
      source: ROUTE_SOURCE,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: { "line-color": "#111", "line-width": 3.5 },
    });
  }

  // ── endpoint markers (start / end of shape) ──────────────────────────────
  const coords = shapeGeoJSON.geometry.coordinates;
  const endpointFeatures = coords.length >= 2 ? [
    {
      type: "Feature",
      geometry: { type: "Point", coordinates: coords[0] },
      properties: { label: "Start", kind: "start" },
    },
    {
      type: "Feature",
      geometry: { type: "Point", coordinates: coords[coords.length - 1] },
      properties: { label: "End", kind: "end" },
    },
  ] : [];
  const endpointGeoJSON = { type: "FeatureCollection", features: endpointFeatures };

  if (map.getSource(ENDPOINTS_SOURCE)) {
    map.getSource(ENDPOINTS_SOURCE).setData(endpointGeoJSON);
  } else {
    map.addSource(ENDPOINTS_SOURCE, { type: "geojson", data: endpointGeoJSON });

    // Filled diamond shape via a rotated square: use a circle + distinct border
    map.addLayer({
      id: ENDPOINTS_DOT,
      type: "circle",
      source: ENDPOINTS_SOURCE,
      paint: {
        "circle-radius": 7,
        "circle-color": ["match", ["get", "kind"], "start", "#1971c2", "#111111"],
        "circle-stroke-color": "#fff",
        "circle-stroke-width": 2,
      },
    });

    // Text label above each endpoint marker
    map.addLayer({
      id: ENDPOINTS_LABEL,
      type: "symbol",
      source: ENDPOINTS_SOURCE,
      layout: {
        "text-field": ["get", "label"],
        "text-font": ["DIN Pro Medium", "Arial Unicode MS Regular"],
        "text-size": 11,
        "text-offset": [0, -1.4],
        "text-anchor": "bottom",
        "text-allow-overlap": true,
        "text-ignore-placement": true,
      },
      paint: {
        "text-color": ["match", ["get", "kind"], "start", "#1971c2", "#111111"],
        "text-halo-color": "#fff",
        "text-halo-width": 1.5,
      },
    });
  }

  // ── stop features ────────────────────────────────────────────────────────
  const stops = gtfsData.stops;
  const stats = stopStatsData ? stopStatsData.stops : {};

  // Compute max value for radius scaling
  let maxVal = 0;
  for (const sid of dirData.stopIDs) {
    const stat = stats[sid];
    const val = getStopValue(stat);
    if (val !== null && Math.abs(val) > maxVal) maxVal = Math.abs(val);
  }
  if (maxVal === 0) maxVal = 420; // fallback scale

  const features = dirData.stopIDs
    .filter(sid => stops[sid])
    .map(sid => {
      const stop = stops[sid];
      const stat = stats[sid] || null;
      const val = getStopValue(stat);
      const color = toHex(getStopColor(val));
      const radius = stopRadius(val, maxVal);
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [stop.lon, stop.lat] },
        properties: {
          stop_id:        sid,
          stop_name:      stop.stop_name,
          color,
          radius,
          p50_delay_s:    stat ? (stat.p50_delay_s ?? null) : null,
          p95_delay_s:    stat ? (stat.p95_delay_s ?? null) : null,
          stddev_delay_s: stat ? (stat.stddev_delay_s ?? null) : null,
          n:              stat ? stat.n : null,
        },
      };
    });

  const stopGeoJSON = { type: "FeatureCollection", features };

  if (map.getSource(STOPS_SOURCE)) {
    map.getSource(STOPS_SOURCE).setData(stopGeoJSON);
  } else {
    map.addSource(STOPS_SOURCE, { type: "geojson", data: stopGeoJSON });

    // Outer semi-transparent adherence circle
    map.addLayer({
      id: STOPS_OUTER,
      type: "circle",
      source: STOPS_SOURCE,
      paint: {
        "circle-radius": ["get", "radius"],
        "circle-color": ["get", "color"],
        "circle-opacity": 0.55,
        "circle-stroke-width": 0,
      },
    });

    // Inner subway-dot: white fill, black stroke
    map.addLayer({
      id: STOPS_INNER,
      type: "circle",
      source: STOPS_SOURCE,
      paint: {
        "circle-radius": 4,
        "circle-color": "#ffffff",
        "circle-stroke-color": "#111",
        "circle-stroke-width": 1.5,
      },
    });
  }

  // ── speed-segment layer (built between consecutive stops on the rep trip) ─
  const segGeoJSON = buildSpeedSegmentFeatures();
  if (map.getSource(SPEED_SEG_SOURCE)) {
    map.getSource(SPEED_SEG_SOURCE).setData(segGeoJSON);
  } else {
    map.addSource(SPEED_SEG_SOURCE, { type: "geojson", data: segGeoJSON });
    // Insert below endpoint dots so the start/end markers stay visible
    // on top of the colored segment line, matching how the solid black
    // route line stacks in adherence mode.
    const beforeId = map.getLayer(ENDPOINTS_DOT) ? ENDPOINTS_DOT : undefined;
    map.addLayer({
      id: SPEED_SEG_LAYER,
      type: "line",
      source: SPEED_SEG_SOURCE,
      layout: { "line-join": "round", "line-cap": "round" },
      paint: {
        "line-color": ["get", "color"],
        "line-width": 5.5,
        "line-opacity": 0.95,
      },
    }, beforeId);
  }
  applyCategoryVisibility();

  // ── fit bounds to shape ──────────────────────────────────────────────────
  if (shapeArr && shapeArr.length > 0) {
    const lngs = shapeArr.map(([, lon]) => lon);
    const lats = shapeArr.map(([lat]) => lat);
    map.fitBounds(
      [[Math.min(...lngs), Math.min(...lats)], [Math.max(...lngs), Math.max(...lats)]],
      { padding: { top: 80, bottom: 60, left: 220, right: 340 }, duration: 600 },
    );
  }
}

// Recolor stops and refresh speed segments without rebuilding sources
// (direction hasn't changed). Called when the user switches view mode
// within the current direction.
function recolorStops() {
  if (!map || !map.getSource(STOPS_SOURCE)) {
    renderDirection();
    return;
  }
  const stops = gtfsData.stops;
  const stats = stopStatsData ? stopStatsData.stops : {};
  const dirData = directions[activeDir] || directions[Object.keys(directions)[0]];

  let maxVal = 0;
  for (const sid of dirData.stopIDs) {
    const val = getStopValue(stats[sid]);
    if (val !== null && Math.abs(val) > maxVal) maxVal = Math.abs(val);
  }
  if (maxVal === 0) maxVal = 420;

  const features = dirData.stopIDs
    .filter(sid => stops[sid])
    .map(sid => {
      const stop = stops[sid];
      const stat = stats[sid] || null;
      const val = getStopValue(stat);
      const color = toHex(getStopColor(val));
      const radius = stopRadius(val, maxVal);
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [stop.lon, stop.lat] },
        properties: {
          stop_id:        sid,
          stop_name:      stop.stop_name,
          color,
          radius,
          p50_delay_s:    stat ? (stat.p50_delay_s ?? null) : null,
          p95_delay_s:    stat ? (stat.p95_delay_s ?? null) : null,
          stddev_delay_s: stat ? (stat.stddev_delay_s ?? null) : null,
          n:              stat ? stat.n : null,
        },
      };
    });

  map.getSource(STOPS_SOURCE).setData({ type: "FeatureCollection", features });

  if (map.getSource(SPEED_SEG_SOURCE)) {
    map.getSource(SPEED_SEG_SOURCE).setData(buildSpeedSegmentFeatures());
  }
  applyCategoryVisibility();
}

// ── direction button state ─────────────────────────────────────────────────

function updateDirButtons() {
  const hasBoth = directions && directions[0] && directions[1];
  ["btn-dir0", "btn-dir1"].forEach(id => {
    const btn = document.getElementById(id);
    const d = parseInt(btn.dataset.dir, 10);
    btn.classList.toggle("active", d === activeDir);
    btn.disabled = !hasBoth && d !== activeDir;
  });
}

function updateModeButtons() {
  const adh = activeCategory === "adherence";
  const speed = activeCategory === "speed";
  document.getElementById("btn-p95").classList.toggle("active", adh && activeMode === "adherence" && activePct === "p95");
  document.getElementById("btn-p50").classList.toggle("active", adh && activeMode === "adherence" && activePct === "p50");
  document.getElementById("btn-vol").classList.toggle("active", adh && activeMode === "volatility");
  document.getElementById("btn-sp-mean").classList.toggle("active", speed && activeSpeedMetric === "mean");
  document.getElementById("btn-sp-p50").classList.toggle("active",  speed && activeSpeedMetric === "p50");
  document.getElementById("btn-sp-std").classList.toggle("active",  speed && activeSpeedMetric === "stddev");
}

// ── control wiring ─────────────────────────────────────────────────────────

document.getElementById("btn-dir0").addEventListener("click", () => {
  if (activeDir === 0) return;
  activeDir = 0; syncURL(); updateDirButtons(); renderDirection();
});
document.getElementById("btn-dir1").addEventListener("click", () => {
  if (activeDir === 1) return;
  activeDir = 1; syncURL(); updateDirButtons(); renderDirection();
});

document.getElementById("btn-p95").addEventListener("click", () => {
  activeCategory = "adherence"; activePct = "p95"; activeMode = "adherence";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});
document.getElementById("btn-p50").addEventListener("click", () => {
  activeCategory = "adherence"; activePct = "p50"; activeMode = "adherence";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});
document.getElementById("btn-vol").addEventListener("click", () => {
  activeCategory = "adherence"; activeMode = "volatility";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});

document.getElementById("btn-sp-mean").addEventListener("click", () => {
  activeCategory = "speed"; activeSpeedMetric = "mean";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});
document.getElementById("btn-sp-p50").addEventListener("click", () => {
  activeCategory = "speed"; activeSpeedMetric = "p50";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});
document.getElementById("btn-sp-std").addEventListener("click", () => {
  activeCategory = "speed"; activeSpeedMetric = "stddev";
  syncURL(); updateModeButtons(); updateInfoPanel(); recolorStops();
});

// Info panel collapse
document.getElementById("info-toggle").addEventListener("click", () => {
  const panel = document.getElementById("info-panel");
  const collapsed = panel.classList.toggle("collapsed");
  document.getElementById("info-toggle").textContent = collapsed ? "▼" : "▲";
});

// ── bootstrap ─────────────────────────────────────────────────────────────

async function boot() {
  const data = await loadData();
  if (!data) return;

  gtfsData = data.gtfs;
  stopStatsData = data.stopStats;
  speedData = data.speedStats;
  directions = buildDirections(gtfsData);

  // Validate activeDir
  if (!directions[activeDir]) {
    activeDir = parseInt(Object.keys(directions)[0], 10);
  }

  // Update page title
  const rLabel = routeID || (gtfsData.route_id || "route");
  document.getElementById("page-title").textContent = `Route ${rLabel} — stop adherence map`;
  document.title = `Route ${rLabel} — AC Transit`;
  if (weekEnd) {
    document.getElementById("meta").textContent = `Week ending ${weekEnd}`;
  }

  // Reflect statsOpen into the <details> element + listen for changes.
  const details = document.getElementById("wait-details");
  if (details) {
    details.open = statsOpen;
    details.addEventListener("toggle", () => {
      statsOpen = details.open;
      syncURL();
    });
  }

  const speedDetails = document.getElementById("speed-details");
  if (speedDetails) {
    speedDetails.open = speedOpen;
    speedDetails.addEventListener("toggle", () => {
      speedOpen = speedDetails.open;
      syncURL();
    });
  }

  renderWaitTime(data.waitStats);
  renderSpeed(data.speedStats);

  updateDirButtons();
  updateModeButtons();
  updateInfoPanel();

  document.getElementById("loading").classList.add("hidden");

  if (!data.stopStats) {
    const banner = document.createElement("div");
    banner.id = "no-stats-banner";
    banner.innerHTML = `
      <strong>Stop adherence data not available yet for week ${weekEnd || "selected"}.</strong>
      Route shape and stops are shown, but circle colors and sizes require weekly stats to be generated.
      <button onclick="this.parentElement.remove()" style="margin-left:10px;cursor:pointer;background:none;border:1px solid #856404;border-radius:3px;padding:1px 6px;color:#856404;font-size:11px">Dismiss</button>
    `;
    document.getElementById("map-wrap").appendChild(banner);
  }

  initMap();
}

// ── wait-time section rendering ────────────────────────────────────────────

const WAIT_HIST_VISIBLE_BINS = 60; // show only [0, 60) min in the histogram
const WAIT_DAY_TYPE_COLORS = {
  weekday: "#1971c2",
  weekend: "#d6336c",
};

function fmtMaybeMin(v) {
  return v === null || v === undefined ? "—" : `${Number(v).toFixed(1)} min`;
}

function renderWaitTime(wait) {
  const empty = document.getElementById("wait-empty");

  if (!wait || !wait.days || Object.keys(wait.days).length === 0) {
    document.getElementById("wait-cards").innerHTML = "";
    empty.textContent = weekEnd
      ? "Wait-time stats not yet computed for this week — check back after the next Sunday roll-up."
      : "Open this page via a route's map link on the weekly dashboard to load wait-time stats.";
    empty.hidden = false;
    return;
  }
  empty.hidden = true;

  const wd = wait.days.weekday || {};
  const we = wait.days.weekend || {};
  const wdS = wd.summary || {};
  const weS = we.summary || {};

  renderCards("#wait-cards", [
    { label: "Median wait — weekday", val: fmtMaybeMin(wdS.median_wait_min) },
    { label: "Mean wait — weekday",   val: fmtMaybeMin(wdS.mean_wait_min) },
    { label: "p95 wait — weekday",    val: fmtMaybeMin(wdS.p95_wait_min) },
    { label: "p99 wait — weekday",    val: fmtMaybeMin(wdS.p99_wait_min) },
    { label: "Median wait — weekend", val: fmtMaybeMin(weS.median_wait_min) },
    { label: "Mean wait — weekend",   val: fmtMaybeMin(weS.mean_wait_min) },
    { label: "p95 wait — weekend",    val: fmtMaybeMin(weS.p95_wait_min) },
    { label: "p99 wait — weekend",    val: fmtMaybeMin(weS.p99_wait_min) },
  ]);

  renderWaitHistogram(wd.histogram, we.histogram);
  renderWaitHourLine(wd.by_hour, we.by_hour);
  renderWaitHourTail(wd.by_hour, we.by_hour);
}

function renderWaitHistogram(weekdayHist, weekendHist) {
  const canvas = document.getElementById("wait-hist-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const labels = [];
  for (let i = 0; i < WAIT_HIST_VISIBLE_BINS; i++) labels.push(String(i));

  const slice = (h) => {
    if (!h || !Array.isArray(h.density)) return null;
    return h.density.slice(0, WAIT_HIST_VISIBLE_BINS);
  };
  const wdD = slice(weekdayHist);
  const weD = slice(weekendHist);

  const datasets = [];
  if (wdD) datasets.push({
    label: "weekday",
    data: wdD,
    backgroundColor: "rgba(25,113,194,0.40)",
    borderColor: WAIT_DAY_TYPE_COLORS.weekday,
    borderWidth: 1,
    barPercentage: 1.0,
    categoryPercentage: 1.0,
  });
  if (weD) datasets.push({
    label: "weekend",
    data: weD,
    backgroundColor: "rgba(214,51,108,0.40)",
    borderColor: WAIT_DAY_TYPE_COLORS.weekend,
    borderWidth: 1,
    barPercentage: 1.0,
    categoryPercentage: 1.0,
  });

  new Chart(ctx, {
    type: "bar",
    data: { labels, datasets },
    options: {
      indexAxis: "y",
      plugins: {
        legend: { display: true, position: "top", labels: { boxWidth: 14 } },
        tooltip: {
          callbacks: {
            title: (ctx) => `${ctx[0].label}–${parseInt(ctx[0].label, 10) + 1} min`,
            label: (ctx) => `${ctx.dataset.label}: ${(ctx.parsed.x * 100).toFixed(2)}% of riders`,
          },
        },
      },
      scales: {
        y: {
          title: { display: true, text: "wait (min)" },
          grid: { display: false },
          ticks: { autoSkip: true, maxTicksLimit: 13 },
          reverse: true,
          stacked: false,
        },
        x: {
          title: { display: true, text: "density (per min)" },
          beginAtZero: true,
          stacked: false,
          ticks: { callback: (v) => `${(v * 100).toFixed(0)}%` },
        },
      },
    },
  });
}

function renderWaitHourLine(weekdayHours, weekendHours) {
  const canvas = document.getElementById("wait-hour-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");

  const seriesFor = (hours) => {
    const out = new Array(24).fill(null);
    if (!Array.isArray(hours)) return out;
    for (const c of hours) {
      if (c.hour >= 0 && c.hour < 24) out[c.hour] = c.median_wait_min ?? null;
    }
    return out;
  };

  const labels = Array.from({ length: 24 }, (_, h) => h);
  const datasets = [
    {
      label: "weekday",
      data: seriesFor(weekdayHours),
      borderColor: WAIT_DAY_TYPE_COLORS.weekday,
      backgroundColor: WAIT_DAY_TYPE_COLORS.weekday,
      borderWidth: 2,
      pointRadius: 2,
      spanGaps: false,
      tension: 0.2,
    },
    {
      label: "weekend",
      data: seriesFor(weekendHours),
      borderColor: WAIT_DAY_TYPE_COLORS.weekend,
      backgroundColor: WAIT_DAY_TYPE_COLORS.weekend,
      borderWidth: 2,
      pointRadius: 2,
      spanGaps: false,
      tension: 0.2,
    },
  ];

  new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      plugins: {
        legend: { display: true, position: "top", labels: { boxWidth: 14 } },
        tooltip: {
          callbacks: {
            title: (ctx) => `${ctx[0].label}:00 PT`,
            label: (ctx) => ctx.parsed.y === null
              ? `${ctx.dataset.label}: no data`
              : `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)} min`,
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "hour of day (PT)" }, grid: { display: false } },
        y: { title: { display: true, text: "median wait (min)" }, beginAtZero: true },
      },
    },
  });
}

// renderWaitHourTail draws p95 + p99 wait by hour of day. Solid lines
// are p95, dashed are p99. Weekday and weekend share the colour palette
// with the median chart so the eye links related series.
function renderWaitHourTail(weekdayHours, weekendHours) {
  const canvas = document.getElementById("wait-hour-tail-chart");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");

  const seriesFor = (hours, key) => {
    const out = new Array(24).fill(null);
    if (!Array.isArray(hours)) return out;
    for (const c of hours) {
      if (c.hour >= 0 && c.hour < 24) out[c.hour] = c[key] ?? null;
    }
    return out;
  };

  const labels = Array.from({ length: 24 }, (_, h) => h);
  const blue = WAIT_DAY_TYPE_COLORS.weekday;
  const pink = WAIT_DAY_TYPE_COLORS.weekend;
  const datasets = [
    {
      label: "weekday p95",
      data: seriesFor(weekdayHours, "p95_wait_min"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekday p99",
      data: seriesFor(weekdayHours, "p99_wait_min"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend p95",
      data: seriesFor(weekendHours, "p95_wait_min"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend p99",
      data: seriesFor(weekendHours, "p99_wait_min"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
  ];

  new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      plugins: {
        legend: { display: true, position: "top", labels: { boxWidth: 14 } },
        tooltip: {
          callbacks: {
            title: (ctx) => `${ctx[0].label}:00 PT`,
            label: (ctx) => ctx.parsed.y === null
              ? `${ctx.dataset.label}: no data`
              : `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)} min`,
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "hour of day (PT)" }, grid: { display: false } },
        y: { title: { display: true, text: "wait (min)" }, beginAtZero: true },
      },
    },
  });
}

// ── speed-by-hour section rendering ────────────────────────────────────────

const SPEED_DAY_TYPE_COLORS = {
  weekday: "#1971c2",
  weekend: "#d6336c",
};

// Direction → canvas id pair (central, tail). Inbound = direction_id "1",
// Outbound = direction_id "0", matching the dir-toggle button labels.
const SPEED_DIRECTIONS = [
  { id: "1", label: "inbound",  centralCanvas: "speed-central-in-chart",  tailCanvas: "speed-tail-in-chart"  },
  { id: "0", label: "outbound", centralCanvas: "speed-central-out-chart", tailCanvas: "speed-tail-out-chart" },
];

function fmtMaybeMph(v) {
  return v === null || v === undefined ? "—" : `${Number(v).toFixed(1)} mph`;
}

function renderSpeed(speed) {
  const empty = document.getElementById("speed-empty");
  if (!speed || !speed.directions || Object.keys(speed.directions).length === 0) {
    document.getElementById("speed-cards").innerHTML = "";
    empty.textContent = weekEnd
      ? "Speed stats not yet computed for this week — check back after the next Sunday roll-up."
      : "Open this page via a route's map link on the weekly dashboard to load speed stats.";
    empty.hidden = false;
    return;
  }
  empty.hidden = true;

  renderSpeedCards(speed);
  for (const dir of SPEED_DIRECTIONS) {
    const block = speed.directions[dir.id] || { days: {} };
    const wd = block.days?.weekday;
    const we = block.days?.weekend;
    renderSpeedCentralChart(dir.centralCanvas, wd?.by_hour, we?.by_hour);
    renderSpeedTailChart(dir.tailCanvas, wd?.by_hour, we?.by_hour);
  }
}

function renderSpeedCards(speed) {
  const items = [];
  for (const dir of SPEED_DIRECTIONS) {
    const block = speed.directions[dir.id] || { days: {} };
    const wdS = block.days?.weekday?.summary || {};
    const weS = block.days?.weekend?.summary || {};
    items.push(
      { label: `Mean — ${dir.label} weekday`, val: fmtMaybeMph(wdS.mean_mph) },
      { label: `p95 — ${dir.label} weekday`,  val: fmtMaybeMph(wdS.p95_mph) },
      { label: `Stddev — ${dir.label} weekday`, val: fmtMaybeMph(wdS.stddev_mph) },
      { label: `Mean — ${dir.label} weekend`, val: fmtMaybeMph(weS.mean_mph) },
      { label: `p95 — ${dir.label} weekend`,  val: fmtMaybeMph(weS.p95_mph) },
      { label: `Stddev — ${dir.label} weekend`, val: fmtMaybeMph(weS.stddev_mph) },
    );
  }
  renderCards("#speed-cards", items);
}

function speedSeriesFor(hours, key) {
  const out = new Array(24).fill(null);
  if (!Array.isArray(hours)) return out;
  for (const c of hours) {
    if (c.hour >= 0 && c.hour < 24) out[c.hour] = c[key] ?? null;
  }
  return out;
}

function renderSpeedCentralChart(canvasId, weekdayHours, weekendHours) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const blue = SPEED_DAY_TYPE_COLORS.weekday;
  const pink = SPEED_DAY_TYPE_COLORS.weekend;
  const labels = Array.from({ length: 24 }, (_, h) => h);
  const datasets = [
    {
      label: "weekday mean",
      data: speedSeriesFor(weekdayHours, "mean_mph"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekday p50",
      data: speedSeriesFor(weekdayHours, "p50_mph"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend mean",
      data: speedSeriesFor(weekendHours, "mean_mph"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend p50",
      data: speedSeriesFor(weekendHours, "p50_mph"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
  ];
  new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: speedChartOptions("speed (mph)"),
  });
}

function renderSpeedTailChart(canvasId, weekdayHours, weekendHours) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const blue = SPEED_DAY_TYPE_COLORS.weekday;
  const pink = SPEED_DAY_TYPE_COLORS.weekend;
  const labels = Array.from({ length: 24 }, (_, h) => h);
  const datasets = [
    {
      label: "weekday p95",
      data: speedSeriesFor(weekdayHours, "p95_mph"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekday p99",
      data: speedSeriesFor(weekdayHours, "p99_mph"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekday stddev",
      data: speedSeriesFor(weekdayHours, "stddev_mph"),
      borderColor: blue, backgroundColor: blue,
      borderWidth: 2, borderDash: [2, 3], pointRadius: 1.5, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend p95",
      data: speedSeriesFor(weekendHours, "p95_mph"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend p99",
      data: speedSeriesFor(weekendHours, "p99_mph"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, borderDash: [4, 4], pointRadius: 2, spanGaps: false, tension: 0.2,
    },
    {
      label: "weekend stddev",
      data: speedSeriesFor(weekendHours, "stddev_mph"),
      borderColor: pink, backgroundColor: pink,
      borderWidth: 2, borderDash: [2, 3], pointRadius: 1.5, spanGaps: false, tension: 0.2,
    },
  ];
  new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: speedChartOptions("mph"),
  });
}

function speedChartOptions(yLabel) {
  return {
    plugins: {
      legend: { display: true, position: "top", labels: { boxWidth: 14, font: { size: 10 } } },
      tooltip: {
        callbacks: {
          title: (ctx) => `${ctx[0].label}:00 PT`,
          label: (ctx) => ctx.parsed.y === null
            ? `${ctx.dataset.label}: no data`
            : `${ctx.dataset.label}: ${ctx.parsed.y.toFixed(1)} mph`,
        },
      },
    },
    scales: {
      x: { title: { display: true, text: "hour of day (PT)" }, grid: { display: false } },
      y: { title: { display: true, text: yLabel }, beginAtZero: true },
    },
  };
}

boot();

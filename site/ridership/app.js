const RIDERSHIP_LATEST_URL = `${GCS_BASE}/ridership/latest.json`;
const RIDERSHIP_HISTORY_URL = `${GCS_BASE}/ridership/24h.json`;
const RIDERSHIP_SNAPSHOT_MAX_AGE_MS = 10 * 60_000;

const STATUS_ORDER = ["Not Crowded", "Some Crowding", "Crowded"];
const STATUS_COLORS = {
  "Not Crowded": "#45a17c",
  "Some Crowding": "#d8932f",
  "Crowded": "#bd4d49",
};

let ridershipMap;
let ridershipChart;
let lastRidershipSnapshot;
let currentVehicleGeoJSON = { type: "FeatureCollection", features: [] };

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function percent(numerator, denominator) {
  return denominator > 0 ? Math.round(100 * numerator / denominator) : 0;
}

function formatAge(timestamp) {
  const seconds = Math.max(0, Math.round((Date.now() - new Date(timestamp).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m ago`;
}

function freshTimestamp(timestamp, reference) {
  if (!timestamp) return false;
  const age = new Date(reference).getTime() - new Date(timestamp).getTime();
  return age >= -60_000 && age <= 5 * 60_000;
}

function currentSnapshot(timestamp) {
  const observedAt = new Date(timestamp).getTime();
  const age = Date.now() - observedAt;
  return Number.isFinite(observedAt) && age >= -60_000 && age <= RIDERSHIP_SNAPSHOT_MAX_AGE_MS;
}

function renderSummary(snapshot) {
  const summary = snapshot.summary;
  const estimate = summary.estimated_riders;
  const coverage = percent(summary.apc_reporting_vehicles, summary.active_vehicles);

  document.querySelector("#rider-count").textContent = estimate == null
    ? "—"
    : intFmt(estimate);
  document.querySelector("#rider-headline-copy").textContent = " people are riding AC Transit right now.";
  document.querySelector("#active-vehicles").textContent = intFmt(summary.active_vehicles);
  document.querySelector("#apc-coverage").textContent = `${coverage}%`;
  document.querySelector("#modeled-vehicles").textContent = `${intFmt(summary.estimated_vehicles)} / ${intFmt(summary.active_vehicles)}`;
  document.querySelector("#fleet-capacity").textContent = intFmt(summary.total_capacity);
  document.querySelector("#updated-at").textContent = `Updated ${formatAge(snapshot.observed_at)}`;

  const direct = summary.passenger_count_reporting_vehicles;
  const coverageCopy = direct > 0
    ? `${intFmt(direct)} vehicles are reporting direct passenger counts; the remaining estimate uses fresh APC crowding data on ${intFmt(summary.apc_reporting_vehicles)} of ${intFmt(summary.active_vehicles)} active vehicles.`
    : `Modeled from fresh APC crowding data on ${intFmt(summary.apc_reporting_vehicles)} of ${intFmt(summary.active_vehicles)} active vehicles (${coverage}% coverage). AC Transit currently publishes status bands rather than direct passenger counts.`;
  document.querySelector("#coverage-copy").textContent = coverageCopy;
}

function renderUnavailable(snapshot) {
  const observedAt = snapshot?.observed_at;
  const hasObservedAt = Number.isFinite(new Date(observedAt).getTime());
  document.querySelector("#rider-count").textContent = "Live estimate temporarily unavailable";
  document.querySelector("#rider-headline-copy").textContent = ".";
  document.querySelector("#coverage-copy").textContent = hasObservedAt
    ? `The most recent snapshot is ${formatAge(observedAt)}. Current ridership, coverage, routes, and vehicle locations are hidden until fresh data returns.`
    : "Current ridership data is unavailable. The page will retry automatically in one minute.";
  document.querySelector("#updated-at").textContent = hasObservedAt
    ? `Last successful update ${formatAge(observedAt)}`
    : "Live data is temporarily unavailable";
  for (const id of ["active-vehicles", "apc-coverage", "modeled-vehicles", "fleet-capacity"]) {
    document.querySelector(`#${id}`).textContent = "—";
  }
  document.querySelector("#status-bars").innerHTML = '<p class="empty-state">Current APC statuses are unavailable.</p>';
  document.querySelector("#route-rows").innerHTML = '<tr><td colspan="3" class="empty-state">Current route estimates are unavailable.</td></tr>';
  updateMap({ vehicles: [] });
}

function renderStatusBars(summary) {
  const total = summary.apc_reporting_vehicles || 0;
  const rows = STATUS_ORDER.map((status) => {
    const count = Number(summary.status_counts?.[status] || 0);
    const share = percent(count, total);
    return `
      <div class="status-row">
        <div class="status-label">
          <span>${escapeHTML(status)}</span>
          <strong>${intFmt(count)} · ${share}%</strong>
        </div>
        <div class="status-track" aria-hidden="true">
          <div class="status-fill" style="width:${share}%;background:${STATUS_COLORS[status]}"></div>
        </div>
      </div>`;
  }).join("");
  document.querySelector("#status-bars").innerHTML = rows || '<p class="empty-state">No current APC statuses.</p>';
}

function renderRoutes(vehicles) {
  const routes = new Map();
  for (const vehicle of vehicles) {
    if (!vehicle.route_id || vehicle.estimated_riders == null) continue;
    const current = routes.get(vehicle.route_id) || { route: vehicle.route_id, vehicles: 0, riders: 0 };
    current.vehicles++;
    current.riders += Number(vehicle.estimated_riders);
    routes.set(vehicle.route_id, current);
  }
  const ranked = [...routes.values()]
    .sort((a, b) => b.riders - a.riders || a.route.localeCompare(b.route, undefined, { numeric: true }))
    .slice(0, 20);
  document.querySelector("#route-rows").innerHTML = ranked.length
    ? ranked.map((route) => `
      <tr>
        <td><span class="route-pill">${escapeHTML(route.route)}</span></td>
        <td>${intFmt(route.vehicles)}</td>
        <td><strong>${intFmt(route.riders)}</strong></td>
      </tr>`).join("")
    : '<tr><td colspan="3" class="empty-state">No route estimates are available.</td></tr>';
}

function vehicleGeoJSON(snapshot) {
  return {
    type: "FeatureCollection",
    features: snapshot.vehicles
      .filter((vehicle) => Number.isFinite(vehicle.longitude) && Number.isFinite(vehicle.latitude))
      .filter((vehicle) => freshTimestamp(vehicle.position_reported_at, snapshot.observed_at))
      .map((vehicle) => ({
        type: "Feature",
        geometry: { type: "Point", coordinates: [vehicle.longitude, vehicle.latitude] },
        properties: {
          vehicle_id: vehicle.vehicle_id,
          route_id: vehicle.route_id || "—",
          trip_id: vehicle.trip_id || "",
          status: vehicle.occupancy_status || "No APC status",
          estimated_riders: vehicle.estimated_riders,
          capacity: vehicle.capacity,
        },
      })),
  };
}

function updateMap(snapshot) {
  currentVehicleGeoJSON = vehicleGeoJSON(snapshot);
  const source = ridershipMap?.getSource("vehicles");
  if (source) source.setData(currentVehicleGeoJSON);
}

function initializeMap() {
  const container = document.querySelector("#vehicle-map");
  if (typeof mapboxgl === "undefined") {
    container.innerHTML = '<p class="empty-state" style="padding:24px">The live map could not load.</p>';
    return;
  }
  mapboxgl.accessToken = MAPBOX_TOKEN;
  ridershipMap = new mapboxgl.Map({
    container: "vehicle-map",
    style: "mapbox://styles/mapbox/light-v11",
    center: [-122.15, 37.76],
    zoom: 9.25,
    attributionControl: true,
  });
  ridershipMap.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "top-right");
  ridershipMap.on("load", () => {
    ridershipMap.addSource("vehicles", { type: "geojson", data: currentVehicleGeoJSON });
    ridershipMap.addLayer({
      id: "vehicle-halo",
      type: "circle",
      source: "vehicles",
      paint: {
        "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 5, 12, 10],
        "circle-color": [
          "match", ["get", "status"],
          "Not Crowded", STATUS_COLORS["Not Crowded"],
          "Some Crowding", STATUS_COLORS["Some Crowding"],
          "Crowded", STATUS_COLORS.Crowded,
          "#7d8e87",
        ],
        "circle-opacity": 0.88,
        "circle-stroke-color": "#ffffff",
        "circle-stroke-width": 1.5,
      },
    });
    ridershipMap.on("mouseenter", "vehicle-halo", () => { ridershipMap.getCanvas().style.cursor = "pointer"; });
    ridershipMap.on("mouseleave", "vehicle-halo", () => { ridershipMap.getCanvas().style.cursor = ""; });
    ridershipMap.on("click", "vehicle-halo", (event) => {
      const feature = event.features?.[0];
      if (!feature) return;
      const properties = feature.properties;
      const estimate = properties.estimated_riders == null
        ? "Rider estimate unavailable"
        : `${intFmt(properties.estimated_riders)} modeled riders · capacity ${intFmt(properties.capacity)}`;
      new mapboxgl.Popup({ offset: 10 })
        .setLngLat(feature.geometry.coordinates)
        .setHTML(`<div class="vehicle-popup"><strong>Route ${escapeHTML(properties.route_id)} · Bus ${escapeHTML(properties.vehicle_id)}</strong><span>${escapeHTML(properties.status)}</span><span>${escapeHTML(estimate)}</span></div>`)
        .addTo(ridershipMap);
    });
  });
}

function renderTrend(history) {
  const points = Array.isArray(history.points)
    ? history.points.filter((point) => point.estimated_riders != null)
    : [];
  const labels = points.map((point) => new Date(point.observed_at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }));
  const values = points.map((point) => Number(point.estimated_riders));
  if (ridershipChart) {
    ridershipChart.data.labels = labels;
    ridershipChart.data.datasets[0].data = values;
    ridershipChart.update("none");
    return;
  }
  const context = document.querySelector("#ridership-trend").getContext("2d");
  const gradient = context.createLinearGradient(0, 0, 0, 210);
  gradient.addColorStop(0, "rgba(69, 161, 124, 0.42)");
  gradient.addColorStop(1, "rgba(69, 161, 124, 0.01)");
  ridershipChart = new Chart(context, {
    type: "line",
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor: "#1f6a50",
        backgroundColor: gradient,
        borderWidth: 2,
        pointRadius: 0,
        pointHitRadius: 10,
        tension: 0.28,
        fill: true,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { intersect: false, mode: "index" },
      plugins: {
        legend: { display: false },
        tooltip: {
          displayColors: false,
          callbacks: { label: (item) => `${intFmt(item.raw)} estimated riders` },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { color: "#75857f", maxTicksLimit: 8, maxRotation: 0 },
          border: { display: false },
        },
        y: {
          beginAtZero: true,
          grid: { color: "rgba(24, 49, 42, 0.08)" },
          ticks: { color: "#75857f", callback: (value) => Number(value).toLocaleString() },
          border: { display: false },
        },
      },
    },
  });
}

async function loadRidership() {
  try {
    const [snapshot, history] = await Promise.all([
      fetchJSON(RIDERSHIP_LATEST_URL),
      fetchJSON(RIDERSHIP_HISTORY_URL).catch(() => ({ points: [] })),
    ]);
    lastRidershipSnapshot = snapshot;
    renderTrend(history);
    if (!currentSnapshot(snapshot.observed_at)) {
      renderUnavailable(snapshot);
      return;
    }
    renderSummary(snapshot);
    renderStatusBars(snapshot.summary);
    renderRoutes(snapshot.vehicles || []);
    updateMap(snapshot);
  } catch (error) {
    if (lastRidershipSnapshot && currentSnapshot(lastRidershipSnapshot.observed_at)) {
      document.querySelector("#updated-at").textContent = `Updated ${formatAge(lastRidershipSnapshot.observed_at)}`;
      return;
    }
    renderUnavailable(lastRidershipSnapshot);
  }
}

initializeMap();
loadRidership();
setInterval(loadRidership, 60_000);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") loadRidership();
});

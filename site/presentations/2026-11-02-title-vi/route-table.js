const tableBody = document.querySelector("#route-table tbody");
const searchInput = document.querySelector("#route-search");
const classSelect = document.querySelector("#route-class");
const countLabel = document.querySelector("#route-count");
const headerButtons = document.querySelectorAll("#route-table th button");
const classificationBody = document.querySelector("#classification-table tbody");
const classificationSearch = document.querySelector("#classification-search");
const classificationCount = document.querySelector("#classification-count");
const riderWindowBars = document.querySelector("#rider-window-bars");
const riderWindowClass = document.querySelector("#rider-window-class");
const riderWindowLimit = document.querySelector("#rider-window-limit");
const riderWindowCount = document.querySelector("#rider-window-count");

let routes = [];
let sortKey = "route_id";
let sortDirection = 1;

function formatNumber(value, digits = 1) {
  return value == null || Number.isNaN(value) ? "—" : Number(value).toFixed(digits);
}

function routeSortValue(route, key) {
  const value = route[key];
  if (key === "route_id") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return value == null ? -Infinity : value;
}

function renderRoutes() {
  const query = searchInput.value.trim().toLowerCase();
  const classification = classSelect.value;
  const filtered = routes
    .filter((route) => String(route.route_id).toLowerCase().includes(query))
    .filter((route) => classification === "all"
      || (classification === "minority" && route.provisional_minority_route)
      || (classification === "other" && !route.provisional_minority_route))
    .sort((left, right) => {
      const a = routeSortValue(left, sortKey);
      const b = routeSortValue(right, sortKey);
      if (typeof a === "string") return sortDirection * a.localeCompare(b, undefined, { numeric: true });
      return sortDirection * (a - b);
    });

  countLabel.textContent = `${filtered.length} of ${routes.length} routes`;
  tableBody.innerHTML = filtered.map((route) => `
    <tr>
      <td><strong>${route.route_id}</strong></td>
      <td><span class="badge ${route.provisional_minority_route ? "minority" : "other"}">${route.provisional_minority_route ? "Minority" : "Other"}</span></td>
      <td>${formatNumber(route.minority_mileage_share * 100)}%</td>
      <td>${Number(route.scheduled_trips || 0).toLocaleString()}</td>
      <td>${formatNumber(route.service_operated_pct)}%</td>
      <td>${formatNumber(route.otp_operated_pct)}%</td>
      <td>${formatNumber(route.on_time_service_delivered_pct)}%</td>
      <td>${formatNumber(route.expected_wait_min)} min</td>
      <td>${formatNumber(route.long_gap_pct)}%</td>
    </tr>`).join("");
}

function renderClassificationReference() {
  if (!classificationBody) return;
  const query = classificationSearch.value.trim().toLowerCase();
  const filtered = routes
    .filter((route) => String(route.route_id).toLowerCase().includes(query))
    .sort((left, right) => String(left.route_id).localeCompare(String(right.route_id), undefined, { numeric: true }));

  classificationCount.textContent = `${filtered.length} of ${routes.length} routes`;
  classificationBody.innerHTML = filtered.map((route) => `
    <tr>
      <td><strong>${route.route_id}</strong></td>
      <td><span class="badge ${route.provisional_minority_route ? "minority" : "other"}">${route.provisional_minority_route ? "Minority route" : "Other route"}</span></td>
      <td>${formatNumber(route.minority_mileage_share * 100)}%</td>
    </tr>`).join("");
}

function renderRiderWindowChart() {
  if (!riderWindowBars) return;
  const classification = riderWindowClass.value;
  const eligible = routes
    .filter((route) => route.scheduled_trips >= 100)
    .filter((route) => route.on_time_service_delivered_pct != null)
    .filter((route) => route.on_time_service_delivered_pct >= 20)
    .filter((route) => classification === "all"
      || (classification === "minority" && route.provisional_minority_route)
      || (classification === "other" && !route.provisional_minority_route))
    .map((route) => ({
      ...route,
      outside_rider_window_pct: Math.max(0, Math.min(100, 100 - route.on_time_service_delivered_pct)),
    }))
    .sort((left, right) => right.outside_rider_window_pct - left.outside_rider_window_pct
      || String(left.route_id).localeCompare(String(right.route_id), undefined, { numeric: true }));
  const limit = riderWindowLimit.value === "all" ? eligible.length : Number(riderWindowLimit.value);
  const visible = eligible.slice(0, limit);

  riderWindowCount.textContent = `Showing ${visible.length} of ${eligible.length} eligible routes`;
  riderWindowBars.innerHTML = visible.map((route) => {
    const outside = route.outside_rider_window_pct;
    const routeClass = route.provisional_minority_route ? "minority" : "other";
    const classLabel = route.provisional_minority_route ? "Minority" : "Other";
    const routeID = String(route.route_id);
    return `<div class="rider-window-row" role="listitem">
      <div class="rider-window-route"><a href="../../route/?week_end=2026-10-31&amp;route_id=${encodeURIComponent(routeID)}">Line ${routeID}</a><span class="badge ${routeClass}">${classLabel}</span></div>
      <div class="rider-window-track" role="img" aria-label="Line ${routeID}: ${outside.toFixed(1)} percent outside rider window">
        <span class="rider-window-bar ${routeClass}" style="width:${outside.toFixed(2)}%"></span>
      </div>
      <strong class="rider-window-value">${outside.toFixed(1)}%</strong>
    </div>`;
  }).join("");
}

searchInput.addEventListener("input", renderRoutes);
classSelect.addEventListener("change", renderRoutes);
classificationSearch.addEventListener("input", renderClassificationReference);
riderWindowClass.addEventListener("change", renderRiderWindowChart);
riderWindowLimit.addEventListener("change", renderRiderWindowChart);
headerButtons.forEach((button) => button.addEventListener("click", () => {
  const nextKey = button.dataset.key;
  if (sortKey === nextKey) {
    sortDirection *= -1;
  } else {
    sortKey = nextKey;
    sortDirection = nextKey === "route_id" ? 1 : -1;
  }
  renderRoutes();
}));

fetch("data/route-metrics.json")
  .then((response) => {
    if (!response.ok) throw new Error(`route data request failed: ${response.status}`);
    return response.json();
  })
  .then((data) => {
    routes = data;
    renderRoutes();
    renderClassificationReference();
    renderRiderWindowChart();
  })
  .catch(() => {
    countLabel.textContent = "Route data could not be loaded.";
    classificationCount.textContent = "Route data could not be loaded.";
    riderWindowBars.textContent = "Route data could not be loaded.";
    riderWindowCount.textContent = "";
  });

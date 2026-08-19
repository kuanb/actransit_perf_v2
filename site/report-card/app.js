// Bus report card. Aggregates the last ~28 daily stats files (per-route)
// from GCS in the browser, computes a 0-100 composite score per route, maps
// it to a school letter grade, and renders a sortable best-to-worst table.
// GCS_BASE, fetchJSON, fmt, intFmt, gradeColor, routeBadge live in ../lib.js.

const WINDOW_DAYS = 28;
const IDEAL_SPEED_MPH = 13;
const HISTORY_MONTHS = 6;
const HISTORY_STEP_DAYS = 7;
const HISTORY_FETCH_BATCH = 24;

// Composite weights. Components with no data for a route are dropped and
// the remaining weights renormalized at score time.
const WEIGHTS = {
  stop_sd: 60,
  on_time: 15,
  speed: 15,
  headway: 10,
};

// Letter-grade bands keyed by inclusive lower bound. Standard US scale:
// A+ 97-100, A 93-96, A- 90-92, repeating through D, then F below 60.
const GRADE_BANDS = [
  { min: 97, label: "A+" },
  { min: 93, label: "A" },
  { min: 90, label: "A-" },
  { min: 87, label: "B+" },
  { min: 83, label: "B" },
  { min: 80, label: "B-" },
  { min: 77, label: "C+" },
  { min: 73, label: "C" },
  { min: 70, label: "C-" },
  { min: 67, label: "D+" },
  { min: 63, label: "D" },
  { min: 60, label: "D-" },
  { min: 0, label: "F" },
];

function letterGrade(score) {
  for (const b of GRADE_BANDS) {
    if (score >= b.min) return b.label;
  }
  return "F";
}

// Map a 0-100 score onto the shared red→orange→green gradient. F (<60)
// clamps to deep red; 100 is full green.
function scoreColor(score) {
  return gradeColor((score - 60) / 40);
}

async function loadDailyIndex() {
  try {
    const idx = await fetchJSON(`${GCS_BASE}/stats/_index.json`);
    return Array.isArray(idx.dates) ? idx.dates : [];
  } catch (e) {
    return [];
  }
}

// Most recent week_end (Saturday) that has a weekly stats file. Used to
// point per-route links at a week the route page can actually load.
async function latestWeekEnd() {
  try {
    const idx = await fetchJSON(`${GCS_BASE}/stats/weekly/_index.json`);
    return Array.isArray(idx.weeks) && idx.weeks.length ? idx.weeks[0] : "";
  } catch (e) {
    return "";
  }
}

// Aggregate every route across the fetched daily payloads. On Time Service Delivered
// uses exact qualifying/scheduled counts; observed-only metrics are weighted
// by that day's observation count.
function aggregateRoutes(dailies) {
  const acc = new Map();
  for (const d of dailies) {
    if (!d || !Array.isArray(d.routes)) continue;
    for (const r of d.routes) {
      const w = Number(r.observations) || 0;
      let a = acc.get(r.route_id);
      if (!a) {
        a = {
          route_id: r.route_id,
          color: r.color,
          text_color: r.text_color,
          obs: 0,
          trips: 0,
          days: 0,
          scheduled_runs: 0,
          scheduled_days: 0,
          sd_delivered: 0, sd_n: 0,
          // weighted sums + the weight actually present for each metric
          ot_sum: 0, ot_w: 0,
          sp_sum: 0, sp_w: 0,
          p95_sum: 0, p95_w: 0,
          dist_sum: 0, dist_w: 0,
        };
        acc.set(r.route_id, a);
      }
      const scheduled = Number(r.scheduled_trips) || 0;
      if (scheduled > 0) {
        a.scheduled_runs += scheduled;
        a.scheduled_days += 1;
      }
      const stopN = Number(r.stop_sd_n) || 0;
      if (stopN > 0) {
        a.sd_n += stopN;
        a.sd_delivered += Number(r.stop_sd_delivered_n) || 0;
      }
      a.color = r.color || a.color;
      a.text_color = r.text_color || a.text_color;
      if (w <= 0) continue;
      a.obs += w;
      a.trips += Number(r.trips_observed) || 0;
      a.days += 1;

      if (r.on_time_pct != null) { a.ot_sum += r.on_time_pct * w; a.ot_w += w; }
      if (r.avg_speed_mph != null) { a.sp_sum += r.avg_speed_mph * w; a.sp_w += w; }
      if (r.p95_delay_minutes != null) { a.p95_sum += r.p95_delay_minutes * w; a.p95_w += w; }
      if (r.p50_distortion_pct != null) { a.dist_sum += r.p50_distortion_pct * w; a.dist_w += w; }
    }
  }

  const out = [];
  for (const a of acc.values()) {
    const scheduledRunsPerDay = a.scheduled_days
      ? a.scheduled_runs / a.scheduled_days
      : 0;
    const metrics = {
      stop_sd_pct: a.sd_n ? 100 * a.sd_delivered / a.sd_n : null,
      on_time_pct: a.ot_w ? a.ot_sum / a.ot_w : null,
      avg_speed_mph: a.sp_w ? a.sp_sum / a.sp_w : null,
      p95_delay_minutes: a.p95_w ? a.p95_sum / a.p95_w : null,
      p50_distortion_pct: a.dist_w ? a.dist_sum / a.dist_w : null,
    };

    out.push({
      route_id: a.route_id,
      color: a.color,
      text_color: a.text_color,
      trips_observed: a.trips,
      observations: a.obs,
      scheduled_stops_scored: a.sd_n,
      days: a.days,
      scheduled_runs: a.scheduled_runs,
      scheduled_runs_per_day: scheduledRunsPerDay,
      limited: isLimitedRoute({
        route_id: a.route_id,
        scheduled_runs_per_day: scheduledRunsPerDay,
      }),
      ...metrics,
      score: compositeScore(metrics),
    });
  }
  return out;
}

function clamp(v) {
  return Math.max(0, Math.min(100, v));
}

// Composite of the four sub-scores from already-0-100 metric values.
function compositeScore({ stop_sd_pct, on_time_pct, avg_speed_mph, p50_distortion_pct }) {
  const parts = [];
  if (stop_sd_pct != null) parts.push([WEIGHTS.stop_sd, clamp(stop_sd_pct)]);
  if (on_time_pct != null) parts.push([WEIGHTS.on_time, clamp(on_time_pct)]);
  if (avg_speed_mph != null) parts.push([WEIGHTS.speed, clamp((avg_speed_mph / IDEAL_SPEED_MPH) * 100)]);
  if (p50_distortion_pct != null) parts.push([WEIGHTS.headway, clamp(100 - Math.min(100, Math.abs(p50_distortion_pct)))]);
  const wsum = parts.reduce((s, [w]) => s + w, 0);
  return wsum ? parts.reduce((s, [w, v]) => s + w * v, 0) / wsum : null;
}

// Agency-wide rollup: each per-route metric averaged across routes, weighted
// by that route's measured trip-stops (a route's share of all finalized
// (trip, stop) observations — bigger routes, with more trips and more stops,
// carry proportionally more weight). The agency score is the same composite
// formula applied to those agency-wide metric values.
function aggregateAgency(routes) {
  const acc = { obs: 0, trips: 0 };
  const m = { stop_sd_pct: [0, 0], on_time_pct: [0, 0], avg_speed_mph: [0, 0], p95_delay_minutes: [0, 0], p50_distortion_pct: [0, 0] };
  for (const r of routes) {
    const w = r.observations || 0;
    if (w <= 0) continue;
    acc.obs += w;
    acc.trips += r.trips_observed || 0;
    for (const k of Object.keys(m)) {
      if (r[k] != null) { m[k][0] += r[k] * w; m[k][1] += w; }
    }
  }
  const mean = (k) => (m[k][1] ? m[k][0] / m[k][1] : null);
  const agency = {
    stop_sd_pct: mean("stop_sd_pct"),
    on_time_pct: mean("on_time_pct"),
    avg_speed_mph: mean("avg_speed_mph"),
    p95_delay_minutes: mean("p95_delay_minutes"),
    p50_distortion_pct: mean("p50_distortion_pct"),
    trips: acc.trips,
    observations: acc.obs,
    routes: routes.length,
  };
  agency.score = compositeScore(agency);
  return agency;
}

function renderAgencyHero(a) {
  const el = document.getElementById("agency-hero");
  if (!el) return;
  if (a.score == null) { el.hidden = true; return; }
  const g = scoreColor(a.score);
  const grade = letterGrade(a.score);
  const speedPct = a.avg_speed_mph == null ? null : Math.min(100, (a.avg_speed_mph / IDEAL_SPEED_MPH) * 100);
  const stat = (label, val, sub = "") =>
    `<div class="agency-stat"><span class="as-val">${val}</span><span class="as-label">${label}</span>${sub ? `<span class="as-sub">${sub}</span>` : ""}</div>`;
  el.innerHTML = `
    <div class="agency-card">
      <div class="agency-grade" style="background:${g.bg};color:${g.fg}">
        <div class="agency-grade-letter">${grade}</div>
        <div class="agency-grade-score">${fmt(a.score)}<span> / 100</span></div>
      </div>
      <div class="agency-body">
        <div class="agency-title">Agency-wide grade</div>
        <div class="agency-sub">Non-limited routes over the last four weeks, weighted by each route's share of measured trip-stops. Lettered Transbay routes remain included.</div>
        <div class="agency-stats">
          ${stat("On Time Service Delivered", a.stop_sd_pct == null ? "—" : fmt(a.stop_sd_pct) + "%")}
          ${stat("On time (≤3 min)", a.on_time_pct == null ? "—" : fmt(a.on_time_pct) + "%")}
          ${stat("p95 delay", a.p95_delay_minutes == null ? "—" : fmt(a.p95_delay_minutes) + " min")}
          ${stat("Avg speed", fmt(a.avg_speed_mph) + " mph", speedPct == null ? "" : `${fmt(speedPct, 0)}% of ideal`)}
          ${stat("Headway p50 Δ", a.p50_distortion_pct == null ? "—" : fmt(a.p50_distortion_pct) + "%")}
          ${stat("Routes graded", intFmt(a.routes))}
          ${stat("Unique trips", intFmt(a.trips))}
          ${stat("Stops measured", intFmt(a.observations))}
        </div>
      </div>
    </div>`;
  el.hidden = false;
}

function renderGradeLegend() {
  const el = document.getElementById("grade-legend");
  if (!el) return;
  // One chip per band, colored at the band's midpoint score.
  const chips = GRADE_BANDS.map((b, i) => {
    const upper = i === 0 ? 100 : GRADE_BANDS[i - 1].min - 1;
    const mid = b.label === "F" ? 50 : (b.min + upper) / 2;
    const g = scoreColor(mid);
    const range = b.label === "F" ? "&lt; 60" : `${b.min}–${upper}`;
    return `<span class="grade-chip" style="background:${g.bg};color:${g.fg}">${b.label}<small>${range}</small></span>`;
  });
  el.innerHTML = chips.join("");
}

function historyEndpointIndexes(dates) {
  if (!dates.length) return [];
  const latest = new Date(`${dates[0]}T00:00:00Z`);
  const cutoff = new Date(latest);
  cutoff.setUTCMonth(cutoff.getUTCMonth() - HISTORY_MONTHS);

  const indexes = [];
  for (let i = 0; i + WINDOW_DAYS <= dates.length; i += HISTORY_STEP_DAYS) {
    const endpoint = new Date(`${dates[i]}T00:00:00Z`);
    if (endpoint < cutoff) break;
    indexes.push(i);
  }
  return indexes;
}

async function fetchDailyHistory(dates, dailyByDate) {
  const missing = dates.filter((date) => !dailyByDate.has(date));
  for (let i = 0; i < missing.length; i += HISTORY_FETCH_BATCH) {
    const batch = missing.slice(i, i + HISTORY_FETCH_BATCH);
    const payloads = await Promise.all(batch.map(async (date) => {
      const daily = await fetchJSON(`${GCS_BASE}/stats/${date}.json`).catch(() => null);
      return [date, daily];
    }));
    for (const [date, daily] of payloads) {
      if (daily) dailyByDate.set(date, daily);
    }
  }
}

function historyDateLabel(date) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(new Date(`${date}T00:00:00Z`));
}

function historyChartOptions(scores) {
  const valid = scores.filter((score) => score != null && Number.isFinite(score));
  let min = valid.length ? Math.max(0, Math.floor(Math.min(...valid) / 10) * 10) : 0;
  let max = valid.length ? Math.min(100, Math.ceil(Math.max(...valid) / 10) * 10) : 100;
  if (max - min < 20) {
    const missingRange = 20 - (max - min);
    const addAbove = Math.min(missingRange, 100 - max);
    max += addAbove;
    min = Math.max(0, min - (missingRange - addAbove));
  }
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
      x: {
        grid: { display: false },
        ticks: { maxRotation: 0, autoSkipPadding: 18 },
      },
      y: {
        min,
        max,
        ticks: { stepSize: 10 },
        title: { display: true, text: "Score / 100" },
      },
    },
    plugins: {
      legend: {
        position: "bottom",
        labels: { usePointStyle: true, boxWidth: 8, padding: 12 },
      },
      tooltip: {
        callbacks: {
          label: (ctx) => `${ctx.dataset.label}: ${fmt(ctx.parsed.y)} (${letterGrade(ctx.parsed.y)})`,
        },
      },
    },
  };
}

function renderGradeHistory(points, topRoutes) {
  const agencyMeta = document.getElementById("agency-history-meta");
  const routeMeta = document.getElementById("route-history-meta");
  if (!points.length) {
    agencyMeta.textContent = "Not enough daily data for a complete four-week historical point yet.";
    routeMeta.textContent = "Not enough daily data for a complete four-week historical point yet.";
    return;
  }

  const labels = points.map((point) => historyDateLabel(point.endDate));
  const first = historyDateLabel(points[0].endDate);
  const last = historyDateLabel(points[points.length - 1].endDate);
  agencyMeta.textContent = `${points.length} weekly points · ${first}–${last}`;
  routeMeta.textContent = `Top 10 by scheduled trips in the current four-week window · ${first}–${last}`;
  const agencyScores = points.map((point) => point.agencyScore);

  new Chart(document.getElementById("agency-grade-history"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Agency",
        data: agencyScores,
        borderColor: "#1971c2",
        backgroundColor: "rgba(25, 113, 194, 0.12)",
        fill: true,
        tension: 0.2,
        pointRadius: 3,
        pointHoverRadius: 5,
      }],
    },
    options: historyChartOptions(agencyScores),
  });

  const colors = [
    "#1971c2", "#e8590c", "#2b8a3e", "#9c36b5", "#d6336c",
    "#0b7285", "#f08c00", "#5f3dc4", "#495057", "#74b816",
  ];
  const routeDatasets = topRoutes.map((route, i) => ({
    label: `Route ${route.route_id}`,
    data: points.map((point) => point.routeScores.get(route.route_id) ?? null),
    borderColor: colors[i],
    backgroundColor: colors[i],
    borderWidth: 2,
    tension: 0.2,
    pointRadius: 2,
    pointHoverRadius: 5,
    spanGaps: false,
  }));
  new Chart(document.getElementById("route-grade-history"), {
    type: "line",
    data: {
      labels,
      datasets: routeDatasets,
    },
    options: historyChartOptions(routeDatasets.flatMap((dataset) => dataset.data)),
  });
}

async function loadGradeHistory(dates, currentDailies, currentRoutes) {
  const endpointIndexes = historyEndpointIndexes(dates);
  if (!endpointIndexes.length) {
    renderGradeHistory([], []);
    return;
  }

  const lastEndpoint = endpointIndexes[endpointIndexes.length - 1];
  const neededDates = dates.slice(0, lastEndpoint + WINDOW_DAYS);
  const dailyByDate = new Map(
    currentDailies.filter((daily) => daily && daily.service_date)
      .map((daily) => [daily.service_date, daily])
  );
  await fetchDailyHistory(neededDates, dailyByDate);

  const topRoutes = [...currentRoutes]
    .filter((route) => route.score != null)
    .sort((a, b) => b.scheduled_runs - a.scheduled_runs)
    .slice(0, 10);
  const points = endpointIndexes.map((endpointIndex) => {
    const windowDates = dates.slice(endpointIndex, endpointIndex + WINDOW_DAYS);
    const dailies = windowDates.map((date) => dailyByDate.get(date)).filter(Boolean);
    if (dailies.length !== WINDOW_DAYS) return null;
    const routes = aggregateRoutes(dailies).filter((route) => route.score != null);
    const agencyRoutes = routes.filter((route) => !isLimitedRoute(route));
    return {
      endDate: dates[endpointIndex],
      agencyScore: aggregateAgency(agencyRoutes).score,
      routeScores: new Map(routes.map((route) => [route.route_id, route.score])),
    };
  }).filter((point) => point && point.agencyScore != null).reverse();

  renderGradeHistory(points, topRoutes);
}

async function load() {
  const yearEl = document.getElementById("footer-year");
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  renderGradeLegend();

  const [dates, weekEnd] = await Promise.all([loadDailyIndex(), latestWeekEnd()]);
  if (!dates.length) {
    document.body.insertAdjacentHTML(
      "afterbegin",
      `<div style="padding:16px;background:#fee;color:#900;">Couldn't load the stats index.</div>`
    );
    return;
  }

  const recent = dates.slice(0, WINDOW_DAYS);
  const dailies = (await Promise.all(
    recent.map((d) =>
      fetchJSON(`${GCS_BASE}/stats/${d}.json`).catch(() => null)
    )
  )).filter(Boolean);

  const routes = aggregateRoutes(dailies).filter((r) => r.score != null);
  const includedRoutes = routes.filter((r) => !isLimitedRoute(r));

  renderAgencyHero(aggregateAgency(includedRoutes));

  const observed = dailies
    .map((d) => d.service_date)
    .filter(Boolean)
    .sort();
  const first = observed[0];
  const last = observed[observed.length - 1];
  document.getElementById("meta").textContent =
    `Last ${dailies.length} service days` +
    (first && last ? ` · ${first} → ${last}` : "") +
    ` · ${includedRoutes.length} routes included` +
    (routes.length > includedRoutes.length
      ? ` · ${routes.length - includedRoutes.length} limited routes hidden`
      : "");

  render(routes, weekEnd);
  await loadGradeHistory(dates, dailies, routes);
}

function render(routes, weekEnd) {
  const tbody = document.querySelector("#report-table tbody");
  let sortKey = "score";
  let sortDir = -1;
  let filterQ = "";
  let showLimitedRoutes = false;
  const limitedRoutes = routes.filter(isLimitedRoute);
  const limitedToggle = document.getElementById("routes-limited-toggle");
  const limitedStatus = document.getElementById("routes-limited-status");
  // route_ids whose detail row is expanded; persists across re-sort/filter.
  const expanded = new Set();

  const routeHref = (rid) =>
    weekEnd
      ? `../route/?week_end=${encodeURIComponent(weekEnd)}&route_id=${encodeURIComponent(rid)}`
      : `../route/?route_id=${encodeURIComponent(rid)}`;

  // Render a metric value as a rounded, padded colour pill (or plain text
  // / em-dash when there's no data or no colour scale).
  const pill = (pct, fn) => {
    if (pct == null) return "—";
    const text = fmt(pct) + "%";
    if (!fn) return text;
    const g = fn(pct);
    return `<span class="metric-pill" style="background:${g.bg};color:${g.fg}">${text}</span>`;
  };

  function renderRows() {
    const rows = routes
      .filter((r) =>
        (showLimitedRoutes || !isLimitedRoute(r)) &&
        (!filterQ || r.route_id.toLowerCase().includes(filterQ)))
      .sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        if (av === null || av === undefined) return 1;
        if (bv === null || bv === undefined) return -1;
        if (typeof av === "string") return sortDir * av.localeCompare(bv);
        return sortDir * (av - bv);
      });

    tbody.innerHTML = rows
      .map((r) => {
        const g = scoreColor(r.score);
        const grade = letterGrade(r.score);
        const isOpen = expanded.has(r.route_id);
        const detailHidden = isOpen ? "" : "hidden";
        return `
      <tr class="route-row ${isOpen ? "is-open" : ""}" data-rid="${r.route_id}">
        <td><span class="grade-badge" style="background:${g.bg};color:${g.fg}" title="composite score ${fmt(r.score)} / 100">${grade}</span></td>
        <td>${routeBadge(r)}${limitedRouteTag(r)}</td>
        <td title="composite score / 100">${fmt(r.score)}</td>
        <td>${intFmt(r.trips_observed)}</td>
        <td>${intFmt(r.observations)}</td>
        <td><a class="route-page-btn" href="${routeHref(r.route_id)}" title="Full performance analysis for route ${r.route_id}">Route page →</a></td>
        <td class="expand-cell" aria-hidden="true">${isOpen ? "▾" : "▸"}</td>
      </tr>
      <tr class="route-detail" data-rid="${r.route_id}" ${detailHidden}>
        <td colspan="7">
          <dl class="route-detail-list">
            <div><dt>Composite score</dt><dd>${fmt(r.score)} / 100</dd></div>
            <div><dt>On Time Service Delivered (stops, −1 to +7 min)</dt><dd>${pill(r.stop_sd_pct, gradeStopSD)}</dd></div>
            <div><dt>On time (≤3 min)</dt><dd>${pill(r.on_time_pct, gradeOnTime)}</dd></div>
            <div><dt>p95 delay</dt><dd>${r.p95_delay_minutes == null ? "—" : fmt(r.p95_delay_minutes) + " min"}</dd></div>
            <div><dt>Avg speed</dt><dd>${fmt(r.avg_speed_mph)} mph (speed sub-score ${fmt(Math.min(100, (r.avg_speed_mph || 0) / IDEAL_SPEED_MPH * 100), 0)} / 100, capped at the ${IDEAL_SPEED_MPH} mph ideal)</dd></div>
            <div><dt>p50 headway distortion</dt><dd>${r.p50_distortion_pct == null ? "—" : fmt(r.p50_distortion_pct) + "%"}</dd></div>
            <div><dt>Unique trips observed</dt><dd>${intFmt(r.trips_observed)}</dd></div>
            <div><dt>Stops measured</dt><dd>${intFmt(r.observations)}</dd></div>
            <div><dt>Scheduled stops scored</dt><dd>${intFmt(r.scheduled_stops_scored)}</dd></div>
            <div><dt>Service days in window</dt><dd>${intFmt(r.days)}</dd></div>
          </dl>
        </td>
      </tr>`;
      })
      .join("");

    document.querySelectorAll("#report-table th").forEach((th) => {
      th.classList.remove("sorted-asc", "sorted-desc");
      if (th.dataset.key === sortKey) {
        th.classList.add(sortDir > 0 ? "sorted-asc" : "sorted-desc");
      }
    });
    limitedToggle.textContent = showLimitedRoutes ? "Hide Limited Routes" : "Show Limited Routes";
    limitedToggle.setAttribute("aria-pressed", String(showLimitedRoutes));
    limitedStatus.textContent = limitedRoutes.length
      ? `${limitedRoutes.length} limited route${limitedRoutes.length === 1 ? "" : "s"} ${showLimitedRoutes ? "shown" : "hidden"}`
      : "No limited routes in this window";
  }

  document.querySelectorAll("#report-table th").forEach((th) => {
    if (!th.dataset.key) return;
    th.addEventListener("click", () => {
      const k = th.dataset.key;
      if (k === sortKey) sortDir = -sortDir;
      else { sortKey = k; sortDir = -1; }
      renderRows();
    });
  });

  document.getElementById("route-filter").addEventListener("input", (e) => {
    filterQ = e.target.value.toLowerCase().trim();
    renderRows();
  });

  limitedToggle.addEventListener("click", () => {
    showLimitedRoutes = !showLimitedRoutes;
    renderRows();
  });

  // Row click toggles the detail row; clicks on the route-page link pass through.
  tbody.addEventListener("click", (e) => {
    if (e.target.closest("a")) return;
    const tr = e.target.closest("tr.route-row");
    if (!tr) return;
    const rid = tr.dataset.rid;
    if (expanded.has(rid)) expanded.delete(rid);
    else expanded.add(rid);
    const detail = tbody.querySelector(`tr.route-detail[data-rid="${CSS.escape(rid)}"]`);
    if (detail) detail.toggleAttribute("hidden");
    tr.classList.toggle("is-open");
    const chev = tr.querySelector(".expand-cell");
    if (chev) chev.textContent = expanded.has(rid) ? "▾" : "▸";
  });

  renderRows();
}

load();

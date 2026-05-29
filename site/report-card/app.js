// Bus report card. Aggregates the last ~28 daily stats files (per-route)
// from GCS in the browser, computes a 0-100 composite score per route, maps
// it to a school letter grade, and renders a sortable best-to-worst table.
// GCS_BASE, fetchJSON, fmt, intFmt, gradeColor, routeBadge live in ../lib.js.

const WINDOW_DAYS = 28;
const IDEAL_SPEED_MPH = 13;

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

// Aggregate every route across the fetched daily payloads. Each metric is
// weighted by that day's stop-observation count so busy days count more.
function aggregateRoutes(dailies) {
  const acc = new Map();
  for (const d of dailies) {
    if (!d || !Array.isArray(d.routes)) continue;
    for (const r of d.routes) {
      const w = Number(r.observations) || 0;
      if (w <= 0) continue;
      let a = acc.get(r.route_id);
      if (!a) {
        a = {
          route_id: r.route_id,
          color: r.color,
          text_color: r.text_color,
          obs: 0,
          trips: 0,
          days: 0,
          // weighted sums + the weight actually present for each metric
          sd_sum: 0, sd_w: 0,
          ot_sum: 0, ot_w: 0,
          sp_sum: 0, sp_w: 0,
          p50_sum: 0, p50_w: 0,
          dist_sum: 0, dist_w: 0,
        };
        acc.set(r.route_id, a);
      }
      a.color = r.color || a.color;
      a.text_color = r.text_color || a.text_color;
      a.obs += w;
      a.trips += Number(r.trips_observed) || 0;
      a.days += 1;

      const sd = r.stop_sd_pct != null ? r.stop_sd_pct : r.service_delivered_pct;
      if (sd != null) { a.sd_sum += sd * w; a.sd_w += w; }
      if (r.on_time_pct != null) { a.ot_sum += r.on_time_pct * w; a.ot_w += w; }
      if (r.avg_speed_mph != null) { a.sp_sum += r.avg_speed_mph * w; a.sp_w += w; }
      if (r.p50_delay_minutes != null) { a.p50_sum += r.p50_delay_minutes * w; a.p50_w += w; }
      if (r.p50_distortion_pct != null) { a.dist_sum += r.p50_distortion_pct * w; a.dist_w += w; }
    }
  }

  const out = [];
  for (const a of acc.values()) {
    const stop_sd_pct = a.sd_w ? a.sd_sum / a.sd_w : null;
    const on_time_pct = a.ot_w ? a.ot_sum / a.ot_w : null;
    const avg_speed_mph = a.sp_w ? a.sp_sum / a.sp_w : null;
    const p50_delay_minutes = a.p50_w ? a.p50_sum / a.p50_w : null;
    const p50_distortion_pct = a.dist_w ? a.dist_sum / a.dist_w : null;

    // Per-component sub-scores on a 0-100 scale.
    const parts = [];
    if (stop_sd_pct != null) parts.push([WEIGHTS.stop_sd, clamp(stop_sd_pct)]);
    if (on_time_pct != null) parts.push([WEIGHTS.on_time, clamp(on_time_pct)]);
    if (avg_speed_mph != null) parts.push([WEIGHTS.speed, clamp((avg_speed_mph / IDEAL_SPEED_MPH) * 100)]);
    if (p50_distortion_pct != null) parts.push([WEIGHTS.headway, clamp(100 - Math.min(100, Math.abs(p50_distortion_pct)))]);

    const wsum = parts.reduce((s, [w]) => s + w, 0);
    const score = wsum ? parts.reduce((s, [w, v]) => s + w * v, 0) / wsum : null;

    out.push({
      route_id: a.route_id,
      color: a.color,
      text_color: a.text_color,
      trips_observed: a.trips,
      observations: a.obs,
      days: a.days,
      stop_sd_pct,
      on_time_pct,
      avg_speed_mph,
      p50_delay_minutes,
      p50_distortion_pct,
      score,
    });
  }
  return out;
}

function clamp(v) {
  return Math.max(0, Math.min(100, v));
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

  const observed = dailies
    .map((d) => d.service_date)
    .filter(Boolean)
    .sort();
  const first = observed[0];
  const last = observed[observed.length - 1];
  document.getElementById("meta").textContent =
    `Last ${dailies.length} service days` +
    (first && last ? ` · ${first} → ${last}` : "") +
    ` · ${routes.length} routes graded`;

  render(routes, weekEnd);
}

function render(routes, weekEnd) {
  const tbody = document.querySelector("#report-table tbody");
  let sortKey = "score";
  let sortDir = -1;
  let filterQ = "";
  // route_ids whose detail row is expanded; persists across re-sort/filter.
  const expanded = new Set();

  const routeHref = (rid) =>
    weekEnd
      ? `../route/?week_end=${encodeURIComponent(weekEnd)}&route_id=${encodeURIComponent(rid)}`
      : `../route/?route_id=${encodeURIComponent(rid)}`;

  const cellStyle = (pct, fn) => {
    if (pct === null || pct === undefined) return "";
    const g = fn(pct);
    return `style="background:${g.bg};color:${g.fg};"`;
  };

  function renderRows() {
    const rows = routes
      .filter((r) => !filterQ || r.route_id.toLowerCase().includes(filterQ))
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
        <td>${routeBadge(r)}</td>
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
            <div><dt>Service delivered (stops, −1 to +7 min)</dt><dd ${cellStyle(r.stop_sd_pct, gradeStopSD)}>${r.stop_sd_pct == null ? "—" : fmt(r.stop_sd_pct) + "%"}</dd></div>
            <div><dt>On time (≤3 min)</dt><dd ${cellStyle(r.on_time_pct, gradeOnTime)}>${r.on_time_pct == null ? "—" : fmt(r.on_time_pct) + "%"}</dd></div>
            <div><dt>p50 delay</dt><dd>${fmt(r.p50_delay_minutes)} min</dd></div>
            <div><dt>Avg speed</dt><dd>${fmt(r.avg_speed_mph)} mph (speed sub-score ${fmt(Math.min(100, (r.avg_speed_mph || 0) / IDEAL_SPEED_MPH * 100), 0)} / 100, capped at the ${IDEAL_SPEED_MPH} mph ideal)</dd></div>
            <div><dt>p50 headway distortion</dt><dd>${r.p50_distortion_pct == null ? "—" : fmt(r.p50_distortion_pct) + "%"}</dd></div>
            <div><dt>Unique trips observed</dt><dd>${intFmt(r.trips_observed)}</dd></div>
            <div><dt>Stops measured</dt><dd>${intFmt(r.observations)}</dd></div>
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

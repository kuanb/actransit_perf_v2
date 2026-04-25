const GCS_BASE = "https://storage.googleapis.com/transit-203605-actransit-cache";
const isLocal = ["localhost", "127.0.0.1"].includes(window.location.hostname);

const fmt = (v, d = 1) =>
  v === null || v === undefined ? "—" : Number(v).toFixed(d);
const intFmt = (v) =>
  v === null || v === undefined ? "—" : Number(v).toLocaleString();

function statsURL(date) {
  return date ? `${GCS_BASE}/stats/${date}.json` : `${GCS_BASE}/stats/latest.json`;
}

async function fetchJSON(url) {
  const res = await fetch(url, { cache: "no-cache" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function loadIndex() {
  try {
    const idx = await fetchJSON(`${GCS_BASE}/stats/_index.json`);
    return Array.isArray(idx.dates) ? idx.dates : [];
  } catch (e) {
    return [];
  }
}

function renderDateSelector(dates, current) {
  const el = document.getElementById("date-selector");
  if (!dates.length && !current) {
    el.innerHTML = "";
    return;
  }
  const last10 = dates.slice(0, 10);
  const customSelected = current && !last10.includes(current);
  el.innerHTML = `
    <label>View date:
      <select id="date-dropdown">
        <option value="" ${!current ? "selected" : ""}>Latest</option>
        ${last10.map((d) => `<option value="${d}" ${d === current ? "selected" : ""}>${d}</option>`).join("")}
        ${customSelected ? `<option value="${current}" selected>${current}</option>` : ""}
        ${dates.length > 10 ? `<option value="__more__">Older date…</option>` : ""}
      </select>
    </label>
  `;
  document.getElementById("date-dropdown").addEventListener("change", (e) => {
    const v = e.target.value;
    if (v === "__more__") {
      const oldest = dates[dates.length - 1];
      const newest = dates[0];
      const picked = prompt(`Enter a date (YYYY-MM-DD).\nAvailable range: ${oldest} – ${newest}`, "");
      if (picked && /^\d{4}-\d{2}-\d{2}$/.test(picked)) navigateTo(picked);
      else e.target.value = current || "";
      return;
    }
    navigateTo(v || null);
  });
}

function navigateTo(date) {
  const url = new URL(window.location.href);
  if (date) url.searchParams.set("date", date);
  else url.searchParams.delete("date");
  window.location.href = url.toString();
}

// gradeColor maps t (0=worst, 1=best) to a {bg, fg} CSS color via a 3-stop
// gradient: dark red → orange → green. Text flips to white when bg is darkest.
function gradeColor(t) {
  t = Math.max(0, Math.min(1, t));
  const stops = [
    { t: 0,   rgb: [173,  30,  35] },  // dark red
    { t: 0.5, rgb: [232, 140,  35] },  // orange
    { t: 1,   rgb: [160, 200, 130] },  // green
  ];
  let lo = stops[0], hi = stops[1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (t <= stops[i + 1].t) {
      lo = stops[i];
      hi = stops[i + 1];
      break;
    }
  }
  const f = (t - lo.t) / (hi.t - lo.t);
  const r = Math.round(lo.rgb[0] + (hi.rgb[0] - lo.rgb[0]) * f);
  const g = Math.round(lo.rgb[1] + (hi.rgb[1] - lo.rgb[1]) * f);
  const b = Math.round(lo.rgb[2] + (hi.rgb[2] - lo.rgb[2]) * f);
  return {
    bg: `rgb(${r},${g},${b})`,
    fg: t < 0.3 ? "#ffffff" : "#1a1a1a",
  };
}

// On-time: 98%+ = green, 80%- = dark red, linear in between.
function gradeOnTime(pct) {
  return gradeColor((pct - 80) / (98 - 80));
}

// Late: 2%- = green, 20%+ = dark red, linear in between (inverted).
function gradeLate(pct) {
  return gradeColor(1 - (pct - 2) / (20 - 2));
}

// Service delivered: 99%+ = green, 90%- = dark red, linear in between.
function gradeServiceDelivered(pct) {
  return gradeColor((pct - 90) / (99 - 90));
}

// Trips not completed (as % of running trips): 0% = green, 25%+ = dark red.
// Higher means more buses didn't finish their route.
function gradeNotCompleted(pct) {
  return gradeColor(1 - pct / 25);
}

// distortionColor: blue (very early, -100%) → light gray (around 0) → red (very
// late, +100%). Used to color the 42 bars in the distortion histogram.
function distortionColor(centerPct) {
  const t = Math.max(-1, Math.min(1, centerPct / 100));
  if (t < 0) {
    // -1 (deep blue) → 0 (near-neutral)
    const f = t + 1;
    const r = Math.round(50 + (220 - 50) * f);
    const g = Math.round(110 + (230 - 110) * f);
    const b = Math.round(180 + (235 - 180) * f);
    return `rgb(${r},${g},${b})`;
  }
  // 0 (near-neutral) → +1 (deep red)
  const r = Math.round(220 + (170 - 220) * t);
  const g = Math.round(230 + (35 - 230) * t);
  const b = Math.round(235 + (35 - 235) * t);
  return `rgb(${r},${g},${b})`;
}

function routeBadge(r) {
  const bg = r.color || "FFFFFF";
  const fg = r.text_color || "000000";
  return `<span class="route-badge" style="background:#${bg};color:#${fg}">${r.route_id}</span>`;
}

// Inline horizontal box plot of delay (minutes), x-axis fixed at -3 to +15 min
// for cross-route comparability. Whiskers are p5/p95 (not min/max) to keep the
// plot readable when a route has tail outliers.
function routeBoxPlot(r) {
  if (r.p50_delay_minutes === null || r.p50_delay_minutes === undefined) return "";
  const W = 140, H = 22;
  const xMin = -3, xMax = 15;
  const xScale = (m) => {
    const c = Math.max(xMin, Math.min(xMax, m));
    return ((c - xMin) / (xMax - xMin)) * W;
  };
  const x_p5  = xScale(r.p5_delay_minutes);
  const x_p25 = xScale(r.p25_delay_minutes);
  const x_p50 = xScale(r.p50_delay_minutes);
  const x_p75 = xScale(r.p75_delay_minutes);
  const x_p95 = xScale(r.p95_delay_minutes);
  const x0    = xScale(0);
  const tip = `delay (min) — p5: ${fmt(r.p5_delay_minutes)}  p25: ${fmt(r.p25_delay_minutes)}  p50: ${fmt(r.p50_delay_minutes)}  p75: ${fmt(r.p75_delay_minutes)}  p95: ${fmt(r.p95_delay_minutes)}`;
  return `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" class="boxplot">
    <title>${tip}</title>
    <line x1="${x0}"  x2="${x0}"  y1="0" y2="${H}" stroke="#1971c2" stroke-width="1" stroke-dasharray="2,2"/>
    <line x1="${x_p5}"  x2="${x_p95}" y1="${H/2}" y2="${H/2}" stroke="#888"/>
    <line x1="${x_p5}"  x2="${x_p5}"  y1="${H/2 - 4}" y2="${H/2 + 4}" stroke="#888"/>
    <line x1="${x_p95}" x2="${x_p95}" y1="${H/2 - 4}" y2="${H/2 + 4}" stroke="#888"/>
    <rect x="${x_p25}" y="${H/2 - 6}" width="${Math.max(1, x_p75 - x_p25)}" height="12" fill="#e7eef5" stroke="#5b7eaa" stroke-width="1"/>
    <line x1="${x_p50}" x2="${x_p50}" y1="${H/2 - 6}" y2="${H/2 + 6}" stroke="#1f3a5f" stroke-width="1.5"/>
  </svg>`;
}

async function load() {
  const yearEl = document.getElementById("footer-year");
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date");

  // Load order: GCS first, then local fallback (only when no specific date
  // is requested AND we're on localhost — supports `python3 scripts/generate_stats.py`
  // for offline iteration).
  const sources = [statsURL(date)];
  if (isLocal && !date) sources.push("data/stats.json");

  let data = null;
  let lastErr = null;
  for (const url of sources) {
    try {
      data = await fetchJSON(url);
      break;
    } catch (e) {
      lastErr = e;
    }
  }
  if (!data) {
    document.body.insertAdjacentHTML(
      "afterbegin",
      `<div style="padding:16px;background:#fee;color:#900;">
        Couldn't load stats for ${date || "latest"}: ${lastErr ? lastErr.message : "unknown"}.
        ${isLocal ? "For local iteration, run <code>python3 scripts/generate_stats.py</code>." : ""}
      </div>`
    );
    return;
  }

  render(data);
  const dates = await loadIndex();
  renderDateSelector(dates, date || data.service_date);
}

function render(data) {
  document.getElementById("meta").textContent =
    `Service date: ${data.service_date} · generated ${data.generated_at}`;

  // ---- system cards ----
  const s = data.system;
  const renderCards = (selector, items) => {
    document.querySelector(selector).innerHTML = items
      .map(({ label, val, grade }) => {
        const style = grade
          ? `background:${grade.bg};color:${grade.fg};border-color:transparent;`
          : "";
        return `
          <div class="card" style="${style}">
            <div class="label">${label}</div>
            <div class="val">${val}</div>
          </div>`;
      })
      .join("");
  };

  // System summary holds stop-level metrics; trip counts live in
  // schedule compliance (Observed running / Scheduled / Service delivered).
  renderCards("#system-cards", [
    { label: "Stops observed",   val: intFmt(s.total_observations) },
    { label: "Vehicles seen",    val: intFmt(s.vehicles_observed) },
    { label: "On time (≤3 min)", val: `${fmt(s.on_time_pct)}%`, grade: gradeOnTime(s.on_time_pct) },
    { label: "Late (>3 min)",    val: `${fmt(s.late_pct)}%`,    grade: gradeLate(s.late_pct) },
    { label: "Early",            val: `${fmt(s.early_pct)}%` },
    { label: "p50 delay",        val: `${fmt(s.p50_delay_minutes)} min` },
    { label: "p95 delay",        val: `${fmt(s.p95_delay_minutes)} min` },
    { label: "Avg speed",        val: `${fmt(s.avg_speed_mph)} mph` },
  ]);

  renderCards("#system-secondary", [
    { label: "Within 5 min", val: `${fmt(s.within_5min_pct)}%`, grade: gradeOnTime(s.within_5min_pct) },
    { label: "Within 7 min", val: `${fmt(s.within_7min_pct)}%`, grade: gradeOnTime(s.within_7min_pct) },
  ]);

  // ---- 1-min delay histogram ----
  const minBuckets = data.delay_minute_histogram || [];
  if (minBuckets.length) {
    const minM = Math.min(...minBuckets.map((b) => b.minute));
    const maxM = Math.max(...minBuckets.map((b) => b.minute));
    const labels = [];
    const counts = [];
    const bgColors = [];
    const byMinute = Object.fromEntries(minBuckets.map((b) => [b.minute, b.count]));
    for (let m = minM; m <= maxM; m++) {
      labels.push(m === 0 ? "0" : m > 0 ? `+${m}` : `${m}`);
      counts.push(byMinute[m] || 0);
      // color: red for late, blue for early, green for on-time band 0..3
      let color;
      if (m < 0) color = "#a6c3e0";        // early — soft blue
      else if (m <= 3) color = "#a1d99b";  // on-time — green
      else if (m <= 7) color = "#fdae6b";  // mildly late — orange
      else color = "#d62728";              // late — red
      bgColors.push(color);
    }
    const total = counts.reduce((a, b) => a + b, 0);
    const minCtx = document.getElementById("delay-minute-chart").getContext("2d");
    new Chart(minCtx, {
      type: "bar",
      data: {
        labels,
        datasets: [{
          label: "Stop arrivals",
          data: counts,
          backgroundColor: bgColors,
          borderWidth: 0,
        }],
      },
      options: {
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              title: (ctx) => `${ctx[0].label} min`,
              label: (ctx) => {
                const cnt = ctx.raw;
                const pct = total > 0 ? (100 * cnt) / total : 0;
                return `${cnt.toLocaleString()} stops (${pct.toFixed(1)}%)`;
              },
            },
          },
        },
        scales: {
          x: { title: { display: true, text: "delay (min)" }, grid: { display: false } },
          y: { beginAtZero: true, title: { display: true, text: "stops observed" } },
        },
      },
    });
  }

  // ---- schedule compliance ----
  const sc = data.schedule_compliance;
  const sdPct = sc.scheduled_trips
    ? (100 * sc.ran_trips) / sc.scheduled_trips
    : 0;
  const droppedPct = 100 - sdPct;

  // Trips that ran but never reached their final scheduled stop, as a
  // percent of trips that ran. Counts buses that broke down mid-route,
  // were reassigned mid-trip, or lost GPS before completion.
  const notCompleted = sc.trips_not_completed || 0;
  const notCompletedPct = sc.ran_trips ? (100 * notCompleted) / sc.ran_trips : 0;

  renderCards("#schedule-cards", [
    { label: "Scheduled trips",        val: intFmt(sc.scheduled_trips) },
    { label: "Observed running",       val: intFmt(sc.ran_trips) },
    { label: "Service delivered",      val: `${fmt(sdPct)}%`,
      grade: gradeServiceDelivered(sdPct) },
    { label: "Dropped / not observed", val: `${intFmt(sc.dropped_trips)} (${fmt(droppedPct)}%)` },
    { label: "Trips not completed",    val: `${intFmt(notCompleted)} (${fmt(notCompletedPct)}%)`,
      grade: gradeNotCompleted(notCompletedPct) },
  ]);

  // ---- distortion histogram (42 buckets: under, 40 × 5%, over) ----
  const dh = data.distortion_histogram || { buckets: [], counts: [] };
  const distCtx = document.getElementById("histogram-chart").getContext("2d");
  const dCounts = dh.counts || [];
  const dTotal = dCounts.reduce((a, b) => a + b, 0);
  const dPcts = dCounts.map((c) => (dTotal > 0 ? (c / dTotal) * 100 : 0));

  // Map bucket index → bucket center percent.
  //  index 0       → -100 (under)
  //  index 1..40   → -97.5, -92.5, ..., +97.5 (5% buckets)
  //  index 41      → +100 (over)
  const centerOf = (i) => {
    if (i === 0) return -100;
    if (i === 41) return 100;
    return -100 + (i - 1) * 5 + 2.5;
  };
  const distColors = dCounts.map((_, i) => distortionColor(centerOf(i)));

  // X-axis labels: only show every 4th bucket (every 20%) to keep readable.
  const xLabels = dh.buckets.map((b, i) => {
    if (i === 0) return "≤ -100";
    if (i === 41) return "≥ +100";
    const lo = -100 + (i - 1) * 5;
    return lo % 20 === 0 ? `${lo > 0 ? "+" : ""}${lo}` : "";
  });

  new Chart(distCtx, {
    type: "bar",
    data: {
      labels: xLabels,
      datasets: [{
        label: "% of stop arrivals",
        data: dPcts,
        backgroundColor: distColors,
        borderWidth: 0,
        categoryPercentage: 1.0,
        barPercentage: 1.0,
      }],
    },
    options: {
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (ctx) => dh.buckets[ctx[0].dataIndex],
            label: (ctx) => {
              const pct = ctx.raw;
              const cnt = dCounts[ctx.dataIndex];
              return `${pct.toFixed(2)}%  (${cnt.toLocaleString()} stops of ${dTotal.toLocaleString()})`;
            },
          },
        },
      },
      scales: {
        x: {
          title: { display: true, text: "headway distortion (%)" },
          grid: { display: false },
          ticks: { autoSkip: false, maxRotation: 0 },
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: "% of stop arrivals" },
          ticks: { callback: (v) => `${v}%` },
        },
      },
    },
  });

  // ---- routes table ----
  const tbody = document.querySelector("#routes-table tbody");
  let sortKey = "trips_observed";
  let sortDir = -1; // descending

  function renderRoutes() {
    const rows = [...data.routes].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      if (typeof av === "string") return sortDir * av.localeCompare(bv);
      return sortDir * (av - bv);
    });

    const cellGrade = (pct, fn) => {
      if (pct === null || pct === undefined) return "";
      const g = fn(pct);
      return `style="background:${g.bg};color:${g.fg};"`;
    };

    tbody.innerHTML = rows
      .map((r) => {
        const sd = r.service_delivered_pct;
        const sdTitle = `ran ${intFmt(r.ran_trips)} of ${intFmt(r.scheduled_trips)} scheduled`;
        return `
      <tr>
        <td>${routeBadge(r)}</td>
        <td>${routeBoxPlot(r)}</td>
        <td>${intFmt(r.trips_observed)}</td>
        <td>${intFmt(r.observations)}</td>
        <td ${cellGrade(sd, gradeServiceDelivered)} title="${sdTitle}">${sd === null ? "—" : fmt(sd)}</td>
        <td ${cellGrade(r.on_time_pct, gradeOnTime)}>${fmt(r.on_time_pct)}</td>
        <td ${cellGrade(r.late_pct, gradeLate)}>${fmt(r.late_pct)}</td>
        <td>${fmt(r.p50_delay_minutes)}</td>
        <td>${fmt(r.p95_delay_minutes)}</td>
        <td>${fmt(r.p50_distortion_pct)}</td>
        <td>${fmt(r.p95_distortion_pct)}</td>
        <td>${fmt(r.avg_speed_mph)}</td>
      </tr>`;
      })
      .join("");

    document.querySelectorAll("#routes-table th").forEach((th) => {
      th.classList.remove("sorted-asc", "sorted-desc");
      if (th.dataset.key === sortKey) {
        th.classList.add(sortDir > 0 ? "sorted-asc" : "sorted-desc");
      }
    });
  }

  document.querySelectorAll("#routes-table th").forEach((th) => {
    th.addEventListener("click", () => {
      const k = th.dataset.key;
      if (k === sortKey) sortDir = -sortDir;
      else { sortKey = k; sortDir = -1; }
      renderRoutes();
    });
  });

  renderRoutes();
}

load();

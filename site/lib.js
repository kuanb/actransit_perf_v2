// Shared helpers used by both the daily dashboard (/) and the weekly
// dashboard (/weekly/). Page-specific code (URL builders, page-specific
// renderers, page-specific colormaps) lives in each page's app.js.

const GCS_BASE = "https://storage.googleapis.com/transit-203605-actransit-cache";
const isLocal = ["localhost", "127.0.0.1"].includes(window.location.hostname);
// Public Mapbox token — same key used on https://kuanbutts.com/actransit/
const MAPBOX_TOKEN = "pk.eyJ1Ijoia3VhbmIiLCJhIjoiY21wN3VneDJkMDd5ZDJzcTFtM2w1d3V1ZSJ9.y5iV4TyPeSLLm3B6KN3vvA";

const fmt = (v, d = 1) =>
  v === null || v === undefined ? "—" : Number(v).toFixed(d);
const intFmt = (v) =>
  v === null || v === undefined ? "—" : Number(v).toLocaleString();

const LIMITED_ROUTE_SCHEDULED_RUNS_PER_DAY = 10;

function isTransbayRoute(routeID) {
  return /^[A-Za-z]+$/.test(String(routeID || ""));
}

function scheduledRunsPerServiceDay(route) {
  if (route && route.scheduled_runs_per_day !== null && route.scheduled_runs_per_day !== undefined) {
    const precomputed = Number(route.scheduled_runs_per_day);
    if (Number.isFinite(precomputed)) return precomputed;
  }

  if (route && Array.isArray(route.by_day)) {
    const activeDays = route.by_day
      .map((day) => Number(day.scheduled))
      .filter((scheduled) => Number.isFinite(scheduled) && scheduled > 0);
    if (activeDays.length) {
      return activeDays.reduce((sum, scheduled) => sum + scheduled, 0) / activeDays.length;
    }
  }

  if (route && route.scheduled_trips !== null && route.scheduled_trips !== undefined) {
    const daily = Number(route.scheduled_trips);
    if (Number.isFinite(daily)) return daily;
  }
  return null;
}

function isLimitedRoute(route) {
  if (route && typeof route.limited === "boolean") return route.limited;
  const scheduled = scheduledRunsPerServiceDay(route);
  return scheduled !== null &&
    scheduled < LIMITED_ROUTE_SCHEDULED_RUNS_PER_DAY &&
    !isTransbayRoute(route && route.route_id);
}

function limitedRouteTag(route) {
  return isLimitedRoute(route)
    ? `<span class="limited-route-tag" title="Fewer than 10 scheduled runs per active service day">Limited</span>`
    : "";
}

async function fetchJSON(url) {
  const res = await fetch(url, { cache: "no-cache" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// gradeColor maps t (0=worst, 1=best) to a {bg, fg} CSS color via a 3-stop
// gradient: dark red → orange → green. Text flips to white when bg is darkest.
function gradeColor(t) {
  t = Math.max(0, Math.min(1, t));
  const stops = [
    { t: 0,   rgb: [173,  30,  35] },
    { t: 0.5, rgb: [232, 140,  35] },
    { t: 1,   rgb: [160, 200, 130] },
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

// On Time Service Delivered: 99%+ = green, 90%- = dark red, linear in between.
function gradeServiceDelivered(pct) {
  return gradeColor((pct - 90) / (99 - 90));
}

// Multi-stop gradient interpolator keyed to absolute percentage values.
// stops must be sorted ascending by pct. Values outside the range clamp.
function gradePctStops(pct, stops) {
  pct = Math.max(stops[0].pct, Math.min(stops[stops.length - 1].pct, pct));
  let lo = stops[0], hi = stops[1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (pct <= stops[i + 1].pct) { lo = stops[i]; hi = stops[i + 1]; break; }
  }
  const f = (pct - lo.pct) / (hi.pct - lo.pct);
  const r = Math.round(lo.rgb[0] + (hi.rgb[0] - lo.rgb[0]) * f);
  const g = Math.round(lo.rgb[1] + (hi.rgb[1] - lo.rgb[1]) * f);
  const b = Math.round(lo.rgb[2] + (hi.rgb[2] - lo.rgb[2]) * f);
  const lum = 0.299 * r + 0.587 * g + 0.114 * b;
  return { bg: `rgb(${r},${g},${b})`, fg: lum < 110 ? "#ffffff" : "#1a1a1a" };
}

// Stop-level On Time Service Delivered colour scale. Anything ≥95% is green; the scale
// transitions through orange at 90%, into reds at 80–75%, and
// clamps to darkest red at 65% and below.
function gradeStopSD(pct) {
  return gradePctStops(pct, [
    { pct: 65, rgb: [110,  10,  15] }, // darkest red — clamps here and below
    { pct: 75, rgb: [173,  30,  35] }, // dark red
    { pct: 80, rgb: [210,  65,  40] }, // medium red
    { pct: 90, rgb: [232, 150,  35] }, // orange
    { pct: 95, rgb: [140, 190, 110] }, // green
  ]);
}

function routeBadge(r) {
  const bg = r.color || "FFFFFF";
  const fg = r.text_color || "000000";
  const n = r.trips_observed;
  const nSuffix = n === null || n === undefined
    ? ""
    : `<span class="route-n">N=${intFmt(n)}</span>`;
  return `<span class="route-badge" style="background:#${bg};color:#${fg}">${r.route_id}</span>${nSuffix}`;
}

// renderCards drops a uniform "labeled value" card grid into the
// element matching `selector`. Items: { label, val, grade? } where
// `grade` is { bg, fg } from gradeColor / gradeOnTime / etc.; cards
// without a grade use the default neutral background.
function renderCards(selector, items) {
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
}

function apiHealthSourceLabel(source) {
  if (source === "vehicle_locations") return "Vehicle locations";
  if (source === "ridership") return "Ridership attributes";
  return String(source || "Unknown source").replaceAll("_", " ");
}

function formatAPILatency(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(Number(ms))) return "—";
  const value = Number(ms);
  return value >= 1000 ? `${fmt(value / 1000, 2)} s` : `${fmt(value, 0)} ms`;
}

function apiHealthPercent(successful, requests) {
  return requests ? 100 * Number(successful || 0) / Number(requests) : 0;
}

function apiHealthCountLabel(value, label) {
  return `${intFmt(value)} ${label}${Number(value) === 1 ? "" : "s"}`;
}

function apiHealthStatus(successPct) {
  if (successPct >= 99.5) return { className: "is-healthy", label: "Healthy" };
  if (successPct >= 95) return { className: "is-degraded", label: "Degraded" };
  return { className: "is-unhealthy", label: "Unhealthy" };
}

function apiHealthQuantile(values, fraction) {
  const sorted = values
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (!sorted.length) return null;
  return sorted[Math.round((sorted.length - 1) * fraction)];
}

function aggregateDailyAPIHealth(dailies) {
  const bySource = new Map();
  for (const daily of dailies || []) {
    const health = daily && daily.api_health;
    if (!health || !Array.isArray(health.sources)) continue;
    for (const source of health.sources) {
      if (!source || !source.requests) continue;
      if (!bySource.has(source.source)) {
        bySource.set(source.source, {
          source: source.source,
          requests: 0,
          successful_requests: 0,
          timeout_count: 0,
          http_4xx_count: 0,
          http_5xx_count: 0,
          other_error_count: 0,
          buckets: [],
        });
      }
      const aggregate = bySource.get(source.source);
      aggregate.requests += Number(source.requests || 0);
      aggregate.successful_requests += Number(source.successful_requests || 0);
      aggregate.timeout_count += Number(source.timeout_count || 0);
      aggregate.http_4xx_count += Number(source.http_4xx_count || 0);
      aggregate.http_5xx_count += Number(source.http_5xx_count || 0);
      aggregate.other_error_count += Number(source.other_error_count || 0);
      aggregate.buckets.push({
        started_at: daily.service_date,
        requests: Number(source.requests || 0),
        successful_requests: Number(source.successful_requests || 0),
        success_pct: Number(source.success_pct || 0),
        p50_latency_ms: source.p50_latency_ms,
        p95_latency_ms: source.p95_latency_ms,
        p99_latency_ms: source.p99_latency_ms,
      });
    }
  }

  const sources = Array.from(bySource.values());
  for (const source of sources) {
    source.buckets.sort((a, b) => a.started_at.localeCompare(b.started_at));
    source.success_pct = apiHealthPercent(source.successful_requests, source.requests);
    source.p50_latency_ms = apiHealthQuantile(source.buckets.map((b) => b.p50_latency_ms), 0.5);
    source.p95_latency_ms = apiHealthQuantile(source.buckets.map((b) => b.p95_latency_ms), 0.5);
    source.p99_latency_ms = apiHealthQuantile(source.buckets.map((b) => b.p99_latency_ms), 0.5);
  }
  sources.sort((a, b) => {
    const order = { vehicle_locations: 0, ridership: 1 };
    return (order[a.source] ?? 99) - (order[b.source] ?? 99) || a.source.localeCompare(b.source);
  });
  if (!sources.length) return null;

  const dates = (dailies || []).map((d) => d && d.service_date).filter(Boolean).sort();
  return {
    period_start: dates[0],
    period_end: dates[dates.length - 1],
    bucket_granularity: "day",
    sources,
  };
}

function renderAPIHealth(health) {
  const section = document.getElementById("api-health-section");
  if (!section) return;
  const sources = health && Array.isArray(health.sources)
    ? health.sources.filter((source) => Number(source.requests) > 0)
    : [];
  section.hidden = sources.length === 0;
  if (!sources.length) return;

  const daily = health.bucket_granularity === "day";
  const period = health.period_start === health.period_end
    ? health.period_start
    : `${health.period_start} → ${health.period_end}`;
  document.getElementById("api-health-window").textContent =
    `${period} · ${daily ? "daily" : "hourly"} latency buckets`;

  document.getElementById("api-health-cards").innerHTML = sources.map((source) => {
    const successPct = Number(source.success_pct ?? apiHealthPercent(source.successful_requests, source.requests));
    const status = apiHealthStatus(successPct);
    const failures = Math.max(0, Number(source.requests) - Number(source.successful_requests || 0));
    const latencyLabels = daily
      ? { p50: "Typical p50", p95: "Typical p95", p99: "Typical p99" }
      : { p50: "Median", p95: "p95", p99: "p99" };
    const errors = failures === 0
      ? "No failures recorded in this window"
      : `${apiHealthCountLabel(failures, "failure")} · ${apiHealthCountLabel(source.timeout_count || 0, "timeout")} · ` +
        `${intFmt(source.http_4xx_count || 0)} 4xx · ${intFmt(source.http_5xx_count || 0)} 5xx · ` +
        `${intFmt(source.other_error_count || 0)} other`;
    return `
      <article class="api-health-card">
        <div class="api-health-card-heading">
          <div>
            <p>${apiHealthSourceLabel(source.source)}</p>
            <strong>${fmt(successPct)}% successful</strong>
          </div>
          <span class="api-health-status ${status.className}">${status.label}</span>
        </div>
        <dl class="api-health-metrics">
          <div><dt>${latencyLabels.p50}</dt><dd>${formatAPILatency(source.p50_latency_ms)}</dd></div>
          <div><dt>${latencyLabels.p95}</dt><dd>${formatAPILatency(source.p95_latency_ms)}</dd></div>
          <div><dt>${latencyLabels.p99}</dt><dd>${formatAPILatency(source.p99_latency_ms)}</dd></div>
          <div><dt>Requests</dt><dd>${intFmt(source.requests)}</dd></div>
        </dl>
        <p class="api-health-errors">${errors}</p>
      </article>`;
  }).join("");

  const bucketKeys = Array.from(new Set(
    sources.flatMap((source) => (source.buckets || []).map((bucket) => bucket.started_at))
  )).sort();
  const controls = document.getElementById("api-health-controls");
  const chartCard = document.getElementById("api-health-chart-card");
  chartCard.hidden = bucketKeys.length === 0;
  if (!bucketKeys.length) return;

  const formatBucket = (value) => {
    if (daily) {
      const [year, month, day] = value.slice(0, 10).split("-").map(Number);
      return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" })
        .format(new Date(Date.UTC(year, month - 1, day)));
    }
    const date = new Date(value);
    return new Intl.DateTimeFormat("en-US", {
      weekday: health.period_start === health.period_end ? undefined : "short",
      hour: "numeric",
      timeZone: "America/Los_Angeles",
    }).format(date);
  };
  const colors = { vehicle_locations: "#1971c2", ridership: "#d97706" };
  const canvas = document.getElementById("api-health-chart");

  const draw = (percentile) => {
    if (canvas.apiHealthChart) canvas.apiHealthChart.destroy();
    const metric = `${percentile}_latency_ms`;
    canvas.apiHealthChart = new Chart(canvas.getContext("2d"), {
      type: "line",
      data: {
        labels: bucketKeys.map(formatBucket),
        datasets: sources.map((source, index) => {
          const byBucket = new Map((source.buckets || []).map((bucket) => [bucket.started_at, bucket]));
          return {
            label: apiHealthSourceLabel(source.source),
            data: bucketKeys.map((key) => byBucket.get(key)?.[metric] ?? null),
            borderColor: colors[source.source] || ["#7c3aed", "#0f766e"][index % 2],
            backgroundColor: "transparent",
            borderWidth: 2.5,
            pointRadius: daily ? 3 : 1.5,
            pointHoverRadius: 5,
            spanGaps: false,
            tension: 0.22,
          };
        }),
      },
      options: {
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" },
        plugins: {
          legend: { position: "bottom", labels: { usePointStyle: true, boxWidth: 8 } },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.dataset.label}: ${formatAPILatency(ctx.raw)}`,
            },
          },
        },
        scales: {
          x: {
            grid: { display: false },
            ticks: { maxTicksLimit: daily ? 14 : 12, maxRotation: 0 },
            title: { display: true, text: daily ? "Service date" : "Pacific time" },
          },
          y: {
            beginAtZero: true,
            title: { display: true, text: "Response time (ms)" },
          },
        },
      },
    });
  };

  controls.onchange = (event) => {
    if (event.target.name === "api-health-percentile") draw(event.target.value);
  };
  const selected = controls.querySelector('input[name="api-health-percentile"]:checked');
  draw(selected ? selected.value : "p95");
}

function busSpacingAvailable(bunching) {
  return Boolean(
    bunching &&
    bunching.methodology_version === 3 &&
    bunching.status === "available" &&
    bunching.eligibility &&
    bunching.eligibility.eligible === true &&
    bunching.headway_cv !== null &&
    bunching.headway_cv !== undefined
  );
}

function busSpacingScore(cv) {
  if (cv === null || cv === undefined) return null;
  return Math.max(0, Math.min(100, 100 * (1 - Number(cv))));
}

function busSpacingStatusLabel(bunching) {
  if (busSpacingAvailable(bunching)) return "Available";
  if (!bunching || bunching.methodology_version !== 3 || bunching.status === "missing") {
    return "Missing";
  }
  if (bunching.status === "partial") return "Partial window";
  const reason = bunching.eligibility && bunching.eligibility.reason;
  if (reason === "too_low_frequency" || bunching.status === "too_low_frequency") {
    return "Too low-frequency";
  }
  if (reason === "not_enough_comparable_headways") return "Not enough headways";
  if (reason === "low_comparable_headway_coverage") return "Low coverage";
  return "Insufficient data";
}

function busSpacingTooLowFrequency(bunching) {
  return Boolean(
    bunching &&
    (bunching.status === "too_low_frequency" ||
      (bunching.eligibility && bunching.eligibility.reason === "too_low_frequency"))
  );
}

function busSpacingWarningText(bunching, gradeWeight = null) {
  const suffix = gradeWeight === null
    ? "No CV or bunching metric is reported for this window."
    : `The ${gradeWeight}% bus-spacing component is skipped and the other available grade components are renormalized.`;
  if (!bunching || bunching.methodology_version !== 3 || bunching.status === "missing") {
    return `Bus-spacing data is missing for this window. ${suffix}`;
  }
  if (bunching.status === "partial") {
    return `Bus-spacing data covers ${intFmt(bunching.days_available)} of ${intFmt(bunching.days_expected)} expected service days. ${suffix}`;
  }
  const eligibility = bunching.eligibility || {};
  if (eligibility.reason === "too_low_frequency" || bunching.status === "too_low_frequency") {
    return `This route has no scheduled service window with buses every ${fmt(eligibility.maximum_frequency_min || 40, 0)} minutes or better, so it is too low-frequency for a meaningful bunching calculation. ${suffix}`;
  }
  if (eligibility.reason === "not_enough_comparable_headways") {
    return `Only ${intFmt(eligibility.cv_headway_n)} comparable headways qualify; at least ${intFmt(eligibility.minimum_cv_headway_n || 100)} are required. ${suffix}`;
  }
  if (eligibility.reason === "low_comparable_headway_coverage") {
    return `Only ${fmt(eligibility.cv_coverage_pct)}% of measured eligible headways are comparable; at least ${fmt(eligibility.minimum_coverage_pct || 10, 0)}% coverage is required. ${suffix}`;
  }
  return `Bus-spacing data is not sufficient for this window. ${suffix}`;
}

function busSpacingCardItems(bunching, includeScore = false) {
  if (!busSpacingAvailable(bunching)) {
    return [{ label: "Bus spacing status", val: busSpacingStatusLabel(bunching) }];
  }
  const items = [
    { label: "Headway CV", val: fmt(bunching.headway_cv, 2), grade: gradeColor(1 - bunching.headway_cv) },
    { label: "Bunched arrivals", val: `${fmt(bunching.bunched_headway_pct)}%` },
    { label: "Long gaps", val: `${fmt(bunching.long_gap_pct)}%` },
    { label: "Comparable headways", val: intFmt(bunching.eligibility.cv_headway_n) },
    { label: "Comparable coverage", val: `${fmt(bunching.eligibility.cv_coverage_pct)}%` },
    { label: "Mean headway", val: `${fmt(bunching.mean_headway_min)} min` },
    { label: "Expected wait", val: `${fmt(bunching.expected_wait_min)} min` },
    { label: "Even-spacing wait", val: `${fmt(bunching.even_spacing_wait_min)} min` },
    { label: "Spacing penalty", val: `+${fmt(bunching.spacing_penalty_min)} min` },
  ];
  if (includeScore) {
    items.splice(1, 0, {
      label: "Bus-spacing sub-score",
      val: `${fmt(busSpacingScore(bunching.headway_cv), 0)} / 100`,
      grade: gradeColor(1 - bunching.headway_cv),
    });
  }
  return items;
}

function busSpacingMethodologyHTML(includeScore = false) {
  const scoreCopy = includeScore
    ? " For report-card scoring, the bus-spacing sub-score is <code>100 × (1 − CV)</code>, clamped to 0–100."
    : "";
  return `
    <p class="muted bunching-methodology">
      <strong>How to read CV.</strong> The headway coefficient of variation is
      <code>standard deviation ÷ mean</code>, calculated within comparable route,
      direction, stop, and Pacific-hour cells and then weighted by contributing
      headways. A CV of 0 means perfectly even spacing; higher values mean increasingly
      uneven gaps between buses.${scoreCopy}
    </p>
    <p class="muted bunching-methodology">
      A bus is marked <strong>bunched</strong> when its realized gap is below half
      the scheduled gap; a <strong>long gap</strong> exceeds 1.5 times the scheduled
      gap. Bunched-arrival values are already percentages: <strong>0.1 means 0.1%</strong>
      (about one in 1,000 comparable headways), not 10%. Only schedule windows with
      service every 40 minutes or better qualify.
      At least 100 comparable headways and 10% comparable coverage are required;
      realized gaps below 30 seconds or above 90 minutes are excluded as duplicate
      and overnight/outlier guards. Expected wait assumes riders arrive at random,
      while spacing penalty compares it with evenly spaced service at the same
      realized frequency. Method inspired by
      <a href="https://sal-khan.com/bus-bunching.html" target="_blank" rel="noopener">Salman Khan’s bus-bunching project</a>,
      adapted to AC Transit GTFS-Realtime arrivals and published GTFS schedules.
    </p>`;
}

// renderDelayMinuteHistogram draws a 1-minute-bucket bar chart of stop
// delays into the canvas with id `canvasId`. Bars colored by lateness
// band (early=blue, on-time=green, mildly-late=orange, late=red).
// Buckets at the boundary minutes (-15 / +45) are clamps for outliers
// and the tooltip says so via the bare label values.
function renderDelayMinuteHistogram(canvasId, minBuckets) {
  if (!minBuckets || !minBuckets.length) return;
  const minM = Math.min(...minBuckets.map((b) => b.minute));
  const maxM = Math.max(...minBuckets.map((b) => b.minute));
  const labels = [];
  const counts = [];
  const bgColors = [];
  const byMinute = Object.fromEntries(minBuckets.map((b) => [b.minute, b.count]));
  for (let m = minM; m <= maxM; m++) {
    labels.push(m === 0 ? "0" : m > 0 ? `+${m}` : `${m}`);
    counts.push(byMinute[m] || 0);
    let color;
    if (m < 0) color = "#a6c3e0";
    else if (m <= 3) color = "#a1d99b";
    else if (m <= 7) color = "#fdae6b";
    else color = "#d62728";
    bgColors.push(color);
  }
  const total = counts.reduce((a, b) => a + b, 0);
  const ctx = document.getElementById(canvasId).getContext("2d");
  return new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{ label: "Stop arrivals", data: counts, backgroundColor: bgColors, borderWidth: 0 }],
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

// pickTextColor picks black or white based on the perceived luminance
// of an "rgb(r,g,b)" CSS color, so foreground text stays legible against
// any background in a colormap that spans dark→light→dark.
function pickTextColor(rgbStr) {
  const m = rgbStr && rgbStr.match(/rgb\((\d+)\s*,\s*(\d+)\s*,\s*(\d+)\)/);
  if (!m) return "#1a1a1a";
  const r = +m[1], g = +m[2], b = +m[3];
  // Standard relative-luminance approximation (Rec. 709 coefficients).
  const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  return lum > 150 ? "#1a1a1a" : "#ffffff";
}

// delayDivergingColor maps a signed delay value (minutes) to a CSS color
// on a 3-stop gradient: blue (early) → white (on-time / mid) → red (late).
// `lo`, `mid`, `hi` set the scale: anything ≤ lo is saturated blue,
// anything ≥ hi is saturated red. Use different (lo, mid, hi) for p50
// (typically -3, 0, +5) vs p95 (0, +3, +15) so the visual range matches
// where the bulk of values actually fall.
function delayDivergingColor(min, lo, mid, hi) {
  if (min === null || min === undefined) return null;
  const blueRGB = [40, 100, 180];
  const whiteRGB = [255, 255, 255];
  const redRGB = [180, 40, 40];
  const blend = (a, b, f) => {
    const r = Math.round(a[0] + (b[0] - a[0]) * f);
    const g = Math.round(a[1] + (b[1] - a[1]) * f);
    const bl = Math.round(a[2] + (b[2] - a[2]) * f);
    return `rgb(${r},${g},${bl})`;
  };
  if (min <= lo) return `rgb(${blueRGB.join(",")})`;
  if (min >= hi) return `rgb(${redRGB.join(",")})`;
  if (min <= mid) return blend(blueRGB, whiteRGB, (min - lo) / (mid - lo));
  return blend(whiteRGB, redRGB, (min - mid) / (hi - mid));
}

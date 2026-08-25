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

function busSpacingAvailable(bunching) {
  return Boolean(
    bunching &&
    bunching.methodology_version === 2 &&
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
  if (!bunching || bunching.methodology_version !== 2 || bunching.status === "missing") {
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
  if (!bunching || bunching.methodology_version !== 2 || bunching.status === "missing") {
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

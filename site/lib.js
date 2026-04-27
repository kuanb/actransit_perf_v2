// Shared helpers used by both the daily dashboard (/) and the weekly
// dashboard (/weekly/). Page-specific code (URL builders, page-specific
// renderers, page-specific colormaps) lives in each page's app.js.

const GCS_BASE = "https://storage.googleapis.com/transit-203605-actransit-cache";
const isLocal = ["localhost", "127.0.0.1"].includes(window.location.hostname);

const fmt = (v, d = 1) =>
  v === null || v === undefined ? "—" : Number(v).toFixed(d);
const intFmt = (v) =>
  v === null || v === undefined ? "—" : Number(v).toLocaleString();

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

// Service delivered: 99%+ = green, 90%- = dark red, linear in between.
function gradeServiceDelivered(pct) {
  return gradeColor((pct - 90) / (99 - 90));
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

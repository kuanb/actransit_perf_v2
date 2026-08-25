// Bus report card. Aggregates the last ~28 daily stats files (per-route)
// from GCS in the browser, computes a 0-100 composite score per route, maps
// it to a school letter grade, and renders a sortable best-to-worst table.
// GCS_BASE, fetchJSON, fmt, intFmt, gradeColor, routeBadge live in ../lib.js.

const WINDOW_DAYS = 28;
const IDEAL_SPEED_MPH = 13;
const HISTORY_MONTHS = 6;
const HISTORY_STEP_DAYS = 7;
const HISTORY_FETCH_BATCH = 24;
const BUNCHING_PROGRESS_POINTS = [10, 25, 40, 55, 70, 85];
const BUNCHING_METHODOLOGY_VERSION = 2;
const BUNCHING_MAX_FREQUENCY_MIN = 40;
const BUNCHING_MIN_CV_WEIGHT = 100;
const BUNCHING_MIN_CV_COVERAGE = 0.10;
const BUNCHING_GRADE_WEIGHT = 35;

// Composite weights. Components with no data for a route are dropped and
// the remaining weights renormalized at score time.
const WEIGHTS = {
  stop_sd: 40,
  on_time: 10,
  speed: 15,
  bunching: BUNCHING_GRADE_WEIGHT,
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

function emptyBunchingAccumulator() {
  return {
    headway_n: 0,
    cell_n: 0,
    comparison_n: 0,
    bunched_headway_n: 0,
    long_gap_n: 0,
    scheduled_headway_n: 0,
    all_scheduled_headway_n: 0,
    cv_weighted_sum: 0,
    cv_weight: 0,
    observed_headway_seconds: 0,
    observed_headway_squared_seconds: 0,
    comparable_headway_seconds: 0,
    comparable_headway_squared_seconds: 0,
    even_spacing_wait_area_seconds_squared: 0,
    scheduled_headway_seconds: 0,
    scheduled_headway_squared_seconds: 0,
    all_scheduled_headway_seconds: 0,
  };
}

function addBunchingMetrics(acc, metrics) {
  if (!metrics) return;
  const aggregation = metrics.aggregation || metrics;
  for (const key of [
    "headway_n", "cell_n", "comparison_n", "bunched_headway_n",
    "long_gap_n", "scheduled_headway_n", "all_scheduled_headway_n",
  ]) {
    acc[key] += Number(metrics[key]) || 0;
  }
  for (const key of [
    "cv_weighted_sum", "cv_weight", "observed_headway_seconds",
    "observed_headway_squared_seconds", "comparable_headway_seconds",
    "comparable_headway_squared_seconds", "even_spacing_wait_area_seconds_squared",
    "scheduled_headway_seconds", "scheduled_headway_squared_seconds",
    "all_scheduled_headway_seconds",
  ]) {
    acc[key] += Number(aggregation[key]) || 0;
  }
}

function metricsFromBunchingAccumulator(acc) {
  const expectedWait = acc.comparable_headway_seconds > 0
    ? acc.comparable_headway_squared_seconds / (2 * acc.comparable_headway_seconds) / 60
    : null;
  const evenSpacingWait = acc.comparable_headway_seconds > 0
    ? acc.even_spacing_wait_area_seconds_squared / acc.comparable_headway_seconds / 60
    : null;
  const scheduledWait = acc.scheduled_headway_seconds > 0
    ? acc.scheduled_headway_squared_seconds / (2 * acc.scheduled_headway_seconds) / 60
    : null;
  return {
    headway_n: acc.headway_n,
    cell_n: acc.cell_n,
    comparison_n: acc.comparison_n,
    bunched_headway_n: acc.bunched_headway_n,
    long_gap_n: acc.long_gap_n,
    scheduled_headway_n: acc.scheduled_headway_n,
    all_scheduled_headway_n: acc.all_scheduled_headway_n,
    headway_cv: acc.cv_weight > 0 ? acc.cv_weighted_sum / acc.cv_weight : null,
    bunched_headway_pct: acc.comparison_n > 0
      ? 100 * acc.bunched_headway_n / acc.comparison_n
      : null,
    long_gap_pct: acc.comparison_n > 0
      ? 100 * acc.long_gap_n / acc.comparison_n
      : null,
    mean_headway_min: acc.headway_n > 0
      ? acc.observed_headway_seconds / acc.headway_n / 60
      : null,
    expected_wait_min: expectedWait,
    scheduled_expected_wait_min: scheduledWait,
    even_spacing_wait_min: evenSpacingWait,
    spacing_penalty_min: expectedWait != null && evenSpacingWait != null
      ? Math.max(0, expectedWait - evenSpacingWait)
      : null,
    aggregation: {
      cv_weighted_sum: acc.cv_weighted_sum,
      cv_weight: acc.cv_weight,
      observed_headway_seconds: acc.observed_headway_seconds,
      observed_headway_squared_seconds: acc.observed_headway_squared_seconds,
      comparable_headway_seconds: acc.comparable_headway_seconds,
      comparable_headway_squared_seconds: acc.comparable_headway_squared_seconds,
      even_spacing_wait_area_seconds_squared: acc.even_spacing_wait_area_seconds_squared,
      scheduled_headway_seconds: acc.scheduled_headway_seconds,
      scheduled_headway_squared_seconds: acc.scheduled_headway_squared_seconds,
      all_scheduled_headway_seconds: acc.all_scheduled_headway_seconds,
    },
  };
}

function bunchingEligibility(bunching) {
  const cvWeight = Number(bunching.aggregation && bunching.aggregation.cv_weight) || 0;
  const headways = Number(bunching.headway_n) || 0;
  const coveragePct = headways > 0 ? 100 * cvWeight / headways : 0;
  const shared = {
    cv_headway_n: cvWeight,
    cv_coverage_pct: coveragePct,
    minimum_cv_headway_n: BUNCHING_MIN_CV_WEIGHT,
    minimum_coverage_pct: 100 * BUNCHING_MIN_CV_COVERAGE,
    maximum_frequency_min: BUNCHING_MAX_FREQUENCY_MIN,
  };
  if ((Number(bunching.scheduled_headway_n) || 0) === 0 &&
      (Number(bunching.all_scheduled_headway_n) || 0) > 0) {
    return { eligible: false, reason: "too_low_frequency", ...shared };
  }
  if (cvWeight < BUNCHING_MIN_CV_WEIGHT) {
    return { eligible: false, reason: "not_enough_comparable_headways", ...shared };
  }
  if (coveragePct < 100 * BUNCHING_MIN_CV_COVERAGE) {
    return { eligible: false, reason: "low_comparable_headway_coverage", ...shared };
  }
  return { eligible: true, ...shared };
}

function aggregateBunchingPayloads(payloads, expectedDays) {
  const values = Array.isArray(payloads) ? payloads : [];
  const expected = expectedDays == null
    ? values.reduce((sum, value) => sum + (Number(value && value.days_expected) || 1), 0)
    : expectedDays;
  const total = emptyBunchingAccumulator();
  const progress = new Map();
  let available = 0;

  for (const value of values) {
    if (!value || value.methodology_version !== BUNCHING_METHODOLOGY_VERSION || !value.aggregation) continue;
    available += Number(value.days_available) || 0;
    addBunchingMetrics(total, value);
    for (const point of value.by_progress || []) {
      let acc = progress.get(point.progress_pct);
      if (!acc) {
        acc = emptyBunchingAccumulator();
        progress.set(point.progress_pct, acc);
      }
      addBunchingMetrics(acc, point);
    }
  }

  available = Math.min(available, expected);
  const metrics = metricsFromBunchingAccumulator(total);
  const eligibility = bunchingEligibility(metrics);
  let status = "available";
  if (available === 0) status = "missing";
  else if (available < expected) status = "partial";
  else if (!eligibility.eligible) status = eligibility.reason;

  return {
    methodology_version: BUNCHING_METHODOLOGY_VERSION,
    days_expected: expected,
    days_available: available,
    missing_days: Math.max(0, expected - available),
    status,
    eligibility,
    ...metrics,
    by_progress: BUNCHING_PROGRESS_POINTS.map((progressPct) => {
      const acc = progress.get(progressPct);
      if (!acc) return { progress_pct: progressPct, status: "insufficient_data", headway_cv: null };
      const point = metricsFromBunchingAccumulator(acc);
      return {
        progress_pct: progressPct,
        status: point.headway_cv == null ? "insufficient_data" : "available",
        ...point,
      };
    }),
  };
}

function bunchingViewMetrics(bunching) {
  if (!bunching) return {
    bunching: null,
    bunching_grade_eligible: false,
    headway_cv: null,
    headway_n: 0,
    progress_cv: BUNCHING_PROGRESS_POINTS.map(() => null),
  };
  return {
    bunching,
    bunching_grade_eligible: bunching.status === "available" && Boolean(bunching.eligibility && bunching.eligibility.eligible),
    headway_cv: bunching.headway_cv,
    headway_n: bunching.headway_n,
    bunched_headway_pct: bunching.bunched_headway_pct,
    long_gap_pct: bunching.long_gap_pct,
    mean_headway_min: bunching.mean_headway_min,
    expected_wait_min: bunching.expected_wait_min,
    scheduled_expected_wait_min: bunching.scheduled_expected_wait_min,
    even_spacing_wait_min: bunching.even_spacing_wait_min,
    spacing_penalty_min: bunching.spacing_penalty_min,
    progress_cv: (bunching.by_progress || []).map((point) => point.headway_cv),
  };
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
          bunching: [],
        };
        acc.set(r.route_id, a);
      }
      const scheduled = Number(r.scheduled_trips) || 0;
      if (scheduled > 0) {
        a.scheduled_runs += scheduled;
        a.scheduled_days += 1;
      }
      if (scheduled > 0 || w > 0) a.bunching.push(r.bunching || null);
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
    };
    const bunching = aggregateBunchingPayloads(a.bunching, a.bunching.length);
    Object.assign(metrics, bunchingViewMetrics(bunching));

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

function bunchingScore(cv) {
  return clamp(100 * (1 - Number(cv)));
}

function compositeScore(metrics) {
  const { stop_sd_pct, on_time_pct, avg_speed_mph, headway_cv } = metrics;
  const parts = [];
  if (stop_sd_pct != null) parts.push([WEIGHTS.stop_sd, clamp(stop_sd_pct)]);
  if (on_time_pct != null) parts.push([WEIGHTS.on_time, clamp(on_time_pct)]);
  if (avg_speed_mph != null) parts.push([WEIGHTS.speed, clamp((avg_speed_mph / IDEAL_SPEED_MPH) * 100)]);
  if (metrics.bunching_grade_eligible && headway_cv != null) {
    parts.push([WEIGHTS.bunching, bunchingScore(headway_cv)]);
  }
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
  const m = { stop_sd_pct: [0, 0], on_time_pct: [0, 0], avg_speed_mph: [0, 0], p95_delay_minutes: [0, 0] };
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
  const expectedBunchingDays = routes.reduce(
    (sum, route) => sum + (Number(route.bunching && route.bunching.days_expected) || 0),
    0
  );
  const bunching = aggregateBunchingPayloads(
    routes.map((route) => route.bunching),
    expectedBunchingDays
  );
  const agency = {
    stop_sd_pct: mean("stop_sd_pct"),
    on_time_pct: mean("on_time_pct"),
    avg_speed_mph: mean("avg_speed_mph"),
    p95_delay_minutes: mean("p95_delay_minutes"),
    trips: acc.trips,
    observations: acc.obs,
    routes: routes.length,
    ...bunchingViewMetrics(bunching),
  };
  agency.score = compositeScore(agency);
  return agency;
}

function bunchingGradeWarning(bunching, unit = "service days") {
  if (!bunching) {
    return `Bunching data is missing for this four-week window. Its ${BUNCHING_GRADE_WEIGHT}% grade component is not used; the other available components are renormalized.`;
  }
  const suffix = `Its ${BUNCHING_GRADE_WEIGHT}% grade component is not used; the other available components are renormalized.`;
  if (bunching.status === "partial") {
    return `Bunching data is partial (${intFmt(bunching.days_available)} of ${intFmt(bunching.days_expected)} ${unit}). ${suffix}`;
  }
  if (bunching.status === "missing") {
    return `Bunching data is missing or uses an earlier methodology for this four-week window. ${suffix}`;
  }
  const eligibility = bunching.eligibility || {};
  if (eligibility.eligible) return "";
  if (eligibility.reason === "too_low_frequency") {
    return `This route has no scheduled headways of ${fmt(eligibility.maximum_frequency_min || BUNCHING_MAX_FREQUENCY_MIN, 0)} minutes or less. Low-frequency routes are ungraded on bunching. ${suffix}`;
  }
  if (eligibility.reason === "not_enough_comparable_headways") {
    return `Only ${intFmt(eligibility.cv_headway_n)} comparable headways qualify for the CV; at least ${intFmt(eligibility.minimum_cv_headway_n || BUNCHING_MIN_CV_WEIGHT)} are required. ${suffix}`;
  }
  if (eligibility.reason === "low_comparable_headway_coverage") {
    return `Only ${fmt(eligibility.cv_coverage_pct)}% of measured headways qualify for the CV; at least ${fmt(eligibility.minimum_coverage_pct || 100 * BUNCHING_MIN_CV_COVERAGE, 0)}% coverage is required. ${suffix}`;
  }
  return `Bunching is not eligible for grading in this window. ${suffix}`;
}

function renderAgencyHero(a) {
  const el = document.getElementById("agency-hero");
  if (!el) return;
  if (a.score == null) { el.hidden = true; return; }
  const g = scoreColor(a.score);
  const grade = letterGrade(a.score);
  const hasBunching = a.headway_cv != null;
  const bunchingWarning = bunchingGradeWarning(a.bunching, "route-days");
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
        <div class="agency-sub">Non-limited routes over the last four weeks, weighted by each route's share of measured trip-stops. Lettered Transbay routes remain included.${a.bunching_grade_eligible ? ` Bus spacing contributes ${BUNCHING_GRADE_WEIGHT}% of the score.` : ""}</div>
        ${bunchingWarning ? `<div class="grade-data-warning" role="status"><strong>Bunching excluded from this grade.</strong> ${bunchingWarning}</div>` : ""}
        <div class="agency-stats">
          ${stat("On Time Service Delivered", a.stop_sd_pct == null ? "—" : fmt(a.stop_sd_pct) + "%")}
          ${stat("On time (≤3 min)", a.on_time_pct == null ? "—" : fmt(a.on_time_pct) + "%")}
          ${stat("p95 delay", a.p95_delay_minutes == null ? "—" : fmt(a.p95_delay_minutes) + " min")}
          ${stat("Avg speed", fmt(a.avg_speed_mph) + " mph", speedPct == null ? "" : `${fmt(speedPct, 0)}% of ideal`)}
          ${hasBunching
            ? stat("Headway CV", fmt(a.headway_cv, 2), a.bunching_grade_eligible
              ? `${fmt(bunchingScore(a.headway_cv), 0)} / 100 sub-score`
              : "not used in this grade")
            : stat("Headway CV", "—", "not used in this grade")}
          ${hasBunching ? stat("Bunched arrivals", `${fmt(a.bunched_headway_pct)}%`) : ""}
          ${hasBunching ? stat("Spacing penalty", `+${fmt(a.spacing_penalty_min)} min`) : ""}
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

function componentHistoryChartOptions(values, yTitle, tooltipLabel) {
  const valid = values.filter((value) => value != null && Number.isFinite(value));
  let min = valid.length ? Math.max(0, Math.floor(Math.min(...valid) / 5) * 5 - 5) : 0;
  let max = valid.length ? Math.min(100, Math.ceil(Math.max(...valid) / 5) * 5 + 5) : 100;
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
        ticks: { stepSize: 5 },
        title: { display: true, text: yTitle },
      },
    },
    plugins: {
      legend: { display: false },
      tooltip: { callbacks: { label: tooltipLabel } },
    },
  };
}

function renderComponentHistoryChart(canvasID, labels, values, label, color, fillColor, yTitle, tooltipLabel) {
  new Chart(document.getElementById(canvasID), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label,
        data: values,
        borderColor: color,
        backgroundColor: fillColor,
        borderWidth: 2.5,
        fill: true,
        tension: 0.2,
        pointRadius: 3,
        pointHoverRadius: 5,
        spanGaps: false,
      }],
    },
    options: componentHistoryChartOptions(values, yTitle, tooltipLabel),
  });
}

function renderGradeHistory(points, topRoutes) {
  const agencyMeta = document.getElementById("agency-history-meta");
  const routeMeta = document.getElementById("route-history-meta");
  const componentMeta = document.getElementById("component-history-meta");
  if (!points.length) {
    agencyMeta.textContent = "Not enough daily data for a complete four-week historical point yet.";
    routeMeta.textContent = "Not enough daily data for a complete four-week historical point yet.";
    componentMeta.textContent = "Not enough daily data for a complete four-week historical point yet.";
    return;
  }

  const labels = points.map((point) => historyDateLabel(point.endDate));
  const first = historyDateLabel(points[0].endDate);
  const last = historyDateLabel(points[points.length - 1].endDate);
  agencyMeta.textContent = `${points.length} weekly points · ${first}–${last}`;
  routeMeta.textContent = `Top 10 by scheduled trips in the current four-week window · ${first}–${last}`;
  componentMeta.textContent = `${points.length} weekly points · ${first}–${last} · each point is a trailing four-week window`;
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

  const serviceDelivered = points.map((point) => point.agencyComponents.stopSDPct);
  const onTime = points.map((point) => point.agencyComponents.onTimePct);
  const spacingScores = points.map((point) => point.agencyComponents.bunchingScore);
  renderComponentHistoryChart(
    "service-delivered-history",
    labels,
    serviceDelivered,
    "On Time Service Delivered",
    "#5f3dc4",
    "rgba(95, 61, 196, 0.12)",
    "Percent",
    (ctx) => `On Time Service Delivered: ${fmt(ctx.parsed.y)}%`
  );
  renderComponentHistoryChart(
    "on-time-history",
    labels,
    onTime,
    "On time within 3 minutes",
    "#2b8a3e",
    "rgba(43, 138, 62, 0.12)",
    "Percent",
    (ctx) => `On time within 3 minutes: ${fmt(ctx.parsed.y)}%`
  );
  renderComponentHistoryChart(
    "bunching-score-history",
    labels,
    spacingScores,
    "Bus-spacing sub-score",
    "#e8590c",
    "rgba(232, 89, 12, 0.12)",
    "Score / 100",
    (ctx) => {
      const cv = points[ctx.dataIndex].agencyComponents.headwayCV;
      return `Bus-spacing score: ${fmt(ctx.parsed.y)} / 100 · headway CV ${fmt(cv, 2)}`;
    }
  );
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
    const agency = aggregateAgency(agencyRoutes);
    const bunchingEligible = agency.bunching_grade_eligible && agency.headway_cv != null;
    return {
      endDate: dates[endpointIndex],
      agencyScore: agency.score,
      agencyComponents: {
        stopSDPct: agency.stop_sd_pct,
        onTimePct: agency.on_time_pct,
        bunchingScore: bunchingEligible ? bunchingScore(agency.headway_cv) : null,
        headwayCV: bunchingEligible ? agency.headway_cv : null,
      },
      routeScores: new Map(routes.map((route) => [route.route_id, route.score])),
    };
  }).filter((point) => point && point.agencyScore != null).reverse();

  renderGradeHistory(points, topRoutes);
}

function renderBunchingMethodology() {
  const el = document.getElementById("score-methodology");
  el.innerHTML = `
    <p class="muted defn">The composite score blends four rider-performance components over the trailing four-week window:</p>
    <p class="muted defn"><strong>On Time Service Delivered — 40%.</strong> Percent of scheduled intermediate passenger-pickup stops reached between <strong>1 min early</strong> and <strong>7 min late</strong> (<code>−60&nbsp;s&nbsp;≤&nbsp;delay&nbsp;≤&nbsp;420&nbsp;s</code>).</p>
    <p class="muted defn"><strong>On time — 10%.</strong> Percent of observed arrivals within <code>0&nbsp;≤&nbsp;delay&nbsp;≤&nbsp;3&nbsp;min</code> of schedule.</p>
    <p class="muted defn"><strong>Speed vs. ideal — 15%.</strong> Average per-leg bus speed as a fraction of a <strong>13 mph</strong> ideal, capped at 100%.</p>
    <p class="muted defn"><strong>Bus spacing and bunching — ${BUNCHING_GRADE_WEIGHT}%.</strong> Headway CV means <strong>coefficient of variation</strong>: <code>standard deviation ÷ mean</code>, calculated within comparable route, direction, stop, and hour cells and then weighted by contributing headways. A CV of 0 means perfectly even spacing; higher values mean increasingly uneven gaps. Only scheduled windows with headways of <strong>${BUNCHING_MAX_FREQUENCY_MIN} minutes or less</strong> are eligible; realized gaps below 30 seconds or above 90 minutes are excluded as duplicate and overnight/outlier guards. The sub-score is <code>100 × (1 − CV)</code>, clamped to 0–100. Bunched-arrival values are already percentages: <strong>0.1 means 0.1%</strong> (about one in 1,000 comparable headways), not 10%. Method inspired by <a href="https://sal-khan.com/bus-bunching.html" target="_blank" rel="noopener">Salman Khan’s bus-bunching project</a> and adapted to AC Transit arrivals and schedules.</p>
    <p class="muted defn"><strong>Eligibility and missing-data behavior.</strong> Bunching is used only when the complete evaluation window has at least ${BUNCHING_MIN_CV_WEIGHT} comparable headways covering at least ${fmt(100 * BUNCHING_MIN_CV_COVERAGE, 0)}% of measured eligible headways. Routes without any scheduled headways of ${BUNCHING_MAX_FREQUENCY_MIN} minutes or less are considered too low-frequency and are ungraded on bunching. Missing, partial, and ineligible bunching data drops the ${BUNCHING_GRADE_WEIGHT}% component, renormalizes the other available components, and produces a grade warning.</p>
  `;
}

function tailFrequencyText(pct) {
  if (pct == null) return "—";
  if (pct <= 0) return "None measured";
  return `About 1 in ${intFmt(Math.max(1, Math.round(100 / pct)))}`;
}

function renderBunchingSection(routes, agency) {
  const section = document.getElementById("bunching-section");
  if (!section) return;
  section.hidden = false;

  renderCards("#bunching-cards", [
    {
      label: "Headway CV",
      val: fmt(agency.headway_cv, 2),
      grade: agency.headway_cv == null ? null : gradeColor(1 - agency.headway_cv),
    },
    {
      label: "Bunched arrivals",
      val: agency.bunched_headway_pct == null ? "—" : `${fmt(agency.bunched_headway_pct)}%`,
      grade: agency.bunched_headway_pct == null ? null : gradeColor(1 - agency.bunched_headway_pct / 25),
    },
    {
      label: "Long gaps",
      val: agency.long_gap_pct == null ? "—" : `${fmt(agency.long_gap_pct)}%`,
      grade: agency.long_gap_pct == null ? null : gradeColor(1 - agency.long_gap_pct / 30),
    },
    {
      label: "Headways measured",
      val: intFmt(agency.headway_n),
    },
  ]);

  const comparisonN = Number(agency.bunching && agency.bunching.comparison_n) || 0;
  const bunchedN = Number(agency.bunching && agency.bunching.bunched_headway_n) || 0;
  const longGapN = Number(agency.bunching && agency.bunching.long_gap_n) || 0;
  const eitherTailPct = comparisonN > 0 ? 100 * (bunchedN + longGapN) / comparisonN : null;
  const tailItems = [
    {
      valueID: "bunching-tail-short",
      detailID: "bunching-tail-short-detail",
      pct: agency.bunched_headway_pct,
      detail: "comparable arrivals; gap below half the scheduled gap",
    },
    {
      valueID: "bunching-tail-long",
      detailID: "bunching-tail-long-detail",
      pct: agency.long_gap_pct,
      detail: "comparable arrivals; gap above 1.5 times the scheduled gap",
    },
    {
      valueID: "bunching-tail-either",
      detailID: "bunching-tail-either-detail",
      pct: eitherTailPct,
      detail: "comparable arrivals fell into either measured tail",
    },
  ];
  for (const item of tailItems) {
    document.getElementById(item.valueID).textContent = tailFrequencyText(item.pct);
    document.getElementById(item.detailID).textContent = item.pct == null
      ? item.detail
      : `${fmt(item.pct)}% of ${intFmt(comparisonN)} ${item.detail}`;
  }

  document.getElementById("bunching-even-wait").textContent = agency.even_spacing_wait_min == null
    ? "—"
    : `${fmt(agency.even_spacing_wait_min)} min`;
  document.getElementById("bunching-observed-wait").textContent = agency.expected_wait_min == null
    ? "—"
    : `${fmt(agency.expected_wait_min)} min`;
  const spacingPenaltyPct = agency.spacing_penalty_min != null && agency.even_spacing_wait_min > 0
    ? 100 * agency.spacing_penalty_min / agency.even_spacing_wait_min
    : null;
  document.getElementById("bunching-wait-tax").textContent = spacingPenaltyPct == null
    ? "—"
    : `+${fmt(spacingPenaltyPct)}%`;
  document.getElementById("bunching-wait-tax-detail").textContent = spacingPenaltyPct == null
    ? "increase over the evenly spaced average wait"
    : `${fmt(agency.spacing_penalty_min)} min more than the ${fmt(agency.even_spacing_wait_min)}-min evenly spaced average`;
  document.getElementById("bunching-scheduled-wait").textContent = agency.scheduled_expected_wait_min == null
    ? "—"
    : `${fmt(agency.scheduled_expected_wait_min)} min`;

  const ordered = [...routes].filter((route) => route.headway_cv != null)
    .sort((a, b) => b.headway_cv - a.headway_cv);
  const chartRoutes = [...ordered]
    .sort((a, b) => b.scheduled_runs - a.scheduled_runs)
    .slice(0, 20)
    .sort((a, b) => a.headway_cv - b.headway_cv);
  document.querySelector(".bunching-cv-canvas").style.height = `${Math.max(330, chartRoutes.length * 27)}px`;
  const benchmarkPlugin = {
    id: "cvBenchmark",
    afterDraw(chart) {
      const x = chart.scales.x.getPixelForValue(0.3);
      const { top, bottom } = chart.chartArea;
      chart.ctx.save();
      chart.ctx.strokeStyle = "#2f8f46";
      chart.ctx.lineWidth = 2;
      chart.ctx.setLineDash([5, 4]);
      chart.ctx.beginPath();
      chart.ctx.moveTo(x, top);
      chart.ctx.lineTo(x, bottom);
      chart.ctx.stroke();
      chart.ctx.restore();
    },
  };
  const progressStatus = document.getElementById("bunching-progress-highlight");
  const defaultProgressStatus = "Hover over or select a route bar at left to highlight the same route here.";
  const mutedProgressColor = "rgba(100, 116, 139, 0.18)";
  let progressChart = null;
  let lockedProgressRouteID = null;
  const setProgressHighlight = (routeID) => {
    if (!progressChart) return;
    const route = chartRoutes.find((candidate) => candidate.route_id === routeID);
    for (const dataset of progressChart.data.datasets) {
      const active = dataset.routeID === routeID;
      dataset.borderColor = active ? dataset.highlightColor : mutedProgressColor;
      dataset.backgroundColor = active ? dataset.highlightColor : mutedProgressColor;
      dataset.borderWidth = active ? 3.5 : 1.25;
      dataset.pointRadius = active ? 4 : 0;
      dataset.pointHoverRadius = active ? 6 : 3;
      dataset.order = active ? 0 : 1;
    }
    if (!route) {
      progressStatus.textContent = defaultProgressStatus;
    } else if (route.progress_cv.filter((value) => value != null).length < 2) {
      progressStatus.textContent = `Route ${route.route_id} does not have enough along-run progress points for a line.`;
    } else {
      progressStatus.textContent = `Route ${route.route_id} highlighted · overall headway CV ${fmt(route.headway_cv, 2)}.`;
    }
    progressChart.update("none");
  };
  new Chart(document.getElementById("bunching-route-chart"), {
    type: "bar",
    data: {
      labels: chartRoutes.map((route) => `Route ${route.route_id}`),
      datasets: [{
        label: "Headway CV",
        data: chartRoutes.map((route) => route.headway_cv),
        backgroundColor: chartRoutes.map((route) => gradeColor(1 - route.headway_cv).bg),
        borderRadius: 4,
        borderSkipped: false,
      }],
    },
    plugins: [benchmarkPlugin],
    options: {
      indexAxis: "y",
      maintainAspectRatio: false,
      onHover: (event, elements) => {
        if (event.native && event.native.target) {
          event.native.target.style.cursor = elements.length ? "pointer" : "default";
        }
        const hoveredRouteID = elements.length
          ? chartRoutes[elements[0].index].route_id
          : lockedProgressRouteID;
        setProgressHighlight(hoveredRouteID);
      },
      onClick: (_event, elements) => {
        const selectedRouteID = elements.length
          ? chartRoutes[elements[0].index].route_id
          : null;
        lockedProgressRouteID = selectedRouteID === lockedProgressRouteID
          ? null
          : selectedRouteID;
        setProgressHighlight(lockedProgressRouteID);
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const route = chartRoutes[ctx.dataIndex];
              return `CV ${fmt(route.headway_cv, 2)} · ${fmt(route.bunched_headway_pct)}% bunched · +${fmt(route.spacing_penalty_min)} min wait`;
            },
          },
        },
      },
      scales: {
        x: {
          beginAtZero: true,
          max: 0.8,
          title: { display: true, text: "headway coefficient of variation" },
          ticks: { callback: (value) => Number(value).toFixed(1) },
        },
        y: { grid: { display: false } },
      },
    },
  });

  const progressValues = chartRoutes.flatMap((route) =>
    route.progress_cv.filter((value) => value != null)
  );
  const progressYMax = Math.max(
    0.8,
    Math.ceil((Math.max(...progressValues, 0) + 0.1) * 10) / 10
  );
  progressChart = new Chart(document.getElementById("bunching-progress-chart"), {
    type: "line",
    data: {
      labels: BUNCHING_PROGRESS_POINTS.map((point) => `${point}%`),
      datasets: chartRoutes.map((route) => ({
        label: `Route ${route.route_id}`,
        routeID: route.route_id,
        highlightColor: gradeColor(1 - route.headway_cv).bg,
        data: route.progress_cv,
        borderColor: mutedProgressColor,
        backgroundColor: mutedProgressColor,
        borderWidth: 1.25,
        pointRadius: 0,
        pointHoverRadius: 3,
        pointHitRadius: 8,
        tension: 0.22,
        spanGaps: false,
        order: 1,
      })),
    },
    options: {
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: CV ${fmt(ctx.parsed.y, 2)} at ${ctx.label} progress`,
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "progress along route" }, grid: { display: false } },
        y: { beginAtZero: true, max: progressYMax, title: { display: true, text: "headway CV" } },
      },
    },
  });

  document.querySelector("#bunching-table tbody").innerHTML = ordered.map((route) => {
    const progress = route.progress_cv || [];
    const availableProgress = progress.filter((value) => value != null);
    const first = availableProgress.length ? availableProgress[0] : null;
    const last = availableProgress.length ? availableProgress[availableProgress.length - 1] : null;
    const cvGrade = gradeColor(1 - route.headway_cv);
    return `
      <tr>
        <td>${routeBadge(route)}</td>
        <td><span class="metric-pill" style="background:${cvGrade.bg};color:${cvGrade.fg}">${fmt(route.headway_cv, 2)}</span></td>
        <td>${fmt(route.bunched_headway_pct)}%</td>
        <td>${fmt(route.long_gap_pct)}%</td>
        <td>+${fmt(route.spacing_penalty_min)} min</td>
        <td>${first == null ? "—" : `${fmt(first, 2)} → ${fmt(last, 2)}`}</td>
        <td>${intFmt(route.headway_n)}</td>
      </tr>`;
  }).join("");
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

  const agency = aggregateAgency(includedRoutes);
  renderAgencyHero(agency);
  renderBunchingSection(routes, agency);
  renderBunchingMethodology();

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
        const routeBunchingWarning = bunchingGradeWarning(r.bunching);
        const gradeWarningFlag = routeBunchingWarning
          ? `<span class="grade-data-flag" title="Bunching is excluded from this grade; expand the route for details" aria-label="Bunching excluded from this grade">!</span>`
          : "";
        const bunchingDetail = `${routeBunchingWarning ? `<div class="route-grade-warning"><dt>Grade warning</dt><dd><strong>Bunching excluded.</strong> ${routeBunchingWarning}</dd></div>` : ""}
            <div><dt>Headway CV</dt><dd>${r.headway_cv == null
              ? "—"
              : `${fmt(r.headway_cv, 2)} (${r.bunching_grade_eligible
                ? `bunching sub-score ${fmt(bunchingScore(r.headway_cv), 0)} / 100`
                : "not used in this grade"})`}</dd></div>
            <div><dt>Bunched arrivals</dt><dd>${r.bunched_headway_pct == null ? "—" : `${fmt(r.bunched_headway_pct)}%`}</dd></div>
            <div><dt>Long gaps</dt><dd>${r.long_gap_pct == null ? "—" : `${fmt(r.long_gap_pct)}%`}</dd></div>
            <div><dt>Observed vs evenly spaced wait</dt><dd>${r.expected_wait_min == null
              ? "—"
              : `${fmt(r.expected_wait_min)} min observed vs ${fmt(r.even_spacing_wait_min)} min at the same realized frequency`}</dd></div>
            <div><dt>Published schedule wait</dt><dd>${r.scheduled_expected_wait_min == null ? "—" : `${fmt(r.scheduled_expected_wait_min)} min for eligible scheduled headways`}</dd></div>
            <div><dt>Spacing penalty</dt><dd>${r.spacing_penalty_min == null ? "—" : `+${fmt(r.spacing_penalty_min)} min per random-arrival rider`}</dd></div>
            <div><dt>Headways measured</dt><dd>${intFmt(r.headway_n)}</dd></div>`;
        return `
      <tr class="route-row ${isOpen ? "is-open" : ""}" data-rid="${r.route_id}">
        <td><span class="grade-badge" style="background:${g.bg};color:${g.fg}" title="composite score ${fmt(r.score)} / 100">${grade}</span>${gradeWarningFlag}</td>
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
            ${bunchingDetail}
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

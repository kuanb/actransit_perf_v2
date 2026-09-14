const MONTHLY_INDEX_URL = `${GCS_BASE}/stats/monthly/_index.json`;
const WEEKLY_INDEX_URL = `${GCS_BASE}/stats/weekly/_index.json`;
const PUBLISHED_KPI_URL = `${GCS_BASE}/stats/published-kpis/latest.json`;
let serviceOperatedChart;
let serviceOperatedVolumeChart;
let otpChart;

function monthLabel(month) {
  const [year, number] = month.split("-").map(Number);
  return new Intl.DateTimeFormat("en-US", { month: "long", year: "numeric", timeZone: "UTC" })
    .format(new Date(Date.UTC(year, number - 1, 1)));
}

function shortDate(date) {
  const [year, month, day] = date.split("-").map(Number);
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", timeZone: "UTC" })
    .format(new Date(Date.UTC(year, month - 1, day)));
}

function pct(value, digits = 1, missingLabel = "Unavailable") {
  return value === null || value === undefined ? missingLabel : `${Number(value).toFixed(digits)}%`;
}

function countRatio(numerator, denominator) {
  return `${intFmt(numerator)} of ${intFmt(denominator)}`;
}

function publishedMap(values) {
  return new Map((values || []).map((value) => [value.month, value]));
}

function chartDataset(label, data, color, dash = [], extra = {}) {
  return {
    label,
    data,
    borderColor: color,
    backgroundColor: color,
    borderWidth: 2.5,
    borderDash: dash,
    tension: 0.2,
    pointRadius: 4,
    pointHoverRadius: 6,
    spanGaps: false,
    ...extra,
  };
}

function rangeBandDatasets(label, minimum, maximum, color, fillColor) {
  return [
    {
      label: `${label} minimum`,
      data: minimum,
      borderWidth: 0,
      pointRadius: 0,
      fill: false,
      order: 10,
      hideLegend: true,
      hideTooltip: true,
    },
    {
      label,
      data: maximum,
      borderColor: color,
      backgroundColor: fillColor,
      borderWidth: 0,
      pointRadius: 0,
      pointStyle: "rect",
      fill: "-1",
      order: 10,
      rangeBand: true,
      rangeMinimum: minimum,
    },
  ];
}

function scopedPercentRange(values) {
  const valid = values.filter((value) => value !== null && value !== undefined && Number.isFinite(Number(value))).map(Number);
  if (!valid.length) return { min: 0, max: 100 };

  const observedMin = Math.min(...valid);
  const observedMax = Math.max(...valid);
  const padding = Math.max(2, (observedMax - observedMin) * 0.25);
  let min = Math.max(0, Math.floor((observedMin - padding) / 5) * 5);
  let max = Math.min(100, Math.ceil((observedMax + padding) / 5) * 5);
  if (max - min < 10) {
    if (min === 0) max = Math.min(100, min + 10);
    else if (max === 100) min = Math.max(0, max - 10);
    else {
      min = Math.max(0, min - 5);
      max = Math.min(100, max + 5);
    }
  }
  return { min, max };
}

function comparisonChartOptions(values) {
  const range = scopedPercentRange(values);
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
      x: { grid: { display: false } },
      y: {
        min: range.min,
        max: range.max,
        ticks: { callback: (value) => `${value}%` },
        title: { display: true, text: "Percent" },
      },
    },
    plugins: {
      legend: {
        position: "bottom",
        labels: {
          usePointStyle: true,
          boxWidth: 8,
          padding: 16,
          filter: (item, data) => !data.datasets[item.datasetIndex].hideLegend,
        },
      },
      tooltip: {
        filter: (item) => !item.dataset.hideTooltip,
        callbacks: {
          label: (ctx) => {
            if (ctx.dataset.rangeBand) {
              const minimum = ctx.dataset.rangeMinimum[ctx.dataIndex];
              return `${ctx.dataset.label}: ${Number(minimum).toFixed(1)}%–${Number(ctx.parsed.y).toFixed(1)}%`;
            }
            return `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}%`;
          },
        },
      },
    },
  };
}

function renderServiceOperatedVolumeChart(months, servicePublished, mode = "delivered") {
  const chronological = [...months].sort((a, b) => a.month.localeCompare(b.month));
  const labels = chronological.map((month) => monthLabel(month.month));
  const notDelivered = mode === "not-delivered";
  const tripCount = (service) => notDelivered
    ? service.scheduled_trips - service.operated_trips
    : service.operated_trips;
  const maxWeekSegments = Math.max(0, ...chronological.map((month) => (month.weeks || []).length));
  const weekColors = ["#b9dcf5", "#8fc7ed", "#63b0e2", "#3b95d0", "#1971c2", "#114f89"];
  const weekDatasets = Array.from({ length: maxWeekSegments }, (_, index) => ({
    label: `Our calculation — week segment ${index + 1}`,
    data: chronological.map((month) => {
      const service = month.weeks?.[index]?.agency_kpi?.service_operated;
      return service ? tripCount(service) : 0;
    }),
    backgroundColor: weekColors[index % weekColors.length],
    borderColor: "#fff",
    borderWidth: 0.5,
    stack: "ours",
    periods: chronological.map((month) => month.weeks?.[index] || null),
  }));
  const publishedPcts = chronological.map((month) => servicePublished.get(month.month)?.pct ?? null);
  const publishedEquivalent = chronological.map((month, index) => {
    const scheduled = month.agency_kpi.service_operated.scheduled_trips;
    if (publishedPcts[index] === null) return null;
    const operated = Math.round(scheduled * publishedPcts[index] / 100);
    return notDelivered ? scheduled - operated : operated;
  });
  const publishedDisplayPcts = publishedPcts.map((value) =>
    value === null || !notDelivered ? value : 100 - value
  );
  const publishedDataset = {
    label: `AC Transit published % — equivalent ${notDelivered ? "not delivered" : "delivered"}`,
    data: publishedEquivalent,
    backgroundColor: "#e8590c",
    borderColor: "#c44d08",
    borderWidth: 1,
    stack: "published",
    publishedEquivalent: true,
  };

  if (serviceOperatedVolumeChart) serviceOperatedVolumeChart.destroy();
  serviceOperatedVolumeChart = new Chart(document.getElementById("service-operated-volume-chart"), {
    type: "bar",
    data: { labels, datasets: [...weekDatasets, publishedDataset] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: true },
      scales: {
        x: { stacked: true, grid: { display: false } },
        y: {
          stacked: true,
          beginAtZero: true,
          ticks: {
            callback: (value) => new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 }).format(value),
          },
          title: { display: true, text: notDelivered ? "Trips not delivered" : "Trips delivered" },
        },
      },
      plugins: {
        legend: {
          position: "bottom",
          labels: { usePointStyle: true, boxWidth: 8, padding: 14 },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              if (ctx.dataset.publishedEquivalent) {
                return `${ctx.dataset.label}: ${intFmt(ctx.parsed.y)} trips (${pct(publishedDisplayPcts[ctx.dataIndex], 2)})`;
              }
              const period = ctx.dataset.periods[ctx.dataIndex];
              const dates = period ? ` · ${shortDate(period.period_start)}–${shortDate(period.period_end)}` : "";
              return `${ctx.dataset.label}${dates}: ${intFmt(ctx.parsed.y)} trips`;
            },
            footer: (items) => {
              const month = chronological[items[0].dataIndex];
              const service = month.agency_kpi.service_operated;
              const label = notDelivered ? "not delivered" : "delivered";
              return `Our month: ${intFmt(tripCount(service))} ${label} of ${intFmt(service.scheduled_trips)} planned`;
            },
          },
        },
      },
    },
  });
  document.getElementById("service-operated-volume-chart").setAttribute(
    "aria-label",
    `Monthly Service Operated trip volumes showing trips ${notDelivered ? "not delivered" : "delivered"}`,
  );
}

function weeklyChartPoints(months) {
  return months.flatMap((month) => {
    const weeks = (month.weeks || []).filter((week) => week.status !== "missing");
    return weeks.map((week, index) => ({
      month: month.month,
      week,
      monthEnd: index === weeks.length - 1,
      label: `${shortDate(week.period_start)}–${shortDate(week.period_end)}`,
    }));
  });
}

function renderComparisonCharts(months, servicePublished, otpPublished, weekly = false) {
  const chronological = [...months].sort((a, b) => a.month.localeCompare(b.month));
  const points = weekly ? weeklyChartPoints(chronological) : chronological;
  const labels = weekly ? points.map((point) => point.label) : points.map((month) => monthLabel(month.month));
  const serviceOurs = weekly
    ? points.map((point) => point.week.agency_kpi.service_operated.operated_pct)
    : points.map((month) => month.agency_kpi.service_operated.operated_pct);
  const serviceAgency = weekly
    ? points.map((point) => point.monthEnd ? servicePublished.get(point.month)?.pct ?? null : null)
    : points.map((month) => servicePublished.get(month.month)?.pct ?? null);
  const serviceMinimum = weekly
    ? points.map((point) => point.week.daily_range?.service_operated?.min_pct ?? null)
    : [];
  const serviceMaximum = weekly
    ? points.map((point) => point.week.daily_range?.service_operated?.max_pct ?? null)
    : [];
  const serviceDatasets = weekly
    ? [
        ...rangeBandDatasets("Daily min–max — Service Operated", serviceMinimum, serviceMaximum, "rgba(25, 113, 194, 0.45)", "rgba(25, 113, 194, 0.14)"),
        chartDataset("Our calculation — weekly", serviceOurs, "#1971c2"),
        chartDataset("AC Transit published — monthly", serviceAgency, "#e8590c", [6, 4], { spanGaps: true }),
      ]
    : [
        chartDataset("Our calculation — monthly", serviceOurs, "#1971c2"),
        chartDataset("AC Transit published — monthly", serviceAgency, "#e8590c", [6, 4]),
      ];

  if (serviceOperatedChart) serviceOperatedChart.destroy();
  serviceOperatedChart = new Chart(document.getElementById("service-operated-chart"), {
    type: "line",
    data: { labels, datasets: serviceDatasets },
    options: comparisonChartOptions([...serviceOurs, ...serviceAgency, ...serviceMinimum, ...serviceMaximum]),
  });

  const otpOperated = weekly
    ? points.map((point) => point.week.agency_kpi.on_time_performance.of_operated_pct)
    : points.map((month) => month.agency_kpi.on_time_performance.of_operated_pct);
  const otpScheduled = weekly
    ? points.map((point) => point.week.agency_kpi.on_time_performance.of_scheduled_pct)
    : points.map((month) => month.agency_kpi.on_time_performance.of_scheduled_pct);
  const otpAgency = weekly
    ? points.map((point) => point.monthEnd ? otpPublished.get(point.month)?.pct ?? null : null)
    : points.map((month) => otpPublished.get(month.month)?.pct ?? null);
  const otpOperatedMinimum = weekly
    ? points.map((point) => point.week.daily_range?.otp_of_operated?.min_pct ?? null)
    : [];
  const otpOperatedMaximum = weekly
    ? points.map((point) => point.week.daily_range?.otp_of_operated?.max_pct ?? null)
    : [];
  const otpScheduledMinimum = weekly
    ? points.map((point) => point.week.daily_range?.otp_of_scheduled?.min_pct ?? null)
    : [];
  const otpScheduledMaximum = weekly
    ? points.map((point) => point.week.daily_range?.otp_of_scheduled?.max_pct ?? null)
    : [];
  const otpDatasets = weekly
    ? [
        ...rangeBandDatasets("Daily min–max — operated-trip OTP", otpOperatedMinimum, otpOperatedMaximum, "rgba(25, 113, 194, 0.45)", "rgba(25, 113, 194, 0.12)"),
        ...rangeBandDatasets("Daily min–max — all-scheduled OTP", otpScheduledMinimum, otpScheduledMaximum, "rgba(95, 61, 196, 0.45)", "rgba(95, 61, 196, 0.12)"),
        chartDataset("Our OTP — operated trips — weekly", otpOperated, "#1971c2"),
        chartDataset("Our OTP — all scheduled — weekly", otpScheduled, "#5f3dc4"),
        chartDataset("AC Transit published — monthly", otpAgency, "#e8590c", [6, 4], { spanGaps: true }),
      ]
    : [
        chartDataset("Our OTP — operated trips — monthly", otpOperated, "#1971c2"),
        chartDataset("Our OTP — all scheduled — monthly", otpScheduled, "#5f3dc4"),
        chartDataset("AC Transit published — monthly", otpAgency, "#e8590c", [6, 4]),
      ];

  if (otpChart) otpChart.destroy();
  otpChart = new Chart(document.getElementById("otp-chart"), {
    type: "line",
    data: { labels, datasets: otpDatasets },
    options: comparisonChartOptions([
      ...otpOperated,
      ...otpScheduled,
      ...otpAgency,
      ...otpOperatedMinimum,
      ...otpOperatedMaximum,
      ...otpScheduledMinimum,
      ...otpScheduledMaximum,
    ]),
  });

  const detail = weekly ? "Weekly" : "Monthly";
  document.getElementById("service-operated-chart").setAttribute("aria-label", `${detail} Service Operated comparison`);
  document.getElementById("otp-chart").setAttribute("aria-label", `${detail} On-Time Performance comparison`);
}

function initializeComparisonCharts(months, servicePublished, otpPublished) {
  const toggle = document.getElementById("weekly-chart-toggle");
  renderComparisonCharts(months, servicePublished, otpPublished, toggle.checked);
  renderServiceOperatedVolumeChart(months, servicePublished);
  toggle.addEventListener("change", () => {
    renderComparisonCharts(months, servicePublished, otpPublished, toggle.checked);
  });
  document.querySelectorAll("[data-volume-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll("[data-volume-mode]").forEach((option) => {
        const active = option === button;
        option.classList.toggle("is-active", active);
        option.setAttribute("aria-pressed", String(active));
      });
      renderServiceOperatedVolumeChart(months, servicePublished, button.dataset.volumeMode);
    });
  });
}

function sampleKPI(scheduledTrips, operatedTrips, partialTrips, onTime, operatedTimepoints, scheduledTimepoints) {
  const ratio = (n, d) => d ? Math.round(1000 * n / d) / 10 : null;
  return {
    methodology_version: 2,
    service_operated: {
      scheduled_trips: scheduledTrips,
      operated_trips: operatedTrips,
      partial_trips: partialTrips,
      operated_pct: ratio(operatedTrips, scheduledTrips),
      partial_of_operated_pct: ratio(partialTrips, operatedTrips),
    },
    on_time_performance: {
      on_time_timepoints: onTime,
      operated_timepoints: operatedTimepoints,
      scheduled_timepoints: scheduledTimepoints,
      of_operated_pct: ratio(onTime, operatedTimepoints),
      of_scheduled_pct: ratio(onTime, scheduledTimepoints),
    },
  };
}

function scaledKPI(stats, factor) {
  const service = stats.service_operated;
  const otp = stats.on_time_performance;
  return sampleKPI(
    Math.round(service.scheduled_trips * factor),
    Math.round(service.operated_trips * factor),
    Math.round(service.partial_trips * factor),
    Math.round(otp.on_time_timepoints * factor),
    Math.round(otp.operated_timepoints * factor),
    Math.round(otp.scheduled_timepoints * factor),
  );
}

function sampleBreakdown(stats) {
  const periods = (factor) => ({
    owl: scaledKPI(stats, factor * 0.05),
    morning: scaledKPI(stats, factor * 0.24),
    midday: scaledKPI(stats, factor * 0.31),
    afternoon: scaledKPI(stats, factor * 0.25),
    evening: scaledKPI(stats, factor * 0.15),
  });
  return { weekday: periods(0.72), weekend: periods(0.28) };
}

function sampleRoutes(scale = 1) {
  const values = [
    ["1T", "E15525", "FFFFFF", [820, 702, 35, 1930, 2790, 3240]],
    ["18", "2B589C", "FFFFFF", [1020, 954, 29, 2480, 3510, 3890]],
    ["40", "5C2D91", "FFFFFF", [910, 796, 42, 2010, 3060, 3490]],
    ["51A", "008C95", "FFFFFF", [980, 885, 61, 2160, 3310, 3740]],
    ["72", "D97706", "FFFFFF", [760, 718, 20, 1880, 2740, 3010]],
  ];
  return values.map(([routeID, color, textColor, counts]) => {
    const stats = sampleKPI(...counts.map((value) => Math.round(value * scale)));
    return {
      route_id: routeID,
      color,
      text_color: textColor,
      agency_kpi: stats,
      agency_kpi_by_day_type: sampleBreakdown(stats),
    };
  });
}

function sampleRange(value, spread) {
  return {
    min_pct: Math.max(0, Math.round((value - spread) * 10) / 10),
    max_pct: Math.min(100, Math.round((value + spread) * 10) / 10),
  };
}

function sampleMonth(month, ranges, totals) {
  const dayCounts = ranges.map(([start, end]) =>
    Math.round((Date.parse(`${end}T00:00:00Z`) - Date.parse(`${start}T00:00:00Z`)) / 86400000) + 1
  );
  const totalDays = dayCounts.reduce((sum, days) => sum + days, 0);
  const allocate = (total) => {
    let assigned = 0;
    return dayCounts.map((days, index) => {
      const value = index === dayCounts.length - 1 ? total - assigned : Math.round(total * days / totalDays);
      assigned += value;
      return value;
    });
  };
  const allocated = totals.map(allocate);
  return {
    month,
    status: "complete",
    agency_kpi: sampleKPI(...totals),
    routes: sampleRoutes(totalDays / 7),
    weeks: ranges.map(([period_start, period_end], index) => {
      const agencyKPI = sampleKPI(...allocated.map((values) => values[index]));
      return {
        period_start,
        period_end,
        status: "complete",
        agency_kpi: agencyKPI,
        daily_range: {
          service_operated: sampleRange(agencyKPI.service_operated.operated_pct, 1.2 + (index % 2) * 0.4),
          otp_of_operated: sampleRange(agencyKPI.on_time_performance.of_operated_pct, 2.8 + (index % 2) * 0.6),
          otp_of_scheduled: sampleRange(agencyKPI.on_time_performance.of_scheduled_pct, 3.2 + (index % 2) * 0.6),
        },
      };
    }),
  };
}

function localPreviewData() {
  const months = [
    sampleMonth("2026-07", [
      ["2026-07-01", "2026-07-04"], ["2026-07-05", "2026-07-11"], ["2026-07-12", "2026-07-18"],
      ["2026-07-19", "2026-07-25"], ["2026-07-26", "2026-07-31"],
    ], [166000, 154000, 4200, 443000, 585000, 630000]),
    sampleMonth("2026-06", [
      ["2026-06-01", "2026-06-06"], ["2026-06-07", "2026-06-13"], ["2026-06-14", "2026-06-20"],
      ["2026-06-21", "2026-06-27"], ["2026-06-28", "2026-06-30"],
    ], [158830, 153090, 3130, 444900, 602100, 622200]),
    sampleMonth("2026-05", [
      ["2026-05-01", "2026-05-02"], ["2026-05-03", "2026-05-09"], ["2026-05-10", "2026-05-16"],
      ["2026-05-17", "2026-05-23"], ["2026-05-24", "2026-05-30"], ["2026-05-31", "2026-05-31"],
    ], [165000, 160500, 2800, 451000, 596500, 618000]),
  ];
  const weekly = [
    { week_start: "2026-08-30", week_end: "2026-09-05", route_daily_service_delivered: sampleRoutes(1) },
    { week_start: "2026-08-23", week_end: "2026-08-29", route_daily_service_delivered: sampleRoutes(0.97) },
  ];
  return {
    months,
    weekly,
    published: {
      fetched_at: "2026-08-29T17:00:00Z",
      service_operated: [
        { month: "2026-05", pct: 95.5 }, { month: "2026-06", pct: 94.24 }, { month: "2026-07", pct: 89.93 },
      ],
      on_time_performance: [{ month: "2026-05", pct: 73.25 }, { month: "2026-06", pct: 74.72 }],
    },
  };
}

const KPI_TIME_PERIODS = [
  { id: "owl", label: "Owl", hours: "12–6am" },
  { id: "morning", label: "Morning", hours: "6–10am" },
  { id: "midday", label: "Midday", hours: "10am–3pm" },
  { id: "afternoon", label: "Afternoon", hours: "3–7pm" },
  { id: "evening", label: "Evening", hours: "7pm–12am" },
];
const KPI_DAY_TYPES = [
  { id: "weekday", label: "Weekday" },
  { id: "weekend", label: "Weekend" },
];
const routeCollator = new Intl.Collator("en-US", { numeric: true, sensitivity: "base" });
const routeTableState = {
  frequency: "weekly",
  indexes: { weekly: [], monthly: [] },
  selected: { weekly: "", monthly: "" },
  monthlyData: new Map(),
  weeklyData: new Map(),
  data: null,
  filter: "",
  sortKey: "service_operated",
  sortDirection: "asc",
  openRoutes: new Set(),
  loadToken: 0,
};

function aggregateKPIValues(values) {
  const totals = [0, 0, 0, 0, 0, 0];
  for (const stats of values.filter(Boolean)) {
    const service = stats.service_operated || {};
    const otp = stats.on_time_performance || {};
    totals[0] += Number(service.scheduled_trips) || 0;
    totals[1] += Number(service.operated_trips) || 0;
    totals[2] += Number(service.partial_trips) || 0;
    totals[3] += Number(otp.on_time_timepoints) || 0;
    totals[4] += Number(otp.operated_timepoints) || 0;
    totals[5] += Number(otp.scheduled_timepoints) || 0;
  }
  return sampleKPI(...totals);
}

function serviceOperatedDetailCell(stats) {
  const metric = stats && stats.service_operated;
  if (!metric || !metric.scheduled_trips) {
    return `<td class="kpi-period-empty">—<small>No planned trips</small></td>`;
  }
  return `<td>
    <strong>${pct(metric.operated_pct, 1, "—")}</strong>
    <small>${countRatio(metric.operated_trips, metric.scheduled_trips)} trips</small>
    <small>${intFmt(metric.partial_trips)} partial · ${pct(metric.partial_of_operated_pct, 1, "—")} of operated</small>
  </td>`;
}

function otpDetailCell(stats) {
  const metric = stats && stats.on_time_performance;
  if (!metric || !metric.scheduled_timepoints) {
    return `<td class="kpi-period-empty">—<small>No eligible timepoints</small></td>`;
  }
  return `<td>
    <strong>${pct(metric.of_operated_pct, 1, "—")}</strong>
    <small>${countRatio(metric.on_time_timepoints, metric.operated_timepoints)} operated-service timepoints</small>
    <small>${pct(metric.of_scheduled_pct, 1, "—")} of all scheduled · n=${intFmt(metric.scheduled_timepoints)}</small>
  </td>`;
}

function timeBreakdownTable(route, metricName) {
  const breakdown = route.agency_kpi_by_day_type || {};
  const renderCell = metricName === "service_operated" ? serviceOperatedDetailCell : otpDetailCell;
  const rows = KPI_DAY_TYPES.map((dayType) => {
    const periods = breakdown[dayType.id] || {};
    const allDay = aggregateKPIValues(Object.values(periods));
    return `<tr>
      <th scope="row">${dayType.label}</th>
      ${KPI_TIME_PERIODS.map((period) => renderCell(periods[period.id])).join("")}
      ${renderCell(allDay)}
    </tr>`;
  }).join("");
  return `<div class="table-wrap kpi-period-table-wrap">
    <table class="kpi-period-table">
      <thead><tr>
        <th>Day type</th>
        ${KPI_TIME_PERIODS.map((period) => `<th>${period.label}<small>${period.hours}</small></th>`).join("")}
        <th>All day</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function routeDetailHTML(route) {
  const service = route.agency_kpi.service_operated;
  const otp = route.agency_kpi.on_time_performance;
  const hasBreakdown = route.agency_kpi_by_day_type && Object.keys(route.agency_kpi_by_day_type).length;
  const breakdownHTML = hasBreakdown
    ? `<div class="kpi-route-metric-detail">
        <h3>Service Operated by time of day</h3>
        <p class="muted">Trips are grouped by their scheduled first departure. Partial trips count as operated and are reported separately.</p>
        ${timeBreakdownTable(route, "service_operated")}
      </div>
      <div class="kpi-route-metric-detail">
        <h3>On-Time Performance by time of day</h3>
        <p class="muted">The prominent value uses eligible timepoints on operated trips. Each cell also shows the all-scheduled result.</p>
        ${timeBreakdownTable(route, "on_time_performance")}
      </div>`
    : `<p class="warning kpi-breakdown-warning">Time-of-day detail has not been generated for this period yet.</p>`;
  return `<div class="kpi-route-detail-inner">
    <div class="kpi-route-count-summary">
      <div><span>Service Operated</span><strong>${countRatio(service.operated_trips, service.scheduled_trips)} planned trips</strong></div>
      <div><span>Partial operated trips</span><strong>${intFmt(service.partial_trips)} · ${pct(service.partial_of_operated_pct, 1, "—")}</strong></div>
      <div><span>On-time timepoints</span><strong>${countRatio(otp.on_time_timepoints, otp.operated_timepoints)} on operated trips</strong></div>
      <div><span>All scheduled OTP</span><strong>${pct(otp.of_scheduled_pct, 1, "—")} · n=${intFmt(otp.scheduled_timepoints)}</strong></div>
    </div>
    ${breakdownHTML}
  </div>`;
}

function routesForSelectedPeriod() {
  if (!routeTableState.data) return [];
  if (routeTableState.frequency === "weekly") {
    return routeTableState.data.route_daily_service_delivered || [];
  }
  return routeTableState.data.routes || [];
}

function routeSortValue(route, key) {
  if (key === "route_id") return route.route_id || "";
  if (key === "service_operated") return route.agency_kpi?.service_operated?.operated_pct;
  return route.agency_kpi?.on_time_performance?.of_operated_pct;
}

function sortedFilteredRoutes() {
  const query = routeTableState.filter.toLowerCase().trim();
  const routes = routesForSelectedPeriod().filter((route) =>
    !query || String(route.route_id || "").toLowerCase().includes(query)
  );
  routes.sort((a, b) => {
    const av = routeSortValue(a, routeTableState.sortKey);
    const bv = routeSortValue(b, routeTableState.sortKey);
    if (av === null || av === undefined) return bv === null || bv === undefined ? 0 : 1;
    if (bv === null || bv === undefined) return -1;
    const compared = routeTableState.sortKey === "route_id"
      ? routeCollator.compare(String(av), String(bv))
      : Number(av) - Number(bv);
    return routeTableState.sortDirection === "asc" ? compared : -compared;
  });
  return routes;
}

function renderRouteTable() {
  const tbody = document.querySelector("#kpi-route-table tbody");
  const routes = sortedFilteredRoutes();
  if (!routes.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">No routes match this filter.</td></tr>`;
  } else {
    tbody.innerHTML = routes.map((route) => {
      const routeID = String(route.route_id);
      const open = routeTableState.openRoutes.has(routeID);
      const servicePct = route.agency_kpi?.service_operated?.operated_pct;
      const otpPct = route.agency_kpi?.on_time_performance?.of_operated_pct;
      return `<tr class="kpi-route-row ${open ? "is-open" : ""}" data-route-id="${routeID}" tabindex="0" role="button" aria-expanded="${open}">
          <td>${routeBadge(route)}${limitedRouteTag(route)}</td>
          <td class="kpi-overall-value">${pct(servicePct, 1, "—")}</td>
          <td class="kpi-overall-value">${pct(otpPct, 1, "—")}</td>
          <td class="expand-cell" aria-hidden="true">${open ? "▾" : "▸"}</td>
        </tr>
        <tr class="kpi-route-detail" ${open ? "" : "hidden"}>
          <td colspan="4">${routeDetailHTML(route)}</td>
        </tr>`;
    }).join("");
  }
  const total = routesForSelectedPeriod().length;
  document.getElementById("kpi-route-count").textContent = routeTableState.filter
    ? `${routes.length} of ${total} routes`
    : `${total} routes`;
  document.querySelectorAll("#kpi-route-table th[data-sort]").forEach((th) => {
    th.classList.toggle("sorted-asc", th.dataset.sort === routeTableState.sortKey && routeTableState.sortDirection === "asc");
    th.classList.toggle("sorted-desc", th.dataset.sort === routeTableState.sortKey && routeTableState.sortDirection === "desc");
  });
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function weekLabel(weekEnd) {
  const end = new Date(`${weekEnd}T00:00:00Z`);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - 6);
  return `${shortDate(isoDate(start))}–${shortDate(weekEnd)}, ${end.getUTCFullYear()}`;
}

function selectedPeriodLabel() {
  const data = routeTableState.data;
  if (!data) return "";
  return routeTableState.frequency === "weekly"
    ? `Week ${shortDate(data.week_start)}–${shortDate(data.week_end)}, ${data.week_end.slice(0, 4)} · Sunday–Saturday`
    : `${monthLabel(data.month)} · calendar month`;
}

function updateRouteTableURL() {
  const url = new URL(window.location.href);
  url.searchParams.set("view", routeTableState.frequency);
  if (routeTableState.frequency === "weekly") {
    url.searchParams.set("week_end", routeTableState.selected.weekly);
    url.searchParams.delete("month");
  } else {
    url.searchParams.set("month", routeTableState.selected.monthly);
    url.searchParams.delete("week_end");
  }
  window.history.replaceState({}, "", url);
}

function renderRoutePeriodControls() {
  const frequency = routeTableState.frequency;
  const keys = routeTableState.indexes[frequency];
  const selected = routeTableState.selected[frequency];
  const selectedIndex = keys.indexOf(selected);
  const select = document.getElementById("kpi-period-select");
  select.innerHTML = keys.map((key) => {
    const label = frequency === "weekly" ? weekLabel(key) : monthLabel(key);
    return `<option value="${key}" ${key === selected ? "selected" : ""}>${label}</option>`;
  }).join("");
  document.getElementById("kpi-period-older").disabled = selectedIndex < 0 || selectedIndex >= keys.length - 1;
  document.getElementById("kpi-period-newer").disabled = selectedIndex <= 0;
  for (const option of ["weekly", "monthly"]) {
    const button = document.getElementById(`kpi-view-${option}`);
    const active = option === frequency;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", String(active));
  }
}

async function loadSelectedRoutePeriod() {
  const frequency = routeTableState.frequency;
  const key = routeTableState.selected[frequency];
  const token = ++routeTableState.loadToken;
  routeTableState.openRoutes.clear();
  routeTableState.data = null;
  renderRoutePeriodControls();
  document.getElementById("kpi-route-period").textContent = "Loading route KPI data…";
  const cache = frequency === "weekly" ? routeTableState.weeklyData : routeTableState.monthlyData;
  try {
    let data = cache.get(key);
    if (!data && frequency === "weekly") {
      data = await fetchJSON(`${GCS_BASE}/stats/weekly/${key}.json`);
      cache.set(key, data);
    }
    if (!data) throw new Error("period data is unavailable");
    if (token !== routeTableState.loadToken) return;
    routeTableState.data = data;
    document.getElementById("kpi-route-period").textContent = selectedPeriodLabel();
    renderRouteTable();
    updateRouteTableURL();
  } catch (error) {
    if (token !== routeTableState.loadToken) return;
    document.getElementById("kpi-route-period").textContent = `Couldn't load this period: ${error.message}.`;
    document.querySelector("#kpi-route-table tbody").innerHTML =
      `<tr><td colspan="4" class="warning">Route KPI data is unavailable for this period.</td></tr>`;
  }
}

function changeSelectedPeriod(offset) {
  const frequency = routeTableState.frequency;
  const keys = routeTableState.indexes[frequency];
  const current = keys.indexOf(routeTableState.selected[frequency]);
  const next = current + offset;
  if (next < 0 || next >= keys.length) return;
  routeTableState.selected[frequency] = keys[next];
  loadSelectedRoutePeriod();
}

function setRouteFrequency(frequency) {
  if (frequency === routeTableState.frequency || !routeTableState.indexes[frequency].length) return;
  routeTableState.frequency = frequency;
  loadSelectedRoutePeriod();
}

function toggleRouteDetail(row) {
  const routeID = row.dataset.routeId;
  if (routeTableState.openRoutes.has(routeID)) routeTableState.openRoutes.delete(routeID);
  else routeTableState.openRoutes.add(routeID);
  renderRouteTable();
  const replacement = document.querySelector(`.kpi-route-row[data-route-id="${routeID}"]`);
  if (replacement) replacement.focus();
}

function wireRouteTableControls() {
  document.getElementById("kpi-view-weekly").addEventListener("click", () => setRouteFrequency("weekly"));
  document.getElementById("kpi-view-monthly").addEventListener("click", () => setRouteFrequency("monthly"));
  document.getElementById("kpi-period-older").addEventListener("click", () => changeSelectedPeriod(1));
  document.getElementById("kpi-period-newer").addEventListener("click", () => changeSelectedPeriod(-1));
  document.getElementById("kpi-period-select").addEventListener("change", (event) => {
    routeTableState.selected[routeTableState.frequency] = event.target.value;
    loadSelectedRoutePeriod();
  });
  const filter = document.getElementById("kpi-route-filter");
  filter.addEventListener("input", () => {
    routeTableState.filter = filter.value;
    renderRouteTable();
  });
  document.getElementById("kpi-route-filter-clear").addEventListener("click", () => {
    filter.value = "";
    routeTableState.filter = "";
    renderRouteTable();
    filter.focus();
  });
  document.querySelector("#kpi-route-table thead").addEventListener("click", (event) => {
    const th = event.target.closest("th[data-sort]");
    if (!th) return;
    if (routeTableState.sortKey === th.dataset.sort) {
      routeTableState.sortDirection = routeTableState.sortDirection === "asc" ? "desc" : "asc";
    } else {
      routeTableState.sortKey = th.dataset.sort;
      routeTableState.sortDirection = "asc";
    }
    renderRouteTable();
  });
  const tbody = document.querySelector("#kpi-route-table tbody");
  tbody.addEventListener("click", (event) => {
    const row = event.target.closest(".kpi-route-row");
    if (row) toggleRouteDetail(row);
  });
  tbody.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target.closest(".kpi-route-row");
    if (!row) return;
    event.preventDefault();
    toggleRouteDetail(row);
  });
}

async function initializeRouteTable(weeks, months, weeklyData = []) {
  routeTableState.indexes.weekly = [...weeks];
  routeTableState.indexes.monthly = months.map((month) => month.month);
  routeTableState.monthlyData = new Map(months.map((month) => [month.month, month]));
  routeTableState.weeklyData = new Map(weeklyData.map((week) => [week.week_end, week]));
  const params = new URLSearchParams(window.location.search);
  const requestedWeek = params.get("week_end");
  const requestedMonth = params.get("month");
  routeTableState.selected.weekly = weeks.includes(requestedWeek) ? requestedWeek : weeks[0] || "";
  routeTableState.selected.monthly = routeTableState.indexes.monthly.includes(requestedMonth)
    ? requestedMonth
    : routeTableState.indexes.monthly[0] || "";
  const requestedFrequency = params.get("view");
  routeTableState.frequency = requestedFrequency === "monthly" || !weeks.length ? "monthly" : "weekly";
  wireRouteTableControls();
  await loadSelectedRoutePeriod();
}

async function loadComparison() {
  if (isLocal) {
    const preview = localPreviewData();
    const servicePublished = publishedMap(preview.published.service_operated);
    const otpPublished = publishedMap(preview.published.on_time_performance);
    initializeComparisonCharts(preview.months, servicePublished, otpPublished);
    await initializeRouteTable(preview.weekly.map((week) => week.week_end), preview.months, preview.weekly);
    document.getElementById("meta").textContent = "Local preview data · May–July 2026";
    return;
  }
  const [monthlyIndex, weeklyIndex, published] = await Promise.all([
    fetchJSON(MONTHLY_INDEX_URL),
    fetchJSON(WEEKLY_INDEX_URL),
    fetchJSON(PUBLISHED_KPI_URL).catch(() => ({ service_operated: [], on_time_performance: [] })),
  ]);
  const monthFiles = await Promise.all((monthlyIndex.months || []).map((month) =>
    fetchJSON(`${GCS_BASE}/stats/monthly/${month}.json`).catch(() => null)
  ));
  const completeMonths = monthFiles
    .filter((month) => month && month.status === "complete" && month.agency_kpi)
    .sort((a, b) => b.month.localeCompare(a.month));
  const servicePublished = publishedMap(published.service_operated);
  const otpPublished = publishedMap(published.on_time_performance);
  initializeComparisonCharts(completeMonths, servicePublished, otpPublished);
  await initializeRouteTable(weeklyIndex.weeks || [], completeMonths);

  const fetched = new Date(published.fetched_at);
  const publishedText = Number.isNaN(fetched.getTime())
    ? "AC Transit publication date unavailable"
    : `AC Transit values checked ${fetched.toLocaleString()}`;
  document.getElementById("meta").textContent =
    `${completeMonths.length} complete month${completeMonths.length === 1 ? "" : "s"} · ${publishedText}`;
}

document.getElementById("footer-year").textContent = new Date().getFullYear();
loadComparison().catch((error) => {
  document.getElementById("kpi-content").hidden = true;
  const message = document.getElementById("load-error");
  message.hidden = false;
  message.textContent = `Couldn't load KPI comparison data: ${error.message}.`;
  document.getElementById("meta").textContent = "Comparison data unavailable";
});

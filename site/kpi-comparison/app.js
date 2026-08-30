const MONTHLY_INDEX_URL = `${GCS_BASE}/stats/monthly/_index.json`;
const PUBLISHED_KPI_URL = `${GCS_BASE}/stats/published-kpis/latest.json`;

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

function pct(value, digits = 1) {
  return value === null || value === undefined ? "Pending" : `${Number(value).toFixed(digits)}%`;
}

function countRatio(numerator, denominator) {
  return `${intFmt(numerator)} of ${intFmt(denominator)}`;
}

function publishedMap(values) {
  return new Map((values || []).map((value) => [value.month, value]));
}

function chartDataset(label, data, color, dash = []) {
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
  };
}

function comparisonChartOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    scales: {
      x: { grid: { display: false } },
      y: {
        min: 0,
        max: 100,
        ticks: { callback: (value) => `${value}%` },
        title: { display: true, text: "Percent" },
      },
    },
    plugins: {
      legend: {
        position: "bottom",
        labels: { usePointStyle: true, boxWidth: 8, padding: 16 },
      },
      tooltip: {
        callbacks: { label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}%` },
      },
    },
  };
}

function renderComparisonCharts(months, servicePublished, otpPublished) {
  const chronological = [...months].sort((a, b) => a.month.localeCompare(b.month));
  const labels = chronological.map((month) => monthLabel(month.month));

  new Chart(document.getElementById("service-operated-chart"), {
    type: "line",
    data: {
      labels,
      datasets: [
        chartDataset("Our calculation", chronological.map((month) => month.agency_kpi.service_operated.operated_pct), "#1971c2"),
        chartDataset("AC Transit published", chronological.map((month) => servicePublished.get(month.month)?.pct ?? null), "#e8590c", [6, 4]),
      ],
    },
    options: comparisonChartOptions(),
  });

  new Chart(document.getElementById("otp-chart"), {
    type: "line",
    data: {
      labels,
      datasets: [
        chartDataset("Our OTP — operated trips", chronological.map((month) => month.agency_kpi.on_time_performance.of_operated_pct), "#1971c2"),
        chartDataset("Our OTP — all scheduled", chronological.map((month) => month.agency_kpi.on_time_performance.of_scheduled_pct), "#5f3dc4"),
        chartDataset("AC Transit published", chronological.map((month) => otpPublished.get(month.month)?.pct ?? null), "#e8590c", [6, 4]),
      ],
    },
    options: comparisonChartOptions(),
  });
}

function sampleKPI(scheduledTrips, operatedTrips, partialTrips, onTime, operatedTimepoints, scheduledTimepoints) {
  const ratio = (n, d) => d ? Math.round(1000 * n / d) / 10 : null;
  return {
    methodology_version: 1,
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
    weeks: ranges.map(([period_start, period_end], index) => ({
      period_start,
      period_end,
      agency_kpi: sampleKPI(...allocated.map((values) => values[index])),
    })),
  };
}

function localPreviewData() {
  return {
    months: [
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
    ],
    published: {
      fetched_at: "2026-08-29T17:00:00Z",
      service_operated: [
        { month: "2026-05", pct: 95.5 }, { month: "2026-06", pct: 94.24 }, { month: "2026-07", pct: 89.93 },
      ],
      on_time_performance: [{ month: "2026-05", pct: 73.25 }, { month: "2026-06", pct: 74.72 }],
    },
  };
}

function weeklyServiceOperated(month) {
  const monthly = month.agency_kpi.service_operated;
  return `
    <div class="kpi-detail-inner">
      <h3>${monthLabel(month.month)} weekly detail</h3>
      <p class="muted kpi-month-detail">
        Partial operated trips for the month: ${pct(monthly.partial_of_operated_pct)}
        (${countRatio(monthly.partial_trips, monthly.operated_trips)} operated trips).
      </p>
      <table class="kpi-week-table">
        <thead><tr><th>Period</th><th>Service Operated</th><th>Operated / planned</th><th>Partial operated trips</th></tr></thead>
        <tbody>${month.weeks.map((week) => {
          const metric = week.agency_kpi.service_operated;
          return `<tr>
            <td>${shortDate(week.period_start)}–${shortDate(week.period_end)}</td>
            <td>${pct(metric.operated_pct)}</td>
            <td>${countRatio(metric.operated_trips, metric.scheduled_trips)}</td>
            <td>${pct(metric.partial_of_operated_pct)} (${intFmt(metric.partial_trips)})</td>
          </tr>`;
        }).join("")}</tbody>
      </table>
    </div>`;
}

function weeklyOTP(month) {
  return `
    <div class="kpi-detail-inner">
      <h3>${monthLabel(month.month)} weekly detail</h3>
      <table class="kpi-week-table">
        <thead><tr><th>Period</th><th>Operated trips</th><th>All scheduled</th><th>On-time / operated timepoints</th><th>Scheduled timepoints</th></tr></thead>
        <tbody>${month.weeks.map((week) => {
          const metric = week.agency_kpi.on_time_performance;
          return `<tr>
            <td>${shortDate(week.period_start)}–${shortDate(week.period_end)}</td>
            <td>${pct(metric.of_operated_pct)}</td>
            <td>${pct(metric.of_scheduled_pct)}</td>
            <td>${countRatio(metric.on_time_timepoints, metric.operated_timepoints)}</td>
            <td>${intFmt(metric.scheduled_timepoints)}</td>
          </tr>`;
        }).join("")}</tbody>
      </table>
    </div>`;
}

function renderServiceOperated(months, published) {
  const tbody = document.querySelector("#service-operated-table tbody");
  tbody.innerHTML = months.map((month) => {
    const metric = month.agency_kpi.service_operated;
    const external = published.get(month.month);
    return `
      <tr class="kpi-row" data-month="${month.month}" tabindex="0" role="button" aria-expanded="false">
        <td>${monthLabel(month.month)}</td>
        <td>${pct(metric.operated_pct)}</td>
        <td>${pct(external && external.pct, 2)}</td>
        <td class="expand-cell" aria-hidden="true">▸</td>
      </tr>
      <tr class="kpi-detail" data-detail-month="${month.month}" hidden>
        <td colspan="4">${weeklyServiceOperated(month)}</td>
      </tr>`;
  }).join("");
  attachExpansion(tbody);
}

function renderOTP(months, published) {
  const tbody = document.querySelector("#otp-table tbody");
  tbody.innerHTML = months.map((month) => {
    const metric = month.agency_kpi.on_time_performance;
    const external = published.get(month.month);
    return `
      <tr class="kpi-row" data-month="${month.month}" tabindex="0" role="button" aria-expanded="false">
        <td>${monthLabel(month.month)}</td>
        <td>${pct(metric.of_operated_pct)}</td>
        <td>${pct(metric.of_scheduled_pct)}</td>
        <td>${pct(external && external.pct, 2)}</td>
        <td class="expand-cell" aria-hidden="true">▸</td>
      </tr>
      <tr class="kpi-detail" data-detail-month="${month.month}" hidden>
        <td colspan="5">${weeklyOTP(month)}</td>
      </tr>`;
  }).join("");
  attachExpansion(tbody);
}

function attachExpansion(tbody) {
  const toggle = (row) => {
    const detail = tbody.querySelector(`[data-detail-month="${row.dataset.month}"]`);
    const open = row.getAttribute("aria-expanded") !== "true";
    row.setAttribute("aria-expanded", String(open));
    row.classList.toggle("is-open", open);
    row.querySelector(".expand-cell").textContent = open ? "▾" : "▸";
    detail.hidden = !open;
  };
  tbody.querySelectorAll(".kpi-row").forEach((row) => {
    row.addEventListener("click", () => toggle(row));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        toggle(row);
      }
    });
  });
}

async function loadComparison() {
  if (isLocal) {
    const preview = localPreviewData();
    const servicePublished = publishedMap(preview.published.service_operated);
    const otpPublished = publishedMap(preview.published.on_time_performance);
    renderComparisonCharts(preview.months, servicePublished, otpPublished);
    renderServiceOperated(preview.months, servicePublished);
    renderOTP(preview.months, otpPublished);
    document.getElementById("meta").textContent = "Local preview data · May–July 2026";
    return;
  }
  const [index, published] = await Promise.all([
    fetchJSON(MONTHLY_INDEX_URL),
    fetchJSON(PUBLISHED_KPI_URL).catch(() => ({ service_operated: [], on_time_performance: [] })),
  ]);
  const monthFiles = await Promise.all((index.months || []).map((month) =>
    fetchJSON(`${GCS_BASE}/stats/monthly/${month}.json`).catch(() => null)
  ));
  const completeMonths = monthFiles
    .filter((month) => month && month.status === "complete" && month.agency_kpi)
    .sort((a, b) => b.month.localeCompare(a.month));
  const servicePublished = publishedMap(published.service_operated);
  const otpPublished = publishedMap(published.on_time_performance);
  renderComparisonCharts(completeMonths, servicePublished, otpPublished);
  renderServiceOperated(completeMonths, servicePublished);
  renderOTP(completeMonths, otpPublished);

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

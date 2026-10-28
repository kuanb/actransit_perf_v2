const SCHOOL_STATS_BASE = `${GCS_BASE}/stats/schools`;
const PT_TIME = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  hour: "numeric",
  minute: "2-digit",
});
const SHORT_DATE_LABEL = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC",
  weekday: "short",
  month: "short",
  day: "numeric",
});
const MONTH_DAY_LABEL = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC",
  month: "short",
  day: "numeric",
});

let trendChart;
let routeCharts = [];

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function parseDate(value) {
  return new Date(`${value}T12:00:00Z`);
}

function isoDate(value) {
  return value.toISOString().slice(0, 10);
}

function addDays(value, days) {
  const date = parseDate(value);
  date.setUTCDate(date.getUTCDate() + days);
  return isoDate(date);
}

function weekStartFor(value) {
  const date = parseDate(value);
  date.setUTCDate(date.getUTCDate() - date.getUTCDay());
  return isoDate(date);
}

function requestedWeekStart() {
  const params = new URLSearchParams(window.location.search);
  const week = params.get("week");
  if (week) return weekStartFor(week);
  const legacyDate = params.get("date");
  return legacyDate ? weekStartFor(legacyDate) : "";
}

function formatShortDate(value) {
  return SHORT_DATE_LABEL.format(parseDate(value));
}

function weekLabel(start) {
  const end = addDays(start, 6);
  const startDate = parseDate(start);
  const endDate = parseDate(end);
  return `${MONTH_DAY_LABEL.format(startDate)}–${MONTH_DAY_LABEL.format(endDate)}, ${endDate.getUTCFullYear()}`;
}

function formatTime(value) {
  return value ? PT_TIME.format(new Date(value)) : "—";
}

function signedMinutes(value) {
  if (value === null || value === undefined) return "";
  const minutes = Math.abs(Number(value));
  if (Number(value) > 0) return `${fmt(minutes, 1)} min after start`;
  if (Number(value) < 0) return `${fmt(minutes, 1)} min before start`;
  return "At school start";
}

function arrivalLabel(status) {
  return {
    by_school_start: "By school start",
    after_school_start: "After school start",
    arrival_not_observed: "Arrival not observed",
    not_observed: "Dropped / not observed",
  }[status] || status;
}

function departureLabel(status) {
  return {
    within_15_minutes: "Within 15 min",
    more_than_15_minutes_late: ">15 min late",
    uncertain: "Time uncertain",
    not_observed: "Not observed",
  }[status] || status;
}

function departureCell(trip) {
  if (!trip.actual_departure) {
    return `<span class="time-main">—</span><span class="time-note">${escapeHTML(departureLabel(trip.departure_status))}</span>`;
  }
  const delay = trip.departure_delay_minutes;
  const delayText = delay === null || delay === undefined
    ? departureLabel(trip.departure_status)
    : `${fmt(delay, 1)} min ${Number(delay) >= 0 ? "late" : "early"}`;
  const confidence = trip.departure_confidence === "confirmed_250m_crossing"
    ? delayText
    : `First seen away · ${departureLabel(trip.departure_status)}`;
  return `<span class="time-main">${formatTime(trip.actual_departure)}</span><span class="time-note">${escapeHTML(confidence)}</span>`;
}

function arrivalCell(trip) {
  if (!trip.actual_arrival) {
    return `<span class="time-main">—</span><span class="time-note">${escapeHTML(arrivalLabel(trip.arrival_status))}</span>`;
  }
  const confidence = trip.arrival_confidence === "confirmed_250m_crossing"
    ? signedMinutes(trip.arrival_minutes_from_start)
    : `${signedMinutes(trip.arrival_minutes_from_start)} · first seen inside`;
  return `<span class="time-main">${formatTime(trip.actual_arrival)}</span><span class="time-note">${escapeHTML(confidence)}</span>`;
}

async function loadIndex() {
  try {
    if (isLocal) return await fetchJSON("data/index.json");
    return await fetchJSON(`${SCHOOL_STATS_BASE}/_index.json`);
  } catch (_) {
    return { dates: [] };
  }
}

async function loadReport(date) {
  if (isLocal) {
    const report = await fetchJSON("data/sample.json");
    report.service_date = date;
    return report;
  }
  return fetchJSON(`${SCHOOL_STATS_BASE}/${date}.json`);
}

function aggregateSchools(reports) {
  const ids = [...new Set(reports.flatMap((report) => report.schools.map((school) => school.school_id)))];
  return ids.map((schoolID) => {
    const days = reports.flatMap((report) => {
      const school = report.schools.find((item) => item.school_id === schoolID);
      return school ? [{ serviceDate: report.service_date, school }] : [];
    });
    const latest = days.at(-1).school;
    const trips = days.flatMap(({ serviceDate, school }) =>
      (school.trips || []).map((trip) => ({ ...trip, service_date: serviceDate }))
    );
    const observedArrivals = trips.filter((trip) => Boolean(trip.actual_arrival)).length;
    return {
      ...latest,
      days,
      trips,
      scheduled_trips: trips.length,
      observed_arrivals: observedArrivals,
      arrivals_after_start: trips.filter((trip) => trip.arrival_status === "after_school_start").length,
      trips_not_observed: trips.filter((trip) => trip.arrival_status === "not_observed").length,
      all_scheduled_arrived: trips.length > 0 && observedArrivals === trips.length,
    };
  });
}

function renderSummary(reports) {
  const trips = reports.flatMap((report) => report.schools.flatMap((school) => school.trips || []));
  const arrivedByStart = trips.filter((trip) => trip.arrival_status === "by_school_start").length;
  const afterStart = trips.filter((trip) => trip.arrival_status === "after_school_start").length;
  const noSchoolArrival = trips.filter((trip) => !trip.actual_arrival).length;
  const lateDeparture = trips.filter((trip) => trip.departure_status === "more_than_15_minutes_late").length;
  const total = trips.length;
  renderCards("#summary-cards", [
    { label: "Arrived by start", val: `${arrivedByStart} / ${total}`, grade: gradeColor(total ? arrivedByStart / total : 0) },
    { label: "After school starts", val: intFmt(afterStart), grade: afterStart ? gradeColor(0.2) : gradeColor(1) },
    { label: "No school arrival observed", val: intFmt(noSchoolArrival), grade: noSchoolArrival ? gradeColor(0.2) : gradeColor(1) },
    { label: "Confirmed departures >15 min late", val: intFmt(lateDeparture), grade: lateDeparture ? gradeColor(0.2) : gradeColor(1) },
  ]);
}

function renderDayCard({ serviceDate, school }) {
  const trips = school.trips || [];
  const byStart = trips.filter((trip) => trip.arrival_status === "by_school_start").length;
  const afterStart = trips.filter((trip) => trip.arrival_status === "after_school_start").length;
  const missing = trips.filter((trip) => !trip.actual_arrival).length;
  const state = missing ? "is-bad" : afterStart ? "is-warning" : "is-good";
  return `
    <div class="day-card ${state}">
      <strong>${escapeHTML(formatShortDate(serviceDate))}</strong>
      <span>${byStart} of ${trips.length} by start</span>
      <small>${afterStart} after · ${missing} no arrival</small>
    </div>`;
}

function routeChartID(schoolID, routeID) {
  const safeSchoolID = String(schoolID).replace(/[^a-zA-Z0-9_-]/g, "-");
  const safeRouteID = String(routeID).replace(/[^a-zA-Z0-9_-]/g, "-");
  return `route-chart-${safeSchoolID}-${safeRouteID}`;
}

function renderRouteChartsSection(school) {
  const routeIDs = [...new Set(school.trips.map((trip) => trip.route_id))].sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));
  if (!routeIDs.length) return "";
  const charts = routeIDs.map((routeID) => `
    <article class="route-chart-panel">
      <div class="route-chart-head">
        <span class="route-pill">${escapeHTML(routeID)}</span>
        <div>
          <h3>Route ${escapeHTML(routeID)}</h3>
          <p>Daily scheduled trips by outcome</p>
        </div>
      </div>
      <div class="route-chart-canvas">
        <canvas id="${escapeHTML(routeChartID(school.school_id, routeID))}" aria-label="Daily arrival and departure outcomes for route ${escapeHTML(routeID)}" role="img"></canvas>
      </div>
    </article>`).join("");
  return `
    <div class="route-chart-section">
      <div class="route-chart-intro">
        <div>
          <p class="school-kicker">Route consistency</p>
          <h3>Met and missed by day</h3>
        </div>
        <p>Each date has one arrival stack and one departure stack, so a single bad day stands apart from a repeated pattern.</p>
      </div>
      <div class="route-chart-grid">${charts}</div>
      <p class="route-chart-note">Arrival misses include late, unobserved, and dropped trips. Departure misses include confirmed departures over 15 minutes late and unobserved trips; uncertain first-seen departures remain gray.</p>
    </div>`;
}

function renderSchool(school) {
  const complete = school.all_scheduled_arrived;
  const capacityText = school.scheduled_trips
    ? `${school.observed_arrivals} of ${school.scheduled_trips} scheduled buses entered the school geofence across ${school.days.length} reported school days.`
    : "No qualifying school trips were scheduled.";
  const startTimes = [...new Set(school.days.map(({ school: day }) => day.start_time_local))];
  const source = school.start_time_source_url
    ? ` · <a href="${escapeHTML(school.start_time_source_url)}" target="_blank" rel="noopener">bell schedule source</a>`
    : "";
  const rows = school.trips.map((trip) => `
    <tr>
      <td><span class="day-main">${escapeHTML(formatShortDate(trip.service_date))}</span></td>
      <td><span class="route-pill">${escapeHTML(trip.route_id)}</span></td>
      <td><span class="time-main">${formatTime(trip.scheduled_departure)}</span><span class="time-note">Scheduled</span></td>
      <td>${departureCell(trip)}</td>
      <td><span class="time-main">${formatTime(trip.scheduled_arrival)}</span><span class="time-note">Scheduled</span></td>
      <td>${arrivalCell(trip)}</td>
      <td><span class="status-chip status-${escapeHTML(trip.arrival_status)}">${escapeHTML(arrivalLabel(trip.arrival_status))}</span></td>
      <td><span class="status-chip status-${escapeHTML(trip.departure_status)}">${escapeHTML(departureLabel(trip.departure_status))}</span></td>
    </tr>`).join("");
  return `
    <section class="school-panel">
      <div class="school-panel-head">
        <div>
          <p class="school-kicker">${escapeHTML(school.school_id === "el-cerrito-high" ? "Routes 667 + 684" : "Route 675")}</p>
          <h2>${escapeHTML(school.name)}</h2>
          <p class="muted">Each day uses its effective-dated start time${source}</p>
        </div>
        <div class="bell-time"><span class="muted">School starts</span><strong>${escapeHTML(startTimes.join(" / "))}</strong></div>
      </div>
      <div class="capacity-banner ${complete ? "is-complete" : "is-incomplete"}">
        <strong>${complete ? "All scheduled buses observed" : "Expected capacity was not fully observed"}</strong>
        <span>${escapeHTML(capacityText)}</span>
      </div>
      <div class="day-strip">${school.days.map(renderDayCard).join("")}</div>
      ${renderRouteChartsSection(school)}
      <div class="school-table-wrap">
        <table class="school-table">
          <thead><tr>
            <th>Day</th><th>Route</th><th>Scheduled departure</th><th>Observed departure</th>
            <th>Scheduled arrival</th><th>Observed school arrival</th>
            <th>Arrival status</th><th>Departure status</th>
          </tr></thead>
          <tbody>${rows || '<tr><td colspan="8">No qualifying trips scheduled.</td></tr>'}</tbody>
        </table>
      </div>
    </section>`;
}

function renderWeek(reports, weekStart) {
  document.querySelector("#meta").textContent = `${weekLabel(weekStart)} · ${reports.length} reported school ${reports.length === 1 ? "day" : "days"}`;
  renderSummary(reports);
  const schools = aggregateSchools(reports);
  document.querySelector("#school-sections").innerHTML = schools.map(renderSchool).join("");
  renderRouteCharts(schools);
}

function renderRouteCharts(schools) {
  routeCharts.forEach((chart) => chart.destroy());
  routeCharts = [];

  schools.forEach((school) => {
    const routeIDs = [...new Set(school.trips.map((trip) => trip.route_id))];
    routeIDs.forEach((routeID) => {
      const canvas = document.querySelector(`#${routeChartID(school.school_id, routeID)}`);
      if (!canvas) return;
      const labels = school.days.map(({ serviceDate }) => formatShortDate(serviceDate));
      const outcomes = school.days.map(({ school: day }) => {
        const trips = (day.trips || []).filter((trip) => trip.route_id === routeID);
        return {
          arrivalMet: trips.filter((trip) => trip.arrival_status === "by_school_start").length,
          arrivalMissed: trips.filter((trip) => trip.arrival_status !== "by_school_start").length,
          departureMet: trips.filter((trip) => trip.departure_status === "within_15_minutes").length,
          departureMissed: trips.filter((trip) => ["more_than_15_minutes_late", "not_observed"].includes(trip.departure_status)).length,
          departureUncertain: trips.filter((trip) => trip.departure_status === "uncertain").length,
        };
      });
      const chart = new Chart(canvas, {
        type: "bar",
        data: {
          labels,
          datasets: [
            { label: "Arrival · met", data: outcomes.map((day) => day.arrivalMet), backgroundColor: "#297657", stack: "arrival" },
            { label: "Arrival · missed", data: outcomes.map((day) => day.arrivalMissed), backgroundColor: "#a83a3a", stack: "arrival" },
            { label: "Departure · met", data: outcomes.map((day) => day.departureMet), backgroundColor: "#2f6f9f", stack: "departure" },
            { label: "Departure · missed", data: outcomes.map((day) => day.departureMissed), backgroundColor: "#a66013", stack: "departure" },
            { label: "Departure · uncertain", data: outcomes.map((day) => day.departureUncertain), backgroundColor: "#8a949c", stack: "departure" },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          scales: {
            x: { stacked: true, title: { display: true, text: "Service date" } },
            y: { stacked: true, beginAtZero: true, ticks: { precision: 0, stepSize: 1 }, title: { display: true, text: "Scheduled trips" } },
          },
          plugins: {
            legend: { position: "bottom", labels: { boxWidth: 12, boxHeight: 12 } },
          },
        },
      });
      routeCharts.push(chart);
    });
  });
}

function renderWeekSelector(weeks, current) {
  const select = document.querySelector("#week-selector");
  select.innerHTML = weeks.map((week) => `<option value="${escapeHTML(week)}" ${week === current ? "selected" : ""}>${escapeHTML(weekLabel(week))}</option>`).join("");
  select.addEventListener("change", () => {
    const url = new URL(window.location.href);
    url.searchParams.delete("date");
    url.searchParams.set("week", select.value);
    window.location.href = url.toString();
  });
}

function renderWeeklyTrend(reports) {
  if (reports.length < 2) return;
  const schoolIDs = [...new Set(reports.flatMap((report) => report.schools.map((school) => school.school_id)))];
  const colors = ["#2f6f9f", "#a66013"];
  const datasets = schoolIDs.map((schoolID, index) => ({
    label: reports.flatMap((report) => report.schools).find((school) => school.school_id === schoolID)?.name || schoolID,
    data: reports.map((report) => {
      const school = report.schools.find((item) => item.school_id === schoolID);
      if (!school || !school.scheduled_trips) return null;
      const byStart = school.trips.filter((trip) => trip.arrival_status === "by_school_start").length;
      return Math.round(1000 * byStart / school.scheduled_trips) / 10;
    }),
    borderColor: colors[index % colors.length],
    backgroundColor: colors[index % colors.length],
    borderWidth: 2,
    pointRadius: 4,
    tension: 0.18,
  }));

  document.querySelector("#trend-section").hidden = false;
  if (trendChart) trendChart.destroy();
  trendChart = new Chart(document.querySelector("#school-trend-chart"), {
    type: "line",
    data: { labels: reports.map((report) => formatShortDate(report.service_date)), datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        y: { min: 0, max: 100, ticks: { callback: (value) => `${value}%` }, title: { display: true, text: "Scheduled buses arriving by school start" } },
      },
      plugins: { legend: { position: "bottom" } },
    },
  });
}

async function init() {
  document.querySelector("#footer-year").textContent = new Date().getFullYear();
  try {
    const index = await loadIndex();
    let dates = [...new Set(index.dates || [])].sort().reverse();
    if (!dates.length) {
      const latest = isLocal
        ? await fetchJSON("data/sample.json")
        : await fetchJSON(`${SCHOOL_STATS_BASE}/latest.json`);
      dates = [latest.service_date];
    }
    const weeks = [...new Set(dates.map(weekStartFor))].sort().reverse();
    const requested = requestedWeekStart();
    const selectedWeek = weeks.includes(requested) ? requested : weeks[0];
    const selectedDates = dates.filter((date) => weekStartFor(date) === selectedWeek).sort();
    const reports = (await Promise.all(selectedDates.map((date) => loadReport(date).catch(() => null)))).filter(Boolean);
    if (!reports.length) throw new Error("No daily reports were available for this week");

    renderWeek(reports, selectedWeek);
    renderWeekSelector(weeks, selectedWeek);
    renderWeeklyTrend(reports);
  } catch (error) {
    const box = document.querySelector("#error");
    box.hidden = false;
    box.textContent = `School report unavailable: ${error.message}`;
  }
}

init();

// Weekly dashboard. Reads stats/weekly/<sat>.json (or latest.json) from
// GCS and renders four views: SD% bar by day, day×hour delay heatmap
// with p50/p95 toggle, per-route hour-of-day delay line plot with
// outliers highlighted (top-10 worst by week's median delay), and a
// route×day SD% grid sorted worst→best.

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function weeklyURL(weekEnd) {
  return weekEnd
    ? `${GCS_BASE}/stats/weekly/${weekEnd}.json`
    : `${GCS_BASE}/stats/weekly/latest.json`;
}

async function loadWeeklyIndex() {
  try {
    const idx = await fetchJSON(`${GCS_BASE}/stats/weekly/_index.json`);
    return Array.isArray(idx.weeks) ? idx.weeks : [];
  } catch (e) {
    return [];
  }
}

function renderWeekSelector(weeks, current) {
  const el = document.getElementById("week-selector");
  if (!weeks.length && !current) {
    el.innerHTML = "";
    return;
  }
  const last10 = weeks.slice(0, 10);
  const customSelected = current && !last10.includes(current);
  el.innerHTML = `
    <label>View week ending:
      <select id="week-dropdown">
        <option value="" ${!current ? "selected" : ""}>Latest</option>
        ${last10.map((d) => `<option value="${d}" ${d === current ? "selected" : ""}>${d}</option>`).join("")}
        ${customSelected ? `<option value="${current}" selected>${current}</option>` : ""}
      </select>
    </label>
  `;
  document.getElementById("week-dropdown").addEventListener("change", (e) => {
    const v = e.target.value;
    const url = new URL(window.location.href);
    if (v) url.searchParams.set("week_end", v);
    else url.searchParams.delete("week_end");
    window.location.href = url.toString();
  });
}

async function load() {
  const yearEl = document.getElementById("footer-year");
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  const params = new URLSearchParams(window.location.search);
  const weekEnd = params.get("week_end");

  let data;
  try {
    data = await fetchJSON(weeklyURL(weekEnd));
  } catch (e) {
    document.body.insertAdjacentHTML(
      "afterbegin",
      `<div style="padding:16px;background:#fee;color:#900;">
        Couldn't load weekly stats for ${weekEnd || "latest"}: ${e.message}.
      </div>`
    );
    return;
  }

  render(data);
  const weeks = await loadWeeklyIndex();
  renderWeekSelector(weeks, weekEnd || data.week_end);
}

function render(data) {
  document.getElementById("meta").textContent =
    `Week ${data.week_start} → ${data.week_end} · generated ${data.generated_at}`;

  renderDailySD(data);
  renderDelayHeatmap(data);
  renderRouteLineChart(data);
  renderRouteDayGrid(data);
}

// ---- chart 1: SD% by day-of-week (bar) ----
function renderDailySD(data) {
  const days = data.daily_service_delivered;
  const labels = days.map((d) => `${d.day} ${d.service_date.slice(5)}`);
  const values = days.map((d) => d.pct);
  const colors = days.map((d) => gradeServiceDelivered(d.pct).bg);
  const ctx = document.getElementById("daily-sd-chart").getContext("2d");
  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{ label: "Service delivered", data: values, backgroundColor: colors, borderWidth: 0 }],
    },
    options: {
      onClick: (_e, els) => {
        if (!els.length) return;
        window.location.href = `../?date=${days[els[0].index].service_date}`;
      },
      onHover: (e, els) => {
        e.native.target.style.cursor = els.length ? "pointer" : "default";
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (ctx) => `${days[ctx[0].dataIndex].day} ${days[ctx[0].dataIndex].service_date}`,
            label: (ctx) => {
              const d = days[ctx.dataIndex];
              return `${fmt(d.pct)}% — ran ${intFmt(d.ran)} of ${intFmt(d.scheduled)} scheduled`;
            },
          },
        },
      },
      scales: {
        x: { grid: { display: false } },
        y: {
          beginAtZero: false,
          min: 80,
          max: 100,
          title: { display: true, text: "% service delivered" },
          ticks: { callback: (v) => `${v}%` },
        },
      },
    },
  });
}

// ---- chart 2: day × hour delay heatmap ----
function renderDelayHeatmap(data) {
  const heatmap = data.system_delay_heatmap;
  const container = document.getElementById("delay-heatmap");

  // p50 typically lives in [-3, +5]; p95 in [0, +15]. Different scales
  // keep both visualizations readable.
  const SCALES = {
    p50: { lo: -3, mid: 0, hi: 5 },
    p95: { lo: 0, mid: 3, hi: 15 },
  };

  const draw = (stat) => {
    const { lo, mid, hi } = SCALES[stat];

    let html = `<div class="hm-grid">`;
    html += `<div class="hm-corner"></div>`;
    for (let h = 0; h < 24; h++) {
      html += `<div class="hm-hcol">${h}</div>`;
    }
    for (let i = 0; i < 7; i++) {
      const day = DAY_NAMES[i];
      const cells = heatmap[day] || [];
      const date = data.daily_service_delivered[i].service_date;
      html += `<div class="hm-row" data-date="${date}" title="View ${day} ${date}">${day}</div>`;
      for (let h = 0; h < 24; h++) {
        const cell = cells[h] || { hour: h, n: 0 };
        const v = cell[stat];
        const color = delayDivergingColor(v, lo, mid, hi) || "#f0f0f0";
        const tip =
          v === null || v === undefined
            ? `${day} ${h}:00 PT — no data (n=${intFmt(cell.n || 0)})`
            : `${day} ${h}:00 PT — ${stat} = ${fmt(v)} min (n=${intFmt(cell.n)})`;
        html += `<div class="hm-cell" style="background:${color}" title="${tip}"></div>`;
      }
    }
    html += `</div>`;

    // Color legend (11 swatches across the scale).
    html += `<div class="hm-legend"><span>${fmt(lo)} min (early)</span>`;
    for (let i = 0; i <= 10; i++) {
      const v = lo + (hi - lo) * (i / 10);
      html += `<div class="hm-legend-cell" style="background:${delayDivergingColor(v, lo, mid, hi)}" title="${fmt(v)} min"></div>`;
    }
    html += `<span>${fmt(hi)} min (late)</span></div>`;

    container.innerHTML = html;

    container.querySelectorAll(".hm-row").forEach((row) => {
      row.addEventListener("click", () => {
        window.location.href = `../?date=${row.dataset.date}`;
      });
    });
  };

  draw("p50");
  document.querySelectorAll('input[name="hm-stat"]').forEach((input) => {
    input.addEventListener("change", (e) => draw(e.target.value));
  });
}

// ---- chart 3: per-route delay by hour ----
function renderRouteLineChart(data) {
  const ctx = document.getElementById("route-line-chart").getContext("2d");
  const routes = Object.keys(data.route_delay_by_hour).sort();

  // Top-10 worst by week-overall p50. The route_daily_service_delivered
  // list is already sorted worst→best in the JSON.
  const topWorstIds = data.route_daily_service_delivered
    .filter((r) => r.overall_p50_delay_min !== null)
    .slice(0, 10)
    .map((r) => r.route_id);
  const topWorstSet = new Set(topWorstIds);

  const routeMeta = {};
  for (const r of data.route_daily_service_delivered) {
    routeMeta[r.route_id] = { color: r.color, text_color: r.text_color };
  }

  const buildDatasets = (stat) =>
    routes.map((rid) => {
      const points = data.route_delay_by_hour[rid] || [];
      const byHour = new Array(24).fill(null);
      for (const p of points) byHour[p.hour] = p[stat];
      const isOutlier = topWorstSet.has(rid);
      const color = `#${(routeMeta[rid] && routeMeta[rid].color) || "555555"}`;
      return {
        label: rid,
        data: byHour,
        spanGaps: false,
        borderColor: isOutlier ? color : "rgba(150,150,150,0.22)",
        backgroundColor: isOutlier ? color : "rgba(150,150,150,0.22)",
        borderWidth: isOutlier ? 2.5 : 1,
        pointRadius: isOutlier ? 2 : 0,
        order: isOutlier ? 0 : 1,
      };
    });

  const labels = Array.from({ length: 24 }, (_, h) => h);
  const chart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets: buildDatasets("p50") },
    options: {
      plugins: {
        legend: {
          display: true,
          labels: {
            // Only show legend entries for the bold-colored outlier routes.
            filter: (legend) => topWorstSet.has(legend.text),
            boxWidth: 14,
            boxHeight: 2,
          },
        },
        tooltip: {
          callbacks: {
            title: (ctx) => `${ctx[0].label}:00 PT`,
            label: (ctx) => {
              if (ctx.parsed.y === null) return null;
              const tag = topWorstSet.has(ctx.dataset.label) ? " ★" : "";
              return `Route ${ctx.dataset.label}${tag}: ${fmt(ctx.parsed.y)} min`;
            },
          },
        },
      },
      scales: {
        x: { title: { display: true, text: "hour of day (PT)" }, grid: { display: false } },
        y: { title: { display: true, text: "delay (min)" }, suggestedMin: -2, suggestedMax: 10 },
      },
    },
  });

  document.querySelectorAll('input[name="rline-stat"]').forEach((input) => {
    input.addEventListener("change", (e) => {
      chart.data.datasets = buildDatasets(e.target.value);
      // p95 wants more headroom on the y-axis.
      chart.options.scales.y.suggestedMax = e.target.value === "p95" ? 20 : 10;
      chart.update();
    });
  });
}

// ---- chart 4: route × day SD% grid ----
function renderRouteDayGrid(data) {
  const container = document.getElementById("route-day-grid");
  const routes = data.route_daily_service_delivered;
  const days = data.daily_service_delivered;

  let html = `<div class="rdgrid">`;
  // Header row
  html += `<div class="rdgrid-corner"></div>`;
  for (const d of days) {
    html += `<div class="rdgrid-hcol" data-date="${d.service_date}" title="View ${d.day} ${d.service_date}">${d.day}<br><small>${d.service_date.slice(5)}</small></div>`;
  }
  // Route rows
  for (const r of routes) {
    const p50Suffix =
      r.overall_p50_delay_min !== null && r.overall_p50_delay_min !== undefined
        ? `<span class="rdgrid-p50">p50 ${fmt(r.overall_p50_delay_min)}m</span>`
        : "";
    const badgeR = { route_id: r.route_id, color: r.color, text_color: r.text_color };
    html += `<div class="rdgrid-rlabel">${routeBadge(badgeR)}${p50Suffix}</div>`;
    for (const cell of r.by_day) {
      if (cell.pct === null || cell.pct === undefined) {
        html += `<div class="rdgrid-cell rdgrid-empty" title="${r.route_id} ${cell.day} — no data">—</div>`;
      } else {
        const grade = gradeServiceDelivered(cell.pct);
        const tip = `${r.route_id} ${cell.day} ${cell.service_date}: ${fmt(cell.pct)}% delivered`;
        html += `<div class="rdgrid-cell" style="background:${grade.bg};color:${grade.fg}" title="${tip}" data-date="${cell.service_date}">${fmt(cell.pct, 0)}</div>`;
      }
    }
  }
  html += `</div>`;
  container.innerHTML = html;

  container.querySelectorAll(".rdgrid-hcol").forEach((h) => {
    h.addEventListener("click", () => {
      window.location.href = `../?date=${h.dataset.date}`;
    });
  });
  container.querySelectorAll(".rdgrid-cell").forEach((c) => {
    if (c.classList.contains("rdgrid-empty")) return;
    c.addEventListener("click", () => {
      window.location.href = `../?date=${c.dataset.date}`;
    });
  });
}

load();

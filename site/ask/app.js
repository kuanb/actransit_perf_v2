const CONFIG_URL = "https://storage.googleapis.com/transit-203605-actransit-cache/analysis/config.json";
const params = new URLSearchParams(window.location.search);
const routeID = sanitizeRoute(params.get("route_id"));
const weekEnd = sanitizeDate(params.get("week_end"));
const form = document.getElementById("ask-form");
const composer = document.getElementById("question-composer");
const questionFormHome = document.getElementById("question-form-home");
const followupFormHome = document.getElementById("followup-form-home");
const question = document.getElementById("question");
const askButton = document.getElementById("ask-button");
const message = document.getElementById("message");
const responseSection = document.getElementById("response");
const statusDot = document.getElementById("status-dot");
const limitTitle = document.getElementById("limit-title");
const limitCopy = document.getElementById("limit-copy");
const charts = [];
const mapInstances = [];
const threadQuestions = [];
let config = null;
let status = null;
let nextAllowedAt = null;
let submitting = false;
let statusTimer = null;
let parentRequestID = null;
let followUpsRemaining = null;

const sessionID = getSessionID();
configureScope();
renderSuggestions();
question.addEventListener("input", updateFormState);
form.addEventListener("submit", submitQuestion);
document.getElementById("clear-response").addEventListener("click", clearResponse);
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") refreshStatus();
});

initialize();

async function initialize() {
  try {
    const response = await fetch(CONFIG_URL, { cache: "no-store" });
    if (!response.ok) throw new Error("Analysis configuration is not available yet.");
    config = await response.json();
    question.maxLength = config.max_prompt_chars || 500;
    await refreshStatus();
    statusTimer = window.setInterval(() => {
      if (document.visibilityState === "visible") refreshStatus();
    }, 8000);
  } catch (error) {
    setUnavailable(error.message);
  }
  updateFormState();
  window.setInterval(updateFormState, 500);
}

function configureScope() {
  if (!routeID) return;
  document.getElementById("page-title").textContent = `Ask about Route ${routeID}`;
  document.getElementById("scope-copy").textContent = `This interface is locked to Route ${routeID}. Start a systemwide question from the report card.`;
  const badge = document.getElementById("route-scope");
  badge.textContent = `Route ${routeID}`;
  badge.hidden = false;
}

function renderSuggestions() {
  const suggestions = routeID
    ? [
        `How did Route ${routeID} perform?`,
        `Where does Route ${routeID} fall furthest behind schedule?`,
        `How many Route ${routeID} trips were Dropped / not observed each day, and which scheduled start hours were worst?`,
        `Show scheduled arrivals for Route ${routeID} from 7:00 to 9:00 a.m.`,
      ]
    : [
        "How did AC Transit perform overall?",
        "Which routes had the most Dropped / not observed trips?",
        "Compare On Time Service Delivered for routes 51A and 72.",
      ];
  const wrap = document.getElementById("suggestions");
  suggestions.forEach(text => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "suggestion";
    button.textContent = text;
    button.addEventListener("click", () => {
      question.value = text;
      question.focus();
      updateFormState();
    });
    wrap.appendChild(button);
  });
}

async function refreshStatus() {
  if (!config?.api_url) return;
  try {
    const response = await fetch(`${config.api_url}/v1/status`, { cache: "no-store" });
    if (!response.ok) throw new Error("Status request failed.");
    status = await response.json();
    renderStatus();
  } catch (_error) {
    status = null;
    statusDot.className = "status-dot offline";
    limitTitle.textContent = "Analysis service is temporarily unreachable";
    limitCopy.textContent = "Questions remain disabled until the service reports that it is ready.";
  }
  updateFormState();
}

function renderStatus() {
  if (!status) return;
  const active = status.active_queries ?? 0;
  const max = status.max_active_queries ?? config.max_active_queries ?? 2;
  const cooldown = status.cooldown_seconds ?? config.cooldown_seconds ?? 20;
  const full = active >= max;
  const dailyExhausted = status.global_questions_remaining_today === 0;
  statusDot.className = `status-dot ${status.enabled && !full && !dailyExhausted ? "ready" : full ? "busy" : "offline"}`;
  if (!status.enabled) {
    limitTitle.textContent = "Public analysis is temporarily disabled";
  } else if (dailyExhausted) {
    limitTitle.textContent = "Today's shared analysis budget has been used";
  } else {
    limitTitle.textContent = `${active} of ${max} concurrent visitor slots currently in use`;
  }
  const shared = status.global_questions_remaining_today;
  const sharedCopy = Number.isInteger(shared) ? ` ${shared} questions remain in today's shared budget.` : "";
  const followUps = status.max_follow_ups ?? config.max_follow_ups ?? 3;
  limitCopy.textContent = `To control misuse and model/database costs, each visitor can submit one question every ${cooldown} seconds. Each answer supports up to ${followUps} follow-ups, and every turn counts toward this browser's ${status.max_session_daily ?? 10} questions per day.${sharedCopy}`;
}

function updateFormState() {
  const max = config?.max_prompt_chars || 500;
  document.getElementById("character-count").textContent = `${question.value.length} / ${max}`;
  const remaining = secondsUntil(nextAllowedAt);
  const full = status && status.active_queries >= status.max_active_queries;
  const dailyExhausted = status?.global_questions_remaining_today === 0;
  const threadExhausted = Boolean(parentRequestID) && followUpsRemaining <= 0;
  const ready = config?.enabled && status?.enabled && status?.accepting && !full && remaining <= 0 && !submitting && !threadExhausted;
  askButton.disabled = !ready || question.value.trim().length < 3;
  if (submitting) {
    askButton.textContent = "Analyzing…";
  } else if (threadExhausted) {
    askButton.textContent = "Follow-up limit reached";
  } else if (remaining > 0) {
    askButton.textContent = `Ask again in ${remaining}s`;
  } else if (full) {
    askButton.textContent = "Analysis slots busy";
  } else if (dailyExhausted) {
    askButton.textContent = "Daily budget reached";
  } else if (status && !status.enabled) {
    askButton.textContent = "Analysis unavailable";
  } else {
    askButton.textContent = parentRequestID ? "Ask follow-up" : "Ask";
  }
  renderThreadState();
}

async function submitQuestion(event) {
  event.preventDefault();
  if (askButton.disabled || !config?.api_url) return;
  hideMessage();
  submitting = true;
  updateFormState();
  const requestID = crypto.randomUUID();
  const prompt = question.value.trim();
  try {
    const response = await fetch(`${config.api_url}/v1/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        prompt,
        session_id: sessionID,
        request_id: requestID,
        parent_request_id: parentRequestID,
        scope: { route_id: routeID || null, week_end: weekEnd || null },
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      if (payload.retry_after_seconds) {
        nextAllowedAt = new Date(Date.now() + payload.retry_after_seconds * 1000);
      }
      if (payload.code === "follow_up_limit") followUpsRemaining = 0;
      if (payload.code === "thread_unavailable") {
        parentRequestID = null;
        followUpsRemaining = null;
      }
      throw new Error(payload.message || "The analysis request failed.");
    }
    nextAllowedAt = payload.next_allowed_at ? new Date(payload.next_allowed_at) : null;
    status = {
      ...(status || {}),
      active_queries: Math.max(0, (payload.active_queries || 1) - 1),
      max_active_queries: payload.max_active_queries,
      cooldown_seconds: payload.cooldown_seconds,
      enabled: true,
    };
    parentRequestID = payload.request_id;
    followUpsRemaining = payload.follow_ups_remaining ?? 0;
    threadQuestions.push(prompt);
    question.value = "";
    renderThreadState();
    renderResponse(payload);
    renderStatus();
    responseSection.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (error) {
    showMessage(error.message);
  } finally {
    submitting = false;
    updateFormState();
    refreshStatus();
  }
}

function renderResponse(payload) {
  responseSection.hidden = false;
  renderQuestionHistory();
  renderTimeframes(payload.timeframes || []);
  renderAnswer(payload.answer || "No answer was returned.");
  renderWarnings(payload.warnings || []);
  renderMaps(payload.maps || []);
  renderCharts(payload.charts || []);
  renderTables(payload.tables || []);
  renderSources(payload.sources || []);
}

function renderQuestionHistory() {
  const list = document.getElementById("question-history");
  list.replaceChildren();
  threadQuestions.forEach((text, index) => {
    const item = document.createElement("li");
    const label = document.createElement("span");
    label.className = "thread-question-label";
    label.textContent = `Question ${index + 1}`;
    const prompt = document.createElement("span");
    prompt.className = "thread-question-text";
    prompt.textContent = text;
    item.append(label, prompt);
    if (index === threadQuestions.length - 1) {
      const current = document.createElement("span");
      current.className = "thread-current";
      current.textContent = "Latest";
      item.appendChild(current);
    }
    list.appendChild(item);
  });
}

function renderTimeframes(timeframes) {
  const wrap = document.getElementById("timeframes");
  wrap.replaceChildren();
  const timeframe = timeframes[0];
  if (!timeframe) return;
  const note = document.createElement("div");
  note.className = `timeframe-note ${timeframe.assumed ? "" : "explicit"}`;
  note.textContent = timeframe.disclosure || timeframeLabel(timeframe);
  wrap.appendChild(note);
}

function renderAnswer(text) {
  const wrap = document.getElementById("answer");
  wrap.replaceChildren();
  String(text).split(/\n\s*\n/).filter(Boolean).forEach(paragraph => {
    const p = document.createElement("p");
    p.textContent = paragraph.trim();
    wrap.appendChild(p);
  });
}

function renderWarnings(items) {
  const wrap = document.getElementById("warnings");
  wrap.replaceChildren();
  items.forEach(text => {
    const warning = document.createElement("div");
    warning.className = "warning";
    warning.textContent = text;
    wrap.appendChild(warning);
  });
}

function renderMaps(items) {
  mapInstances.splice(0).forEach(instance => instance.remove());
  const wrap = document.getElementById("maps");
  wrap.replaceChildren();
  items.slice(0, 3).forEach((spec, index) => {
    const card = document.createElement("article");
    card.className = "map-card";
    const title = document.createElement("h3");
    title.textContent = spec.title || "Route performance map";
    const description = document.createElement("p");
    description.className = "map-description";
    description.textContent = spec.description || "";
    const container = document.createElement("div");
    container.className = "analysis-map";
    container.setAttribute("role", "img");
    container.setAttribute("aria-label", spec.title || "Route performance map");
    const legend = buildMapLegend(spec.style || {});
    card.append(title, description, container, legend);
    wrap.appendChild(card);

    if (typeof mapboxgl === "undefined" || typeof MAPBOX_TOKEN === "undefined") {
      container.textContent = "The map renderer is unavailable.";
      container.classList.add("map-unavailable");
      return;
    }
    mapboxgl.accessToken = MAPBOX_TOKEN;
    const instance = new mapboxgl.Map({
      container,
      style: "mapbox://styles/mapbox/light-v11",
      center: [-122.2, 37.78],
      zoom: 10,
      attributionControl: true,
    });
    instance.addControl(new mapboxgl.NavigationControl({ showCompass: false }), "bottom-right");
    instance.on("load", () => initializeAnalysisMap(instance, spec, index));
    mapInstances.push(instance);
  });
}

function initializeAnalysisMap(instance, spec, mapIndex) {
  const sourceID = `analysis-map-source-${mapIndex}`;
  const routeLayer = `analysis-map-route-${mapIndex}`;
  const segmentLayer = `analysis-map-segments-${mapIndex}`;
  const stopLayer = `analysis-map-stops-${mapIndex}`;
  const style = spec.style || {};
  const target = style.target === "segment" ? "segment" : "stop";
  instance.addSource(sourceID, { type: "geojson", data: spec.geojson });
  instance.addLayer({
    id: routeLayer,
    type: "line",
    source: sourceID,
    filter: ["==", ["get", "feature_kind"], "route"],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: { "line-color": "#40515d", "line-width": 3.5, "line-opacity": 0.55 },
  });
  instance.addLayer({
    id: segmentLayer,
    type: "line",
    source: sourceID,
    filter: ["==", ["get", "feature_kind"], "segment"],
    layout: { "line-cap": "round", "line-join": "round" },
    paint: {
      "line-color": target === "segment" ? mapColorExpression(style) : "#40515d",
      "line-width": target === "segment" ? 7 : 3,
      "line-opacity": target === "segment" ? 0.9 : 0.5,
    },
  });
  instance.addLayer({
    id: stopLayer,
    type: "circle",
    source: sourceID,
    filter: ["==", ["get", "feature_kind"], "stop"],
    paint: {
      "circle-radius": target === "stop" ? 7 : 4.5,
      "circle-color": target === "stop" ? mapColorExpression(style) : "#ffffff",
      "circle-opacity": 0.92,
      "circle-stroke-color": "#26343d",
      "circle-stroke-width": target === "stop" ? 1.25 : 1.5,
    },
  });

  const interactiveLayer = target === "segment" ? segmentLayer : stopLayer;
  const popup = new mapboxgl.Popup({ closeButton: false, closeOnClick: false, maxWidth: "280px" });
  instance.on("mouseenter", interactiveLayer, event => {
    instance.getCanvas().style.cursor = "pointer";
    const feature = event.features?.[0];
    if (!feature) return;
    popup.setLngLat(event.lngLat)
      .setDOMContent(buildMapPopup(feature.properties || {}, spec.popup_fields || []))
      .addTo(instance);
  });
  instance.on("mouseleave", interactiveLayer, () => {
    instance.getCanvas().style.cursor = "";
    popup.remove();
  });
  fitAnalysisMap(instance, spec.geojson);
}

function mapColorExpression(style) {
  const property = style.property || "metric_value";
  const stops = (style.stops || []).flatMap(stop => [Number(stop.value), stop.color]);
  if (stops.length < 4) return style.no_data_color || "#a8b2bd";
  return [
    "case",
    ["==", ["get", "has_metric"], true],
    ["interpolate", ["linear"], ["to-number", ["get", property]], ...stops],
    style.no_data_color || "#a8b2bd",
  ];
}

function buildMapLegend(style) {
  const legend = document.createElement("div");
  legend.className = "map-legend";
  const label = document.createElement("strong");
  label.textContent = style.value_label || "Performance";
  legend.appendChild(label);
  (style.stops || []).forEach(stop => {
    const item = document.createElement("span");
    const swatch = document.createElement("i");
    swatch.style.backgroundColor = stop.color;
    item.append(swatch, document.createTextNode(stop.label || String(stop.value)));
    legend.appendChild(item);
  });
  return legend;
}

function buildMapPopup(properties, fields) {
  const wrap = document.createElement("div");
  wrap.className = "map-popup";
  fields.forEach((field, index) => {
    const row = document.createElement("div");
    const label = document.createElement(index === 0 ? "strong" : "span");
    label.textContent = field.label;
    const value = document.createElement("span");
    value.textContent = displayValue(properties[field.property]);
    row.append(label, value);
    wrap.appendChild(row);
  });
  return wrap;
}

function fitAnalysisMap(instance, geojson) {
  const bounds = new mapboxgl.LngLatBounds();
  (geojson?.features || []).forEach(feature => extendMapBounds(bounds, feature.geometry?.coordinates));
  if (!bounds.isEmpty()) instance.fitBounds(bounds, { padding: 42, duration: 0, maxZoom: 14 });
}

function extendMapBounds(bounds, coordinates) {
  if (!Array.isArray(coordinates)) return;
  if (coordinates.length >= 2 && coordinates.slice(0, 2).every(value => typeof value === "number")) {
    bounds.extend(coordinates);
    return;
  }
  coordinates.forEach(value => extendMapBounds(bounds, value));
}

function renderCharts(items) {
  charts.splice(0).forEach(chart => chart.destroy());
  const wrap = document.getElementById("charts");
  wrap.replaceChildren();
  const colors = ["#165d8f", "#b13d58", "#1d7a5b", "#9a5a08", "#6b4c9a"];
  const visibleItems = items.slice(0, 3);
  const expandStandardChart = visibleItems.filter(spec => spec.type !== "heatmap").length === 1
    && visibleItems.some(spec => spec.type === "heatmap");
  visibleItems.forEach((spec, chartIndex) => {
    const card = document.createElement("article");
    card.className = "chart-card";
    if (expandStandardChart && spec.type !== "heatmap") card.classList.add("full-width");
    const title = document.createElement("h3");
    title.textContent = spec.title || "Chart";
    card.appendChild(title);
    if (spec.type === "heatmap") {
      card.classList.add("heatmap-card");
      renderHeatmap(card, spec);
      wrap.appendChild(card);
      return;
    }
    const chartWrap = document.createElement("div");
    chartWrap.className = "chart-wrap";
    const canvas = document.createElement("canvas");
    canvas.setAttribute("role", "img");
    canvas.setAttribute("aria-label", spec.title || "Analysis chart");
    chartWrap.appendChild(canvas);
    card.appendChild(chartWrap);
    wrap.appendChild(card);
    const instance = new Chart(canvas, {
      type: spec.type === "bar" ? "bar" : "line",
      data: {
        labels: spec.labels || [],
        datasets: (spec.series || []).map((series, index) => ({
          label: series.name,
          data: series.values,
          borderColor: colors[(chartIndex + index) % colors.length],
          backgroundColor: `${colors[(chartIndex + index) % colors.length]}99`,
          borderWidth: 2,
          pointRadius: spec.type === "bar" ? 0 : 2,
          tension: 0.15,
        })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { position: "bottom" } },
        scales: {
          x: { title: { display: Boolean(spec.x_label), text: spec.x_label } },
          y: { beginAtZero: spec.type === "bar", title: { display: Boolean(spec.y_label), text: spec.y_label } },
        },
      },
    });
    charts.push(instance);
  });
}

function renderHeatmap(card, spec) {
  const note = document.createElement("p");
  note.className = "heatmap-note";
  note.textContent = spec.note || "Cell color represents severity.";

  const cellMap = new Map((spec.cells || []).map(cell => [`${cell.y}\u0000${cell.x}`, cell]));
  const scroll = document.createElement("div");
  scroll.className = "heatmap-scroll";
  const table = document.createElement("table");
  table.className = "heatmap";
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  const corner = document.createElement("th");
  corner.scope = "col";
  corner.textContent = "Weekday";
  headRow.appendChild(corner);
  (spec.x_labels || []).forEach(label => {
    const th = document.createElement("th");
    th.scope = "col";
    th.textContent = String(label).replace(":00", "");
    th.title = String(label);
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");
  (spec.y_labels || []).forEach(yLabel => {
    const row = document.createElement("tr");
    const th = document.createElement("th");
    th.scope = "row";
    th.textContent = String(yLabel).slice(0, 3);
    th.title = String(yLabel);
    row.appendChild(th);
    (spec.x_labels || []).forEach(xLabel => {
      const cell = cellMap.get(`${yLabel}\u0000${xLabel}`) || {};
      const td = document.createElement("td");
      const scheduled = Number(cell.scheduled || 0);
      const dropped = Number(cell.dropped || 0);
      const value = cell.value == null ? null : Number(cell.value);
      if (!scheduled || !Number.isFinite(value)) {
        td.className = "no-service";
        td.textContent = "–";
        td.setAttribute("aria-label", `${yLabel} ${xLabel}: no scheduled trips`);
      } else {
        const intensity = Math.max(0, Math.min(1, value / 100));
        td.style.backgroundColor = `rgba(177, 61, 88, ${0.08 + intensity * 0.82})`;
        if (intensity >= 0.62) td.style.color = "#fff";
        td.textContent = String(dropped);
        const detail = `${yLabel} ${xLabel}: ${dropped} dropped of ${scheduled} scheduled trips (${value.toFixed(1)}%)`;
        td.title = detail;
        td.tabIndex = 0;
        td.setAttribute("aria-label", detail);
      }
      row.appendChild(td);
    });
    tbody.appendChild(row);
  });
  table.append(thead, tbody);
  scroll.appendChild(table);

  const legend = document.createElement("div");
  legend.className = "heatmap-legend";
  legend.innerHTML = '<span>Lower dropped share</span><i aria-hidden="true"></i><span>Higher dropped share</span>';
  card.append(note, scroll, legend);
}

function renderTables(items) {
  const wrap = document.getElementById("tables");
  wrap.replaceChildren();
  items.slice(0, 2).forEach(spec => {
    const section = document.createElement("section");
    section.className = "result-table";
    const title = document.createElement("h3");
    title.textContent = spec.title || "Data";
    const scroll = document.createElement("div");
    scroll.className = "table-scroll";
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    (spec.columns || []).forEach(column => {
      const th = document.createElement("th");
      th.textContent = column.label;
      headRow.appendChild(th);
    });
    thead.appendChild(headRow);
    const tbody = document.createElement("tbody");
    (spec.rows || []).forEach(row => {
      const tr = document.createElement("tr");
      (spec.columns || []).forEach(column => {
        const td = document.createElement("td");
        td.textContent = displayValue(row?.[column.key]);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.append(thead, tbody);
    scroll.appendChild(table);
    section.append(title, scroll);
    wrap.appendChild(section);
  });
}

function renderSources(items) {
  const wrap = document.getElementById("sources-wrap");
  const list = document.getElementById("sources");
  list.replaceChildren();
  items.forEach(source => {
    const li = document.createElement("li");
    li.textContent = sourceLabel(source);
    list.appendChild(li);
  });
  wrap.hidden = items.length === 0;
}

function clearResponse() {
  responseSection.hidden = true;
  charts.splice(0).forEach(chart => chart.destroy());
  mapInstances.splice(0).forEach(instance => instance.remove());
  document.getElementById("maps").replaceChildren();
  parentRequestID = null;
  followUpsRemaining = null;
  threadQuestions.splice(0);
  renderQuestionHistory();
  question.value = "";
  updateFormState();
  question.focus();
  document.getElementById("ask-heading").scrollIntoView({ behavior: "smooth", block: "start" });
}

function renderThreadState() {
  const state = document.getElementById("thread-state");
  const label = document.getElementById("question-label");
  const max = config?.max_follow_ups ?? status?.max_follow_ups ?? 3;
  const composerHome = parentRequestID ? followupFormHome : questionFormHome;
  if (composer.parentElement !== composerHome) composerHome.appendChild(composer);
  if (!parentRequestID) {
    label.textContent = "Question";
    question.placeholder = "For example: Compare On Time Service Delivered for routes 51A and 72.";
    state.textContent = `Each answer can be followed by up to ${max} clarifying questions.`;
    return;
  }
  label.textContent = "Follow-up question";
  question.placeholder = "Ask a clarifying question about the answer above.";
  state.textContent = followUpsRemaining > 0
    ? `${followUpsRemaining} follow-up${followUpsRemaining === 1 ? "" : "s"} remaining in this thread. Choose New question to reset the context.`
    : "Follow-up limit reached. Choose New question to start with fresh context.";
}

function sourceLabel(source) {
  if (source.kind === "daily_stats") return `Published daily reports · ${source.start_date} through ${source.end_date} · ${source.object_count} objects`;
  if (source.kind === "weekly_stats") return `Published weekly report · ${source.start_date} through ${source.end_date}`;
  if (source.kind === "bigquery") return `Deduplicated BigQuery observations · ${source.start_date || source.service_date || "bounded period"}`;
  if (source.kind === "gtfs") return `Current or archived GTFS schedule · service date ${source.service_date}`;
  if (source.kind === "processed_gtfs") return `Current processed GTFS geometry · Route ${source.route_id || routeID || "data"}`;
  if (source.kind === "methodology_registry") return "Metric methodology registry";
  return JSON.stringify(source);
}

function timeframeLabel(timeframe) {
  const dates = timeframe.start_date === timeframe.end_date
    ? timeframe.start_date
    : `${timeframe.start_date} through ${timeframe.end_date}`;
  return `Timeframe: ${dates}.`;
}

function displayValue(value) {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value)) {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.valueOf())) {
      return parsed.toLocaleString([], { dateStyle: "medium", timeStyle: "short", timeZone: "America/Los_Angeles" });
    }
  }
  return String(value);
}

function secondsUntil(value) {
  if (!value) return 0;
  return Math.max(0, Math.ceil((value.getTime() - Date.now()) / 1000));
}

function showMessage(text) {
  message.textContent = text;
  message.hidden = false;
}

function hideMessage() {
  message.hidden = true;
  message.textContent = "";
}

function setUnavailable(text) {
  statusDot.className = "status-dot offline";
  limitTitle.textContent = "Analysis service is not available";
  limitCopy.textContent = text;
  showMessage(text);
}

function getSessionID() {
  const key = "actransit-analysis-session-v1";
  let value = localStorage.getItem(key);
  if (!value) {
    value = crypto.randomUUID();
    localStorage.setItem(key, value);
  }
  return value;
}

function sanitizeRoute(value) {
  const route = (value || "").trim().toUpperCase();
  return /^[A-Z0-9-]{1,16}$/.test(route) ? route : "";
}

function sanitizeDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value || "") ? value : "";
}

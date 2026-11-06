const CONFIG_URL = "https://storage.googleapis.com/transit-203605-actransit-cache/analysis/config.json";
const params = new URLSearchParams(window.location.search);
const routeID = sanitizeRoute(params.get("route_id"));
const weekEnd = sanitizeDate(params.get("week_end"));
const form = document.getElementById("ask-form");
const question = document.getElementById("question");
const askButton = document.getElementById("ask-button");
const message = document.getElementById("message");
const responseSection = document.getElementById("response");
const statusDot = document.getElementById("status-dot");
const limitTitle = document.getElementById("limit-title");
const limitCopy = document.getElementById("limit-copy");
const charts = [];
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
        `Which hours had the worst median delay on Route ${routeID}?`,
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
    question.value = "";
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
  renderTimeframes(payload.timeframes || []);
  renderAnswer(payload.answer || "No answer was returned.");
  renderWarnings(payload.warnings || []);
  renderCharts(payload.charts || []);
  renderTables(payload.tables || []);
  renderSources(payload.sources || []);
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

function renderCharts(items) {
  charts.splice(0).forEach(chart => chart.destroy());
  const wrap = document.getElementById("charts");
  wrap.replaceChildren();
  const colors = ["#165d8f", "#b13d58", "#1d7a5b", "#9a5a08", "#6b4c9a"];
  items.slice(0, 3).forEach((spec, chartIndex) => {
    const card = document.createElement("article");
    card.className = "chart-card";
    const title = document.createElement("h3");
    title.textContent = spec.title || "Chart";
    const chartWrap = document.createElement("div");
    chartWrap.className = "chart-wrap";
    const canvas = document.createElement("canvas");
    canvas.setAttribute("role", "img");
    canvas.setAttribute("aria-label", spec.title || "Analysis chart");
    chartWrap.appendChild(canvas);
    card.append(title, chartWrap);
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
  parentRequestID = null;
  followUpsRemaining = null;
  question.value = "";
  updateFormState();
  question.focus();
}

function renderThreadState() {
  const state = document.getElementById("thread-state");
  const label = document.getElementById("question-label");
  const max = config?.max_follow_ups ?? status?.max_follow_ups ?? 3;
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

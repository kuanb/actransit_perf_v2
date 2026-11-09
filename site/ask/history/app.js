const BUCKET = "transit-203605-actransit-cache";
const PREFIX = "analysis/public-chats";
const MAX_RANGE_DAYS = 31;
const MAX_TURNS = 500;
const LIST_PAGE_SIZE = 1000;
const FETCH_CONCURRENCY = 10;

const form = document.getElementById("history-form");
const startInput = document.getElementById("start-date");
const endInput = document.getElementById("end-date");
const searchInput = document.getElementById("search");
const loadButton = document.getElementById("load-button");
const statusCopy = document.getElementById("history-status");
const archiveSummary = document.getElementById("archive-summary");
const visibleCount = document.getElementById("visible-count");
const threadList = document.getElementById("thread-list");
const emptyState = document.getElementById("empty-state");

let loadedThreads = [];

initializeDates();
form.addEventListener("submit", loadHistory);
searchInput.addEventListener("input", renderFilteredThreads);
loadHistory(new Event("submit"));

function initializeDates() {
  const today = new Date();
  const start = new Date(today);
  start.setUTCDate(start.getUTCDate() - 6);
  const todayValue = utcDateValue(today);
  startInput.value = utcDateValue(start);
  endInput.value = todayValue;
  startInput.max = todayValue;
  endInput.max = todayValue;
}

async function loadHistory(event) {
  event.preventDefault();
  const startDate = startInput.value;
  const endDate = endInput.value;
  const validationError = validateRange(startDate, endDate);
  if (validationError) {
    showStatus(validationError, true);
    return;
  }

  setLoading(true);
  showStatus("Reading the public bucket…");
  try {
    const names = await listObjectNames(startDate, endDate);
    const selectedNames = names.sort().reverse().slice(0, MAX_TURNS);
    const records = await mapWithConcurrency(selectedNames, FETCH_CONCURRENCY, fetchRecord);
    loadedThreads = groupThreads(records.filter(isPublicRecord));
    renderFilteredThreads();
    const limited = names.length > MAX_TURNS ? ` Showing the ${MAX_TURNS} most recent turns.` : "";
    archiveSummary.textContent = `${loadedThreads.length} conversation${loadedThreads.length === 1 ? "" : "s"} · ${records.length} turn${records.length === 1 ? "" : "s"}.${limited}`;
    showStatus(`Loaded directly from gs://${BUCKET}/${PREFIX}/.`);
  } catch (error) {
    loadedThreads = [];
    renderFilteredThreads();
    showStatus(`The public archive could not be loaded: ${error.message}`, true);
  } finally {
    setLoading(false);
  }
}

function validateRange(startDate, endDate) {
  if (!startDate || !endDate) return "Choose both a start and end date.";
  if (startDate > endDate) return "The start date must be on or before the end date.";
  const span = Math.round((parseDate(endDate) - parseDate(startDate)) / 86400000) + 1;
  if (span > MAX_RANGE_DAYS) return `Choose a range of ${MAX_RANGE_DAYS} days or fewer.`;
  return "";
}

async function listObjectNames(startDate, endDate) {
  const names = [];
  let pageToken = "";
  const endExclusive = utcDateValue(addDays(parseDate(endDate), 1));
  do {
    const params = new URLSearchParams({
      prefix: `${PREFIX}/`,
      startOffset: `${PREFIX}/date=${startDate}/`,
      endOffset: `${PREFIX}/date=${endExclusive}/`,
      maxResults: String(LIST_PAGE_SIZE),
    });
    if (pageToken) params.set("pageToken", pageToken);
    const response = await fetch(
      `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(BUCKET)}/o?${params}`,
      { cache: "no-store" },
    );
    if (!response.ok) throw new Error(`bucket listing returned ${response.status}`);
    const payload = await response.json();
    for (const item of payload.items || []) {
      if (item.name.endsWith(".json")) names.push(item.name);
    }
    pageToken = payload.nextPageToken || "";
  } while (pageToken);
  return names;
}

async function fetchRecord(name) {
  const objectPath = name.split("/").map(encodeURIComponent).join("/");
  const response = await fetch(`https://storage.googleapis.com/${BUCKET}/${objectPath}`);
  if (!response.ok) throw new Error(`conversation object returned ${response.status}`);
  return response.json();
}

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;
  async function run() {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await worker(items[index]);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, run));
  return results;
}

function isPublicRecord(record) {
  return record
    && record.schema_version === 1
    && typeof record.conversation_id === "string"
    && typeof record.question === "string"
    && record.response
    && typeof record.response.answer === "string";
}

function groupThreads(records) {
  const grouped = new Map();
  for (const record of records) {
    if (!grouped.has(record.conversation_id)) grouped.set(record.conversation_id, []);
    grouped.get(record.conversation_id).push(record);
  }
  return [...grouped.entries()]
    .map(([id, turns]) => ({
      id,
      turns: turns.sort((a, b) => a.turn_index - b.turn_index),
      latestAt: turns.reduce((latest, turn) => turn.published_at > latest ? turn.published_at : latest, ""),
    }))
    .sort((a, b) => b.latestAt.localeCompare(a.latestAt));
}

function renderFilteredThreads() {
  const query = searchInput.value.trim().toLowerCase();
  const filtered = loadedThreads.filter((thread) => {
    if (!query) return true;
    return thread.turns.some((turn) => [
      turn.question,
      turn.response.answer,
      turn.scope?.route_id,
      ...(turn.response.maps || []).map((map) => map.title),
      ...(turn.response.charts || []).map((chart) => chart.title),
      ...(turn.response.tables || []).map((table) => table.title),
    ].filter(Boolean).join(" ").toLowerCase().includes(query));
  });

  threadList.replaceChildren(...filtered.map((thread, index) => renderThread(thread, index === 0)));
  emptyState.hidden = filtered.length > 0;
  visibleCount.textContent = `${filtered.length} shown`;
}

function renderThread(thread, open) {
  const card = document.createElement("details");
  card.className = "thread-card";
  card.open = open;

  const summary = document.createElement("summary");
  const summaryCopy = document.createElement("div");
  const meta = document.createElement("div");
  meta.className = "thread-meta";
  meta.append(
    textSpan(formatTimestamp(thread.latestAt)),
    textSpan(`${thread.turns.length} turn${thread.turns.length === 1 ? "" : "s"}`),
  );
  const routeID = thread.turns.find((turn) => turn.scope?.route_id)?.scope.route_id;
  if (routeID) meta.append(chip(`Route ${routeID}`, "route-chip"));
  const title = document.createElement("p");
  title.className = "thread-title";
  title.textContent = thread.turns[0].question;
  summaryCopy.append(meta, title);
  summary.append(summaryCopy);

  const turns = document.createElement("div");
  turns.className = "turns";
  turns.append(...thread.turns.map(renderTurn));
  card.append(summary, turns);
  return card;
}

function renderTurn(turn) {
  const article = document.createElement("article");
  article.className = "turn";
  const index = document.createElement("div");
  index.className = "turn-index";
  index.textContent = `Q${turn.turn_index}`;

  const body = document.createElement("div");
  const meta = document.createElement("div");
  meta.className = "turn-meta";
  meta.append(textSpan(formatTimestamp(turn.published_at)));
  if (turn.scope?.route_id) meta.append(chip(`Route ${turn.scope.route_id}`, "route-chip"));

  const questionLabel = label("Question", "question-label");
  const question = document.createElement("p");
  question.className = "turn-question";
  question.textContent = turn.question;

  const answerLabel = label("Answer", "answer-label");
  const answer = document.createElement("div");
  answer.className = "turn-answer";
  for (const paragraphText of turn.response.answer.split(/\n\s*\n/)) {
    const paragraph = document.createElement("p");
    paragraph.textContent = paragraphText;
    answer.append(paragraph);
  }

  body.append(meta, questionLabel, question, answerLabel, answer);
  const timeframe = turn.response.timeframes?.[0]?.disclosure;
  if (timeframe) {
    const timeframeCopy = document.createElement("div");
    timeframeCopy.className = "timeframe-copy";
    timeframeCopy.textContent = timeframe;
    body.append(timeframeCopy);
  }

  const details = document.createElement("div");
  details.className = "turn-details";
  for (const map of turn.response.maps || []) details.append(chip(`Map · ${map.title || "Route performance"}`, "artifact-chip"));
  for (const chart of turn.response.charts || []) details.append(chip(`Chart · ${chart.title || chart.type}`, "artifact-chip"));
  for (const table of turn.response.tables || []) details.append(chip(`Table · ${table.title || "Data"}`, "artifact-chip"));
  if (details.childElementCount) body.append(details);

  article.append(index, body);
  return article;
}

function chip(text, className) {
  const element = document.createElement("span");
  element.className = className;
  element.textContent = text;
  return element;
}

function textSpan(text) {
  const element = document.createElement("span");
  element.textContent = text;
  return element;
}

function label(text, className) {
  const element = document.createElement("p");
  element.className = className;
  element.textContent = text;
  return element;
}

function setLoading(loading) {
  loadButton.disabled = loading;
  loadButton.textContent = loading ? "Loading…" : "Load conversations";
}

function showStatus(message, error = false) {
  statusCopy.textContent = message;
  statusCopy.className = `history-status${error ? " error" : ""}`;
}

function formatTimestamp(value) {
  if (!value) return "Unknown time";
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "UTC",
    timeZoneName: "short",
  }).format(new Date(value));
}

function utcDateValue(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseDate(value) {
  const [year, month, day] = value.split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}

function addDays(date, days) {
  const updated = new Date(date);
  updated.setUTCDate(updated.getUTCDate() + days);
  return updated;
}

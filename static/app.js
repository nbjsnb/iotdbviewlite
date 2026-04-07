const $ = (id) => document.getElementById(id);

let treeData = {};
let allPoints = [];
let pointTypes = {};
let pointPaths = {};
let openPointMenuEl = null;
let pointsPage = 1;
let pointsPageSize = 1000;
let pointsTotal = 0;
let pointsTotalPages = 1;
let pointsKeyword = "";
let pointsReqSeq = 0;
let selectedDevice = "";
let selectedPoints = new Set();
let lastResult = { columns: [], rows: [] };
let trendState = {
  fullMin: null,
  fullMax: null,
  viewMin: null,
  viewMax: null,
  dragStartX: null,
};
let trendRenderCtx = null;
let lastQueryWindow = null;
let rightView = "query";
let currentInfoPath = "";
let currentInfoMode = "database";
let currentDbInfo = null;
let currentPointInfo = null;
let consoleMode = false;
let iotdbVersion = "";

const themes = {
  compact_blue: {
    "--bg-grad-a": "#ecf4f7",
    "--bg-grad-b": "#f7f9fb",
    "--panel": "#ffffff",
    "--panel-soft": "#f8fbfd",
    "--panel-strong": "#fbfdff",
    "--line": "#d9e1e5",
    "--text": "#1f2a30",
    "--muted": "#61717a",
    "--accent": "#0b84a5",
    "--accent-soft": "rgba(11,132,165,0.16)",
    "--splitter-a": "#e8eef2",
    "--splitter-b": "#dce6ed",
    "--splitter-hover-a": "#d2e2ee",
    "--splitter-hover-b": "#c7d9e7",
    "--tab-bg": "#eef3f7",
    "--tab-text": "#31434d",
    "--tab-line": "#c8d6df",
    "--tree-hover": "#eef5f9",
    "--tree-leaf": "#1d4756",
    "--tree-active": "#d9edf8",
    "--trend-bg": "#ffffff",
    "--trend-axis": "#8aa0ad",
    "--trend-grid": "#edf2f6",
    "--trend-label": "#5d727d",
    "--trend-tip-bg": "rgba(16,25,31,0.92)",
    "--trend-tip-text": "#f3f8fb",
    "--trend-tip-line": "#2d4450",
    "--series-colors": "#0b84a5,#f6a021,#35a35d,#db3a34,#7453e8,#2f6fdd",
    "--scroll-track": "#e7edf2",
    "--scroll-thumb": "#b7c5d0",
    "--scroll-thumb-hover": "#9fb0bc",
    "--btn-text": "#ffffff",
  },
  light_clean: {
    "--bg-grad-a": "#fafafa",
    "--bg-grad-b": "#f2f2f2",
    "--panel": "#ffffff",
    "--line": "#d6d6d6",
    "--text": "#1f1f1f",
    "--muted": "#606060",
    "--accent": "#2866c8",
    "--btn-text": "#ffffff",
  },
  mint_pro: {
    "--bg-grad-a": "#edf7f3",
    "--bg-grad-b": "#f7fbf9",
    "--panel": "#ffffff",
    "--line": "#cfe3da",
    "--text": "#1d3029",
    "--muted": "#5c746a",
    "--accent": "#1c9b7a",
    "--btn-text": "#ffffff",
  },
  dark_graph: {
    "--bg-grad-a": "#0f1722",
    "--bg-grad-b": "#0a111a",
    "--panel": "#131b26",
    "--panel-soft": "#111a24",
    "--panel-strong": "#0f1823",
    "--line": "#223447",
    "--text": "#e6eef8",
    "--muted": "#8ca3b8",
    "--accent": "#22adf6",
    "--accent-soft": "rgba(34,173,246,0.2)",
    "--splitter-a": "#182838",
    "--splitter-b": "#132231",
    "--splitter-hover-a": "#1e3347",
    "--splitter-hover-b": "#193044",
    "--tab-bg": "#152334",
    "--tab-text": "#aac2d6",
    "--tab-line": "#25405a",
    "--tree-hover": "#17314a",
    "--tree-leaf": "#48b6df",
    "--tree-active": "#1b3d59",
    "--trend-bg": "#0e1823",
    "--trend-axis": "#5f7990",
    "--trend-grid": "#1b2a3a",
    "--trend-label": "#8da3b6",
    "--trend-tip-bg": "rgba(6,12,18,0.95)",
    "--trend-tip-text": "#d8e8f6",
    "--trend-tip-line": "#27445f",
    "--series-colors": "#00c9ff,#34d399,#fbbf24,#fb7185,#a78bfa,#60a5fa",
    "--scroll-track": "#0e1a27",
    "--scroll-thumb": "#2b4054",
    "--scroll-thumb-hover": "#36536d",
    "--btn-text": "#eaf6ff",
  },
};

const themeVars = [...new Set(Object.values(themes).flatMap((cfg) => Object.keys(cfg)))];

function cssVar(name, fallback = "") {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

function applyTheme(name) {
  const t = themes[name] || themes.compact_blue;
  themeVars.forEach((k) => {
    const v = t[k] ?? themes.compact_blue[k] ?? "";
    if (v) document.documentElement.style.setProperty(k, v);
    else document.documentElement.style.removeProperty(k);
  });
  localStorage.setItem("iotdb_theme", name);
}

function setMsg(text, type = "info") {
  const el = $("msg");
  el.textContent = text || "";
  el.className = "";
  el.classList.add(`msg-${type}`);
}

function setDbInfoMsg(text, type = "info") {
  const el = $("dbInfoMsg");
  if (!el) return;
  el.textContent = text || "";
  el.className = "";
  el.classList.add(`msg-${type}`);
}

function parseVersionMajor(ver) {
  const m = String(ver || "").match(/(\d+)\./);
  if (!m) return null;
  return Number(m[1]);
}

function updateTtlCompatHint() {
  const el = $("ttlCompatHint");
  if (!el) return;
  const major = parseVersionMajor(iotdbVersion);

  if (currentInfoMode === "device") {
    if (major != null && major < 2) {
      el.textContent = "Note: IoTDB < 2.0 may not support device-level TTL strictly. Rule may fallback to upper path/database.";
    } else {
      el.textContent = "Device-level TTL is expected to work on IoTDB 2.0+; verify with SHOW ALL TTL after apply.";
    }
    return;
  }

  if (currentInfoMode === "path") {
    el.textContent = "Path-level TTL support varies by version/deployment. Verify effective scope with SHOW ALL TTL.";
    return;
  }

  if (currentInfoMode === "point") {
    el.textContent = "Point TTL is inherited from upper path/device/database in most versions.";
    return;
  }

  el.textContent = "Database-level TTL is generally stable across versions.";
}
async function fetchServerVersion() {
  try {
    const data = await postJson("/api/version", connPayload());
    iotdbVersion = data.version || "";
    $("serverVersion").textContent = `IoTDB: ${data.version || "unknown"}`;
    updateTtlCompatHint();
  } catch (_) {
    iotdbVersion = "";
    $("serverVersion").textContent = "IoTDB: unavailable";
    updateTtlCompatHint();
  }
}

function setConsoleMode(on) {
  consoleMode = !!on;
  const panel = $("builderPanel");
  if (panel) panel.classList.toggle("hidden", consoleMode);
  const btn = $("toggleConsoleBtn");
  if (btn) btn.textContent = consoleMode ? "Console: ON" : "Console: OFF";
  const buildBtn = $("buildBtn");
  if (buildBtn) buildBtn.disabled = consoleMode;
}
function switchRightView(view) {
  rightView = view === "dbinfo" ? "dbinfo" : "query";
  const q = $("queryView");
  const d = $("dbInfoView");
  if (!q || !d) return;
  if (rightView === "dbinfo") {
    q.classList.add("hidden");
    d.classList.remove("hidden");
  } else {
    d.classList.add("hidden");
    q.classList.remove("hidden");
  }
}

function fillList(listId, values) {
  const ul = $(listId);
  if (!ul) return;
  ul.innerHTML = "";
  const arr = values || [];
  if (!arr.length) {
    const li = document.createElement("li");
    li.textContent = "(empty)";
    ul.appendChild(li);
    return;
  }
  arr.forEach((v) => {
    const li = document.createElement("li");
    li.textContent = String(v);
    ul.appendChild(li);
  });
}

function ttlMsToText(ttlMs) {
  if (!Number.isFinite(Number(ttlMs)) || Number(ttlMs) <= 0) return "never expire";
  const n = Number(ttlMs);
  const dayMs = 24 * 60 * 60 * 1000;
  if (n % dayMs === 0) return `${Math.floor(n / dayMs)}d`;
  return `${n} ms`;
}

function setInfoMode(mode) {
  const m = mode === "point" ? "point" : (mode === "device" ? "device" : (mode === "path" ? "path" : "database"));
  currentInfoMode = m;

  if (m === "database") {
    $("infoViewTitle").textContent = "Database Info";
    $("infoLabelName").textContent = "Name";
    $("infoLabelMetricA").textContent = "Devices";
    $("infoLabelMetricB").textContent = "Timeseries";
    $("infoListTitleA").textContent = "Sample Devices";
    $("infoListTitleB").textContent = "Sample Timeseries";
  } else if (m === "device") {
    $("infoViewTitle").textContent = "Device Info";
    $("infoLabelName").textContent = "Device";
    $("infoLabelMetricA").textContent = "Database";
    $("infoLabelMetricB").textContent = "Points";
    $("infoListTitleA").textContent = "Sample Points";
    $("infoListTitleB").textContent = "Path Notes";
  } else if (m === "path") {
    $("infoViewTitle").textContent = "Path Info";
    $("infoLabelName").textContent = "Path";
    $("infoLabelMetricA").textContent = "Devices";
    $("infoLabelMetricB").textContent = "Timeseries";
    $("infoListTitleA").textContent = "Child Paths";
    $("infoListTitleB").textContent = "Sample Devices";
  } else {
    $("infoViewTitle").textContent = "Point Info";
    $("infoLabelName").textContent = "Point";
    $("infoLabelMetricA").textContent = "Device";
    $("infoLabelMetricB").textContent = "Data Type";
    $("infoListTitleA").textContent = "Meta";
    $("infoListTitleB").textContent = "Notes";
  }

  $("infoLabelLastWrite").textContent = "Last Write Time";

  const ttlCard = $("ttlSettingsCard");
  const pointCard = $("pointActionCard");
  if (ttlCard) ttlCard.classList.toggle("hidden", m === "point");
  if (pointCard) pointCard.classList.toggle("hidden", m !== "point");

  updateTtlCompatHint();
}

function renderDbInfo(info) {
  currentDbInfo = info || null;
  currentPointInfo = null;
  currentInfoPath = info?.database || "";
  setInfoMode("database");

  $("dbInfoName").textContent = info?.database || "-";
  $("dbInfoTtl").textContent = info?.ttl_text || ttlMsToText(info?.ttl_ms);
  $("dbInfoDeviceCount").textContent = String(info?.device_count ?? "-");
  $("dbInfoTsCount").textContent = String(info?.timeseries_count ?? "-");
  $("dbInfoLastWrite").textContent = info?.last_write_text || "-";
  fillList("dbSampleDevices", info?.sample_devices || []);
  fillList("dbSampleTimeseries", info?.sample_timeseries || []);

  const ttlMs = Number(info?.ttl_ms);
  const dayMs = 24 * 60 * 60 * 1000;
  if (Number.isFinite(ttlMs) && ttlMs > 0 && ttlMs % dayMs === 0) {
    const days = Math.floor(ttlMs / dayMs);
    if ([7, 30, 90, 180, 365].includes(days)) {
      $("dbTtlPreset").value = String(days);
      $("dbTtlDays").value = String(days);
    } else {
      $("dbTtlPreset").value = "custom";
      $("dbTtlDays").value = String(days);
    }
  } else {
    $("dbTtlPreset").value = "never";
  }
}

function renderDeviceInfo(info) {
  currentPointInfo = null;
  currentInfoPath = info?.device || "";
  setInfoMode("device");

  $("dbInfoName").textContent = info?.device || "-";
  $("dbInfoTtl").textContent = info?.ttl_text || ttlMsToText(info?.ttl_ms);
  $("dbInfoDeviceCount").textContent = info?.database || "-";
  $("dbInfoTsCount").textContent = String(info?.point_count ?? "-");
  $("dbInfoLastWrite").textContent = info?.last_write_text || "-";
  fillList("dbSampleDevices", info?.sample_points || []);

  const notes = [];
  if (info?.ttl_source_path) {
    if (info.ttl_source_path === info.device) notes.push(`TTL directly set on: ${info.ttl_source_path}`);
    else notes.push(`TTL inherited from: ${info.ttl_source_path}`);
  } else {
    notes.push("TTL source: none (never expire)");
  }
  fillList("dbSampleTimeseries", notes);
}

function renderPathInfo(info) {
  currentPointInfo = null;
  currentInfoPath = info?.path || "";
  setInfoMode("path");

  $("dbInfoName").textContent = info?.path || "-";
  $("dbInfoTtl").textContent = info?.ttl_text || ttlMsToText(info?.ttl_ms);
  $("dbInfoDeviceCount").textContent = String(info?.device_count ?? "-");
  $("dbInfoTsCount").textContent = String(info?.timeseries_count ?? "-");
  $("dbInfoLastWrite").textContent = info?.last_write_text || "-";
  fillList("dbSampleDevices", info?.child_paths || []);
  fillList("dbSampleTimeseries", info?.sample_devices || []);

  const ttlMs = Number(info?.ttl_ms);
  const dayMs = 24 * 60 * 60 * 1000;
  if (Number.isFinite(ttlMs) && ttlMs > 0 && ttlMs % dayMs === 0) {
    const days = Math.floor(ttlMs / dayMs);
    if ([7, 30, 90, 180, 365].includes(days)) {
      $("dbTtlPreset").value = String(days);
      $("dbTtlDays").value = String(days);
    } else {
      $("dbTtlPreset").value = "custom";
      $("dbTtlDays").value = String(days);
    }
  } else {
    $("dbTtlPreset").value = "never";
  }
}

async function openDbInfo(db) {
  switchRightView("dbinfo");
  setDbInfoMsg(`Loading database info: ${db} ...`, "info");
  try {
    const data = await postJson("/api/db_info", { ...connPayload(), database: db });
    renderDbInfo(data);
    setDbInfoMsg("Database info loaded", "ok");
  } catch (e) {
    setDbInfoMsg(e.message, "err");
  }
}

async function openDeviceInfo(device) {
  switchRightView("dbinfo");
  setDbInfoMsg(`Loading device info: ${device} ...`, "info");
  try {
    const data = await postJson("/api/device_info", { ...connPayload(), device });
    renderDeviceInfo(data);
    setDbInfoMsg("Device info loaded", "ok");
  } catch (e) {
    setDbInfoMsg(e.message, "err");
  }
}

async function openPathInfo(path) {
  switchRightView("dbinfo");
  setDbInfoMsg(`Loading path info: ${path} ...`, "info");
  try {
    const data = await postJson("/api/path_info", { ...connPayload(), path });
    renderPathInfo(data);
    setDbInfoMsg("Path info loaded", "ok");
  } catch (e) {
    setDbInfoMsg(e.message, "err");
  }
}
function renderPointInfo(info) {
  currentPointInfo = info || null;
  currentInfoPath = info?.path || "";
  setInfoMode("point");

  $("dbInfoName").textContent = info?.path || "-";
  $("dbInfoTtl").textContent = info?.ttl_text || ttlMsToText(info?.ttl_ms);
  $("dbInfoDeviceCount").textContent = info?.device || "-";
  $("dbInfoTsCount").textContent = info?.data_type || "UNKNOWN";
  $("dbInfoLastWrite").textContent = info?.last_write_text || "-";

  const meta = [];
  if (info?.database) meta.push(`Database: ${info.database}`);
  if (info?.encoding) meta.push(`Encoding: ${info.encoding}`);
  if (info?.compressor) meta.push(`Compressor: ${info.compressor}`);
  if (info?.alias) meta.push(`Alias: ${info.alias}`);
  if (info?.last_value != null && String(info.last_value) !== "") meta.push(`Last Value: ${info.last_value}`);
  if (info?.tags != null && String(info.tags) !== "") meta.push(`Tags: ${JSON.stringify(info.tags)}`);
  if (info?.attributes != null && String(info.attributes) !== "") meta.push(`Attributes: ${JSON.stringify(info.attributes)}`);
  fillList("dbSampleDevices", meta);

  const notes = [];
  if (info?.ttl_source_path) {
    if (info.ttl_source_path === info.device) notes.push(`TTL directly set on: ${info.ttl_source_path}`);
    else notes.push(`TTL inherited from: ${info.ttl_source_path}`);
  } else {
    notes.push("TTL source: none (never expire)");
  }
  fillList("dbSampleTimeseries", notes);

  const retypeInput = $("pointRetypeInput");
  if (retypeInput) retypeInput.value = info?.data_type || "FLOAT";
}

async function openPointInfo(path) {
  switchRightView("dbinfo");
  setDbInfoMsg(`Loading point info: ${path} ...`, "info");
  try {
    const data = await postJson("/api/point_info", { ...connPayload(), path });
    renderPointInfo(data);
    setDbInfoMsg("Point info loaded", "ok");
  } catch (e) {
    setDbInfoMsg(e.message, "err");
  }
}
function connPayload() {
  return {
    host: $("host").value.trim(),
    port: parseInt($("port").value || "6667", 10),
    username: $("username").value.trim(),
    password: $("password").value,
  };
}

function nowMs() { return Date.now(); }
function deltaMs(v) {
  if (v === "15m") return 15 * 60 * 1000;
  if (v === "1h") return 60 * 60 * 1000;
  if (v === "6h") return 6 * 60 * 60 * 1000;
  if (v === "24h") return 24 * 60 * 60 * 1000;
  return 7 * 24 * 60 * 60 * 1000;
}

function quoteIdent(name) { return "`" + name.replace(/`/g, "``") + "`"; }
function normalizeColName(name) {
  return String(name || "").trim().replace(/^`|`$/g, "").replace(/^"|"$/g, "").toLowerCase();
}
function sortPointsByNaturalOrder(points) {
  const idx = new Map(allPoints.map((p, i) => [p, i]));
  return [...points].sort((a, b) => {
    const ia = idx.has(a) ? idx.get(a) : Number.MAX_SAFE_INTEGER;
    const ib = idx.has(b) ? idx.get(b) : Number.MAX_SAFE_INTEGER;
    if (ia !== ib) return ia - ib;
    return String(a).localeCompare(String(b));
  });
}
function desiredMetricColumnsBySelection() {
  const selected = [...selectedPoints];
  const agg = $("agg").value;
  const enableGroup = $("enableGroup").checked;
  if (agg === "raw" || !enableGroup) return selected;
  return selected.map((p) => `${agg}_${p}`);
}
function reorderResultByDesiredColumns(columns, rows, desiredMetricCols) {
  const cols = [...(columns || [])];
  const rs = rows || [];
  const desired = desiredMetricCols || [];
  if (!cols.length || !desired.length) return { columns: cols, rows: rs };

  const timeIdx = [];
  const metricIdx = [];
  cols.forEach((c, i) => {
    const n = normalizeColName(c);
    if (n === "time" || n.includes("timestamp")) timeIdx.push(i);
    else metricIdx.push(i);
  });
  if (!metricIdx.length) return { columns: cols, rows: rs };

  const buckets = new Map();
  metricIdx.forEach((i) => {
    const key = normalizeColName(cols[i]);
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(i);
  });

  const picked = [];
  for (const d of desired) {
    const key = normalizeColName(d);
    const arr = buckets.get(key);
    if (!arr || !arr.length) return { columns: cols, rows: rs };
    picked.push(arr.shift());
  }
  const remain = [];
  buckets.forEach((arr) => arr.forEach((i) => remain.push(i)));
  const order = [...timeIdx, ...picked, ...remain];

  const newCols = order.map((i) => cols[i]);
  const newRows = rs.map((r) => order.map((i) => (r && i < r.length ? r[i] : null)));
  return { columns: newCols, rows: newRows };
}

function resolveTimeRange() {
  const mode = $("range").value;
  if (mode !== "custom") {
    const end = nowMs();
    return [end - deltaMs(mode), end];
  }
  const s = $("startTime").value;
  const e = $("endTime").value;
  if (!s || !e) return [NaN, NaN];
  return [new Date(s).getTime(), new Date(e).getTime()];
}

function buildSql() {
  const device = $("device").value.trim();
  const points = [...selectedPoints];
  const queryPoints = sortPointsByNaturalOrder(points);
  const agg = $("agg").value;
  const interval = $("interval").value.trim();
  const sliding = $("sliding").value.trim();
  const fill = $("fill").value;
  const order = $("order").value;
  const level = $("level").value.trim();
  const limit = parseInt($("limit").value || "1000", 10);
  const enableGroup = $("enableGroup").checked;
  const alignByDevice = $("alignByDevice").checked;
  const withoutNull = $("withoutNull").checked;

  if (!device) return "-- Please select a device";
  if (!points.length) return "-- Please select at least one point";

  const [start, end] = resolveTimeRange();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return "-- Custom time range is incomplete";
  if (end <= start) return "-- End time must be greater than start time";


  const where = `time >= ${start} AND time <= ${end}`;
  if (agg === "raw" || !enableGroup) {
    const fields = queryPoints.map(quoteIdent).join(", ");
    let sql = `SELECT ${fields} FROM ${device} WHERE ${where} ORDER BY TIME ${order} LIMIT ${limit}`;
    if (alignByDevice) sql += " ALIGN BY DEVICE";
    if (withoutNull) sql += " WITHOUT NULL ANY";
    return sql;
  }

  const fnMap = {
    mean: "AVG", max: "MAX_VALUE", min: "MIN_VALUE", sum: "SUM",
    count: "COUNT", first: "FIRST_VALUE", last: "LAST_VALUE",
  };
  const fn = fnMap[agg] || "AVG";
  const fields = queryPoints.map((p) => `${fn}(${quoteIdent(p)}) AS ${quoteIdent(agg + "_" + p)}`).join(", ");
  const fillMap = { none: "", null: "", previous: " FILL(PREVIOUS)", linear: " FILL(LINEAR)", "0": " FILL(0)" };

  let group = `GROUP BY ([${start}, ${end}), ${interval || "15m"})`;
  if (sliding) group = `GROUP BY ([${start}, ${end}), ${interval || "15m"}, ${sliding})`;
  if (level) group += `, LEVEL = ${level}`;

  let sql = `SELECT ${fields} FROM ${device} WHERE ${where} ${group}${fillMap[fill] || ""} ORDER BY TIME ${order} LIMIT ${limit}`;
  if (alignByDevice) sql += " ALIGN BY DEVICE";
  if (withoutNull) sql += " WITHOUT NULL ANY";
  return sql;
}

function parseQueryWindow(sql) {
  const txt = String(sql || "");
  const ge = txt.match(/time\s*>=\s*(\d+)/i);
  const le = txt.match(/time\s*<=\s*(\d+)/i);
  if (!ge || !le) return null;
  let a = Number(ge[1]);
  let b = Number(le[1]);
  if (!Number.isFinite(a) || !Number.isFinite(b) || b <= a) return null;
  if (a < 1_000_000_000_000) a *= 1000;
  if (b < 1_000_000_000_000) b *= 1000;
  return { start: a, end: b };
}

async function postJson(url, body) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const txt = await r.text();
  let data = {};
  try { data = txt ? JSON.parse(txt) : {}; }
  catch (_) { throw new Error(`闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴銏℃櫆闁芥ê顦純鏇㈡⒒娴ｈ櫣銆婇柛鎾寸箞閹柉顦归柟顖氱焸楠炴绱掑Ο琛″亾閸偆绠鹃柟瀵稿剱娴煎棝鏌熸潏鍓х暠闁活厽顨婇悡顐﹀炊閵娧€濮囬梺缁樻尵閸犳牠寮婚敓鐘茬闁靛鍎崑鎾诲传閵夛附娈伴梺鍓插亝濞叉﹢鍩涢幒妤佺厱閻忕偠顕ч埀顒佹礋閹﹢鏁冮崒娑氬幐闁诲繒鍋熼崑鎾剁矆閸愵亞纾肩紓浣诡焽濞插鈧娲栫紞濠囥€佸▎鎴濇瀳閺夊牃鏂侀崑鎾搭槹鎼达絿锛濋梺绋挎湰閻熴劑宕楃仦瑙ｆ斀妞ゆ梻鍋撻弳顒勬煙椤曞棛绡€闁轰焦鎹囬幃鈺呮嚑閼稿灚鍟哄┑鐘垫暩閸嬬偤宕归鐐插瀭闁荤喓澧楅弳婊堟⒑閼姐倕鏋戠紒顔肩灱缁棃鎮介悽鐢电効闂佸湱鍎ら幐鑽ょ礊閸ヮ剚鐓忓┑鐐戝啯濯奸柛鐔烽叄濮婄粯鎷呯粙鎸庡€紓浣风劍閹稿啿顕ｉ幓鎺嗘闁靛繆鏅滈弲婊堟⒑閸偆澧褏鐦? ${txt.slice(0, 180)}`); }
  if (!r.ok || !data.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
}

function buildDeviceHierarchy(db, devices) {
  const root = { children: {} };
  for (const dev of devices) {
    const trimmed = dev.startsWith(db + ".") ? dev.slice(db.length + 1) : dev;
    const parts = trimmed.split(".").filter(Boolean);
    let node = root;
    let curr = db;
    for (const part of parts) {
      curr = curr + "." + part;
      if (!node.children[part]) node.children[part] = { children: {}, device: null, fullPath: curr };
      if (!node.children[part].fullPath) node.children[part].fullPath = curr;
      node = node.children[part];
    }
    node.device = dev;
    node.fullPath = dev;
  }
  return root;
}

function renderNode(parent, node, keyword, level = 0) {
  const names = Object.keys(node.children).sort();
  for (const name of names) {
    const child = node.children[name];
    const fullPath = child.fullPath || "";
    const hit = !keyword || name.toLowerCase().includes(keyword) || fullPath.toLowerCase().includes(keyword);
    const hasChildren = Object.keys(child.children).length > 0;

    if (hasChildren) {
      const details = document.createElement("details");
      details.open = level < 1 || !!keyword;
      const summary = document.createElement("summary");
      summary.title = fullPath || name;

      const cap = document.createElement("span");
      cap.className = "tree-db-name";
      cap.textContent = name;

      const infoBtn = document.createElement("button");
      infoBtn.type = "button";
      infoBtn.className = "tree-db-info-btn";
      infoBtn.textContent = "i";
      infoBtn.title = `View ${fullPath || name} info`;
      infoBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        openPathInfo(fullPath || name);
      });

      summary.appendChild(cap);
      summary.appendChild(infoBtn);
      details.appendChild(summary);
      const box = document.createElement("div");
      box.className = "tree-node";
      renderNode(box, child, keyword, level + 1);
      if (box.childElementCount > 0 || hit) {
        details.appendChild(box);
        parent.appendChild(details);
      }
      continue;
    }

    if (!hit) continue;
    const leaf = document.createElement("div");
    leaf.className = "tree-leaf" + (selectedDevice === child.device ? " active" : "");
    const cap = document.createElement("span");
    cap.textContent = name;
    cap.title = child.device || name;
    cap.style.flex = "1";
    cap.style.cursor = "pointer";
    cap.onclick = () => selectDevice(child.device || fullPath || name);

    const infoBtn = document.createElement("button");
    infoBtn.type = "button";
    infoBtn.className = "tree-db-info-btn";
    infoBtn.textContent = "i";
    infoBtn.title = `View ${child.device || name} info`;
    infoBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      openDeviceInfo(child.device || fullPath || name);
    });

    leaf.appendChild(cap);
    leaf.appendChild(infoBtn);
    parent.appendChild(leaf);
  }
}

function renderTree(tree) {
  const root = $("tree");
  root.innerHTML = "";
  const entries = Object.entries(tree || {});
  const keyword = $("treeFilter").value.trim().toLowerCase();
  if (!entries.length) {
    root.innerHTML = "<div class='muted'>闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴銏℃櫆闁芥ê顦純鏇熺節閻㈤潧孝闁挎洏鍊楅埀顒佸嚬閸ｏ綁濡撮崨鏉戠煑濠㈣泛鐬奸惁鍫熺節閻㈤潧孝闁稿﹥鎮傞、鏃堫敂閸喓鍘卞┑鐐叉缁绘帞绮绘繝姘厸鐎光偓鐎ｎ剛袦濡ょ姷鍋涘ú顓€佸Δ鍛＜婵炴垶鐟ラ弸娑欑節閻㈤潧校妞ゆ梹鐗犲畷鏉课旈崘銊ョ亰闂佽宕橀褔鎷戦悢鍏肩叆闁哄洦顨呮禍楣冩煣娴兼瑧鍒伴柕鍡樺笒椤繈鎮℃惔锝勭敾缂備焦顨嗛崹鍨潖缂佹ɑ濯撮柧蹇曟嚀缁楋繝姊洪悷鐗堝暈闁诡喖鍊搁悾鐑藉箛閺夎法顔愭繛杈剧到濠€閬嶆偩閹惰姤鈷掗柛灞剧懆閸忓瞼绱掗鍛仯婵犫偓娓氣偓閺岋綁鎮╅顫闂備焦瀵уú鏍磹閹间焦鍤嬬憸鐗堝笚閻擄綁鐓崶銊﹀鞍閻犳劧绱曢惀顏堝礈瑜庡▍鏇㈡煃瑜滈崜娆戠不瀹ュ纾块梺顒€绉寸粻鐘绘煙闁箑骞楁繛鍛箻濮婅櫣鎷犻幓鎺濆妷濡炪倖姊归悧鐘茬暦閹剁瓔鏁嬮柍褜鍓欓悾鐑藉箣閻愮數鐦堥梺鎼炲劀閸涱垱姣囬梺鑽ゅ枑缁秹寮婚妸鈺傚仼闁绘垼濮ら崑鍌炲箹鐎涙〞鎴﹀棘閳ь剟姊绘担绋款棌闁稿鎳愮划娆撳箣閻愭娲告繛瀵稿帶閻°劑鍩涢幒鎳ㄥ綊鏁愰崨顔兼殘闁荤姵鍔х换婵嬪蓟濞戞鐔兼偐閸欏顦╅梺绋款儍閸婃繈寮婚弴鐔虹闁绘劦鍓氶悵鏃傜磽娴ｅ搫鞋妞ゎ偄顦垫俊鐢稿礋椤栨氨鐤€闂佸疇妗ㄧ拋鏌ュ磻閹捐鍐€妞ゆ挶鍔庣粙蹇涙⒑閸濆嫭宸濋柛鐘虫尵缁粯銈ｉ崘鈺冨幈闂佹枼鏅涢崢楣冾敂閸喎鈧爼鏌ㄩ弴鐐测偓褰掑磹閸偅鍙忔慨妤€妫楅獮鏍煕濠靛牆鍔嬮柟渚垮妽缁绘繈宕橀埞澶歌檸闁诲氦顫夊ú鏍礊婵犲洢鈧礁顫濈捄鍝勭獩濡炪倖鎸鹃崑娑⑺夊┑鍡忔斀闁绘ɑ鍓氶崯蹇涙煕閻樺磭澧甸柡浣稿暣椤㈡棃宕卞▎搴ｇ憹闂備礁鎼粙渚€宕㈡總鍛婂珔闁绘柨鍚嬮悡銉︾箾閹寸伝顏堫敂瑜庣换娑樼暆婵犱線鍋楅梺璇″枛缂嶅﹤鐣烽崼鏇炍╅柕澹懌鍋℃繝鐢靛У椤旀牠宕伴幘璇茬９婵°倕鍟～鏇㈡煙閹呮憼濠殿垱鎸抽弻褑绠涢弴鐔锋畬缂備焦顨愮换婵嗩潖濞差亝鍊婚柍鍝勫€归悵锕傛⒑閹肩偛濡奸柣鏍с偢楠炲啯瀵奸幖顓熸櫔闂侀€炲苯澧撮柣娑卞櫍楠炴帡骞婇搹顐ｎ棃闁糕斁鍋撳銈嗗坊閸嬫捇鏌℃笟鍥ф珝婵﹦绮粭鐔煎焵椤掆偓椤洩顦归挊婵囥亜閹惧崬鐏╃痪鎯ф健閺岋紕浠︾拠鎻掑婵犮垼娉涜墝闁衡偓娴犲鍊甸柨婵嗛娴滄劙鏌熼柨瀣仢闁哄备鍓濆鍕沪閹存帗鍕冨┑鐘愁問閸垳鍒掑▎蹇曟殾婵°倕鍟╁▽顏嗙磼濞戞﹩鍎戦柛鐐垫暬閹嘲顭ㄩ崘顔煎及闂侀潧妫楅崯顖滄崲濠靛纾兼繝濠傚椤旀洟鏌ｉ悢鍝ョ煂濠⒀勵殘閺侇噣骞掗幘鍐插闂傚倸鍊风粈渚€鎮块崶褜娴栭柕濞炬櫆閸ゅ嫰鏌ら崨濠庡晱婵炲牅绮欓弻娑㈠Ψ椤旂厧顫梺鎶芥敱閸ㄥ潡寮诲☉妯锋婵鐗嗘导鎰節濞堝灝娅欑紒鐘虫崌瀵鈽夐姀鈥充汗闂佸綊顣﹂悞锕傛偪娴ｅ壊娓婚柕鍫濆暙閻忣亪鏌熼崨濠冨€愰柛鈹垮灪閹棃濡搁妷褜鍟嬮梺璇叉捣閺佹悂鈥﹂崼婵愬殨?/div>";
    return;
  }
  let renderedDb = 0;
  for (const [db, devices] of entries) {
    const dbHit = !keyword || db.toLowerCase().includes(keyword);
    const list = dbHit ? devices : devices.filter((d) => d.toLowerCase().includes(keyword));
    if (!list.length && !dbHit) continue;
    const details = document.createElement("details");
    details.open = true;
    const summary = document.createElement("summary");
    summary.title = db;
    const dbName = document.createElement("span");
    dbName.className = "tree-db-name";
    dbName.textContent = db;
    const infoBtn = document.createElement("button");
    infoBtn.type = "button";
    infoBtn.className = "tree-db-info-btn";
    infoBtn.textContent = "i";
    infoBtn.title = `View ${db} info`;
    infoBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      openDbInfo(db);
    });
    summary.appendChild(dbName);
    summary.appendChild(infoBtn);
    details.appendChild(summary);
    const hierarchy = buildDeviceHierarchy(db, list);
    const box = document.createElement("div");
    box.className = "tree-node";
    renderNode(box, hierarchy, keyword, 0);
    details.appendChild(box);
    root.appendChild(details);
    renderedDb += 1;
  }
  if (renderedDb === 0) root.innerHTML = "<div class='muted'>闂傚倸鍊搁崐鎼佸磹閹间礁纾归柣鎴ｅГ閸ゅ嫰鏌涢锝嗙缂佺姷濞€閺岀喖骞戦幇闈涙闁荤喐鐟辩粻鎾诲箖濡ゅ懏鏅查幖绮光偓鎰佹交闂備焦鎮堕崝宥囨崲閸儳宓侀柡宥庣仈鎼搭煈鏁嗛柍褜鍓氭穱濠囨嚃閳哄啯锛忛梺璇″瀻娴ｉ晲鍒掗梻浣告惈閺堫剙煤濡吋宕叉繛鎴欏灪閸婇攱銇勯幒宥堝厡鐟滄澘鎳愮槐鎾诲磼濞嗘垼绐楅梺绋款儏鐎氼喚鍒掗弮鍥ヤ汗闁圭儤鍨奸幗鏇㈡⒑閹稿海绠撻柟鍙夛耿閹垽鎮滃Ο铏瑰酱闂備浇鍋愰埛鍫ュ礈濞戙垹围闁绘垼濮ら埛鎺懨归敐鍫燁仩閻㈩垱鐩弻锝呂旈崘銊愶絿绱掗崒姘毙ら柟鐟板缁楃喖顢涘☉姘扁枆濠电姷鏁搁崑娑樜熸繝鍐洸婵犲﹤鎳愬Λ顖滄喐閺冨牆绠栫憸鐗堝笒缁犳帡鏌熼悜妯虹仴妞ゃ儱閰ｅ娲濞戞艾顣哄┑鐐茬湴閸斿孩绔熼弴銏犵缂佹妗ㄧ花濠氭⒑閸濆嫬鈧湱鈧瑳鍥х畾闁割偅绺鹃弨鑺ャ亜閺冨倸甯堕柍褜鍓欓…鐑藉春閳?/div>";
}

function updatePointStat() {
  $("pointStat").textContent = `${selectedPoints.size} / ${pointsTotal}`;
}

function updatePointPager() {
  const page = Math.max(1, pointsPage || 1);
  const totalPages = Math.max(1, pointsTotalPages || 1);
  $("pointsPageStat").textContent = `${page} / ${totalPages}`;
  $("pointsTotalStat").textContent = `Total: ${pointsTotal}`;
  $("pointsPrevBtn").disabled = page <= 1;
  $("pointsNextBtn").disabled = page >= totalPages;
}

function closePointMenu() {
  if (openPointMenuEl) {
    openPointMenuEl.classList.remove("open");
    openPointMenuEl = null;
  }
  document.querySelectorAll(".point-more[aria-expanded='true']").forEach((btn) => {
    btn.setAttribute("aria-expanded", "false");
  });
}

function resolveDefaultType(v) {
  const t = String(v || "").toUpperCase();
  if (["BOOLEAN","INT32","INT64","FLOAT","DOUBLE","TEXT","STRING","BLOB","DATE","TIMESTAMP"].includes(t)) return t;
  return "FLOAT";
}

async function deletePoint(path, point) {
  const typed = window.prompt(`Type full point path to confirm delete.\nPath: ${path}`);
  if (typed !== path) {
    setMsg("Canceled: confirmation text does not match point path", "err");
    return false;
  }
  try {
    setMsg(`Deleting point ${point} ...`, "info");
    await postJson("/api/point_delete", { ...connPayload(), path });
    await reloadPointsByDevice(selectedDevice, false);
    setMsg(`Point deleted: ${point}`, "ok");
    return true;
  } catch (e) {
    setMsg(e.message, "err");
    return false;
  }
}

async function retypePoint(path, point, oldType, fixedType = null) {
  let newType = "";
  if (fixedType && String(fixedType).trim()) {
    newType = resolveDefaultType(fixedType);
  } else {
    const input = window.prompt(`Change data type for ${path}\nCurrent: ${oldType || "unknown"}\nTarget type (BOOLEAN/INT32/INT64/FLOAT/DOUBLE/TEXT/STRING/BLOB):`, resolveDefaultType(oldType));
    if (!input) return false;
    newType = resolveDefaultType(input);
  }

  const typed = window.prompt(`This operation deletes historical data of this point.\nType full point path to confirm retype.\nPath: ${path}\nNew Type: ${newType}`);
  if (typed !== path) {
    setMsg("Canceled: confirmation text does not match point path", "err");
    return false;
  }

  try {
    setMsg(`Retyping ${point} to ${newType} ...`, "info");
    await postJson("/api/point_retype", { ...connPayload(), path, data_type: newType });
    await reloadPointsByDevice(selectedDevice, false);
    setMsg(`Point retyped: ${point} -> ${newType}`, "ok");
    return true;
  } catch (e) {
    setMsg(e.message, "err");
    return false;
  }
}

function renderPoints() {
  const p = $("points");
  p.innerHTML = "";
  closePointMenu();
  for (const point of allPoints) {
    const row = document.createElement("div");
    row.className = "point-item";
    row.title = pointPaths[point] || point;

    const main = document.createElement("label");
    main.className = "point-main";

    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = point;
    cb.checked = selectedPoints.has(point);
    cb.onchange = (e) => {
      if (e.target.checked) selectedPoints.add(point);
      else selectedPoints.delete(point);
      updatePointStat();
    };

    const nameEl = document.createElement("span");
    nameEl.className = "point-name";
    nameEl.textContent = point;

    const typeEl = document.createElement("span");
    typeEl.className = "point-type";
    typeEl.textContent = pointTypes[point] || "UNKNOWN";

    main.appendChild(cb);
    main.appendChild(nameEl);
    main.appendChild(typeEl);

    const moreBtn = document.createElement("button");
    moreBtn.type = "button";
    moreBtn.className = "point-more";
    moreBtn.textContent = "...";
    moreBtn.title = "Point info";
    moreBtn.setAttribute("aria-label", "Point info");

    moreBtn.onclick = (e) => {
      e.preventDefault();
      e.stopPropagation();
      const path = pointPaths[point] || (selectedDevice ? `${selectedDevice}.${point}` : point);
      openPointInfo(path);
    };

    row.appendChild(main);
    row.appendChild(moreBtn);
    p.appendChild(row);
  }
  if (allPoints.length === 0) p.innerHTML = "<div class='muted'>No points matched</div>";
  updatePointStat();
  updatePointPager();
}

async function reloadPointsByDevice(dev, resetSelection, resetPage = false) {
  if (resetPage) pointsPage = 1;
  const reqId = ++pointsReqSeq;
  const data = await postJson("/api/points", {
    ...connPayload(),
    device: dev,
    page: pointsPage,
    page_size: pointsPageSize,
    keyword: pointsKeyword,
  });
  // Ignore stale responses (e.g. device switched or newer search/page request already sent).
  if (reqId !== pointsReqSeq) return;
  if (dev !== selectedDevice) return;
  allPoints = data.points || [];
  pointTypes = data.point_types || {};
  pointPaths = data.point_paths || {};
  pointsTotal = Number(data.total || 0);
  pointsPage = Number(data.page || 1);
  pointsPageSize = Number(data.page_size || pointsPageSize || 1000);
  pointsTotalPages = Number(data.total_pages || 1);
  if (resetSelection) {
    selectedPoints = new Set();
  }
  renderPoints();
}

async function selectDevice(dev) {
  selectedDevice = dev;
  pointsKeyword = "";
  pointsPage = 1;
  $("pointFilter").value = "";
  switchRightView("query");
  $("device").value = dev;
  renderTree(treeData);
  setMsg(`Loading device points: ${dev} ...`, "info");
  await reloadPointsByDevice(dev, true, true);
  setMsg(`Points loaded: ${pointsTotal}`, "ok");
}

function timeToReadable(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "";
  const ms = n > 1_000_000_000_000 ? n : n * 1000;
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return "";
  const z = (x, w = 2) => String(x).padStart(w, "0");
  return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())} ${z(d.getHours())}:${z(d.getMinutes())}:${z(d.getSeconds())}.${z(d.getMilliseconds(), 3)}`;
}

function renderResult(columns, rows) {
  lastResult = { columns: columns || [], rows: rows || [] };
  trendState.viewMin = null;
  trendState.viewMax = null;
  const t = $("result");
  t.innerHTML = "";

  const timeIdx = [];
  columns.forEach((c, i) => {
    const n = String(c).toLowerCase();
    if (n === "time" || n.includes("timestamp")) timeIdx.push(i);
  });

  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  const thSeq = document.createElement("th");
  thSeq.textContent = "#";
  trh.appendChild(thSeq);
  columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  });
  if (timeIdx.length > 0) {
    const th = document.createElement("th");
    th.textContent = "Time(Readable)";
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  t.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    const tdSeq = document.createElement("td");
    tdSeq.textContent = String(idx + 1);
    tr.appendChild(tdSeq);
    row.forEach((v) => {
      const td = document.createElement("td");
      td.textContent = v == null ? "" : String(v);
      tr.appendChild(td);
    });
    if (timeIdx.length > 0) {
      const td = document.createElement("td");
      td.textContent = timeToReadable(row[timeIdx[0]]);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  });
  t.appendChild(tbody);

  renderTrend();
}

function switchResultTab(tab) {
  const table = $("resultTableWrap");
  const trend = $("resultTrendWrap");
  const t1 = $("tabTable");
  const t2 = $("tabTrend");
  const tools = $("trendTools");

  if (tab === "trend") {
    table.classList.add("hidden");
    trend.classList.remove("hidden");
    t1.classList.remove("active");
    t2.classList.add("active");
    tools.classList.remove("hidden");
    renderTrend();
  } else {
    trend.classList.add("hidden");
    table.classList.remove("hidden");
    t2.classList.remove("active");
    t1.classList.add("active");
    tools.classList.add("hidden");
  }
}

function findTimeIdx(cols, rows) {
  const sample = Math.min(rows.length, 80);
  if (sample === 0) return -1;

  for (let i = 0; i < cols.length; i += 1) {
    const n = String(cols[i]).toLowerCase();
    if (n === "time" || n.includes("timestamp")) return i;
  }

  // Try datetime-like string column first.
  for (let i = 0; i < cols.length; i += 1) {
    let ok = 0;
    for (let r = 0; r < sample; r += 1) {
      const v = rows[r][i];
      if (v == null) continue;
      const ms = Date.parse(String(v));
      if (Number.isFinite(ms) && ms > 946684800000) ok += 1; // > 2000-01-01
    }
    if (ok / sample >= 0.55) return i;
  }

  // Then detect epoch-like numeric column.
  let bestIdx = -1;
  let bestScore = -1;
  for (let i = 0; i < cols.length; i += 1) {
    let ok = 0;
    const nums = [];
    for (let r = 0; r < sample; r += 1) {
      const n = Number(rows[r][i]);
      if (!Number.isFinite(n)) continue;
      ok += 1;
      nums.push(n > 1_000_000_000_000 ? n : n * 1000);
    }
    if (ok / sample < 0.5 || nums.length < 3) continue;
    nums.sort((a, b) => a - b);
    const mid = nums[Math.floor(nums.length / 2)];
    const spread = nums[nums.length - 1] - nums[0];
    let score = 0;
    if (mid > 946684800000 && mid < 4102416000000) score += 2; // 2000..2100
    if (spread > 0) score += 1;
    if (lastQueryWindow && mid >= lastQueryWindow.start - 365 * 24 * 3600 * 1000 && mid <= lastQueryWindow.end + 365 * 24 * 3600 * 1000) score += 2;
    if (score > bestScore) {
      bestScore = score;
      bestIdx = i;
    }
  }
  if (bestIdx >= 0) return bestIdx;

  // Final fallback: first mostly numeric column.
  for (let i = 0; i < cols.length; i += 1) {
    let ok = 0;
    for (let r = 0; r < sample; r += 1) {
      if (Number.isFinite(Number(rows[r][i]))) ok += 1;
    }
    if (ok / sample >= 0.6) return i;
  }
  return -1;
}

function toMillis(v) {
  const n = Number(v);
  if (Number.isFinite(n)) return n > 1_000_000_000_000 ? n : n * 1000;
  const ms = Date.parse(String(v));
  return Number.isFinite(ms) ? ms : NaN;
}

function fmtTime(ms, span) {
  const d = new Date(ms);
  const z = (n, w = 2) => String(n).padStart(w, "0");
  if (span < 60 * 60 * 1000) return `${z(d.getHours())}:${z(d.getMinutes())}:${z(d.getSeconds())}`;
  if (span < 2 * 24 * 60 * 60 * 1000) return `${z(d.getMonth() + 1)}-${z(d.getDate())} ${z(d.getHours())}:${z(d.getMinutes())}`;
  return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())}`;
}

function fmtTimeMs(ms) {
  const d = new Date(ms);
  const z = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())} ${z(d.getHours())}:${z(d.getMinutes())}:${z(d.getSeconds())}.${z(d.getMilliseconds(), 3)}`;
}

function renderTrend() {
  const svg = $("trendSvg");
  const tip = $("trendTip");
  tip.classList.add("hidden");
  svg.innerHTML = "";

  const cols = lastResult.columns || [];
  const rows = lastResult.rows || [];
  if (!cols.length || !rows.length) {
    svg.innerHTML = "<text x='20' y='30' fill='#6b7d86' font-size='12'>闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴銏犵厬闁兼亽鍎抽埥澶愭懚閺嶎厽鐓曟繛鎴濆船楠炴﹢鏌ㄥ☉娆戞噰婵﹥妞介幊锟犲Χ閸涱喚鈧箖鏌ｆ惔銏ｅ闁绘妫濋崺銉﹀緞閹邦剦娼婇梺缁樕戦悥鐘诲礃閸撗冩暩闂佽崵濮惧▍锝囦焊閵娾晛缁╁┑鐘崇閻撶喖骞栭幖顓炴灈濠⒀冪摠閹便劍绻濋崨顕呬哗缂備浇椴哥敮鎺楋綖濠婂牆骞㈡俊顖氬悑濞堜粙姊婚崒娆掑厡缂侇噮鍨堕妴鍐川椤撳洦绋戦埥澶婎潩椤掆偓閻?/text>";
    trendRenderCtx = null;
    return;
  }

  const timeIdx = findTimeIdx(cols, rows);
  if (timeIdx < 0) {
    svg.innerHTML = "<text x='20' y='30' fill='#6b7d86' font-size='12'>闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴銏℃櫆闁芥ê顦純鏇熺節閻㈤潧孝闁挎洏鍊楅埀顒佸嚬閸ｏ綁濡撮崨鏉戣摕闁靛濡囬崣鍡椻攽閻樼粯娑ф俊顐幖鍗辩憸鐗堝笚閻撴洟骞栫划瑙勵潐闂婎剦鍓熼弻鈥崇暆閳ь剟宕伴幇顒夌劷闊洦绋戠粈鍫㈡喐韫囨稑姹查柣妯兼暩绾捐棄霉閿濆棗绲诲ù婊堢畺濮婅櫣鍖栭弴鐐测拤闁煎灕鍏犵懓顭ㄩ崟顓犵厜闂佸搫鐬奸崰鏍嵁瀹ュ鎯炴い鎰剁悼瑜板懘姊绘担绛嬪殭缂佽妫濆鏌ユ偐鐠囪尪鎽曢梺鎸庣箓椤︻垳绮绘繝姘厱闁归偊鍘鹃妶鎾煕鐎ｎ偅宕岀€规洜顭堣灃闁逞屽墴閹锋垿鎮㈤崗鑲╁弳闂佺粯鏌ㄩ幖顐㈢摥缂傚倷璁查崑鎾绘煕閹伴潧鏋熼柣鎾崇箰閳规垿鎮欓幋婵嗘殭闁哄棛鍠栭弻娑樜熼崹顔ф挾绱掔紒妯肩畼闁哥姴锕よ灒婵炶尙绮紞澶愭⒒娴ｄ警鐒炬い鎴濇噽閳ь剚鍑归崜鐔煎灳閿曞倸鐐婃い鎺嗗亾闁诲繐纾埀顒冾潐濞叉牕煤閿曗偓閳绘捇骞嗚閺€鑺ャ亜閺冣偓閺嬬粯绗熷☉銏＄厱閹艰揪绲鹃弳顒傗偓娈垮櫘閸ｏ絽鐣烽幒鎳虫棃鍩€椤掍胶顩插Δ锝呭暞閻撱儲绻濋棃娑欘棦妞ゅ孩顨呴…鑳槾濠⒀勵殜婵＄敻宕熼娑欐珕闁荤姴娲ゅ鍫曟偟濠靛棌鏀芥い鏃傘€嬮弨缁樹繆閻愯埖顥夐柣锝囧厴椤㈡洟鏁冮埀顒傜矆鐎ｎ偁浜滈柟鏉垮缁嬬粯銇勯弬璺ㄐ㈤柍瑙勫灴閹晠骞囨担鍛婃珱闂備礁鎽滄慨闈涚暆缁嬫鍤曟い鎰╁€楅惌娆撳箹鐎涙ɑ灏版い顐㈢Ч濮婂搫效閸パ呬紕濡炪値鍘奸悧蹇曞垝閸儱閱囨繝銏＄箓缂嶅﹪寮幇鏉块唶闁绘洑妞掗崫妤呮⒒娴ｇ懓顕滅紒瀣笧閸掓帒鐣濋崟顐ゅ幋闂佺鎻粻鎴︽偂閿熺姵鐓曢柍鈺佸枤濞堟ê霉閻欌偓閸ㄨ泛顫忛搹鍦煓閻犳亽鍔嶅Σ鈧梻浣侯焾閿曘儵銆冮崼銉ョ?/text>";
    trendRenderCtx = null;
    return;
  }

  const seriesIdx = [];
  for (let i = 0; i < cols.length; i += 1) {
    if (i === timeIdx) continue;
    let ok = 0;
    const sample = Math.min(rows.length, 60);
    for (let r = 0; r < sample; r += 1) {
      if (Number.isFinite(Number(rows[r][i]))) ok += 1;
    }
    if (sample > 0 && ok / sample > 0.5) seriesIdx.push(i);
  }
  if (!seriesIdx.length) {
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>闂傚倸鍊搁崐鎼佸磹閹间礁纾瑰瀣捣閻棗銆掑锝呬壕濡ょ姷鍋為悧鐘汇€侀弴銏℃櫆闁芥ê顦純鏇熺節閻㈤潧孝闁挎洏鍊楅埀顒佸嚬閸ｏ綁濡撮崨鏉戠煑濠㈣泛鐬奸惁鍫ユ⒒閸屾氨澧涚紒瀣笒椤斿繐鈹戠€ｎ偆鍘藉┑鐘绘涧閿曘倝鎮￠幇鐗堢厵濞撴艾鐏濇俊鍏笺亜椤忓嫬鏆熼柟椋庡█閻擃偊顢橀悜鍡橆棥闂傚倷娴囬褍顫濋敃鍌︾稏濠㈣泛鈯曢崫鍕庣喖鎼圭憴鍕暦闂備礁缍婂Λ璺ㄧ矆娴ｈ櫣涓嶉柡宥庡亝閸犳劙鏌￠崒婵囩《闁哄棴绠戦埞鎴﹀磼濮橆厼鏆堥梺绋款儏鐎氫即寮诲☉銏犵闁哄鍨圭粊鐑芥⒑閸濆嫭鍣洪柣鐔叉櫅椤繑绻濆顒傦紲濠电偛妫欑敮鎺楀储閳ユ剚娓婚柕鍫濋楠炴牠鏌ｅΔ浣瑰磳鐎殿噮鍋婂畷姗€顢欓懞銉︾彇闂備胶顭堥張顒勬嚌妤ｅ啫纾荤€广儱顦伴崑鈩冪節婵犲倸顏柣鎾卞劦閺岋綁鏁愰崶銊у姽闂侀潧娲﹂崝娆撶嵁閹烘垟鏀介柛銉ｅ妽閻濇洟姊婚崒娆戭槮濠㈢懓锕畷鎴﹀幢濞戞鐛ラ梺褰掑亰閸忔﹢寮稿澶嬬厸鐎广儱楠搁獮妤呮煟閹捐泛鏋涢柣鎿冨亰瀹曞爼濡搁敂缁㈡К濠电偛顕慨宥夊炊閵娧冨箞闂佽鍑界紞鍡涘磻閸涱厾鏆︾€光偓閸曨剛鍘告繛杈剧秬椤鐣风仦瑙ｆ斀闁挎稑瀚崢鎾煛娴ｇ鈧灝鐣峰鍡╂闂佸摜鍠庢鎼佸煘閹达附鍋愭い鏃囧亹娴煎洤鈹戦悙宸Ч闁烩晩鍨堕妴渚€寮介鐐茶€垮┑鐐叉閸ㄥ綊鎮炴總鍛娾拺闁告挻褰冩禍鏍煕閵娿劌鍚瑰瑙勬礋瀹曟绮潪鎵泿闂備礁鎼ú銊╁磻閻愬搫闂憸鐗堝笚閻撶喖鐓崶銉ュ姎妞も晩鍓熼弻?/text>`;
    trendRenderCtx = null;
    return;
  }

  const data = [];
  rows.forEach((r) => {
    const t = toMillis(r[timeIdx]);
    if (!Number.isFinite(t)) return;
    const vals = {};
    seriesIdx.forEach((idx) => {
      const v = Number(r[idx]);
      if (Number.isFinite(v)) vals[idx] = v;
    });
    data.push({ t, vals });
  });
  if (!data.length) {
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>闂傚倸鍊搁崐鎼佸磹閹间礁纾圭€瑰嫭鍣磋ぐ鎺戠倞鐟滄粌霉閺嶎厽鐓忓┑鐐靛亾濞呭棝鏌涢妶鍛伃闁哄被鍊楃划娆戞崉閵娿倗椹虫繝鐢靛仜閹虫劖鎱ㄩ崹顐も攳濠电姴娲ゅ洿闂佺鏈惌顔界珶閺囥垺鈷掑ù锝夘棑娑撹尙绱掗煫顓犵煓闁诡喗锚椤繄鎹勯搹璇″數闂備礁鎲＄粙鎺戭焽濞嗘挸绠查柤鍝ュ仯娴滄粓鏌熼幆褜鍤熼柍顖涙礋閺岋綀绠涢幘铏濠殿喖锕︾划顖炲箯閸涘瓨鍋￠柡澶婄仢琚樼紓鍌氬€烽懗鍓佸垝椤栫偛绀夋繛鍡楃箳閺嗭箓鏌曡箛鏇烆€岄柣鎾卞劦閺屾盯顢曢敐鍥╃暫闂?/text>`;
    trendRenderCtx = null;
    return;
  }

  const pad = { l: 46, r: 18, t: 12, b: 34 };
  const W = 1000;
  const H = 360;
  const pw = W - pad.l - pad.r;
  const ph = H - pad.t - pad.b;

  const ts = data.map((d) => d.t);
  const dataMin = Math.min(...ts);
  const dataMax = Math.max(...ts);
  const fullMin = lastQueryWindow ? lastQueryWindow.start : dataMin;
  const fullMax = lastQueryWindow ? lastQueryWindow.end : dataMax;
  trendState.fullMin = fullMin;
  trendState.fullMax = fullMax;
  if (trendState.viewMin == null || trendState.viewMax == null) {
    trendState.viewMin = fullMin;
    trendState.viewMax = fullMax;
  }
  let xmin = Math.max(fullMin, trendState.viewMin);
  let xmax = Math.min(fullMax, trendState.viewMax);
  if (!Number.isFinite(xmin) || !Number.isFinite(xmax) || xmax <= xmin) {
    xmin = fullMin;
    xmax = fullMax;
  }

  const dataInView = data.filter((d) => d.t >= xmin && d.t <= xmax);
  if (!dataInView.length) {
    xmin = fullMin;
    xmax = fullMax;
  }
  const view = dataInView.length ? dataInView : data;

  let ymin = Infinity;
  let ymax = -Infinity;
  view.forEach((d) => {
    seriesIdx.forEach((idx) => {
      const v = d.vals[idx];
      if (Number.isFinite(v)) {
        ymin = Math.min(ymin, v);
        ymax = Math.max(ymax, v);
      }
    });
  });
  if (!Number.isFinite(ymin) || !Number.isFinite(ymax)) {
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>闂傚倸鍊搁崐鎼佸磹閹间礁纾圭€瑰嫭鍣磋ぐ鎺戠倞鐟滄粌霉閺嶎厽鐓忓┑鐐靛亾濞呭棝鏌涙繝鍌涘仴闁哄被鍔戝鏉懳旈埀顒佺閹屾富闁靛牆楠搁獮鏍煟韫囨梻绠為柨婵堝仜椤劑宕煎┑鍫濆Е婵＄偑鍊栫敮鎺斺偓姘煎墴瀵即濡烽埡鍌滃帗閻熸粍绮撳畷婊冾潩鐠轰綍锕傛煕閺囥劌鐏犵紒鐘冲▕閺岀喓鈧稒顭囩粻銉ッ归悩鑽ょ暫婵﹥妞介獮鎰償閿濆啠鍋撻幒妤佺厱闁绘ê纾晶鍨殽閻愭彃鏆㈤柕鍥ㄥ姍楠炴帡骞嬮悩鍨緫濠碉紕鍋戦崐鏍蓟閵娾晛瑙﹂悗锝庡枟閸婅泛霉閿濆牊顏犵痪?/text>`;
    trendRenderCtx = null;
    return;
  }
  if (ymin === ymax) ymax = ymin + 1;

  const x = (t) => pad.l + ((t - xmin) / Math.max(1, xmax - xmin)) * pw;
  const y = (v) => pad.t + (1 - (v - ymin) / (ymax - ymin)) * ph;
  const axisColor = cssVar("--trend-axis", "#8aa0ad");
  const gridColor = cssVar("--trend-grid", "#edf2f6");
  const labelColor = cssVar("--trend-label", "#5d727d");
  const seriesColors = cssVar("--series-colors", "#0b84a5,#f6a021,#35a35d,#db3a34,#7453e8,#2f6fdd")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  const mk = (tag) => document.createElementNS("http://www.w3.org/2000/svg", tag);
  const axis = mk("path");
  axis.setAttribute("d", `M${pad.l},${pad.t} L${pad.l},${H - pad.b} L${W - pad.r},${H - pad.b}`);
  axis.setAttribute("stroke", axisColor);
  axis.setAttribute("fill", "none");
  axis.setAttribute("stroke-width", "1");
  svg.appendChild(axis);

  const span = xmax - xmin;
  const tickN = 6;
  for (let i = 0; i <= tickN; i += 1) {
    const t = xmin + (span * i) / tickN;
    const tx = x(t);

    const g = mk("line");
    g.setAttribute("x1", String(tx));
    g.setAttribute("x2", String(tx));
    g.setAttribute("y1", String(pad.t));
    g.setAttribute("y2", String(H - pad.b));
    g.setAttribute("stroke", gridColor);
    g.setAttribute("stroke-width", "1");
    svg.appendChild(g);

    const tk = mk("line");
    tk.setAttribute("x1", String(tx));
    tk.setAttribute("x2", String(tx));
    tk.setAttribute("y1", String(H - pad.b));
    tk.setAttribute("y2", String(H - pad.b + 4));
    tk.setAttribute("stroke", axisColor);
    tk.setAttribute("stroke-width", "1");
    svg.appendChild(tk);

    const txt = mk("text");
    txt.setAttribute("x", String(tx - 22));
    txt.setAttribute("y", String(H - 6));
    txt.setAttribute("fill", labelColor);
    txt.setAttribute("font-size", "10");
    txt.textContent = fmtTime(t, span);
    svg.appendChild(txt);
  }

  const mode = $("trendMode").value;
  seriesIdx.forEach((idx, sidx) => {
    let dstr = "";
    let prev = null;
    view.forEach((row) => {
      const v = row.vals[idx];
      if (!Number.isFinite(v)) return;
      const px = x(row.t);
      const py = y(v);
      if (!prev) dstr += `M${px},${py}`;
      else if (mode === "step") dstr += ` H${px} V${py}`;
      else dstr += ` L${px},${py}`;
      prev = { px, py };
    });
    if (!dstr) return;
    const p = mk("path");
    p.setAttribute("d", dstr);
    p.setAttribute("fill", "none");
    p.setAttribute("stroke", seriesColors[sidx % seriesColors.length]);
    p.setAttribute("stroke-width", "1.6");
    svg.appendChild(p);
  });

  const yTop = mk("text");
  yTop.setAttribute("x", "4");
  yTop.setAttribute("y", String(pad.t + 8));
  yTop.setAttribute("fill", labelColor);
  yTop.setAttribute("font-size", "11");
  yTop.textContent = ymax.toFixed(3);
  svg.appendChild(yTop);

  const yBottom = mk("text");
  yBottom.setAttribute("x", "4");
  yBottom.setAttribute("y", String(H - pad.b));
  yBottom.setAttribute("fill", labelColor);
  yBottom.setAttribute("font-size", "11");
  yBottom.textContent = ymin.toFixed(3);
  svg.appendChild(yBottom);

  trendRenderCtx = { pad, W, H, xmin, xmax, pw, x, y, view, seriesIdx, cols };
}

function timeFromSvgX(svgX) {
  if (!trendRenderCtx) return NaN;
  const { pad, xmin, xmax, pw } = trendRenderCtx;
  const clamped = Math.max(pad.l, Math.min(pad.l + pw, svgX));
  const ratio = (clamped - pad.l) / Math.max(1, pw);
  return xmin + ratio * (xmax - xmin);
}

function svgCoordX(evt) {
  const svg = $("trendSvg");
  const rect = svg.getBoundingClientRect();
  return ((evt.clientX - rect.left) / rect.width) * 1000;
}

function setTrendOverlay(x1, x2) {
  const svg = $("trendSvg");
  let rect = $("trendBrush");
  if (!rect) {
    rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("id", "trendBrush");
    rect.setAttribute("y", "0");
    rect.setAttribute("height", "360");
    rect.setAttribute("fill", cssVar("--accent-soft", "rgba(11,132,165,0.18)"));
    rect.setAttribute("stroke", cssVar("--accent", "#0b84a5"));
    rect.setAttribute("stroke-width", "1");
    svg.appendChild(rect);
  }
  const left = Math.min(x1, x2);
  const right = Math.max(x1, x2);
  rect.setAttribute("x", String(left));
  rect.setAttribute("width", String(Math.max(1, right - left)));
}

function clearTrendOverlay() {
  const rect = $("trendBrush");
  if (rect && rect.parentNode) rect.parentNode.removeChild(rect);
}

function setTrendCursor(x) {
  const svg = $("trendSvg");
  let line = $("trendCursor");
  if (!line) {
    line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("id", "trendCursor");
    line.setAttribute("y1", "0");
    line.setAttribute("y2", "360");
    line.setAttribute("stroke", cssVar("--accent", "#0b84a5"));
    line.setAttribute("stroke-width", "1");
    line.setAttribute("stroke-dasharray", "4 3");
    svg.appendChild(line);
  }
  line.setAttribute("x1", String(x));
  line.setAttribute("x2", String(x));
}

function clearTrendCursor() {
  const line = $("trendCursor");
  if (line && line.parentNode) line.parentNode.removeChild(line);
}

function showTrendTooltip(evt) {
  if (!trendRenderCtx || !(trendRenderCtx.view || []).length) return;
  const tip = $("trendTip");
  const { view, cols, seriesIdx, x } = trendRenderCtx;

  const tx = svgCoordX(evt);
  let best = null;
  let bestDx = Infinity;
  view.forEach((row) => {
    const px = x(row.t);
    const dx = Math.abs(px - tx);
    if (dx < bestDx) {
      bestDx = dx;
      best = row;
    }
  });
  if (!best) return;

  const cx = x(best.t);
  setTrendCursor(cx);

  const lines = [];
  lines.push(`<div><b>${fmtTimeMs(best.t)}</b></div>`);
  let count = 0;
  for (const idx of seriesIdx) {
    const v = best.vals[idx];
    if (!Number.isFinite(v)) continue;
    lines.push(`<div>${String(cols[idx])}: ${v}</div>`);
    count += 1;
    if (count >= 8) break;
  }
  tip.innerHTML = lines.join("");
  tip.classList.remove("hidden");

  const wrap = $("resultTrendWrap");
  const r = wrap.getBoundingClientRect();
  let left = evt.clientX - r.left + 12;
  let top = evt.clientY - r.top + 12;
  if (left > r.width - 240) left = Math.max(8, left - 230);
  if (top > r.height - 120) top = Math.max(8, top - 100);
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
}

function hideTrendTooltip() {
  $("trendTip").classList.add("hidden");
  clearTrendCursor();
}

function bindSplitter() {
  const splitter = $("splitter");
  const main = document.querySelector("main");
  if (!splitter || !main) return;
  let dragging = false;
  splitter.addEventListener("mousedown", () => { dragging = true; });
  window.addEventListener("mouseup", () => { dragging = false; });
  window.addEventListener("mousemove", (e) => {
    if (!dragging) return;
    const rect = main.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const min = 300;
    const max = Math.max(min, rect.width - 480);
    const w = Math.min(max, Math.max(min, x));
    document.documentElement.style.setProperty("--tree-w", `${w}px`);
  });
}

function bindResultSplitter() {
  const splitter = $("hsplitter");
  const query = document.querySelector(".query");
  if (!splitter || !query) return;
  let dragging = false;
  splitter.addEventListener("mousedown", () => { dragging = true; });
  window.addEventListener("mouseup", () => { dragging = false; });
  window.addEventListener("mousemove", (e) => {
    if (!dragging) return;
    const rect = query.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const min = 160;
    const max = Math.max(min, rect.height - 280);
    const h = Math.min(max, Math.max(min, y));
    document.documentElement.style.setProperty("--result-h", `${h}px`);
  });
}

function bindTrendInteractions() {
  const svg = $("trendSvg");
  if (!svg) return;

  svg.addEventListener("mousedown", (e) => {
    if ($("resultTrendWrap").classList.contains("hidden")) return;
    if (!trendRenderCtx) return;
    trendState.dragStartX = svgCoordX(e);
    setTrendOverlay(trendState.dragStartX, trendState.dragStartX);
  });

  svg.addEventListener("mousemove", (e) => {
    if (trendState.dragStartX != null) {
      const x = svgCoordX(e);
      setTrendOverlay(trendState.dragStartX, x);
      hideTrendTooltip();
      return;
    }
    if (!$("resultTrendWrap").classList.contains("hidden")) showTrendTooltip(e);
  });

  svg.addEventListener("mouseleave", () => {
    hideTrendTooltip();
  });

  window.addEventListener("mouseup", (e) => {
    if (trendState.dragStartX == null) return;
    const x1 = trendState.dragStartX;
    const x2 = svgCoordX(e);
    trendState.dragStartX = null;
    clearTrendOverlay();
    if (Math.abs(x2 - x1) < 8) return;
    const t1 = timeFromSvgX(x1);
    const t2 = timeFromSvgX(x2);
    if (!Number.isFinite(t1) || !Number.isFinite(t2)) return;
    trendState.viewMin = Math.min(t1, t2);
    trendState.viewMax = Math.max(t1, t2);
    renderTrend();
  });

  svg.addEventListener("dblclick", () => {
    trendState.viewMin = null;
    trendState.viewMax = null;
    renderTrend();
  });
}

function syncRangeInputs() {
  const custom = $("range").value === "custom";
  $("startTime").disabled = !custom;
  $("endTime").disabled = !custom;
}

function syncGroupInputs() {
  const disabled = !$("enableGroup").checked || $("agg").value === "raw";
  ["interval", "sliding", "fill", "level"].forEach((id) => { $(id).disabled = disabled; });
}

function initCustomTimeDefaults() {
  const now = new Date();
  const start = new Date(now.getTime() - 60 * 60 * 1000);
  const fmt = (d) => {
    const z = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${z(d.getMonth() + 1)}-${z(d.getDate())}T${z(d.getHours())}:${z(d.getMinutes())}`;
  };
  $("startTime").value = fmt(start);
  $("endTime").value = fmt(now);
}

$("loadTreeBtn").onclick = async () => {
  const btn = $("loadTreeBtn");
  try {
    btn.disabled = true;
    setMsg("Loading tree, please wait...", "info");
    await fetchServerVersion();
    const data = await postJson("/api/tree", connPayload());
    treeData = data.tree || {};
    renderTree(treeData);
    setMsg(`Tree loaded: ${Object.keys(treeData).length} databases`, "ok");
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    btn.disabled = false;
  }
};

$("treeFilter").addEventListener("input", () => renderTree(treeData));
let pointFilterTimer = null;
$("pointFilter").addEventListener("input", () => {
  pointsKeyword = $("pointFilter").value.trim();
  if (pointFilterTimer) clearTimeout(pointFilterTimer);
  pointFilterTimer = setTimeout(async () => {
    if (!selectedDevice) return;
    pointsPage = 1;
    await reloadPointsByDevice(selectedDevice, false, true);
  }, 250);
});
$("pointsPrevBtn").addEventListener("click", async () => {
  if (!selectedDevice || pointsPage <= 1) return;
  pointsPage -= 1;
  await reloadPointsByDevice(selectedDevice, false, false);
});
$("pointsNextBtn").addEventListener("click", async () => {
  if (!selectedDevice || pointsPage >= pointsTotalPages) return;
  pointsPage += 1;
  await reloadPointsByDevice(selectedDevice, false, false);
});
$("clearPointsBtn").addEventListener("click", () => {
  selectedPoints = new Set();
  renderPoints();
});
$("range").addEventListener("change", syncRangeInputs);
$("enableGroup").addEventListener("change", syncGroupInputs);
$("agg").addEventListener("change", syncGroupInputs);
$("tabTable").addEventListener("click", () => switchResultTab("table"));
$("tabTrend").addEventListener("click", () => switchResultTab("trend"));
$("trendMode").addEventListener("change", renderTrend);
$("trendReset").addEventListener("click", () => {
  trendState.viewMin = null;
  trendState.viewMax = null;
  renderTrend();
});
$("themeSelect").addEventListener("change", (e) => applyTheme(e.target.value));

document.addEventListener("click", (e) => {
  if (!openPointMenuEl) return;
  const t = e.target;
  if (t && t.closest && t.closest(".point-more-menu")) return;
  if (t && t.closest && t.closest(".point-more")) return;
  closePointMenu();
});

$("toggleConsoleBtn").addEventListener("click", () => {
  setConsoleMode(!consoleMode);
});

$("buildBtn").onclick = () => {
  $("sql").value = buildSql();
};

$("runBtn").onclick = async () => {
  const btn = $("runBtn");
  try {
    btn.disabled = true;
    setMsg("濠电姷鏁告慨鐑藉极閸涘﹥鍙忛柣銏犲閺佸﹪鏌″搴″箹缂佹劖顨嗘穱濠囧Χ閸涱厽娈查悗瑙勬礃閻擄繝寮婚悢鍏肩劷闁挎洍鍋撻柡瀣〒缁辨帡鐓幓鎺嗗亾濠靛钃熼柨鏇楀亾閾伙絽銆掑鐓庣仭妞ゅ骸妫濆娲嚃閳轰緡鏆柣搴ｇ懗閸ヮ灛锕傛煕閺囥劌鏋ら柣銈傚亾闂備浇顫夊畷妯衡枖濞戭潿鈧倿鎼归崷顓狅紳婵炶揪绲介幖顐モ叿闂備胶顭堢€涒晜绻涙繝鍥╁祦濠㈣埖鍔曠粻鐟懊归敐鍛喐妞ゆ挻妞藉娲箰鎼淬埄姊块梺闈涙閸嬫捇姊?SQL...", "info");
    const sql = $("sql").value.trim();
    const data = await postJson("/api/query", { ...connPayload(), sql });
    lastQueryWindow = parseQueryWindow(sql);
    const ordered = reorderResultByDesiredColumns(
      data.columns || [],
      data.rows || [],
      desiredMetricColumnsBySelection(),
    );
    renderResult(ordered.columns, ordered.rows);
    setMsg(`Query completed: ${data.rows ? data.rows.length : 0} rows`, "ok");
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    btn.disabled = false;
  }
};

$("backToQueryBtn").addEventListener("click", () => {
  switchRightView("query");
});

$("dbTtlPreset").addEventListener("change", () => {
  const mode = $("dbTtlPreset").value;
  const isCustom = mode === "custom";
  $("dbTtlDays").disabled = !isCustom;
  if (!isCustom && mode !== "never") $("dbTtlDays").value = mode;
});

$("applyDbTtlBtn").addEventListener("click", async () => {
  if (!currentInfoPath) {
    setDbInfoMsg("Please open a database/device/path info first", "err");
    return;
  }

  const preset = $("dbTtlPreset").value;
  let ttlMs = null;
  if (preset === "never") {
    ttlMs = null;
  } else {
    const daysVal = preset === "custom" ? $("dbTtlDays").value : preset;
    const days = Number(daysVal);
    if (!Number.isFinite(days) || days <= 0) {
      setDbInfoMsg("Invalid TTL days", "err");
      return;
    }
    ttlMs = Math.floor(days * 24 * 60 * 60 * 1000);
  }

  const targetText = ttlMs == null ? "never expire" : ttlMsToText(ttlMs);
  const typed = window.prompt(`Type path to confirm TTL change.\nPath: ${currentInfoPath}\nNew TTL: ${targetText}`);
  if (typed !== currentInfoPath) {
    setDbInfoMsg("Canceled: confirmation text does not match path", "err");
    return;
  }

  try {
    setDbInfoMsg(`Applying TTL for ${currentInfoPath} ...`, "info");
    await postJson("/api/path_ttl", { ...connPayload(), path: currentInfoPath, ttl_ms: ttlMs });
    if (currentInfoMode === "database") await openDbInfo(currentInfoPath);
    else if (currentInfoMode === "device") await openDeviceInfo(currentInfoPath);
    else if (currentInfoMode === "point") await openPointInfo(currentInfoPath);
    else await openPathInfo(currentInfoPath);
    setDbInfoMsg(`TTL updated: ${targetText}`, "ok");
  } catch (e) {
    setDbInfoMsg(e.message, "err");
  }
});

$("pointDeleteBtn").addEventListener("click", async () => {
  if (!currentPointInfo || !currentPointInfo.path) {
    setDbInfoMsg("Open a point info first", "err");
    return;
  }
  const ok = await deletePoint(currentPointInfo.path, currentPointInfo.point || currentPointInfo.path);
  if (!ok) return;
  switchRightView("query");
});

$("pointRetypeBtn").addEventListener("click", async () => {
  if (!currentPointInfo || !currentPointInfo.path) {
    setDbInfoMsg("Open a point info first", "err");
    return;
  }
  const targetType = $("pointRetypeInput").value.trim() || currentPointInfo.data_type || "FLOAT";
  const ok = await retypePoint(
    currentPointInfo.path,
    currentPointInfo.point || currentPointInfo.path,
    currentPointInfo.data_type || "",
    targetType,
  );
  if (!ok) return;
  await openPointInfo(currentPointInfo.path);
});

bindSplitter();
bindResultSplitter();
bindTrendInteractions();
initCustomTimeDefaults();
syncRangeInputs();
syncGroupInputs();
updatePointStat();
updatePointPager();
setConsoleMode(false);
switchRightView("query");
fetchServerVersion();
if ($("dbTtlPreset")) $("dbTtlPreset").dispatchEvent(new Event("change"));

const savedTheme = localStorage.getItem("iotdb_theme") || "compact_blue";
if ($("themeSelect")) $("themeSelect").value = savedTheme;
applyTheme(savedTheme);

















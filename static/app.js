const $ = (id) => document.getElementById(id);

let treeData = {};
let allPoints = [];
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

  if (!device) return "-- 请选择设备";
  if (!points.length) return "-- 请至少选择一个point";

  const [start, end] = resolveTimeRange();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return "-- 自定义时间范围未填写完整";
  if (end <= start) return "-- 结束时间必须大于开始时间";

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
  catch (_) { throw new Error(`服务器返回非JSON: ${txt.slice(0, 180)}`); }
  if (!r.ok || !data.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
}

function buildDeviceHierarchy(db, devices) {
  const root = { children: {} };
  for (const dev of devices) {
    const trimmed = dev.startsWith(db + ".") ? dev.slice(db.length + 1) : dev;
    const parts = trimmed.split(".").filter(Boolean);
    let node = root;
    for (const part of parts) {
      if (!node.children[part]) node.children[part] = { children: {}, device: null, fullPath: "" };
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
      summary.textContent = name;
      summary.title = fullPath || name;
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
    leaf.textContent = name;
    leaf.title = child.device || name;
    leaf.onclick = () => selectDevice(child.device || fullPath || name);
    parent.appendChild(leaf);
  }
}

function renderTree(tree) {
  const root = $("tree");
  root.innerHTML = "";
  const entries = Object.entries(tree || {});
  const keyword = $("treeFilter").value.trim().toLowerCase();
  if (!entries.length) {
    root.innerHTML = "<div class='muted'>未加载到任何数据库（请检查连接参数或权限）</div>";
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
    summary.textContent = db;
    summary.title = db;
    details.appendChild(summary);
    const hierarchy = buildDeviceHierarchy(db, list);
    const box = document.createElement("div");
    box.className = "tree-node";
    renderNode(box, hierarchy, keyword, 0);
    details.appendChild(box);
    root.appendChild(details);
    renderedDb += 1;
  }
  if (renderedDb === 0) root.innerHTML = "<div class='muted'>搜索无匹配结果</div>";
}

function updatePointStat() {
  $("pointStat").textContent = `${selectedPoints.size} / ${allPoints.length}`;
}

function renderPoints() {
  const p = $("points");
  const keyword = $("pointFilter").value.trim().toLowerCase();
  p.innerHTML = "";
  let shown = 0;
  for (const point of allPoints) {
    if (keyword && !point.toLowerCase().includes(keyword)) continue;
    shown += 1;
    const label = document.createElement("label");
    label.title = point;
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = point;
    cb.checked = selectedPoints.has(point);
    cb.onchange = (e) => {
      if (e.target.checked) selectedPoints.add(point);
      else selectedPoints.delete(point);
      updatePointStat();
    };
    label.appendChild(cb);
    label.appendChild(document.createTextNode(point));
    p.appendChild(label);
  }
  if (shown === 0) p.innerHTML = "<div class='muted'>没有匹配点位</div>";
  updatePointStat();
}

async function selectDevice(dev) {
  selectedDevice = dev;
  $("device").value = dev;
  renderTree(treeData);
  setMsg(`正在加载设备点位: ${dev} ...`, "info");
  const data = await postJson("/api/points", { ...connPayload(), device: dev });
  allPoints = data.points || [];
  selectedPoints = new Set();
  $("pointFilter").value = "";
  renderPoints();
  setMsg(`点位加载完成，共 ${allPoints.length} 个`, "ok");
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
    svg.innerHTML = "<text x='20' y='30' fill='#6b7d86' font-size='12'>暂无数据</text>";
    trendRenderCtx = null;
    return;
  }

  const timeIdx = findTimeIdx(cols, rows);
  if (timeIdx < 0) {
    svg.innerHTML = "<text x='20' y='30' fill='#6b7d86' font-size='12'>未识别到时间列，无法绘图</text>";
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
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>未找到数值列，无法绘图</text>`;
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
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>数据为空</text>`;
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
    svg.innerHTML = `<text x='20' y='30' fill='${cssVar("--trend-label", "#6b7d86")}' font-size='12'>数值为空</text>`;
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
    setMsg("正在加载树，请稍候...", "info");
    const data = await postJson("/api/tree", connPayload());
    treeData = data.tree || {};
    renderTree(treeData);
    setMsg(`树加载完成，共 ${Object.keys(treeData).length} 个数据库`, "ok");
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    btn.disabled = false;
  }
};

$("treeFilter").addEventListener("input", () => renderTree(treeData));
$("pointFilter").addEventListener("input", () => renderPoints());
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

$("buildBtn").onclick = () => {
  $("sql").value = buildSql();
};

$("runBtn").onclick = async () => {
  const btn = $("runBtn");
  try {
    btn.disabled = true;
    setMsg("正在执行 SQL...", "info");
    const sql = $("sql").value.trim();
    const data = await postJson("/api/query", { ...connPayload(), sql });
    lastQueryWindow = parseQueryWindow(sql);
    const ordered = reorderResultByDesiredColumns(
      data.columns || [],
      data.rows || [],
      desiredMetricColumnsBySelection(),
    );
    renderResult(ordered.columns, ordered.rows);
    setMsg(`执行完成，返回 ${data.rows ? data.rows.length : 0} 行`, "ok");
  } catch (e) {
    setMsg(e.message, "err");
  } finally {
    btn.disabled = false;
  }
};

bindSplitter();
bindResultSplitter();
bindTrendInteractions();
initCustomTimeDefaults();
syncRangeInputs();
syncGroupInputs();
updatePointStat();

const savedTheme = localStorage.getItem("iotdb_theme") || "compact_blue";
if ($("themeSelect")) $("themeSelect").value = savedTheme;
applyTheme(savedTheme);

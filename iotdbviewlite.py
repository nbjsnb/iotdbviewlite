from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
import os
import re
import sys
from typing import Any, Callable

from flask import Flask, jsonify, render_template, request

try:
    from iotdb.Session import Session  # type: ignore
except Exception:
    Session = None


if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    _BASE_DIR = getattr(sys, "_MEIPASS")
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(_BASE_DIR, "templates"),
    static_folder=os.path.join(_BASE_DIR, "static"),
)
APP_VERSION = "1.0"


@dataclass
class Conn:
    host: str
    port: int
    username: str
    password: str


def _find_op(obj: Any, names: list[str]) -> Callable[..., Any] | None:
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _dataset_to_table(ds: Any) -> dict[str, Any]:
    names_getter = _find_op(ds, ["get_column_names", "getColumnNames"])
    col_names: list[str] = names_getter() if names_getter else []
    if not col_names:
        col_names = ["Time", "Value"]

    # Prefer dataframe conversion to avoid potential field-order mismatch.
    todf_op = _find_op(ds, ["todf"])
    if todf_op:
        try:
            df = todf_op()
            if df is not None:
                df_cols = [str(c) for c in list(df.columns)]
                out_cols = list(df_cols)
                if col_names:
                    # Align df column order to server column names with normalized matching.
                    wanted = [str(c) for c in col_names]
                    if len(wanted) == len(df_cols):
                        used = set()
                        idxs: list[int] = []
                        ok = True
                        for w in wanted:
                            wi = _normalize_col_name(w)
                            found = -1
                            for i, c in enumerate(df_cols):
                                if i in used:
                                    continue
                                if _normalize_col_name(c) == wi:
                                    found = i
                                    break
                            if found < 0:
                                ok = False
                                break
                            used.add(found)
                            idxs.append(found)
                        if ok and len(idxs) == len(df_cols):
                            df = df.iloc[:, idxs]
                            out_cols = wanted

                rows: list[list[Any]] = []
                for rec in df.itertuples(index=False, name=None):
                    row = [_to_json_cell(v) for v in rec]
                    if len(row) < len(out_cols):
                        row.extend([None] * (len(out_cols) - len(row)))
                    rows.append(row[: len(out_cols)])
                return {"columns": out_cols, "rows": rows}
        except Exception:
            # Keep compatibility with old driver behavior.
            pass

    has_next = _find_op(ds, ["has_next", "hasNext"])
    next_row = _find_op(ds, ["next"])
    if not has_next or not next_row:
        return {"columns": col_names, "rows": []}

    rows: list[list[Any]] = []
    while has_next():
        rr = next_row()
        ts_get = _find_op(rr, ["get_timestamp", "getTimestamp"])
        fs_get = _find_op(rr, ["get_fields", "getFields"])

        ts = ts_get() if ts_get else None
        fields = fs_get() if fs_get else []
        vals: list[Any] = []
        for f in fields:
            if f is None:
                vals.append(None)
                continue
            s_get = _find_op(f, ["get_string_value", "getStringValue"])
            vals.append(_to_json_cell(s_get() if s_get else str(f)))

        # Metadata statements (SHOW ...) often don't include a time column.
        if len(col_names) == len(vals):
            row = vals
        else:
            row = [ts] + vals
        if len(row) < len(col_names):
            row.extend([None] * (len(col_names) - len(row)))
        rows.append(row[: len(col_names)])

    return {"columns": col_names, "rows": rows}


def _to_json_cell(v: Any) -> Any:
    if v is None:
        return None
    if type(v).__name__ == "NAType":
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            return bytes(v).decode("utf-8")
        except Exception:
            return bytes(v).hex()
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, time):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    try:
        if math.isnan(v):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    return v


def _normalize_col_name(name: str) -> str:
    return str(name).strip().strip("`").strip('"').lower()


def _first_text_cell(row: list[Any]) -> str:
    for v in row:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "none":
            return s
    return ""


def _find_col_index(cols: list[str], candidates: list[str]) -> int:
    lower = [c.lower() for c in cols]
    cand_lower = [c.lower() for c in candidates]
    for i, c in enumerate(lower):
        for cand in cand_lower:
            if c == cand or cand in c:
                return i
    return -1


def _col_value_by_candidates(cols: list[str], row: list[Any], candidates: list[str]) -> Any:
    idx = _find_col_index(cols, candidates)
    if idx < 0 or idx >= len(row):
        return None
    return row[idx]


def _dtype_default_encoding(dtype: str) -> str:
    t = (dtype or "").upper()
    if t == "BOOLEAN":
        return "RLE"
    if t in {"INT32", "INT64", "FLOAT", "DOUBLE"}:
        return "GORILLA"
    if t in {"TEXT", "STRING", "BLOB"}:
        return "PLAIN"
    return "PLAIN"


def _dtype_default_compressor(dtype: str) -> str:
    return "SNAPPY"


def _parse_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return int(float(str(v).strip()))
    except Exception:
        return None


def _format_ttl_text(ttl_ms: int | None) -> str:
    if ttl_ms is None or ttl_ms <= 0:
        return "never expire"
    day_ms = 24 * 60 * 60 * 1000
    if ttl_ms % day_ms == 0:
        return f"{ttl_ms // day_ms}d"
    return f"{ttl_ms} ms"


def _extract_ttl_map(show_ttl_res: dict[str, Any]) -> dict[str, int]:
    cols = [str(c) for c in show_ttl_res.get("columns", [])]
    rows = show_ttl_res.get("rows", [])
    if not rows:
        return {}

    path_idx = -1
    ttl_idx = -1
    for i, c in enumerate(cols):
        n = _normalize_col_name(c)
        if path_idx < 0 and any(k in n for k in ["path", "database", "storagegroup", "device", "name"]):
            path_idx = i
        if ttl_idx < 0 and "ttl" in n:
            ttl_idx = i

    ttl_map: dict[str, int] = {}
    for row in rows:
        if not row:
            continue
        path = ""
        if path_idx >= 0 and path_idx < len(row):
            path = str(row[path_idx]).strip()
        if not path:
            path = _first_text_cell(row)
        if not path:
            continue

        ttl_val: int | None = None
        if ttl_idx >= 0 and ttl_idx < len(row):
            ttl_val = _parse_int(row[ttl_idx])
        if ttl_val is None:
            for cell in row:
                ttl_val = _parse_int(cell)
                if ttl_val is not None:
                    break
        if ttl_val is None or ttl_val <= 0:
            continue
        ttl_map[path] = ttl_val

    return ttl_map


def _format_time_text(ms: int | None) -> str:
    if ms is None or ms <= 0:
        return ""
    sec = ms / 1000.0 if ms > 1_000_000_000_000 else float(ms)
    try:
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    except Exception:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _extract_ttl_from_show_databases(show_db_res: dict[str, Any], database: str) -> int | None:
    cols = [str(c) for c in show_db_res.get("columns", [])]
    rows = show_db_res.get("rows", [])
    if not cols or not rows:
        return None

    db_idx = 0
    ttl_idx = -1
    for i, c in enumerate(cols):
        name = _normalize_col_name(c)
        if name in {"database", "database_name", "dbname", "name"}:
            db_idx = i
        if "ttl" in name:
            ttl_idx = i

    for row in rows:
        if not row:
            continue
        db_name = str(row[db_idx] if db_idx < len(row) else _first_text_cell(row)).strip()
        if db_name != database:
            continue
        if ttl_idx < 0 or ttl_idx >= len(row):
            return None
        ttl_val = _parse_int(row[ttl_idx])
        if ttl_val is None or ttl_val <= 0:
            return None
        return ttl_val
    return None


def _query_first_timestamp_ms(conn: Conn, database: str) -> int | None:
    # Best effort only. Different IoTDB versions can vary on wildcard support.
    for sql in [f"SELECT LAST * FROM {database}.**"]:
        res = _query(conn, sql)
        if not res.get("ok"):
            continue
        cols = [str(c) for c in res.get("columns", [])]
        rows = res.get("rows", [])
        if not rows:
            continue
        row = rows[0]
        time_idx = -1
        for i, c in enumerate(cols):
            n = _normalize_col_name(c)
            if n == "time" or "timestamp" in n:
                time_idx = i
                break
        if time_idx < 0:
            for i, v in enumerate(row):
                n = _parse_int(v)
                if n is not None and n > 0:
                    time_idx = i
                    break
        if time_idx < 0 or time_idx >= len(row):
            continue
        t = _parse_int(row[time_idx])
        if t is None:
            continue
        return t if t > 1_000_000_000_000 else t * 1000
    return None


def _exec_non_query(conn: Conn, sql: str) -> tuple[bool, str | None]:
    if Session is None:
        return False, "apache-iotdb is not installed"

    session = Session(conn.host, conn.port, conn.username, conn.password)
    try:
        session.open(False)
        exec_op = _find_op(session, ["execute_non_query_statement", "executeNonQueryStatement"])
        if exec_op is None:
            return False, "driver does not support execute_non_query_statement"
        exec_op(sql)
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        try:
            session.close()
        except Exception:
            pass


def _query(conn: Conn, sql: str) -> dict[str, Any]:
    if Session is None:
        return {"ok": False, "error": "鏈畨瑁?apache-iotdb锛岃鍏堝畨瑁?requirements-lite.txt"}

    session = Session(conn.host, conn.port, conn.username, conn.password)
    try:
        session.open(False)
        ds = session.execute_query_statement(sql)
        try:
            tab = _dataset_to_table(ds)
        finally:
            close_op = _find_op(ds, ["close_operation_handle", "closeOperationHandle"])
            if close_op:
                close_op()
        return {"ok": True, **tab}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            session.close()
        except Exception:
            pass


def _conn_from_payload(payload: dict[str, Any]) -> Conn:
    host = (payload.get("host") or "172.16.41.13").strip()
    try:
        port = int(payload.get("port") or 6667)
    except Exception as e:
        raise ValueError(f"invalid port: {payload.get('port')}") from e
    username = (payload.get("username") or "root").strip()
    password = payload.get("password") or "root"
    return Conn(host=host, port=port, username=username, password=password)


def _is_non_query_sql(sql: str) -> bool:
    s = (sql or "").strip().lstrip(";").strip()
    if not s:
        return False
    first = s.split(None, 1)[0].upper()
    non_query_heads = {
        "SET",
        "UNSET",
        "ALTER",
        "CREATE",
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "LOAD",
        "FLUSH",
    }
    return first in non_query_heads


def _set_ttl_for_target(
    conn: Conn, target: str, ttl_ms: int | None, allow_database_fallback: bool = True
) -> tuple[bool, str, list[str]]:
    targets = [target]
    # For many IoTDB deployments, TTL is attached to a path pattern.
    # Try path and path.** to improve compatibility for non-database scopes.
    if not allow_database_fallback and not target.endswith(".**"):
        targets.append(f"{target}.**")

    stmts: list[str] = []
    for t in targets:
        if ttl_ms is None:
            stmts.extend(
                [
                    f"UNSET TTL TO {t}",
                    f"UNSET TTL {t}",
                ]
            )
        else:
            # Keep only the stable syntax. Some driver/server combos throw
            # \"object of type 'NoneType' has no len()\" on the '=' variant.
            stmts.append(f"SET TTL TO {t} {ttl_ms}")

    if allow_database_fallback:
        if ttl_ms is None:
            stmts.extend(
                [
                    f"ALTER DATABASE {target} SET TTL = INF",
                    f"ALTER DATABASE {target} SET TTL = -1",
                ]
            )
        else:
            stmts.append(f"ALTER DATABASE {target} SET TTL = {ttl_ms}")

    errors: list[str] = []
    for sql in stmts:
        ok, err = _exec_non_query(conn, sql)
        if ok:
            return True, sql, errors
        errors.append(f"{sql}: {err}")
    return False, "", errors


def _resolve_ttl_for_path(conn: Conn, path: str) -> tuple[int | None, str | None]:
    show_res = _query(conn, "SHOW ALL TTL")
    if not show_res.get("ok"):
        # Fallback for some versions/permissions.
        show_res = _query(conn, "SHOW TTL")
        if not show_res.get("ok"):
            return None, None

    ttl_map = _extract_ttl_map(show_res)
    if not ttl_map:
        return None, None

    if path in ttl_map:
        return ttl_map[path], path

    best_path = None
    best_ttl: int | None = None
    best_len = -1
    for p, ttl in ttl_map.items():
        if path.startswith(p + ".") and len(p) > best_len:
            best_len = len(p)
            best_path = p
            best_ttl = ttl
    if best_path is not None:
        return best_ttl, best_path
    return None, None


def _is_same_or_pattern_path(requested_path: str, effective_path: str | None) -> bool:
    if not effective_path:
        return False
    req = requested_path.strip()
    eff = effective_path.strip()
    if req == eff:
        return True
    if req.endswith(".**") and req[:-3] == eff:
        return True
    if eff.endswith(".**") and eff[:-3] == req:
        return True
    return False


def _infer_database_for_path(conn: Conn, path: str) -> str:
    db_res = _query(conn, "SHOW DATABASES")
    if not db_res.get("ok"):
        return ""
    best = ""
    for row in db_res.get("rows", []):
        if not row:
            continue
        db = _first_text_cell(row)
        if not db:
            continue
        if path == db or path.startswith(db + "."):
            if len(db) > len(best):
                best = db
    return best


def _query_last_write_for_device(conn: Conn, device: str) -> int | None:
    # Best effort. Works on most versions that support LAST query.
    for sql in [f"SELECT LAST * FROM {device}", f"SELECT LAST * FROM {device}.*"]:
        res = _query(conn, sql)
        if not res.get("ok"):
            continue
        cols = [str(c) for c in res.get("columns", [])]
        rows = res.get("rows", [])
        if not rows:
            continue
        row = rows[0]
        time_idx = -1
        for i, c in enumerate(cols):
            n = _normalize_col_name(c)
            if n == "time" or "timestamp" in n:
                time_idx = i
                break
        if time_idx < 0:
            for i, v in enumerate(row):
                n = _parse_int(v)
                if n is not None and n > 0:
                    time_idx = i
                    break
        if time_idx < 0 or time_idx >= len(row):
            continue
        t = _parse_int(row[time_idx])
        if t is None:
            continue
        return t if t > 1_000_000_000_000 else t * 1000
    return None


def _query_last_write_for_path(conn: Conn, path: str) -> int | None:
    # Best effort for any prefix path.
    for sql in [f"SELECT LAST * FROM {path}.**", f"SELECT LAST * FROM {path}.*"]:
        res = _query(conn, sql)
        if not res.get("ok"):
            continue
        cols = [str(c) for c in res.get("columns", [])]
        rows = res.get("rows", [])
        if not rows:
            continue
        row = rows[0]
        time_idx = -1
        for i, c in enumerate(cols):
            n = _normalize_col_name(c)
            if n == "time" or "timestamp" in n:
                time_idx = i
                break
        if time_idx < 0:
            for i, v in enumerate(row):
                n = _parse_int(v)
                if n is not None and n > 0:
                    time_idx = i
                    break
        if time_idx < 0 or time_idx >= len(row):
            continue
        t = _parse_int(row[time_idx])
        if t is None:
            continue
        return t if t > 1_000_000_000_000 else t * 1000
    return None


def _child_paths_from_devices(path: str, devices: list[str]) -> list[str]:
    prefix = path + "."
    seen: set[str] = set()
    children: list[str] = []
    for dev in devices:
        if dev == path:
            continue
        if not dev.startswith(prefix):
            continue
        rest = dev[len(prefix) :]
        if not rest:
            continue
        first = rest.split(".", 1)[0]
        child = f"{prefix}{first}"
        if child in seen:
            continue
        seen.add(child)
        children.append(child)
    return sorted(children)


@app.get("/")
def index():
    return render_template("index.html", app_version=APP_VERSION)


@app.post("/api/query")
def api_query():
    payload = request.get_json(silent=True) or {}
    sql = (payload.get("sql") or "").strip()

    if not sql:
        return jsonify({"ok": False, "error": "sql is empty"}), 400

    try:
        conn = _conn_from_payload(payload)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    try:
        if _is_non_query_sql(sql):
            ok, err = _exec_non_query(conn, sql)
            if not ok:
                return jsonify({"ok": False, "error": err or "non-query failed"}), 502
            return jsonify({"ok": True, "columns": ["status"], "rows": [["OK"]]})
        else:
            res = _query(conn, sql)
            if not res.get("ok"):
                return jsonify(res), 502
            return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "error": f"internal error: {e}"}), 500


@app.post("/api/version")
def api_version():
    payload = request.get_json(silent=True) or {}
    conn = _conn_from_payload(payload)

    # Try common version statements with fallback.
    candidates = ["SHOW VERSION", "SHOW CURRENT VERSION"]
    errs: list[str] = []
    for sql in candidates:
        res = _query(conn, sql)
        if not res.get("ok"):
            errs.append(str(res.get("error") or "query failed"))
            continue
        rows = res.get("rows", [])
        ver = ""
        if rows:
            ver = _first_text_cell(rows[0])
        if not ver:
            cols = [str(c) for c in res.get("columns", [])]
            if cols:
                ver = cols[0]
        return jsonify({"ok": True, "version": ver or "unknown"})

    return jsonify({"ok": False, "error": "failed to query version", "details": errs}), 502


@app.post("/api/tree")
def api_tree():
    payload = request.get_json(silent=True) or {}
    conn = _conn_from_payload(payload)

    db_res = _query(conn, "SHOW DATABASES")
    if not db_res.get("ok"):
        return jsonify(db_res), 502

    dbs: list[str] = []
    for row in db_res.get("rows", []):
        if row:
            v = _first_text_cell(row)
            if v:
                dbs.append(v)

    tree: dict[str, list[str]] = {}
    for db in dbs:
        dev_res = _query(conn, f"SHOW DEVICES {db}.**")
        if not dev_res.get("ok"):
            tree[db] = []
            continue
        devices: list[str] = []
        for r in dev_res.get("rows", []):
            if not r:
                continue
            v = _first_text_cell(r)
            if v:
                devices.append(v)
        tree[db] = devices

    return jsonify({"ok": True, "tree": tree})


@app.post("/api/points")
def api_points():
    payload = request.get_json(silent=True) or {}
    device = (payload.get("device") or "").strip()
    keyword = str(payload.get("keyword") or "").strip().lower()
    page = _parse_int(payload.get("page")) or 1
    page_size = _parse_int(payload.get("page_size")) or 1000

    if not device:
        return jsonify({"ok": False, "error": "device is empty"}), 400
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1000
    if page_size > 5000:
        page_size = 5000

    conn = _conn_from_payload(payload)

    def _items_from_timeseries_res(timeseries_res: dict[str, Any], dev: str) -> list[tuple[str, str, str]]:
        cols = [str(c) for c in timeseries_res.get("columns", [])]
        ts_idx = _find_col_index(cols, ["timeseries"])
        dtype_idx = _find_col_index(cols, ["datatype", "data_type"])
        prefix = f"{dev}."
        items: list[tuple[str, str, str]] = []
        for row in timeseries_res.get("rows", []):
            if not row:
                continue
            if ts_idx >= 0 and ts_idx < len(row):
                full_path = str(row[ts_idx]).strip()
            else:
                full_path = _first_text_cell(row)
            if not full_path:
                continue
            point = full_path[len(prefix) :] if full_path.startswith(prefix) else full_path
            dtype = ""
            if dtype_idx >= 0 and dtype_idx < len(row):
                dtype = str(row[dtype_idx]).strip().upper()
            items.append((point, full_path, dtype))
        return items

    # Fast path: no keyword => use server LIMIT/OFFSET when possible.
    # Fallback to full fetch if the SQL dialect/version does not support it.
    page_items: list[tuple[str, str, str]] = []
    total: int | None = None
    if not keyword:
        offset = (page - 1) * page_size
        count_res = _query(conn, f"COUNT TIMESERIES {device}.*")
        if count_res.get("ok"):
            for row in count_res.get("rows", []):
                for cell in row:
                    n = _parse_int(cell)
                    if n is not None and n >= 0:
                        total = n
                        break
                if total is not None:
                    break
        page_res = _query(conn, f"SHOW TIMESERIES {device}.* LIMIT {page_size} OFFSET {offset}")
        if page_res.get("ok"):
            page_items = _items_from_timeseries_res(page_res, device)
        else:
            # Some versions/drivers do not support SHOW TIMESERIES LIMIT/OFFSET.
            # Force fallback path below to guarantee correctness.
            total = None

    if total is None:
        # Search-optimized path for huge point counts:
        # If keyword is a simple identifier, try server-side prefix filter first.
        if keyword and re.fullmatch(r"[A-Za-z0-9_]+", keyword):
            offset = (page - 1) * page_size
            search_pattern = f"{device}.{keyword}*"
            count_res = _query(conn, f"COUNT TIMESERIES {search_pattern}")
            if count_res.get("ok"):
                for row in count_res.get("rows", []):
                    for cell in row:
                        n = _parse_int(cell)
                        if n is not None and n >= 0:
                            total = n
                            break
                    if total is not None:
                        break
            page_res = _query(conn, f"SHOW TIMESERIES {search_pattern} LIMIT {page_size} OFFSET {offset}")
            if page_res.get("ok"):
                page_items = _items_from_timeseries_res(page_res, device)
                if total is not None:
                    total_pages = max(1, (total + page_size - 1) // page_size)
                    if page > total_pages:
                        page = total_pages
                        offset = (page - 1) * page_size
                        page_res = _query(conn, f"SHOW TIMESERIES {search_pattern} LIMIT {page_size} OFFSET {offset}")
                        if page_res.get("ok"):
                            page_items = _items_from_timeseries_res(page_res, device)
                        else:
                            page_items = []
                    points: list[str] = []
                    point_types: dict[str, str] = {}
                    point_paths: dict[str, str] = {}
                    for p, full_path, dtype in page_items:
                        points.append(p)
                        point_paths[p] = full_path
                        if dtype:
                            point_types[p] = dtype
                    return jsonify(
                        {
                            "ok": True,
                            "points": points,
                            "point_types": point_types,
                            "point_paths": point_paths,
                            "page": page,
                            "page_size": page_size,
                            "total": total,
                            "total_pages": total_pages,
                            "keyword": keyword,
                        }
                    )

        # Fallback path: full fetch + filter + paginate (used for keyword/global search).
        res = _query(conn, f"SHOW TIMESERIES {device}.*")
        if not res.get("ok"):
            return jsonify(res), 502

        all_items = _items_from_timeseries_res(res, device)

        all_items.sort(key=lambda x: x[0])
        if keyword:
            all_items = [it for it in all_items if keyword in it[0].lower()]

        total = len(all_items)
        total_pages = max(1, (total + page_size - 1) // page_size)
        if page > total_pages:
            page = total_pages
        start = (page - 1) * page_size
        end = start + page_size
        page_items = all_items[start:end]
    else:
        total_pages = max(1, (total + page_size - 1) // page_size)
        if page > total_pages:
            page = total_pages
            page_items = []
            offset = (page - 1) * page_size
            page_res = _query(conn, f"SHOW TIMESERIES {device}.* LIMIT {page_size} OFFSET {offset}")
            if page_res.get("ok"):
                page_items = _items_from_timeseries_res(page_res, device)

    points: list[str] = []
    point_types: dict[str, str] = {}
    point_paths: dict[str, str] = {}
    for p, full_path, dtype in page_items:
        points.append(p)
        point_paths[p] = full_path
        if dtype:
            point_types[p] = dtype

    return jsonify(
        {
            "ok": True,
            "points": points,
            "point_types": point_types,
            "point_paths": point_paths,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "keyword": keyword,
        }
    )


@app.post("/api/point_delete")
def api_point_delete():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400

    conn = _conn_from_payload(payload)
    ok, err = _exec_non_query(conn, f"DELETE TIMESERIES {path}")
    if not ok:
        return jsonify({"ok": False, "error": err or "delete timeseries failed"}), 502
    return jsonify({"ok": True, "path": path})


@app.post("/api/point_retype")
def api_point_retype():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    data_type = str(payload.get("data_type") or "").strip().upper()

    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400
    if not data_type:
        return jsonify({"ok": False, "error": "data_type is empty"}), 400

    supported = {
        "BOOLEAN",
        "INT32",
        "INT64",
        "FLOAT",
        "DOUBLE",
        "TEXT",
        "STRING",
        "BLOB",
        "DATE",
        "TIMESTAMP",
    }
    if data_type not in supported:
        return jsonify({"ok": False, "error": f"unsupported data_type: {data_type}"}), 400

    encoding = str(payload.get("encoding") or _dtype_default_encoding(data_type)).strip().upper()
    compressor = str(payload.get("compressor") or _dtype_default_compressor(data_type)).strip().upper()

    conn = _conn_from_payload(payload)

    ok1, err1 = _exec_non_query(conn, f"DELETE TIMESERIES {path}")
    if not ok1:
        return jsonify({"ok": False, "error": err1 or "delete before recreate failed"}), 502

    create_sql = (
        f"CREATE TIMESERIES {path} "
        f"WITH DATATYPE={data_type}, ENCODING={encoding}, COMPRESSOR={compressor}"
    )
    ok2, err2 = _exec_non_query(conn, create_sql)
    if not ok2:
        return jsonify({"ok": False, "error": err2 or "create timeseries failed", "sql": create_sql}), 502

    return jsonify(
        {
            "ok": True,
            "path": path,
            "data_type": data_type,
            "encoding": encoding,
            "compressor": compressor,
            "sql": create_sql,
        }
    )


@app.post("/api/db_info")
def api_db_info():
    payload = request.get_json(silent=True) or {}
    database = str(payload.get("database") or "").strip()
    if not database:
        return jsonify({"ok": False, "error": "database is empty"}), 400

    conn = _conn_from_payload(payload)

    db_res = _query(conn, "SHOW DATABASES")
    if not db_res.get("ok"):
        return jsonify(db_res), 502

    device_res = _query(conn, f"SHOW DEVICES {database}.**")
    if not device_res.get("ok"):
        return jsonify(device_res), 502

    ts_res = _query(conn, f"SHOW TIMESERIES {database}.**")
    if not ts_res.get("ok"):
        return jsonify(ts_res), 502

    devices: list[str] = []
    for row in device_res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if v:
            devices.append(v)

    timeseries: list[str] = []
    for row in ts_res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if v:
            timeseries.append(v)

    ttl_ms = _extract_ttl_from_show_databases(db_res, database)
    last_write_ms = _query_first_timestamp_ms(conn, database)

    return jsonify(
        {
            "ok": True,
            "database": database,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "device_count": len(devices),
            "timeseries_count": len(timeseries),
            "sample_devices": devices[:12],
            "sample_timeseries": timeseries[:12],
            "last_write_ms": last_write_ms,
            "last_write_text": _format_time_text(last_write_ms),
        }
    )


@app.post("/api/db_ttl")
def api_db_ttl():
    payload = request.get_json(silent=True) or {}
    database = str(payload.get("database") or "").strip()
    ttl_ms_raw = payload.get("ttl_ms")

    if not database:
        return jsonify({"ok": False, "error": "database is empty"}), 400

    ttl_ms = None
    if ttl_ms_raw is not None:
        ttl_ms = _parse_int(ttl_ms_raw)
        if ttl_ms is None or ttl_ms < 0:
            return jsonify({"ok": False, "error": "ttl_ms must be null or non-negative integer"}), 400

    conn = _conn_from_payload(payload)
    ok, used_sql, errors = _set_ttl_for_target(conn, database, ttl_ms, allow_database_fallback=True)
    if not ok:
        return jsonify({"ok": False, "error": "failed to set ttl", "details": errors}), 502

    return jsonify(
        {
            "ok": True,
            "database": database,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "sql": used_sql,
        }
    )


@app.post("/api/path_ttl_get")
def api_path_ttl_get():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400

    conn = _conn_from_payload(payload)
    ttl_ms, source_path = _resolve_ttl_for_path(conn, path)
    return jsonify(
        {
            "ok": True,
            "path": path,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "source_path": source_path,
            "direct": source_path == path if source_path else False,
        }
    )


@app.post("/api/path_ttl")
def api_path_ttl():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    ttl_ms_raw = payload.get("ttl_ms")

    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400

    ttl_ms = None
    if ttl_ms_raw is not None:
        ttl_ms = _parse_int(ttl_ms_raw)
        if ttl_ms is None or ttl_ms < 0:
            return jsonify({"ok": False, "error": "ttl_ms must be null or non-negative integer"}), 400

    conn = _conn_from_payload(payload)
    ok, used_sql, errors = _set_ttl_for_target(conn, path, ttl_ms, allow_database_fallback=False)
    if not ok:
        return jsonify({"ok": False, "error": "failed to set ttl", "details": errors}), 502

    # Strict check: path_ttl should apply to current path (or be removed on current path),
    # not silently escalate to database-level scope.
    after_ttl_ms, after_source_path = _resolve_ttl_for_path(conn, path)
    if ttl_ms is not None and not _is_same_or_pattern_path(path, after_source_path):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "ttl was not applied to the requested path",
                    "path": path,
                    "requested_ttl_ms": ttl_ms,
                    "effective_source_path": after_source_path,
                    "effective_ttl_ms": after_ttl_ms,
                    "hint": "Current IoTDB version may only support database-level TTL for this command.",
                }
            ),
            409,
        )

    return jsonify(
        {
            "ok": True,
            "path": path,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "sql": used_sql,
        }
    )


@app.post("/api/device_info")
def api_device_info():
    payload = request.get_json(silent=True) or {}
    device = str(payload.get("device") or "").strip()
    if not device:
        return jsonify({"ok": False, "error": "device is empty"}), 400

    conn = _conn_from_payload(payload)
    ts_res = _query(conn, f"SHOW TIMESERIES {device}.*")
    if not ts_res.get("ok"):
        return jsonify(ts_res), 502

    points: list[str] = []
    prefix = f"{device}."
    for row in ts_res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if not v:
            continue
        points.append(v[len(prefix) :] if v.startswith(prefix) else v)
    points = sorted(points)

    ttl_ms, source_path = _resolve_ttl_for_path(conn, device)
    last_write_ms = _query_last_write_for_device(conn, device)
    database = _infer_database_for_path(conn, device)

    return jsonify(
        {
            "ok": True,
            "device": device,
            "database": database,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "ttl_source_path": source_path,
            "point_count": len(points),
            "sample_points": points[:50],
            "last_write_ms": last_write_ms,
            "last_write_text": _format_time_text(last_write_ms),
        }
    )


@app.post("/api/point_info")
def api_point_info():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400

    conn = _conn_from_payload(payload)

    ts_res = _query(conn, f"SHOW TIMESERIES {path}")
    if not ts_res.get("ok"):
        return jsonify(ts_res), 502

    rows = ts_res.get("rows", [])
    if not rows:
        return jsonify({"ok": False, "error": f"point not found: {path}"}), 404

    cols = [str(c) for c in ts_res.get("columns", [])]
    row = rows[0]

    full_path = str(_col_value_by_candidates(cols, row, ["timeseries"]) or path).strip()
    data_type = str(_col_value_by_candidates(cols, row, ["datatype", "data_type"]) or "").strip().upper()
    encoding = str(_col_value_by_candidates(cols, row, ["encoding"]) or "").strip().upper()
    compressor = str(_col_value_by_candidates(cols, row, ["compressor", "compression"]) or "").strip().upper()
    alias = str(_col_value_by_candidates(cols, row, ["alias"]) or "").strip()
    database = str(_col_value_by_candidates(cols, row, ["database", "storage_group", "storagegroup"]) or "").strip()
    tags_raw = _col_value_by_candidates(cols, row, ["tags"])
    attrs_raw = _col_value_by_candidates(cols, row, ["attributes"])

    if "." in full_path:
        device, point_name = full_path.rsplit(".", 1)
    else:
        device, point_name = "", full_path

    ttl_ms, ttl_source_path = _resolve_ttl_for_path(conn, device or full_path)
    last_write_ms = _query_last_write_for_device(conn, device) if device else None

    last_value = None
    if device and point_name:
        last_res = _query(conn, f"SELECT LAST `{point_name}` FROM {device}")
        if last_res.get("ok"):
            lcols = [str(c) for c in last_res.get("columns", [])]
            lrows = last_res.get("rows", [])
            if lrows:
                first = lrows[0]
                time_idx = _find_col_index(lcols, ["time", "timestamp"])
                value_idx = -1
                for i, c in enumerate(lcols):
                    n = _normalize_col_name(c)
                    if n not in {"time", "timestamp"}:
                        value_idx = i
                        break
                if time_idx >= 0 and time_idx < len(first):
                    parsed = _parse_int(first[time_idx])
                    if parsed is not None:
                        last_write_ms = parsed if parsed > 1_000_000_000_000 else parsed * 1000
                if value_idx >= 0 and value_idx < len(first):
                    last_value = _to_json_cell(first[value_idx])

    return jsonify(
        {
            "ok": True,
            "path": full_path,
            "point": point_name,
            "device": device,
            "database": database or _infer_database_for_path(conn, full_path),
            "data_type": data_type or "UNKNOWN",
            "encoding": encoding or "-",
            "compressor": compressor or "-",
            "alias": alias,
            "tags": _to_json_cell(tags_raw),
            "attributes": _to_json_cell(attrs_raw),
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "ttl_source_path": ttl_source_path,
            "last_write_ms": last_write_ms,
            "last_write_text": _format_time_text(last_write_ms),
            "last_value": _to_json_cell(last_value),
        }
    )


@app.post("/api/path_info")
def api_path_info():
    payload = request.get_json(silent=True) or {}
    path = str(payload.get("path") or "").strip()
    if not path:
        return jsonify({"ok": False, "error": "path is empty"}), 400

    conn = _conn_from_payload(payload)

    dev_res = _query(conn, f"SHOW DEVICES {path}.**")
    if not dev_res.get("ok"):
        return jsonify(dev_res), 502

    ts_res = _query(conn, f"SHOW TIMESERIES {path}.**")
    if not ts_res.get("ok"):
        return jsonify(ts_res), 502

    devices: list[str] = []
    for row in dev_res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if v:
            devices.append(v)

    timeseries: list[str] = []
    for row in ts_res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if v:
            timeseries.append(v)

    ttl_ms, ttl_source_path = _resolve_ttl_for_path(conn, path)
    last_write_ms = _query_last_write_for_path(conn, path)
    database = _infer_database_for_path(conn, path)
    child_paths = _child_paths_from_devices(path, devices)

    return jsonify(
        {
            "ok": True,
            "path": path,
            "database": database,
            "ttl_ms": ttl_ms,
            "ttl_text": _format_ttl_text(ttl_ms),
            "ttl_source_path": ttl_source_path,
            "device_count": len(devices),
            "timeseries_count": len(timeseries),
            "sample_devices": devices[:50],
            "sample_timeseries": timeseries[:50],
            "child_paths": child_paths[:50],
            "last_write_ms": last_write_ms,
            "last_write_text": _format_time_text(last_write_ms),
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False, use_reloader=False)






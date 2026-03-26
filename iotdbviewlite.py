from __future__ import annotations

from dataclasses import dataclass
import math
import os
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
APP_VERSION = "0.1.1"


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


def _query(conn: Conn, sql: str) -> dict[str, Any]:
    if Session is None:
        return {"ok": False, "error": "未安装 apache-iotdb，请先安装 requirements-lite.txt"}

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


@app.get("/")
def index():
    return render_template("index.html", app_version=APP_VERSION)


@app.post("/api/query")
def api_query():
    payload = request.get_json(silent=True) or {}
    sql = (payload.get("sql") or "").strip()
    host = (payload.get("host") or "172.16.41.13").strip()
    port = int(payload.get("port") or 6667)
    username = (payload.get("username") or "root").strip()
    password = payload.get("password") or "root"

    if not sql:
        return jsonify({"ok": False, "error": "sql is empty"}), 400

    conn = Conn(host=host, port=port, username=username, password=password)
    res = _query(conn, sql)
    if not res.get("ok"):
        return jsonify(res), 502

    return jsonify(res)


@app.post("/api/tree")
def api_tree():
    payload = request.get_json(silent=True) or {}
    host = (payload.get("host") or "172.16.41.13").strip()
    port = int(payload.get("port") or 6667)
    username = (payload.get("username") or "root").strip()
    password = payload.get("password") or "root"

    conn = Conn(host=host, port=port, username=username, password=password)

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
    host = (payload.get("host") or "172.16.41.13").strip()
    port = int(payload.get("port") or 6667)
    username = (payload.get("username") or "root").strip()
    password = payload.get("password") or "root"
    device = (payload.get("device") or "").strip()

    if not device:
        return jsonify({"ok": False, "error": "device is empty"}), 400

    conn = Conn(host=host, port=port, username=username, password=password)
    res = _query(conn, f"SHOW TIMESERIES {device}.*")
    if not res.get("ok"):
        return jsonify(res), 502

    points: list[str] = []
    prefix = f"{device}."
    for row in res.get("rows", []):
        if not row:
            continue
        v = _first_text_cell(row)
        if not v:
            continue
        points.append(v[len(prefix) :] if v.startswith(prefix) else v)

    return jsonify({"ok": True, "points": sorted(points)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False, use_reloader=False)


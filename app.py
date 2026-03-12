from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd
import streamlit as st


try:
    from iotdb.Session import Session  # type: ignore
except Exception:  # pragma: no cover
    Session = None


@dataclass
class ConnConfig:
    host: str
    port: int
    username: str
    password: str
    fetch_size: int = 10000


class IoTDBClient:
    def __init__(self, cfg: ConnConfig):
        if Session is None:
            raise RuntimeError("未安装 apache-iotdb，请先执行: pip install -r requirements.txt")
        self.cfg = cfg
        self.session = Session(cfg.host, cfg.port, cfg.username, cfg.password, fetch_size=cfg.fetch_size)

    def __enter__(self) -> "IoTDBClient":
        self.session.open(False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def query_df(self, sql: str) -> pd.DataFrame:
        ds = self.session.execute_query_statement(sql)
        try:
            return _dataset_to_df(ds)
        finally:
            close_op = _find_op(ds, ["close_operation_handle", "closeOperationHandle"])
            if close_op:
                close_op()

    def list_databases(self) -> list[str]:
        df = self.query_df("SHOW DATABASES")
        if df.empty:
            return []
        col = _pick_column(df, ["database", "database_name", "Database"])
        return sorted(df[col].dropna().astype(str).tolist())

    def list_devices(self, database: str) -> list[str]:
        df = self.query_df(f"SHOW DEVICES {database}.**")
        if df.empty:
            return []
        col = _pick_column(df, ["device", "devices", "Device"])
        return sorted(df[col].dropna().astype(str).tolist())

    def list_points(self, device: str) -> list[str]:
        df = self.query_df(f"SHOW TIMESERIES {device}.*")
        if df.empty:
            return []
        col = _pick_column(df, ["timeseries", "Timeseries"])
        full = df[col].dropna().astype(str).tolist()
        prefix = f"{device}."
        return sorted([x[len(prefix) :] if x.startswith(prefix) else x for x in full])


def _find_op(obj: Any, names: list[str]) -> Callable[..., Any] | None:
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _pick_column(df: pd.DataFrame, preferred: list[str]) -> str:
    cols_lower = {c.lower(): c for c in df.columns}
    for p in preferred:
        key = p.lower()
        if key in cols_lower:
            return cols_lower[key]
    return df.columns[0]


def _dataset_to_df(ds: Any) -> pd.DataFrame:
    todf = _find_op(ds, ["todf", "to_df"])
    if todf:
        try:
            data = todf()
            if isinstance(data, pd.DataFrame):
                return data
        except Exception:
            pass

    names_getter = _find_op(ds, ["get_column_names", "getColumnNames"])
    col_names: list[str] = names_getter() if names_getter else []
    if not col_names:
        col_names = ["Time", "Value"]

    has_next = _find_op(ds, ["has_next", "hasNext"])
    next_row = _find_op(ds, ["next"])
    if not has_next or not next_row:
        return pd.DataFrame(columns=col_names)

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
            vals.append(s_get() if s_get else str(f))

        row = [ts] + vals
        if len(row) < len(col_names):
            row.extend([None] * (len(col_names) - len(row)))
        rows.append(row[: len(col_names)])

    return pd.DataFrame(rows, columns=col_names)


def _state_init() -> None:
    defaults: dict[str, Any] = {
        "dbs": [],
        "dev_map": {},
        "points_map": {},
        "selected_device": "",
        "selected_points": [],
        "sql_text": "",
        "last_result": pd.DataFrame(),
        "tree_filter": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def quote_ident(name: str) -> str:
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def build_sql(
    device: str,
    points: list[str],
    agg: str,
    start_ms: int,
    end_ms: int,
    interval: str,
    fill: str,
    order: str,
    limit: int,
) -> str:
    if not points:
        return "-- 请先选择至少一个 point"

    where = f"time >= {start_ms} AND time <= {end_ms}"

    if agg == "raw":
        select_expr = ", ".join(quote_ident(p) for p in points)
        return f"SELECT {select_expr} FROM {device} WHERE {where} ORDER BY TIME {order} LIMIT {limit}"

    fn_map = {
        "mean": "AVG",
        "max": "MAX_VALUE",
        "min": "MIN_VALUE",
    }
    fn = fn_map.get(agg, "AVG")

    exprs = [f"{fn}({quote_ident(p)}) AS {quote_ident(f'{agg}_{p}')}" for p in points]
    select_expr = ", ".join(exprs)

    fill_clause = ""
    if fill != "none":
        fill_map = {
            "null": "FILL(NULL)",
            "previous": "FILL(PREVIOUS)",
            "linear": "FILL(LINEAR)",
            "0": "FILL(0)",
        }
        fill_clause = " " + fill_map.get(fill, "")

    return (
        f"SELECT {select_expr} FROM {device} "
        f"WHERE {where} "
        f"GROUP BY ([{start_ms}, {end_ms}), {interval})"
        f"{fill_clause} "
        f"ORDER BY TIME {order} LIMIT {limit}"
    ).strip()


def resolve_range(preset: str) -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    mapping = {
        "最近15分钟": timedelta(minutes=15),
        "最近1小时": timedelta(hours=1),
        "最近6小时": timedelta(hours=6),
        "最近24小时": timedelta(hours=24),
        "最近7天": timedelta(days=7),
        "最近30天": timedelta(days=30),
    }
    delta = mapping.get(preset, timedelta(hours=1))
    start = now - delta
    return int(start.timestamp() * 1000), int(now.timestamp() * 1000)


def load_metadata(cfg: ConnConfig) -> tuple[list[str], dict[str, list[str]]]:
    with IoTDBClient(cfg) as client:
        dbs = client.list_databases()
        dev_map: dict[str, list[str]] = {}
        for db in dbs:
            dev_map[db] = client.list_devices(db)
        return dbs, dev_map


def run_query(cfg: ConnConfig, sql: str) -> pd.DataFrame:
    with IoTDBClient(cfg) as client:
        return client.query_df(sql)


def run_points(cfg: ConnConfig, device: str) -> list[str]:
    with IoTDBClient(cfg) as client:
        return client.list_points(device)


def pick_time_col(df: pd.DataFrame) -> str | None:
    preferred = ["time", "timestamp", "ts"]
    lower_map = {c.lower(): c for c in df.columns}
    for p in preferred:
        if p in lower_map:
            return lower_map[p]

    for c in df.columns:
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().mean() > 0.8 and s.max() > 1_000_000_000:
            return c
    return None


def build_chart_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    time_col = pick_time_col(tmp)
    if time_col is None:
        return pd.DataFrame()

    time_num = pd.to_numeric(tmp[time_col], errors="coerce")
    unit = "ms" if time_num.dropna().median() > 10_000_000_000 else "s"
    tmp[time_col] = pd.to_datetime(time_num, unit=unit, errors="coerce", utc=True)

    value_cols: list[str] = []
    for c in tmp.columns:
        if c == time_col:
            continue
        ser = pd.to_numeric(tmp[c], errors="coerce")
        if ser.notna().any():
            tmp[c] = ser
            value_cols.append(c)

    if not value_cols:
        return pd.DataFrame()

    return tmp[[time_col] + value_cols].dropna(subset=[time_col]).set_index(time_col).sort_index()


def render_tree(cfg: ConnConfig) -> None:
    st.subheader("IoTDB 树")
    st.session_state["tree_filter"] = st.text_input("树过滤", value=st.session_state.get("tree_filter", ""))
    keyword = st.session_state["tree_filter"].strip().lower()

    dbs = st.session_state.get("dbs", [])
    dev_map = st.session_state.get("dev_map", {})
    points_map: dict[str, list[str]] = st.session_state.get("points_map", {})

    for db in dbs:
        devices = dev_map.get(db, [])
        if keyword and keyword not in db.lower() and not any(keyword in d.lower() for d in devices):
            continue

        with st.expander(db, expanded=False):
            for dev in devices:
                if keyword and keyword not in dev.lower() and keyword not in db.lower():
                    if dev in points_map and not any(keyword in p.lower() for p in points_map[dev]):
                        continue

                cols = st.columns([3, 1, 1])
                with cols[0]:
                    st.caption(dev)
                with cols[1]:
                    if st.button("选中", key=f"pick_{dev}"):
                        st.session_state["selected_device"] = dev
                        st.session_state["selected_points"] = []
                with cols[2]:
                    if st.button("点位", key=f"loadp_{dev}"):
                        try:
                            st.session_state["points_map"][dev] = run_points(cfg, dev)
                        except Exception as e:
                            st.error(f"加载 {dev} points 失败: {e}")

                if dev == st.session_state.get("selected_device", ""):
                    if dev not in points_map:
                        try:
                            st.session_state["points_map"][dev] = run_points(cfg, dev)
                        except Exception as e:
                            st.error(f"加载 {dev} points 失败: {e}")
                            st.session_state["points_map"][dev] = []

                    for p in st.session_state["points_map"].get(dev, [])[:200]:
                        if keyword and keyword not in p.lower() and keyword not in dev.lower() and keyword not in db.lower():
                            continue
                        pkey = f"tp_{dev}_{p}"
                        checked = p in st.session_state.get("selected_points", [])
                        new_checked = st.checkbox(p, value=checked, key=pkey)
                        if new_checked and p not in st.session_state["selected_points"]:
                            st.session_state["selected_points"].append(p)
                        if (not new_checked) and p in st.session_state["selected_points"]:
                            st.session_state["selected_points"].remove(p)


def main() -> None:
    st.set_page_config(page_title="IoTDB Query Viewer", layout="wide")
    _state_init()
    st.title("IoTDB 简易浏览与查询工具")

    with st.sidebar:
        st.subheader("连接配置")
        host = st.text_input("Host", value="172.16.41.13")
        port = st.number_input("Port", value=6667, step=1)
        username = st.text_input("Username", value="root")
        password = st.text_input("Password", value="root", type="password")
        fetch_size = st.number_input("Fetch Size", min_value=100, max_value=1000000, value=10000, step=100)

        cfg = ConnConfig(
            host=host.strip(),
            port=int(port),
            username=username.strip(),
            password=password,
            fetch_size=int(fetch_size),
        )

        if st.button("测试连接"):
            try:
                df = run_query(cfg, "SHOW VERSION")
                st.success("连接成功")
                st.dataframe(df, use_container_width=True, height=120)
            except Exception as e:
                st.error(f"连接失败: {e}")

        if st.button("加载树形元数据"):
            try:
                dbs, dev_map = load_metadata(cfg)
                st.session_state["dbs"] = dbs
                st.session_state["dev_map"] = dev_map
                st.session_state["points_map"] = {}
                st.success(f"元数据加载完成：{len(dbs)} 个数据库")
            except Exception as e:
                st.error(f"加载失败: {e}")

        if st.button("清空状态"):
            for key in ["selected_device", "selected_points", "sql_text", "last_result", "points_map"]:
                if key == "last_result":
                    st.session_state[key] = pd.DataFrame()
                elif key == "points_map":
                    st.session_state[key] = {}
                else:
                    st.session_state[key] = [] if key == "selected_points" else ""
            st.success("已清空")

    if not st.session_state.get("dbs"):
        st.info("请先在左侧点击“加载树形元数据”。")
        return

    left, right = st.columns([1.2, 2.2])

    with left:
        render_tree(cfg)

    with right:
        device = st.session_state.get("selected_device", "")
        st.subheader("Point 与 SQL 构建")
        if not device:
            st.info("左侧选择一个设备后开始查询。")
            return

        st.write(f"当前设备: `{device}`")
        points_map = st.session_state.get("points_map", {})
        if device not in points_map:
            try:
                st.session_state["points_map"][device] = run_points(cfg, device)
            except Exception:
                st.session_state["points_map"][device] = []

        all_points = st.session_state["points_map"].get(device, [])
        selected_points = st.multiselect(
            "选择 points",
            options=all_points,
            default=[p for p in st.session_state.get("selected_points", []) if p in all_points],
            key="multisel_points",
        )
        st.session_state["selected_points"] = selected_points

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            agg = st.selectbox("聚合", ["raw", "mean", "max", "min"], index=1)
        with c2:
            preset = st.selectbox(
                "时间范围",
                ["最近15分钟", "最近1小时", "最近6小时", "最近24小时", "最近7天", "最近30天", "自定义"],
                index=3,
            )
        with c3:
            interval = st.selectbox("聚合间隔", ["1m", "5m", "15m", "1h", "1d"], index=2)
        with c4:
            fill = st.selectbox("Fill", ["none", "null", "previous", "linear", "0"], index=1)

        if preset == "自定义":
            d1, d2 = st.columns(2)
            with d1:
                start_dt = st.datetime_input("开始时间(UTC)", value=datetime.now(timezone.utc) - timedelta(hours=1))
            with d2:
                end_dt = st.datetime_input("结束时间(UTC)", value=datetime.now(timezone.utc))
            start_ms = int(pd.Timestamp(start_dt).tz_convert("UTC").timestamp() * 1000)
            end_ms = int(pd.Timestamp(end_dt).tz_convert("UTC").timestamp() * 1000)
        else:
            start_ms, end_ms = resolve_range(preset)

        c5, c6 = st.columns(2)
        with c5:
            order = st.selectbox("排序", ["DESC", "ASC"], index=0)
        with c6:
            limit = st.number_input("Limit", min_value=1, max_value=200000, value=1000, step=100)

        st.caption(f"时间戳范围(ms): {start_ms} ~ {end_ms}")

        generated = build_sql(
            device=device,
            points=selected_points,
            agg=agg,
            start_ms=start_ms,
            end_ms=end_ms,
            interval=interval,
            fill=fill,
            order=order,
            limit=int(limit),
        )

        b1, b2 = st.columns([1, 3])
        with b1:
            if st.button("应用预制 SQL"):
                st.session_state["sql_text"] = generated
        with b2:
            st.caption("支持手动修改 SQL；不会被自动覆盖，除非点击“应用预制 SQL”。")

        if not st.session_state.get("sql_text"):
            st.session_state["sql_text"] = generated

        sql_text = st.text_area("SQL（可手动修改）", value=st.session_state["sql_text"], height=180)
        st.session_state["sql_text"] = sql_text

        if st.button("执行 SQL", type="primary"):
            try:
                result = run_query(cfg, sql_text)
                st.session_state["last_result"] = result
                st.success(f"查询成功，返回 {len(result)} 行")
            except Exception as e:
                st.error(f"执行失败: {e}")

        result = st.session_state.get("last_result", pd.DataFrame())
        if isinstance(result, pd.DataFrame) and not result.empty:
            tab1, tab2 = st.tabs(["表格", "折线图"])
            with tab1:
                st.dataframe(result, use_container_width=True, height=420)
            with tab2:
                chart_df = build_chart_df(result)
                if chart_df.empty:
                    st.warning("未识别到可绘图的时间列或数值列。")
                else:
                    st.line_chart(chart_df, use_container_width=True, height=420)


if __name__ == "__main__":
    main()

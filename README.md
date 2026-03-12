# iotdbviewlite

基于 IoTDB 原生 Python Session (`apache-iotdb`) 的轻量查询工具仓库，当前包含两种运行方式：

- Streamlit 版（`app.py`）
- Flask Lite 版（`lite_app.py` + `templates/` + `static/`）

两者都直接连接 IoTDB `host:port`（默认 `172.16.41.13:6667`），不依赖 REST 服务。

## 环境

- Python: `3.12`（按现有脚本路径）
- 默认账号: `root/root`

## 安装依赖

### Streamlit 版

```powershell
& 'C:\Users\neigh\AppData\Local\Programs\Python\Python312\python.exe' -m pip install -r requirements.txt
```

### Flask Lite 版

```powershell
& 'C:\Users\neigh\AppData\Local\Programs\Python\Python312\python.exe' -m pip install -r requirements-lite.txt
```

## 启动方式

### 1) Streamlit 版

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

默认地址通常为：`http://127.0.0.1:8501`

### 2) Flask Lite 版

```powershell
powershell -ExecutionPolicy Bypass -File .\run_lite.ps1
```

默认地址：`http://127.0.0.1:7860`

## 主要功能（当前实现）

- 浏览数据库与设备树（Database -> Device）
- 按设备加载点位（Timeseries）
- SQL 预制构建（时间范围、聚合、interval、fill、排序、limit）
- 支持手工编辑 SQL
- 查询结果表格展示
- Streamlit 版支持结果折线图

## 关键文件

- Streamlit 入口：`app.py`
- Streamlit 启动兼容层：`streamlit_launcher.py`
- Flask Lite 入口：`lite_app.py`
- Lite 页面模板：`templates/index.html`
- Lite 前端脚本：`static/app.js`
- Lite 样式：`static/style.css`
- 启动脚本：`run.ps1`、`run_lite.ps1`

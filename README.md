# IoTDB Lite Viewer (原生 6667 版)

这个版本走 IoTDB 原生 Python 接口，不依赖 IoTDB REST 服务。

- 后端：Flask
- 前端：原生 HTML/CSS/JS
- 连接协议：`apache-iotdb Session`（`host:port`，默认 `172.16.41.13:6667`）

## 1. 安装依赖

```powershell
& 'C:\Users\neigh\AppData\Local\Programs\Python\Python312\python.exe' -m pip install -r requirements-lite.txt
```

## 2. 启动

```powershell
powershell -ExecutionPolicy Bypass -File .\run_lite.ps1
```

浏览器打开：`http://127.0.0.1:7860`

## 3. 默认连接

- Host: `172.16.41.13`
- Port: `6667`
- Username: `root`
- Password: `root`

## 4. 功能

- 左侧树：Database -> Device
- 设备点位加载与勾选
- 预制 SQL 构建：时间范围、聚合、interval、fill、排序、limit
- SQL 手动编辑
- 执行 SQL，展示结果表格

## 5. 文件

- 入口：[lite_app.py](./lite_app.py)
- 页面：[templates/index.html](./templates/index.html)
- 脚本：[static/app.js](./static/app.js)
- 样式：[static/style.css](./static/style.css)

# iotdbviewlite

版本：`1.0`


基于 IoTDB 原生 Python Session (`apache-iotdb`) 的轻量查询工具仓库，当前仅提供 Flask Lite 版本。

- Flask Lite 入口：`iotdbviewlite.py`
- 页面模板：`templates/`
- 前端静态资源：`static/`

应用直接连接 IoTDB `host:port`（默认 `172.16.41.13:6667`），不依赖 REST 服务。

## 环境

- Python: `3.12`（按现有脚本路径）
- 默认账号: `root/root`

## 安装依赖

```powershell
& 'C:\Users\neigh\AppData\Local\Programs\Python\Python312\python.exe' -m pip install -r requirements-lite.txt
```

## 启动方式

```powershell
python iotdbviewlite.py
```

或使用脚本：

```bat
start_iotdbview.bat
```

默认地址：`http://127.0.0.1:7860`

## 主要功能

- 浏览数据库与设备树（Database -> Device）
- 按设备加载点位（Timeseries）
- SQL 预制构建（时间范围、聚合、interval、fill、排序、limit）
- 支持手工编辑 SQL
- 查询结果表格展示
- 查询结果 Trend 折线图

## 关键文件

- Flask Lite 入口：[iotdbviewlite.py](iotdbviewlite.py)
- Lite 页面模板：`templates/index.html`
- Lite 前端脚本：`static/app.js`
- Lite 样式：`static/style.css`
- 启动脚本：`start_iotdbview.bat`
- 打包配置：`iotdbview.spec`、`iotdbview_dir.spec`


## 托盘启动（推荐）

支持启动后缩到系统托盘，托盘菜单包含：

- `Open Browser`：打开默认浏览器访问 `http://127.0.0.1:7860`
- `Open Log Console`：弹出独立控制台窗口，实时查看日志（可直接关闭）
- `Exit`：退出托盘并关闭服务

安装新增依赖：

```powershell
python -m pip install pystray Pillow
```

启动托盘版（启动后会立即返回当前终端，不阻塞）：

```bat
start_iotdbview.bat
```

如需不走托盘，仍可直接运行：

```powershell
python iotdbviewlite.py
```




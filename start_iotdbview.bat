@echo off
setlocal

set "TRAY_SCRIPT=iotdbview_tray.py"
set "LITE_SCRIPT=iotdbviewlite.py"

where python >nul 2>nul
if %errorlevel%==0 (
  python -c "import flask; import iotdb" >nul 2>nul
  if not %errorlevel%==0 (
    echo [ERROR] Missing core dependencies: flask / apache-iotdb
    echo [INFO] Install with: python -m pip install -r requirements-lite.txt
    exit /b 1
  )
  python -c "import pystray, PIL" >nul 2>nul
  if not %errorlevel%==0 (
    echo [WARN] Missing tray dependencies: pystray / Pillow
    echo [INFO] Install with: python -m pip install pystray Pillow
    echo [INFO] Falling back to non-tray mode...
    python %LITE_SCRIPT%
    goto :eof
  )
  where pythonw >nul 2>nul
  if %errorlevel%==0 (
    start "" pythonw %TRAY_SCRIPT%
  ) else (
    start "" python %TRAY_SCRIPT%
  )
  goto :eof
)

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 -c "import flask; import iotdb" >nul 2>nul
  if not %errorlevel%==0 (
    echo [ERROR] Missing core dependencies: flask / apache-iotdb
    echo [INFO] Install with: py -3 -m pip install -r requirements-lite.txt
    exit /b 1
  )
  py -3 -c "import pystray, PIL" >nul 2>nul
  if not %errorlevel%==0 (
    echo [WARN] Missing tray dependencies: pystray / Pillow
    echo [INFO] Install with: py -3 -m pip install pystray Pillow
    echo [INFO] Falling back to non-tray mode...
    py -3 %LITE_SCRIPT%
    goto :eof
  )
  start "" py -3 %TRAY_SCRIPT%
  goto :eof
)

echo Python not found in PATH. Please install Python 3 and retry.
exit /b 1


from __future__ import annotations

import logging
import os
import subprocess
import threading
import webbrowser

from PIL import Image, ImageDraw
import pystray
from pystray import MenuItem as Item
from werkzeug.serving import make_server

from iotdbviewlite import app


HOST = "127.0.0.1"
PORT = 7860
URL = f"http://{HOST}:{PORT}"
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iotdbview.log")


class ServerThread(threading.Thread):
    def __init__(self, host: str, port: int) -> None:
        super().__init__(daemon=True)
        self._server = make_server(host, port, app)

    def run(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()


def _setup_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    )
    root_logger.addHandler(file_handler)

    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)

    werkzeug_logger = logging.getLogger("werkzeug")
    werkzeug_logger.setLevel(logging.INFO)
    werkzeug_logger.addHandler(file_handler)


def _create_icon_image(size: int = 64) -> Image.Image:
    img = Image.new("RGBA", (size, size), (12, 22, 34, 255))
    draw = ImageDraw.Draw(img)

    # Rounded panel background
    pad = 4
    draw.rounded_rectangle((pad, pad, size - pad, size - pad), radius=12, fill=(18, 40, 62, 255))

    # Minimal "database cylinder" motif
    cx1, cy1, cx2, cy2 = 14, 14, size - 14, size - 14
    draw.ellipse((cx1, cy1, cx2, cy1 + 12), fill=(58, 188, 255, 255))
    draw.rectangle((cx1, cy1 + 6, cx2, cy2 - 8), fill=(43, 128, 196, 255))
    draw.ellipse((cx1, cy2 - 20, cx2, cy2 - 8), fill=(34, 101, 158, 255))

    # "I" accent for IoTDB
    draw.rectangle((size // 2 - 2, 20, size // 2 + 2, size - 18), fill=(235, 247, 255, 255))
    return img


def main() -> None:
    _setup_logging()
    server = ServerThread(HOST, PORT)
    server.start()

    icon = pystray.Icon("iotdbviewlite")
    icon.icon = _create_icon_image()
    icon.title = "IoTDB Lite Viewer"

    def on_open(_icon: pystray.Icon, _item: Item) -> None:
        webbrowser.open(URL)

    def on_open_log_console(_icon: pystray.Icon, _item: Item) -> None:
        # Open a separate console window for logs; user can close it at any time.
        safe_log = LOG_FILE.replace("'", "''")
        ps_cmd = (
            f"$p='{safe_log}'; "
            "if (!(Test-Path $p)) { New-Item -ItemType File -Path $p -Force | Out-Null }; "
            "Get-Content -Path $p -Wait"
        )
        subprocess.Popen(
            ["powershell", "-NoLogo", "-NoExit", "-Command", ps_cmd],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )

    def on_exit(_icon: pystray.Icon, _item: Item) -> None:
        try:
            server.shutdown()
        finally:
            icon.stop()

    icon.menu = pystray.Menu(
        Item("Open Browser", on_open, default=True),
        Item("Open Log Console", on_open_log_console),
        Item("Exit", on_exit),
    )

    icon.run()


if __name__ == "__main__":
    main()

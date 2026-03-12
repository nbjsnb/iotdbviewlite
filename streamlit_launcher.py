from __future__ import annotations

import asyncio
import socket
import sys
import time


# Force Proactor on Windows. Some libs may try to switch back to Selector;
# block that to avoid socket.socketpair() path.
if sys.platform.startswith("win") and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
    _orig_set_policy = asyncio.set_event_loop_policy

    def _guard_set_event_loop_policy(policy):
        selector_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
        proactor_cls = getattr(asyncio, "WindowsProactorEventLoopPolicy", None)
        if selector_cls is not None and isinstance(policy, selector_cls):
            return _orig_set_policy(proactor_cls())
        return _orig_set_policy(policy)

    asyncio.set_event_loop_policy = _guard_set_event_loop_policy  # type: ignore[assignment]
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


_original_socketpair = socket.socketpair


def _safe_socketpair(*args, **kwargs):
    last_exc: Exception | None = None
    for _ in range(80):
        try:
            return _original_socketpair(*args, **kwargs)
        except ConnectionError as exc:
            if "Unexpected peer connection" not in str(exc):
                raise
            last_exc = exc
            time.sleep(0.05)
    if last_exc is not None:
        raise last_exc
    return _original_socketpair(*args, **kwargs)


socket.socketpair = _safe_socketpair  # type: ignore[assignment]

from streamlit.web.cli import main  # noqa: E402


if __name__ == "__main__":
    sys.exit(main())

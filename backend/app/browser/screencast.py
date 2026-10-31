"""Live view: stream the active tab as JPEG frames via CDP Page.startScreencast.

Frames are kept base64 (as CDP sends them) so the WebSocket layer can forward
them without re-encoding. Each subscriber gets a small bounded queue; when a
viewer is slow, old frames are dropped rather than buffered.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

from ..config import Settings, get_settings
from .session import BrowserSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScreencastFrame:
    data_b64: str  # JPEG
    device_width: int
    device_height: int
    offset_top: float
    page_scale: float
    timestamp: float
    seq: int


class Screencaster:
    def __init__(self, session: BrowserSession, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings or get_settings()
        self._cdp: Any = None
        self._page: Any = None
        self._subscribers: set[asyncio.Queue[ScreencastFrame]] = set()
        self._last: ScreencastFrame | None = None
        self._last_sent = 0.0
        self._seq = 0
        self.running = False
        self._restart_lock = asyncio.Lock()
        session.on_page_change(self._on_page_change)

    @property
    def last_frame(self) -> ScreencastFrame | None:
        return self._last

    def subscribe(self, maxsize: int = 2) -> asyncio.Queue[ScreencastFrame]:
        q: asyncio.Queue[ScreencastFrame] = asyncio.Queue(maxsize=maxsize)
        if self._last is not None:
            q.put_nowait(self._last)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[ScreencastFrame]) -> None:
        self._subscribers.discard(q)

    async def start(self) -> None:
        async with self._restart_lock:
            await self._attach(self.session.page)
            self.running = True

    async def stop(self) -> None:
        async with self._restart_lock:
            self.running = False
            await self._detach()

    async def _attach(self, page: Any) -> None:
        await self._detach()
        s = self.settings
        self._page = page
        self._cdp = await page.context.new_cdp_session(page)
        self._cdp.on("Page.screencastFrame", self._on_frame)
        await self._cdp.send("Page.startScreencast", {
            "format": "jpeg",
            "quality": s.screencast_quality,
            "maxWidth": s.browser_viewport_width,
            "maxHeight": s.browser_viewport_height,
            "everyNthFrame": 1,
        })

    async def _detach(self) -> None:
        cdp, self._cdp = self._cdp, None
        if cdp is None:
            return
        try:
            await cdp.send("Page.stopScreencast")
        except Exception:
            pass
        try:
            await cdp.detach()
        except Exception:
            pass

    async def _on_page_change(self, session: BrowserSession) -> None:
        if self.running and session.page is not None and session.page is not self._page:
            async with self._restart_lock:
                await self._attach(session.page)

    def _on_frame(self, params: dict[str, Any]) -> None:
        cdp = self._cdp
        if cdp is not None:
            # Chrome stops sending frames until each one is acknowledged.
            asyncio.ensure_future(self._ack(cdp, params["sessionId"]))
        now = time.monotonic()
        if now - self._last_sent < 1.0 / self.settings.screencast_max_fps:
            return
        self._last_sent = now
        meta = params.get("metadata", {})
        self._seq += 1
        frame = ScreencastFrame(
            data_b64=params["data"],
            device_width=int(meta.get("deviceWidth", self.settings.browser_viewport_width)),
            device_height=int(meta.get("deviceHeight", self.settings.browser_viewport_height)),
            offset_top=float(meta.get("offsetTop", 0)),
            page_scale=float(meta.get("pageScaleFactor", 1)),
            timestamp=float(meta.get("timestamp", time.time())),
            seq=self._seq,
        )
        self._last = frame
        for q in list(self._subscribers):
            if q.full():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(frame)

    @staticmethod
    async def _ack(cdp: Any, session_id: int) -> None:
        try:
            await cdp.send("Page.screencastFrameAck", {"sessionId": session_id})
        except Exception:
            pass

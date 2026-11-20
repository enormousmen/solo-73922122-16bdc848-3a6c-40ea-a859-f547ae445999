"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
* Prewarm (POST /cache/prewarm) walks a batch sequentially. Only fills that
  prewarm itself starts consume the global prewarm origin slot (at most one
  such fetch unfinished at a time); normal downloads never wait for it.
  A prewarm item adopts whatever fill/complete generation already exists for
  its path, so a download that raced ahead shares its origin fetch. A purge
  marks every unfinished prewarm item for its path as purged: queued items
  never start a fetch, adopted/in-flight fills keep feeding already-attached
  readers but are never re-registered, and the purged item reports "purged".
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None
        self.prewarm_slot = False  # this fill holds the prewarm origin slot

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
            "prewarm_fill": self.prewarm_slot,
        }


class PrewarmItem:
    """One path inside a prewarm batch, from submission until it is reported.

    ``purged`` is set by a purge of the same path while this item is still
    unfinished: the item then reports ``purged`` and never starts a fetch.
    """

    def __init__(self, key: str, seq: int) -> None:
        self.key = key
        self.seq = seq
        self.purged = False

    def mark_purged(self) -> None:
        self.purged = True


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None
        # Serialises prewarm scheduling; shares self.lock so choosing a fill
        # and registering it is atomic with respect to downloads and purges.
        self.prewarm_cond = asyncio.Condition(self.lock)
        # At most one origin fill started by prewarm may be unfinished.
        self.prewarm_busy = False
        # Unfinished prewarm items, keyed by path, so purge can report them.
        self.prewarm_waiters: dict[str, set[PrewarmItem]] = {}

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Entry:
        """Return the current generation for key, starting a fill if needed."""
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                e.fill_task = asyncio.create_task(self._fill(e))
                # Queued prewarm items for this path adopt this fill.
                self.prewarm_cond.notify_all()
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader.

        Every unfinished prewarm item for this path is marked purged before
        returning: queued items will never start an origin fetch, and running
        items report ``purged`` instead of ``ready``.
        """
        async with self.lock:
            waiters = self.prewarm_waiters.pop(key, None)
            if waiters:
                for item in waiters:
                    item.mark_purged()
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                if e.state == "complete" and e.readers > 0:
                    # Keep serving the readers that already attached, but make
                    # sure a new request cannot adopt this file from disk
                    # while it lingers: hide it under its part name.
                    try:
                        os.replace(e.final_path, e.part_path)
                    except OSError:
                        pass
                self._maybe_reap_locked(e)
                # Wake prewarm items waiting on this generation so they can
                # report "purged" at once; the fill itself keeps running for
                # downloads that already attached.
                async with e.cond:
                    e.cond.notify_all()
                existed = True
            else:
                # Not in memory (e.g. never requested since restart): clean disk.
                existed = False
                final = CACHE_DIR / key
                for p in (final, CACHE_DIR / f"{key}.meta"):
                    if p.exists():
                        _unlink_quiet(p)
                        existed = True
                try:
                    for p in final.parent.glob(final.name + ".part-*"):
                        _unlink_quiet(p)
                        existed = True
                except OSError:
                    pass
            if waiters:
                # Re-check queued items: a purged item must not wait for the
                # origin slot to free up before it can be reported.
                self.prewarm_cond.notify_all()
        return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over."""
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # never keep a half package
        elif e.purged:  # complete or closed
            _unlink_quiet(e.part_path)
            _unlink_quiet(e.final_path)
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: keep the part file for current readers
                    # only; never register this generation.
                    e.state = "closed"
                else:
                    os.replace(e.part_path, e.final_path)  # atomic publish
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                    e.state = "complete"
                self._release_prewarm_slot_locked(e)
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            self._release_prewarm_slot_locked(e)
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    def _release_prewarm_slot_locked(self, e: Entry) -> None:
        """A prewarm-started fill is over: let the next item start one."""
        if not e.prewarm_slot:
            return
        e.prewarm_slot = False
        self.prewarm_busy = False
        self.prewarm_cond.notify_all()

    # ------------------------------------------------------------------ #
    # prewarm                                                             #
    # ------------------------------------------------------------------ #

    async def register_prewarm(self, keys: list[str]) -> list[PrewarmItem]:
        """Register a batch before any item starts, so a purge that arrives
        while an item is still queued can mark it as purged."""
        items = [PrewarmItem(key, seq) for seq, key in enumerate(keys)]
        async with self.lock:
            for item in items:
                self.prewarm_waiters.setdefault(item.key, set()).add(item)
        return items

    def unregister_prewarm(self, items: list[PrewarmItem]) -> None:
        """Drop items that will never be processed (e.g. client disconnect).

        Deliberately synchronous: it runs in a response generator's ``finally``
        while the request is being cancelled, where awaiting is unreliable.
        Asyncio only switches tasks at await points, so this cannot interleave
        with a purge holding ``self.lock``.
        """
        for item in items:
            self._drop_prewarm_waiter_locked(item)

    def _drop_prewarm_waiter_locked(self, item: PrewarmItem) -> None:
        waiters = self.prewarm_waiters.get(item.key)
        if not waiters:
            return
        waiters.discard(item)
        if not waiters:
            self.prewarm_waiters.pop(item.key, None)

    async def prewarm_one(self, item: PrewarmItem) -> dict:
        """Bring one path to a complete cached state and report the outcome.

        Adopts an existing fill (whoever started it) or complete entry; only
        starts an origin fetch when there is none, and then holds the single
        global prewarm origin slot until that fill is over.
        """
        async with self.prewarm_cond:  # condition's lock is self.lock
            while True:
                if item.purged:
                    return self._prewarm_done_locked(item, "purged")
                e = self.entries.get(item.key)
                if e is None:
                    e = self._entry_from_disk(item.key)
                if e is not None:
                    if e.state == "complete":
                        return self._prewarm_done_locked(item, "ready", e)
                    if e.state == "filling":
                        break  # adopt this generation; no origin slot needed
                if not self.prewarm_busy:
                    # Claim the slot and this generation atomically, so a
                    # concurrent download cannot start a second fill.
                    self.prewarm_busy = True
                    e = Entry(item.key)
                    e.prewarm_slot = True
                    self.entries[item.key] = e
                    e.fill_task = asyncio.create_task(self._fill(e))
                    self.prewarm_cond.notify_all()  # let same-path items adopt
                    break
                # Wait for the slot to free, a purge, or a download-created
                # fill for this path (attach notifies).
                await self.prewarm_cond.wait()

        status = await self._await_fill_for_prewarm(e, item)
        async with self.lock:
            # Report under the lock: purge marks the item before removing the
            # entry, so a purged path can never be reported "ready".
            if item.purged:
                status = "purged"
            elif status == "ready" and not (
                self.entries.get(item.key) is e and e.state == "complete"
            ):
                status = "purged" if e.purged else "failed"
            return self._prewarm_done_locked(item, status, e)

    async def _await_fill_for_prewarm(self, e: Entry, item: PrewarmItem) -> str:
        async with e.cond:
            await e.cond.wait_for(
                lambda: item.purged or e.state != "filling"
            )
        if item.purged:
            return "purged"
        if e.state == "complete":
            return "ready"
        if e.state == "failed":
            return "failed"
        return "purged"  # closed: the generation was purged mid-fill

    def _prewarm_done_locked(
        self, item: PrewarmItem, status: str, e: Entry | None = None
    ) -> dict:
        self._drop_prewarm_waiter_locked(item)
        result = {"path": item.key, "status": status}
        if status == "ready" and e is not None:
            result["size"] = e.size
            sha256 = (e.meta or {}).get("sha256")
            if sha256:
                result["sha256"] = sha256
        elif status == "failed":
            result["error"] = (e.error if e is not None else None) or "origin fill failed"
        return result

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await self.detach(e)


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.0.0", lifespan=lifespan)


class PrewarmRequest(BaseModel):
    paths: list[str] = Field(
        ...,
        min_length=1,
        max_length=16,
        description="1-16 distinct package paths, e.g. ['pkg-32m.bin']",
        examples=[["pkg-32m.bin", "pkg-1m.bin"]],
    )
    stream: bool = Field(
        False,
        description="true: NDJSON, one line per item as soon as it settles; "
        "false: one JSON object once the whole batch is done",
    )


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    entry = await cache.attach(key)
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        await cache.detach(entry)
        raise

    if entry.state == "failed":
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion.
    Unfinished prewarm items for this path report ``purged``."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.post("/cache/prewarm")
async def prewarm(req: PrewarmRequest):
    """Warm the cache for 1-16 distinct paths, one item at a time.

    Each item is settled and reported individually, in request order:

        {"path": "pkg-32m.bin", "status": "ready", "size": 33554432, ...}
        {"path": "pkg-1m.bin",  "status": "failed", "error": "..."}
        {"path": "gone.bin",    "status": "purged"}

    With ``"stream": true`` the response is NDJSON and every line is sent the
    moment that item settles; otherwise the response is one JSON object with
    an ``items`` array, sent when the whole batch is done.

    ``ready`` is only reported once the whole package is on disk. Items share
    an existing fill (from a download or another item) instead of starting a
    second origin fetch, and only one origin fetch started by prewarm may be
    unfinished at any time. Normal downloads never wait for that slot.
    """
    keys: list[str] = []
    for raw in req.paths:
        try:
            keys.append(validate_key(raw))
        except ValueError as exc:
            return PlainTextResponse(f"{raw!r}: {exc}", status_code=400)
    if len(set(keys)) != len(keys):
        return PlainTextResponse("paths must be distinct", status_code=400)

    items = await cache.register_prewarm(keys)

    async def reports():
        try:
            for item in items:
                yield await cache.prewarm_one(item)
        finally:
            cache.unregister_prewarm(items)

    if req.stream:
        async def ndjson():
            async for result in reports():
                yield json.dumps(result, ensure_ascii=False) + "\n"

        return StreamingResponse(ndjson(), media_type="application/x-ndjson")

    results = [result async for result in reports()]
    return {"items": results, "ready": sum(r["status"] == "ready" for r in results)}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}

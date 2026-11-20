"""End-to-end tests for POST /cache/prewarm.

The origin/proxy fixtures live in test_e2e.py and are imported here so both
modules run against real subprocesses and real HTTP.
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from test_e2e import (  # noqa: F401 - fixtures are used by pytest
    MIB,
    PKG_BIG,
    PKG_SMALL,
    client,
    download_bytes,
    origin,
    origin_reset,
    origin_stats,
    proxy,
    set_behavior,
    sha256,
)


def prewarm_stream(base: str, paths: list[str], read_timeout: float = 120.0):
    """POST /cache/prewarm (streaming) and yield each NDJSON report line."""
    c = httpx.Client(
        timeout=httpx.Timeout(10.0, read=read_timeout, write=30.0, pool=5.0)
    )
    try:
        with c.stream(
            "POST", f"{base}/cache/prewarm", json={"paths": paths, "stream": True}
        ) as r:
            r.raise_for_status()
            assert r.headers["content-type"].startswith("application/x-ndjson")
            for line in r.iter_lines():
                if line.strip():
                    yield json.loads(line)
    finally:
        c.close()


def prewarm_all(base: str, paths: list[str], read_timeout: float = 120.0) -> list[dict]:
    return list(prewarm_stream(base, paths, read_timeout))


def status(base: str) -> dict:
    with client() as c:
        return c.get(f"{base}/cache/status").json()


def wait_for(predicate, timeout: float = 20.0, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def make_package(origin_url: str, path: str, size: int) -> str:
    with client() as c:
        r = c.post(f"{origin_url}/control/content/{path}", params={"size": size})
        r.raise_for_status()
        return r.json()["sha256"]


# --------------------------------------------------------------------- #
# basics                                                                 #
# --------------------------------------------------------------------- #

def test_prewarm_reports_each_item_ready_and_caches(proxy, origin):
    origin_reset(origin.url)
    reports = prewarm_all(proxy.url, [PKG_SMALL, PKG_BIG])
    assert [r["path"] for r in reports] == [PKG_SMALL, PKG_BIG]
    assert [r["status"] for r in reports] == ["ready", "ready"]
    for r in reports:
        assert r["size"] > 0
        assert "sha256" in r

    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert stats["paths"][PKG_BIG] == 1

    # Both packages are fully cached: downloads do not touch the origin.
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        r2 = c.get(f"{proxy.url}/packages/{PKG_BIG}")
    assert r1.headers["x-cache"] == "HIT" and r2.headers["x-cache"] == "HIT"
    assert sha256(r1.content) == stats["packages"][PKG_SMALL]["sha256"]
    assert sha256(r2.content) == stats["packages"][PKG_BIG]["sha256"]
    assert origin_stats(origin.url)["total_hits"] == 2


def test_prewarm_reports_already_cached_path_ready(proxy, origin):
    origin_reset(origin.url)
    download_bytes(proxy.url, PKG_SMALL)
    reports = prewarm_all(proxy.url, [PKG_SMALL])
    assert len(reports) == 1
    assert reports[0]["path"] == PKG_SMALL
    assert reports[0]["status"] == "ready"
    assert reports[0]["size"] == MIB
    assert reports[0]["sha256"] == origin_stats(origin.url)["packages"][PKG_SMALL]["sha256"]
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no extra fetch


def test_prewarm_validation(proxy):
    with client() as c:
        assert c.post(f"{proxy.url}/cache/prewarm", json={"paths": []}).status_code == 422
        assert (
            c.post(
                f"{proxy.url}/cache/prewarm",
                json={"paths": [f"pkg-{i}.bin" for i in range(17)]},
            ).status_code
            == 422
        )
        dup = c.post(
            f"{proxy.url}/cache/prewarm", json={"paths": [PKG_SMALL, PKG_SMALL]}
        )
        assert dup.status_code == 400
        bad = c.post(f"{proxy.url}/cache/prewarm", json={"paths": ["../etc/passwd"]})
        assert bad.status_code == 400


# --------------------------------------------------------------------- #
# one prewarm origin fetch at a time                                     #
# --------------------------------------------------------------------- #

def test_at_most_one_prewarm_origin_fetch_unfinished(proxy, origin):
    for i in range(4):
        make_package(origin.url, f"pw-{i}.bin", 512 * 1024)
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        paths = [f"pw-{i}.bin" for i in range(4)]
        samples: list[int] = []
        done = threading.Event()

        def sampler():
            while not done.is_set():
                st = status(proxy.url)
                samples.append(sum(1 for e in st.values() if e.get("prewarm_fill")))
                time.sleep(0.02)

        t = threading.Thread(target=sampler)
        t.start()
        try:
            reports = prewarm_all(proxy.url, paths)
        finally:
            done.set()
            t.join(10)

        assert [r["status"] for r in reports] == ["ready"] * 4
        assert max(samples) <= 1, f"saw {max(samples)} concurrent prewarm fills"
        assert max(samples) == 1  # the sampler did observe a prewarm fill
        stats = origin_stats(origin.url)
        assert stats["total_hits"] == 4
        assert stats["max_active"] == 1, stats  # origin never saw two bodies at once
    finally:
        set_behavior(origin.url)


def test_two_prewarm_batches_share_the_single_slot(proxy, origin):
    for i in range(4):
        make_package(origin.url, f"two-{i}.bin", 512 * 1024)
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        samples: list[int] = []
        done = threading.Event()

        def sampler():
            while not done.is_set():
                st = status(proxy.url)
                samples.append(sum(1 for e in st.values() if e.get("prewarm_fill")))
                time.sleep(0.02)

        t = threading.Thread(target=sampler)
        t.start()
        try:
            with ThreadPoolExecutor(2) as ex:
                f1 = ex.submit(prewarm_all, proxy.url, ["two-0.bin", "two-1.bin"])
                f2 = ex.submit(prewarm_all, proxy.url, ["two-2.bin", "two-3.bin"])
                results = [f1.result(60), f2.result(60)]
        finally:
            done.set()
            t.join(10)

        assert all(r["status"] == "ready" for batch in results for r in batch)
        assert max(samples) <= 1
        stats = origin_stats(origin.url)
        assert stats["total_hits"] == 4  # every path fetched exactly once
        assert stats["max_active"] == 1, stats
        assert all(stats["paths"][f"two-{i}.bin"] == 1 for i in range(4))
    finally:
        set_behavior(origin.url)


# --------------------------------------------------------------------- #
# interaction with normal downloads                                      #
# --------------------------------------------------------------------- #

def test_download_joins_running_prewarm_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=100, chunk_size=64 * 1024)  # ~1.6s for 1MiB
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        t = threading.Thread(
            target=lambda: reports.extend(prewarm_all(proxy.url, [PKG_SMALL]))
        )
        t.start()
        wait_for(lambda: status(proxy.url).get(PKG_SMALL, {}).get("state") == "filling")
        wait_for(
            lambda: status(proxy.url).get(PKG_SMALL, {}).get("written", 0) > 0
        )

        t0 = time.monotonic()
        with client(timeout_read=60).stream(
            "GET", f"{proxy.url}/packages/{PKG_SMALL}"
        ) as r:
            assert r.headers["x-cache"] == "MISS"
            chunks = []
            for chunk in r.iter_bytes(64 * 1024):
                if not chunks:
                    ttfb = time.monotonic() - t0
                chunks.append(chunk)
        body = b"".join(chunks)
        t.join(30)

        assert ttfb < 1.0, f"download waited {ttfb:.2f}s for the prewarm fill"
        assert len(body) == MIB
        assert reports[0]["status"] == "ready"
        assert sha256(body) == reports[0]["sha256"]
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # shared fill
    finally:
        set_behavior(origin.url)


def test_download_fill_first_is_adopted_by_queued_prewarm_item(proxy, origin):
    make_package(origin.url, "adopt-me.bin", 2 * MIB)
    # The first batch item is slow, so the second item sits queued behind it.
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        t = threading.Thread(
            target=lambda: reports.extend(
                prewarm_all(proxy.url, [PKG_SMALL, "adopt-me.bin"])
            )
        )
        t.start()
        wait_for(lambda: status(proxy.url).get(PKG_SMALL, {}).get("state") == "filling")

        # The queued item's path is filled by a normal download first.
        body = download_bytes(proxy.url, "adopt-me.bin")
        t.join(60)

        assert [r["status"] for r in reports] == ["ready", "ready"]
        assert len(body) == 2 * MIB
        stats = origin_stats(origin.url)
        assert stats["paths"]["adopt-me.bin"] == 1, "prewarm refetched an adopted fill"
        assert stats["paths"][PKG_SMALL] == 1
        assert reports[1]["sha256"] == stats["packages"]["adopt-me.bin"]["sha256"]
    finally:
        set_behavior(origin.url)


def test_download_does_not_wait_for_prewarm_slot(proxy, origin):
    make_package(origin.url, "slot-holder.bin", 4 * MIB)
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~5s hold
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        t = threading.Thread(
            target=lambda: reports.extend(prewarm_all(proxy.url, ["slot-holder.bin"]))
        )
        t.start()
        wait_for(
            lambda: status(proxy.url).get("slot-holder.bin", {}).get("prewarm_fill")
        )

        t0 = time.monotonic()
        body = download_bytes(proxy.url, PKG_SMALL)  # unrelated path, cold cache
        elapsed = time.monotonic() - t0
        t.join(60)

        assert len(body) == MIB
        assert elapsed < 3.0, f"download waited {elapsed:.1f}s for the prewarm slot"
        assert reports[0]["status"] == "ready"
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1
    finally:
        set_behavior(origin.url)


# --------------------------------------------------------------------- #
# purge interaction                                                      #
# --------------------------------------------------------------------- #

def test_purge_during_prewarm_fill_reports_purged_and_keeps_reader(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~2.6s for 1MiB
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        result: dict = {}

        def prewarm():
            reports.extend(prewarm_all(proxy.url, [PKG_SMALL]))

        def reader():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.02)
                result["body"] = b"".join(chunks)

        tp = threading.Thread(target=prewarm)
        tp.start()
        wait_for(
            lambda: status(proxy.url).get(PKG_SMALL, {}).get("written", 0) > 128 * 1024
        )
        tr = threading.Thread(target=reader)
        tr.start()
        time.sleep(0.1)  # reader is attached to the prewarm fill

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.status_code == 200

        tp.join(30)
        tr.join(60)

        assert reports[0]["status"] == "purged"
        assert len(result["body"]) == MIB  # attached download read to the end
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no refetch

        # The purged fill was never written back to the cache.
        wait_for(lambda: not status(proxy.url))
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"purged fill left files behind: {leftovers}"

        # A later download is a fresh miss that refetches once.
        body2 = download_bytes(proxy.url, PKG_SMALL)
        assert len(body2) == MIB
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    finally:
        set_behavior(origin.url)


def test_purge_of_queued_prewarm_item_reports_purged_without_fetch(proxy, origin):
    make_package(origin.url, "queued.bin", 2 * MIB)
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~2.6s first item
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        t = threading.Thread(
            target=lambda: reports.extend(
                prewarm_all(proxy.url, [PKG_SMALL, "queued.bin"])
            )
        )
        t.start()
        wait_for(
            lambda: status(proxy.url).get(PKG_SMALL, {}).get("written", 0) > 0
        )
        # The second item is still queued behind the first one.
        with client() as c:
            pr = c.post(f"{proxy.url}/purge/queued.bin")
        assert pr.status_code == 200

        t.join(30)
        assert [r["status"] for r in reports] == ["ready", "purged"]
        assert "queued.bin" not in origin_stats(origin.url)["paths"]  # no fetch
        assert not (proxy.cache_dir / "queued.bin").exists()
    finally:
        set_behavior(origin.url)


def test_purged_prewarm_never_refetches_and_does_not_block_next_item(proxy, origin):
    make_package(origin.url, "tail.bin", MIB)
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)
    try:
        origin_reset(origin.url)
        reports: list[dict] = []
        t = threading.Thread(
            target=lambda: reports.extend(
                prewarm_all(proxy.url, [PKG_SMALL, "tail.bin"])
            )
        )
        t.start()
        wait_for(lambda: status(proxy.url).get(PKG_SMALL, {}).get("state") == "filling")
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_SMALL}")

        t.join(60)
        assert [r["status"] for r in reports] == ["purged", "ready"]
        # The purged path was not fetched again and the batch moved on.
        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1
        assert origin_stats(origin.url)["paths"]["tail.bin"] == 1
    finally:
        set_behavior(origin.url)


# --------------------------------------------------------------------- #
# failures                                                               #
# --------------------------------------------------------------------- #

def test_prewarm_reports_failed_item_and_caches_nothing(proxy, origin):
    origin_reset(origin.url)
    set_behavior(origin.url, delay_ms=20, chunk_size=64 * 1024, truncate=200_000)
    try:
        reports = prewarm_all(proxy.url, [PKG_SMALL])
    finally:
        set_behavior(origin.url)

    assert reports[0]["path"] == PKG_SMALL
    assert reports[0]["status"] == "failed"
    assert reports[0]["error"]
    assert PKG_SMALL not in status(proxy.url)
    wait_for(lambda: list(proxy.cache_dir.glob(f"{PKG_SMALL}*")) == [])

    # The next item in a fresh batch retries the origin and succeeds.
    reports = prewarm_all(proxy.url, [PKG_SMALL])
    assert reports[0]["status"] == "ready"


def test_prewarm_summary_mode_returns_one_json_body(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r = c.post(
            f"{proxy.url}/cache/prewarm", json={"paths": [PKG_SMALL, PKG_BIG]}
        )
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    assert [i["path"] for i in body["items"]] == [PKG_SMALL, PKG_BIG]
    assert [i["status"] for i in body["items"]] == ["ready", "ready"]
    assert body["ready"] == 2
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1


def test_prewarm_reports_items_progressively(proxy, origin):
    """Each line is sent as soon as that item settles, not at the end."""
    make_package(origin.url, "slow-second.bin", MIB)
    set_behavior(origin.url, delay_ms=100, chunk_size=64 * 1024)  # ~1.6s each
    try:
        origin_reset(origin.url)
        t0 = time.monotonic()
        arrived: list[tuple[str, float]] = []
        for report in prewarm_stream(proxy.url, [PKG_SMALL, "slow-second.bin"]):
            arrived.append((report["path"], time.monotonic() - t0))
        assert [p for p, _ in arrived] == [PKG_SMALL, "slow-second.bin"]
        # The first item settles well before the whole batch finishes.
        assert arrived[0][1] + 0.8 < arrived[1][1], arrived
    finally:
        set_behavior(origin.url)


def test_prewarm_404_reports_failed(proxy, origin):
    origin_reset(origin.url)
    reports = prewarm_all(proxy.url, ["does-not-exist.bin"])
    assert reports[0]["status"] == "failed"
    assert "404" in reports[0]["error"]

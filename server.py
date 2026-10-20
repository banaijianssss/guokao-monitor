#!/usr/bin/env python3
"""国考关注岗位报名监控 — FastAPI server (local + Vercel serverless)."""

from __future__ import annotations

import os
import sys
from pathlib import Path as _Path

# Prefer project venv when launched as `python server.py` (skip on Vercel)
_ON_VERCEL = bool(os.environ.get("VERCEL") or os.environ.get("VERCEL_ENV"))
_root = _Path(__file__).resolve().parent
if not _ON_VERCEL:
    _candidates = [
        _root / ".venv" / "bin" / "python",
        _root / ".venv" / "Scripts" / "python.exe",
    ]
    for _VENV_PY in _candidates:
        if _VENV_PY.exists() and _Path(sys.executable).resolve() != _VENV_PY.resolve():
            os.execv(str(_VENV_PY), [str(_VENV_PY), str(_Path(__file__).resolve()), *sys.argv[1:]])

import json
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

ROOT = Path(__file__).resolve().parent
# On Vercel the deployment FS is read-only except /tmp; still READ bundled data from ROOT.
IS_VERCEL = bool(os.environ.get("VERCEL") or os.environ.get("VERCEL_ENV"))
if IS_VERCEL:
    DATA_DIR = Path("/tmp/guokao-data")
    CACHE_PATH = DATA_DIR / "cache.json"
    BUNDLED_CACHE_PATH = ROOT / "data" / "cache.json"
else:
    DATA_DIR = ROOT / "data"
    CACHE_PATH = DATA_DIR / "cache.json"
    BUNDLED_CACHE_PATH = CACHE_PATH

POSITIONS_PATH = ROOT / "positions.json"
FENBI_DUMP_PATH = ROOT / "fenbi_gd_tax.json"

EXAM_ID = 967936
# Always also fetch GD + SZ tax (historical watch); plus all depts in positions.json
SEED_DEPARTMENTS = [
    "国家税务总局广东省税务局",
    "国家税务总局深圳市税务局",
]
REFRESH_INTERVAL_SEC = 30 * 60
SHANGHAI = timezone(timedelta(hours=8))
FENBI_PAUSE_SEC = 0.35  # polite rate limit between departments

FENBI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.fenbi.com/",
    "Origin": "https://www.fenbi.com",
}

_lock = threading.Lock()
_state: dict[str, Any] = {
    "positions": [],
    "updated_at": None,
    "matched": 0,
    "unmatched": 0,
    "error": None,
}
_state_bootstrapped = False


def now_shanghai_iso() -> str:
    return datetime.now(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def load_user_positions() -> list[dict]:
    with open(POSITIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def departments_to_fetch(user_positions: list[dict]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for d in SEED_DEPARTMENTS:
        if d not in seen:
            seen.add(d)
            names.append(d)
    for up in user_positions:
        d = str(up.get("dept_name") or "").strip()
        if d and d not in seen:
            seen.add(d)
            names.append(d)
    return names


def parse_passed(data_pair: Any) -> Optional[int]:
    if not isinstance(data_pair, dict):
        return None
    name = str(data_pair.get("name") or "")
    val = data_pair.get("value")
    if val is None or val == "" or val == "***":
        return None
    try:
        n = int(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if "过审" in name or "报名" in name or name:
        return n
    return n


def fetch_fenbi_department(client: httpx.Client, department: str) -> list[dict]:
    all_pos: list[dict] = []
    start = 0
    page_len = 100
    while True:
        url = (
            "https://market-api.fenbi.com/toolkit/api/v1/pc/position/"
            f"listByDepartment?apcid=0&app=web&av=100&hav=100&kav=100"
            f"&examId={EXAM_ID}&department={quote(department)}"
            f"&start={start}&len={page_len}"
        )
        r = client.get(url, headers=FENBI_HEADERS, timeout=30.0)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data") or {}
        positions = data.get("positions") or []
        if not positions:
            break
        all_pos.extend(positions)
        total = data.get("total")
        if total is not None and len(all_pos) >= int(total):
            break
        if len(positions) < page_len:
            break
        start += page_len
    return all_pos


def fetch_all_fenbi(departments: list[str]) -> list[dict]:
    out: list[dict] = []
    with httpx.Client() as client:
        for i, dept in enumerate(departments):
            try:
                chunk = fetch_fenbi_department(client, dept)
                out.extend(chunk)
                print(f"[fenbi] {dept}: {len(chunk)}")
            except Exception as e:
                print(f"[fenbi] fetch failed for {dept}: {e}")
            if i + 1 < len(departments):
                time.sleep(FENBI_PAUSE_SEC)
    return out


def load_fenbi_fallback() -> list[dict]:
    items: list[dict] = []
    if FENBI_DUMP_PATH.exists():
        with open(FENBI_DUMP_PATH, "r", encoding="utf-8") as f:
            items = json.load(f)
    return items


def merge_positions(
    user_positions: list[dict], fenbi_positions: list[dict], updated_at: str
) -> tuple[list[dict], int, int]:
    by_code: dict[str, dict] = {}
    for fp in fenbi_positions:
        code = str(fp.get("positionCode") or "").strip()
        if code:
            by_code[code] = fp

    merged: list[dict] = []
    matched = 0
    for up in user_positions:
        code = str(up.get("position_code") or "").strip()
        fp = by_code.get(code)
        recruit = up.get("recruit")
        try:
            recruit_n = int(recruit) if recruit is not None else None
        except (TypeError, ValueError):
            recruit_n = None

        applicants = None
        passed = None
        fenbi_id = None
        data_source = "excel_only"

        if fp:
            matched += 1
            fenbi_id = fp.get("id")
            passed = parse_passed(fp.get("dataPair"))
            data_source = "fenbi_list"

        ratio = None
        metric = passed if passed is not None else applicants
        if metric is not None and recruit_n and recruit_n > 0:
            ratio = round(metric / recruit_n, 2)

        merged.append(
            {
                "score": up.get("score"),
                "dept_name": up.get("dept_name"),
                "bureau": up.get("bureau"),
                "position": up.get("position"),
                "position_code": code,
                "recruit": recruit_n,
                "applicants": applicants,
                "passed": passed,
                "ratio": ratio,
                "work_place": up.get("work_place"),
                "region": up.get("region") or "",
                "note": up.get("note"),
                "major": up.get("major"),
                "politics": up.get("politics"),
                "updated_at": updated_at,
                "data_source": data_source,
                "matched": fp is not None,
                "fenbi_id": fenbi_id,
                "metric_label": "过审" if passed is not None else None,
            }
        )

    unmatched = len(user_positions) - matched

    def sort_key(row: dict):
        r = row.get("ratio")
        s = row.get("score")
        return (
            r is None,
            -(r if r is not None else 0),
            -(s if s is not None else 0),
        )

    merged.sort(key=sort_key)
    return merged, matched, unmatched


def save_cache(payload: dict) -> None:
    """Write cache; on Vercel tolerate /tmp write failures gracefully."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as e:
        print(f"[cache] write skipped: {e}")


def load_cache() -> Optional[dict]:
    """Prefer writable tmp cache on Vercel, else bundled ROOT/data/cache.json."""
    for path in (CACHE_PATH, BUNDLED_CACHE_PATH):
        if path is None:
            continue
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            continue
    return None


def do_refresh(use_network: bool = True) -> dict:
    updated_at = now_shanghai_iso()
    user = load_user_positions()
    departments = departments_to_fetch(user)
    fenbi: list[dict] = []
    err = None

    if use_network:
        try:
            fenbi = fetch_all_fenbi(departments)
            if not fenbi:
                fenbi = load_fenbi_fallback()
                err = "live fenbi empty; used local dump (GD only)"
        except Exception as e:
            fenbi = load_fenbi_fallback()
            err = f"live fenbi failed ({e}); used local dump"
    else:
        fenbi = load_fenbi_fallback()

    merged, matched, unmatched = merge_positions(user, fenbi, updated_at)
    payload = {
        "updated_at": updated_at,
        "matched": matched,
        "unmatched": unmatched,
        "total": len(merged),
        "positions": merged,
        "error": err,
        "fenbi_count": len(fenbi),
        "departments": departments,
    }
    with _lock:
        _state["positions"] = merged
        _state["updated_at"] = updated_at
        _state["matched"] = matched
        _state["unmatched"] = unmatched
        _state["error"] = err
    save_cache(payload)
    return payload


def background_loop() -> None:
    while True:
        time.sleep(REFRESH_INTERVAL_SEC)
        try:
            do_refresh(use_network=True)
            print(f"[auto-refresh] {_state.get('updated_at')} matched={_state.get('matched')}")
        except Exception as e:
            print(f"[auto-refresh] error: {e}")


def ensure_state_loaded() -> None:
    """Load bundled/tmp cache into _state so cold starts serve immediately."""
    global _state_bootstrapped
    if _state_bootstrapped:
        with _lock:
            if _state["positions"]:
                return
    cache = load_cache()
    if cache and cache.get("positions"):
        with _lock:
            _state["positions"] = cache["positions"]
            _state["updated_at"] = cache.get("updated_at")
            _state["matched"] = cache.get("matched", 0)
            _state["unmatched"] = cache.get("unmatched", 0)
            _state["error"] = cache.get("error")
    _state_bootstrapped = True


app = FastAPI(title="国考关注岗位报名监控")


@app.on_event("startup")
def on_startup() -> None:
    if IS_VERCEL:
        # Serverless: no background thread, no network refresh on cold start.
        # Load bundled /tmp cache so / and /api/positions work immediately.
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        ensure_state_loaded()
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cache = load_cache()
    if cache and cache.get("positions"):
        with _lock:
            _state["positions"] = cache["positions"]
            _state["updated_at"] = cache.get("updated_at")
            _state["matched"] = cache.get("matched", 0)
            _state["unmatched"] = cache.get("unmatched", 0)
            _state["error"] = cache.get("error")
        threading.Thread(target=lambda: do_refresh(True), daemon=True).start()
    else:
        do_refresh(use_network=True)
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()


@app.get("/api/positions")
def api_positions():
    if IS_VERCEL:
        ensure_state_loaded()
    with _lock:
        return {
            "updated_at": _state["updated_at"],
            "matched": _state["matched"],
            "unmatched": _state["unmatched"],
            "total": len(_state["positions"]),
            "error": _state["error"],
            "metric_note": "过审人数来自粉笔 list dataPair（第三方汇总）；报名人数官网/付费墙不可用时为 null。竞争比=过审/招录。",
            "positions": _state["positions"],
        }


@app.post("/api/refresh")
def api_refresh():
    try:
        payload = do_refresh(use_network=True)
        return {
            "ok": True,
            "updated_at": payload["updated_at"],
            "matched": payload["matched"],
            "unmatched": payload["unmatched"],
            "total": payload["total"],
            "fenbi_count": payload.get("fenbi_count"),
            "error": payload.get("error"),
        }
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/")
def index():
    return FileResponse(ROOT / "index.html")


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")


if __name__ == "__main__":
    main()

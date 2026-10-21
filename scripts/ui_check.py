"""Dashboard UI check: opens every tab in a real browser, waits for project data
to render, saves screenshots and reports console errors / failed requests.

Needs the dashboard (scripts\\start_frontend.ps1) and the API running, plus
Playwright in the host venv. It drives an installed Edge or Chrome, so no
browser download is required:

    .\\.venv\\Scripts\\python.exe -m pip install playwright==1.55.0
    .\\.venv\\Scripts\\python.exe scripts\\ui_check.py [--url http://127.0.0.1:5173]

Screenshots and ui_check.json are written to data\\reports\\ui\\.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# tab id -> text that only appears once that tab has rendered real data
TABS = [
    ("overview", "Stream events archived"),
    ("map", "Highest-risk zones"),
    ("zones", "Zone risk explorer"),
    ("incidents", "matching"),
    ("analytics", "Daily incidents and reports by category"),
    ("model", "Held-out evaluation"),
    ("pipeline", "Streaming queries"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:5173")
    ap.add_argument("--out", default=str(ROOT / "data" / "reports" / "ui"))
    args = ap.parse_args()
    from playwright.sync_api import Error as PwError, sync_playwright

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    console_errors, failed_requests, results = [], [], []
    with sync_playwright() as p:
        browser = None
        for channel in ("msedge", "chrome"):
            try:
                browser = p.chromium.launch(channel=channel, headless=True)
                break
            except PwError:
                continue
        if browser is None:
            print(json.dumps({"ok": False, "error": "no installed Edge or Chrome found"}))
            return 2
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("console", lambda m: console_errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: console_errors.append(f"pageerror: {e}"))
        page.on("response", lambda r: failed_requests.append(f"HTTP {r.status} {r.url}")
                if r.status >= 400 and "tile.openstreetmap" not in r.url else None)
        page.on("requestfailed", lambda r: failed_requests.append(f"{r.method} {r.url} {r.failure}")
                if "tile.openstreetmap" not in r.url and "/api/stream" not in r.url else None)
        page.goto(f"{args.url}/#overview", wait_until="domcontentloaded")
        for tab, marker in TABS:
            t0 = time.time()
            rec = {"tab": tab, "marker": marker}
            try:
                page.goto(f"{args.url}/#{tab}", wait_until="domcontentloaded")
                page.get_by_text(marker, exact=False).first.wait_for(timeout=45000)
                if tab == "map":
                    page.wait_for_selector(".leaflet-container", timeout=20000)
                    rec["zone_rectangles"] = page.locator("path.leaflet-interactive").count()
                if tab == "zones":
                    page.locator("table.table tbody tr").first.click()
                    page.get_by_text("Contributions to the rule-based score", exact=False).first.wait_for(timeout=30000)
                    rec["zone_rows"] = page.locator("table.table tbody tr").count()
                if tab == "model":
                    rec["synthetic_banner"] = page.get_by_text("SYNTHETIC", exact=False).count() > 0
                page.mouse.move(2, 2)                # no hover tooltips in the screenshot
                page.wait_for_timeout(2500)          # let charts finish drawing
                shot = out / f"{tab}.png"
                page.screenshot(path=str(shot), full_page=True)
                rec.update(ok=True, screenshot=str(shot.relative_to(ROOT)), seconds=round(time.time() - t0, 1))
            except PwError as exc:
                rec.update(ok=False, error=str(exc).splitlines()[0][:300])
                try:
                    page.screenshot(path=str(out / f"{tab}-FAILED.png"), full_page=True)
                except PwError:
                    pass
            results.append(rec)
            print(f"[{'PASS' if rec['ok'] else 'FAIL'}] {tab:10} {json.dumps({k: v for k, v in rec.items() if k not in ('tab', 'marker', 'ok')})}")
        browser.close()
    report = {"url": args.url, "tabs": results, "console_errors": console_errors[:50],
              "failed_requests": failed_requests[:50],
              "ok": all(r["ok"] for r in results) and not console_errors and not failed_requests}
    (out / "ui_check.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"ok": report["ok"], "console_errors": len(console_errors), "failed_requests": len(failed_requests)}))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

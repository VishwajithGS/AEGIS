"""Render the Mermaid diagrams in docs/ to SVG and PNG.

Uses Mermaid 10.9.1 from the jsDelivr CDN inside the installed Edge/Chrome via
Playwright (the same optional host dependency as scripts/ui_check.py), so no
Node packages or browser downloads are needed. Needs internet access.

    .\\.venv\\Scripts\\python.exe scripts\\render_diagrams.py                 # all three diagrams
    .\\.venv\\Scripts\\python.exe scripts\\render_diagrams.py architecture    # one diagram

Output: docs/<name>.svg (SVG text labels, opens in any viewer) and docs/<name>.png (2x scale).
"""
from __future__ import annotations

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
DEFAULT = ["architecture", "runtime_sequence", "batch_ml_workflow"]
MERMAID = "https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"
HTML = f"""<!doctype html><html><head><meta charset="utf-8"><script src="{MERMAID}"></script>
<style>body{{margin:0;background:#fff;font-family:Arial}} #out{{display:inline-block;padding:16px}}</style></head>
<body><div id="out"></div></body></html>"""
INIT = """() => mermaid.initialize({startOnLoad: false, theme: 'base', securityLevel: 'strict',
    flowchart: {htmlLabels: false, useMaxWidth: false, curve: 'basis', nodeSpacing: 28, rankSpacing: 46},
    sequence: {useMaxWidth: false, wrap: true},
    themeVariables: {fontFamily: 'Arial', fontSize: '15px', primaryColor: '#e8eef7', lineColor: '#44546a',
                     clusterBkg: '#fafbfc', clusterBorder: '#9aa7b8'}})"""


def main() -> int:
    ap = argparse.ArgumentParser(description="Render docs/*.mmd to SVG + PNG")
    ap.add_argument("names", nargs="*", default=DEFAULT)
    ap.add_argument("--browser", default="msedge", help="installed browser channel: msedge or chrome")
    args = ap.parse_args()
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=args.browser)
        page = browser.new_page(device_scale_factor=2, viewport={"width": 2400, "height": 1600})
        page.set_content(HTML, wait_until="networkidle")
        page.evaluate(INIT)
        for name in args.names:
            code = (DOCS / f"{name}.mmd").read_text(encoding="utf-8")
            svg = page.evaluate("async ([id, c]) => (await mermaid.render(id, c)).svg", [f"m_{name}", code])
            (DOCS / f"{name}.svg").write_text(svg, encoding="utf-8")
            page.evaluate("s => { document.getElementById('out').innerHTML = s; }", svg)
            page.locator("#out").screenshot(path=str(DOCS / f"{name}.png"))
            print(f"rendered docs/{name}.svg and docs/{name}.png")
        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

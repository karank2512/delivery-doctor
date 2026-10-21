"""Render a Delivery Doctor report as one self-contained HTML file.

No external CSS, fonts, scripts, or images: the raw report JSON is embedded
in the page, so the single file can be attached to a ticket or sent to
someone as is.
"""

import html
import json
import time

from delivery_doctor.doctor import STOP_POINTS

STAGE_COLORS = {
    "buffered": "#8a94a6", "attempt": "#3b82f6", "retry": "#f59e0b", "split": "#a855f7",
    "acked": "#10b981", "rejected": "#ef4444", "failed": "#ef4444", "dropped": "#ef4444",
    "intake": "#10b981", "intake-rejected": "#ef4444",
}

CSS = """
:root{--bg:#0b0f17;--panel:#121826;--line:#1f2937;--text:#e5e7eb;--muted:#8a94a6;
--ok:#10b981;--bad:#ef4444;--warn:#f59e0b;--accent:#3b82f6}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
main{max-width:1180px;margin:0 auto;padding:32px 24px 64px}
h1{font-size:22px;margin:0 0 4px}h2{font-size:15px;margin:32px 0 12px;color:var(--muted);
text-transform:uppercase;letter-spacing:.08em}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.muted{color:var(--muted)}
.verdict{margin:20px 0;padding:14px 18px;border-radius:10px;font-weight:600;border:1px solid}
.verdict.ok{background:#052e22;border-color:var(--ok);color:#a7f3d0}
.verdict.bad{background:#2d0f12;border-color:var(--bad);color:#fecaca}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card b{display:block;font-size:24px}.card span{color:var(--muted);font-size:12px}
.hist{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 16px}
.hrow{display:grid;grid-template-columns:260px 1fr 48px;gap:12px;align-items:center;margin:6px 0}
.hbar{height:12px;border-radius:6px}
.hint{font-size:12px;color:var(--muted)}
table{width:100%;border-collapse:collapse}
th{font-size:12px;color:var(--muted);text-align:left;font-weight:500;padding:8px;
border-bottom:1px solid var(--line)}
td{padding:8px;border-bottom:1px solid var(--line);vertical-align:top}
.badge{display:inline-block;padding:1px 8px;border-radius:999px;font-size:12px;white-space:nowrap}
.badge.ok{background:#064e3b;color:#a7f3d0}.badge.bad{background:#4c0519;color:#fecdd3}
.badge.wait{background:#3f3f46;color:#e4e4e7}
.track{position:relative;height:22px;min-width:340px;background:linear-gradient(var(--line),var(--line))
center/100% 2px no-repeat}
.dot{position:absolute;top:5px;width:12px;height:12px;margin-left:-6px;border-radius:50%;
border:2px solid var(--bg)}
.dot.end{width:14px;height:14px;top:4px;margin-left:-7px;border-radius:3px}
details{margin-top:6px}summary{cursor:pointer;color:var(--muted);font-size:12px}
pre{background:#05080e;border:1px solid var(--line);border-radius:8px;padding:10px;
overflow:auto;font-size:12px;max-height:260px}
.legend{display:flex;flex-wrap:wrap;gap:14px;font-size:12px;color:var(--muted);margin:8px 0 12px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px;
vertical-align:-1px}
label.filter{font-size:13px;color:var(--muted);cursor:pointer}
body.lost-only tr.landed{display:none}
footer{margin-top:40px;color:var(--muted);font-size:12px}
"""

JS = """
document.getElementById('lost-only').addEventListener('change', function (e) {
  document.body.classList.toggle('lost-only', e.target.checked);
});
"""


def _esc(v):
    return html.escape(str(v), quote=True)


def _fmt_bytes(n):
    if not n:
        return "0 B"
    for unit in ("B", "KB", "MB"):
        if n < 1024 or unit == "MB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def _times(report):
    ts = []
    for f in report["events"]:
        ts += [e["t"] for e in f["evidence"]["ledger"] if "t" in e]
        ts += [e["received_at"] for e in f["evidence"]["intake"] if "received_at" in e]
    return (min(ts), max(ts)) if ts else (0.0, 1.0)


def _timeline(finding, t0, span):
    def pos(t):
        return 2 + 96 * (t - t0) / span

    dots = []
    for e in finding["evidence"]["ledger"]:
        if "t" not in e:
            continue
        extra = ", ".join(f"{k}={v}" for k, v in e.items() if k not in ("event_id", "stage", "t"))
        tip = f"SDK {e['stage']} at +{(e['t'] - t0) * 1000:.0f}ms" + (f" ({extra})" if extra else "")
        color = STAGE_COLORS.get(e["stage"], "#8a94a6")
        dots.append(f'<span class="dot" style="left:{pos(e["t"]):.2f}%;background:{color}" '
                    f'title="{_esc(tip)}"></span>')
    for e in finding["evidence"]["intake"]:
        if "received_at" not in e:
            continue
        key = "intake" if e["status"] == "accepted" else "intake-rejected"
        tip = f"intake {e['status']} at +{(e['received_at'] - t0) * 1000:.0f}ms"
        if e.get("reason"):
            tip += f" ({e['reason']})"
        dots.append(f'<span class="dot end" style="left:{pos(e["received_at"]):.2f}%;'
                    f'background:{STAGE_COLORS[key]}" title="{_esc(tip)}"></span>')
    return '<div class="track">' + "".join(dots) + "</div>"


def render_html(report):
    ok = report["ok"]
    t0, t1 = _times(report)
    span = max(t1 - t0, 1e-3)
    counts = report["counts"]
    top = max(counts.values(), default=1)
    created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(report.get("created_at", time.time())))

    cards = [
        (report["total"], "events tracked"),
        (report["landed"], "landed"),
        (report["total"] - report["landed"], "lost or pending"),
        (report.get("attempts", 0), "send attempts"),
        (_fmt_bytes(report.get("event_bytes", {}).get("total", 0)), "event payload"),
        (f"{report.get('duration_s', span):.1f}s", "run time"),
    ]
    card_html = "".join(f'<div class="card"><b>{_esc(v)}</b><span>{_esc(k)}</span></div>'
                        for v, k in cards)

    hist = []
    for stop, n in sorted(counts.items(), key=lambda kv: (kv[0] != "landed", -kv[1])):
        label, hint = STOP_POINTS.get(stop, (stop, ""))
        color = "var(--ok)" if stop == "landed" else ("#71717a" if stop == "in_flight" else "var(--bad)")
        hist.append(
            f'<div class="hrow"><div><span class="mono">{_esc(stop)}</span>'
            f'<div class="hint">{_esc(label)}{" | fix: " + _esc(hint) if hint else ""}</div></div>'
            f'<div><div class="hbar" style="width:{100 * n / top:.1f}%;background:{color}"></div></div>'
            f'<div class="mono">{n}</div></div>')

    rows = []
    for f in report["events"]:
        stop = f["stop_point"]
        cls = "ok" if stop == "landed" else ("wait" if stop == "in_flight" else "bad")
        evidence = json.dumps(f["evidence"], indent=2)
        rows.append(
            f'<tr class="{"landed" if stop == "landed" else "lost"}">'
            f'<td class="mono">{_esc(f["event_id"])}</td>'
            f'<td><span class="badge {cls}">{_esc(stop)}</span>'
            f'<div class="hint">{_esc(f["detail"])}</div>'
            f'<details><summary>evidence</summary><pre>{_esc(evidence)}</pre></details></td>'
            f'<td class="mono">{_esc(_fmt_bytes(f.get("bytes")))}</td>'
            f'<td class="mono">{f.get("attempts", 0)}</td>'
            f'<td>{_timeline(f, t0, span)}</td></tr>')

    legend = "".join(f'<span><i style="background:{c}"></i>{_esc(k)}</span>'
                     for k, c in STAGE_COLORS.items())
    fail = report.get("fail_mode")
    subtitle = (f"injected failure: {_esc(fail)} ({_esc(report.get('fail_description') or '')})"
                if fail else "no failure injected")
    data = json.dumps(report).replace("</", "<\\/")

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Delivery Doctor report {_esc(report['run_id'])}</title>
<style>{CSS}</style></head>
<body><main>
<h1>Delivery Doctor report <span class="mono muted">{_esc(report['run_id'])}</span></h1>
<div class="muted">{created} | {subtitle} | endpoint <span class="mono">{_esc(report.get('endpoint', ''))}</span></div>
<div class="verdict {'ok' if ok else 'bad'}">{_esc(report['verdict'])}</div>
<div class="cards">{card_html}</div>
<h2>Where events stopped</h2>
<div class="hist">{''.join(hist)}</div>
<h2>Event timeline</h2>
<div class="legend">{legend}</div>
<label class="filter"><input type="checkbox" id="lost-only"> show only events that did not land</label>
<table><thead><tr><th>event</th><th>stop point</th><th>size</th><th>attempts</th>
<th>hops over time (+0ms to +{span * 1000:.0f}ms)</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>
<footer>Generated by Delivery Doctor. The full report JSON is embedded in this file
(script#report-data).</footer>
</main>
<script type="application/json" id="report-data">{data}</script>
<script>{JS}</script>
</body></html>
"""


def write_html(report, path):
    with open(path, "w") as f:
        f.write(render_html(report))
    return path

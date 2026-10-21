"""Delivery Doctor: emit tagged canaries, read them back, localize every miss.

For each event id the doctor joins two sources of evidence:
  - the SDK delivery ledger (what the client did: buffered, attempted, result)
  - the intake log, read back over HTTP (what the server actually holds)
and reports the first hop where the event stopped.
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from collections import Counter

from mock_raindrop.sample_agent import event_ids
from mock_raindrop.server import DEFAULT_API_KEY, IntakeServer

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FAIL_MODES = ("auth", "schema", "buffer", "network")

# stop point -> (label, fix hint)
STOP_POINTS = {
    "landed": ("landed", ""),
    "never_sent": ("never sent by client (buffer drop)",
                   "call flush() before the process exits"),
    "send_failed_auth": ("send failed: auth rejected",
                         "check the API key / write key the SDK is configured with"),
    "send_failed_http": ("send failed: HTTP error", "see the status and body in the event detail"),
    "send_failed_network": ("send failed: connection",
                            "check the endpoint host and port, proxies, and Docker networking"),
    "rejected": ("received but rejected (schema)",
                 "fix the event payload; the reject reason names the field"),
    "acked_not_readable": ("acked by intake but not readable",
                           "ingest or query path lost it; escalate with this event id"),
    "never_tracked": ("never reached the SDK", "the app never called track() for this event"),
}


def free_port():
    """A localhost port with nothing listening on it."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def read_ledger(path):
    by_id = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                entry = json.loads(line)
                by_id.setdefault(entry["event_id"], []).append(entry)
    return by_id


def query_intake(url, ids, api_key=DEFAULT_API_KEY):
    req = urllib.request.Request(f"{url}/v1/events?ids={','.join(ids)}",
                                 headers={"X-Api-Key": api_key})
    with urllib.request.urlopen(req, timeout=2) as resp:
        entries = json.loads(resp.read())["entries"]
    by_id = {}
    for entry in entries:
        by_id.setdefault(entry["event_id"], []).append(entry)
    return by_id


def classify(event_id, ledger, server):
    """Return the per-event finding from client ledger + server log entries."""
    stages = {e["stage"]: e for e in ledger}
    accepted = [e for e in server if e["status"] == "accepted"]
    rejected = [e for e in server if e["status"] == "rejected"]
    hops = {
        "buffered": "buffered" in stages,
        "sent": "attempt" in stages,
        "http_status": None,
        "received": bool(server),
        "landed": bool(accepted),
    }
    evidence = {"ledger": ledger, "intake": server}

    if "acked" in stages or "rejected" in stages:
        hops["http_status"] = 200
    if "failed" in stages and stages["failed"].get("kind") == "http":
        hops["http_status"] = stages["failed"]["status"]

    if accepted:
        stop, detail = "landed", f"in intake log ({len(accepted)} copy)"
    elif rejected:
        stop, detail = "rejected", rejected[-1]["reason"]
    elif "failed" in stages:
        fail = stages["failed"]
        if fail["kind"] == "connection":
            stop = "send_failed_network"
            detail = f"{fail['error']} after {fail['attempts']} attempts"
        elif fail["status"] in (401, 403):
            stop = "send_failed_auth"
            detail = f"HTTP {fail['status']} {fail['body']}"
        else:
            stop = "send_failed_http"
            detail = f"HTTP {fail['status']} after {fail['attempts']} attempts"
    elif "acked" in stages:
        stop, detail = "acked_not_readable", "SDK got a 200 but the intake log has no record"
    elif "buffered" in stages:
        stop, detail = "never_sent", "still in SDK buffer when the process exited"
    else:
        stop, detail = "never_tracked", "no SDK ledger entry"

    return {"event_id": event_id, "stop_point": stop, "label": STOP_POINTS[stop][0],
            "detail": detail, "hops": hops, "evidence": evidence}


def verdict_line(findings):
    total = len(findings)
    counts = Counter(f["stop_point"] for f in findings)
    landed = counts.pop("landed", 0)
    if not counts:
        return f"PASS: {landed}/{total} events landed and were read back."
    parts = []
    for stop, n in counts.most_common():
        label, hint = STOP_POINTS[stop]
        parts.append(f"{n} {label} (fix: {hint})")
    return f"FAIL: {landed}/{total} landed; " + "; ".join(parts) + "."


def emitter_args(fail):
    """Translate a demo failure mode into the sample agent's bug flags."""
    return {
        None: [],
        "auth": ["--api-key", "rd_wrong_key"],
        "schema": ["--bad-timestamp-every", "4"],
        "buffer": ["--skip-flush"],
        "network": [],
    }[fail]


def run_check(n=12, fail=None, workdir=None, timeout=2.0):
    """Run one full check. Returns the JSON-serializable report."""
    if fail not in (None,) + FAIL_MODES:
        raise ValueError(f"unknown fail mode {fail!r}")
    run_id = uuid.uuid4().hex[:8]
    workdir = workdir or os.path.join(REPO_ROOT, ".doctor_runs", run_id)
    os.makedirs(workdir, exist_ok=True)
    ledger_path = os.path.join(workdir, "sdk_ledger.jsonl")
    ids = event_ids(run_id, n)

    with IntakeServer(os.path.join(workdir, "intake_log.jsonl")) as server:
        endpoint = f"http://127.0.0.1:{free_port()}" if fail == "network" else server.url
        cmd = [sys.executable, "-m", "mock_raindrop.sample_agent",
               "--endpoint", endpoint, "--api-key", DEFAULT_API_KEY,
               "--run-id", run_id, "--n", str(n), "--ledger", ledger_path]
        cmd += emitter_args(fail)  # argparse keeps the last --api-key, so auth overrides
        proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)

        # Poll only for ids the SDK believes reached the server, so failed ids don't stall us.
        ledger = read_ledger(ledger_path)
        expect = [i for i in ids if any(e["stage"] in ("acked", "rejected") for e in ledger.get(i, []))]
        deadline = time.time() + timeout
        found = query_intake(server.url, ids)
        while any(i not in found for i in expect) and time.time() < deadline:
            time.sleep(0.1)
            found = query_intake(server.url, ids)

    findings = [classify(i, ledger.get(i, []), found.get(i, [])) for i in ids]
    counts = Counter(f["stop_point"] for f in findings)
    verdict = verdict_line(findings)
    report = {
        "run_id": run_id,
        "fail_mode": fail,
        "endpoint": endpoint,
        "total": n,
        "landed": counts.get("landed", 0),
        "ok": counts.get("landed", 0) == n,
        "counts": dict(counts),
        "verdict": verdict,
        "emitter_exit_code": proc.returncode,
        "emitter_stderr": proc.stderr[-2000:],
        "artifacts": {"sdk_ledger": ledger_path,
                      "intake_log": os.path.join(workdir, "intake_log.jsonl")},
        "events": findings,
    }
    report_path = os.path.join(workdir, "report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    report["artifacts"]["report"] = report_path
    return report


def render_table(report):
    def yn(v):
        return "-" if v is None else ("yes" if v else "no")

    rows = [("EVENT ID", "BUFFERED", "SENT", "HTTP", "LANDED", "STOP POINT / DETAIL")]
    for f in report["events"]:
        h = f["hops"]
        rows.append((f["event_id"], yn(h["buffered"]), yn(h["sent"]),
                     str(h["http_status"] or "-"), yn(h["landed"]),
                     f"{f['label']}: {f['detail']}"))
    widths = [max(len(r[i]) for r in rows) for i in range(5)]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(r[:5], widths)) + "  " + r[5]
                     for r in rows)

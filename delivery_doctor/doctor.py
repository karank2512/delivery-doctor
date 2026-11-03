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
import urllib.parse
import uuid
from collections import Counter

from delivery_doctor.safety import (DoctorError, atomic_write, get_json, private_makedirs,
                                    redact, redact_url, safe_text)
from mock_raindrop.sample_agent import event_ids
from mock_raindrop.server import DEFAULT_API_KEY, EXPIRED_API_KEY, IntakeServer

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def runs_dir():
    """Where run artifacts go: $DELIVERY_DOCTOR_RUNS_DIR, else <repo>/.doctor_runs."""
    return os.path.abspath(os.environ.get("DELIVERY_DOCTOR_RUNS_DIR")
                           or os.path.join(REPO_ROOT, ".doctor_runs"))


WRONG_API_KEY = "rd_wrong_key"
MOCK_KEYS = (DEFAULT_API_KEY, EXPIRED_API_KEY, WRONG_API_KEY)
RUNS_DIR = runs_dir()  # value at import time; code paths call runs_dir()

# Input bounds. Wide enough for real use, tight enough that a typo cannot hang
# the machine or produce a vacuous verdict.
MIN_EVENTS, MAX_EVENTS = 1, 5000
MAX_EVENT_ID_LEN = 200
MAX_LEDGER_BYTES = 256 * 1024 * 1024
MAX_LEDGER_IDS = 100_000
MAX_LEDGER_LINE = 1024 * 1024
READ_TIMEOUT_S = 5.0  # per intake read-back request
EMITTER_TIMEOUT_S = 120.0  # wall clock for the canary emitter in `check`
LEDGER_STAGES = {"buffered", "dropped", "attempt", "retry", "split", "acked", "rejected", "failed"}

# demo failure -> (what it simulates, sample agent flags, intake server knobs)
FAILURES = {
    "auth": ("SDK configured with a wrong API key",
             ["--api-key", WRONG_API_KEY], {}),
    "expired-key": ("SDK configured with an API key that has expired",
                    ["--api-key", EXPIRED_API_KEY], {}),
    "schema": ("every 4th event sends timestamp as a string",
               ["--bad-timestamp-every", "4"], {}),
    "buffer": ("app exits without flush() and without the SDK exit hook",
               ["--skip-flush", "--no-exit-hook"], {}),
    "network": ("SDK points at a port with nothing listening", [], {}),
    "rate-limit": ("intake answers 429 after the first batch",
                   [], {"rate_limit_after": 1}),
    "too-large": ("every 6th event carries a 1 MB attachment (intake limit 256 KB)",
                  ["--oversize-every", "6"], {}),
    "timeout": ("intake hangs longer than the SDK's 0.25s request timeout",
                ["--timeout", "0.25"], {"hang_seconds": 0.6}),
    "overflow": ("SDK queue capped at 8 events; the app bursts past it before flushing",
                 ["--max-queue", "8", "--batch-size", "20"], {}),
}
FAIL_MODES = tuple(FAILURES)

# stop point -> (label, fix hint)
STOP_POINTS = {
    "landed": ("landed", ""),
    "queue_overflow": ("dropped by SDK: queue full",
                       "raise the SDK queue size or flush more often during bursts"),
    "never_sent": ("never sent by client (buffer drop)",
                   "call flush() before the process exits, or keep the SDK exit hook on"),
    "auth_invalid_key": ("send failed: invalid API key",
                         "check the API key / write key the SDK is configured with"),
    "auth_expired_key": ("send failed: API key expired",
                         "rotate the key: issue a new one and update the SDK config"),
    "forbidden": ("send failed: forbidden (403)",
                  "a Host or origin check rejected the request; check proxy and Docker host names"),
    "rate_limited": ("send failed: rate limited (429)",
                     "slow down or batch more; retries ran out while intake kept answering 429"),
    "payload_too_large": ("send failed: payload too large (413)",
                          "shrink the event (drop large properties) or enable gzip"),
    "send_timeout": ("send failed: timed out",
                     "intake or a proxy did not answer in time; raise the SDK timeout or check the path"),
    "send_failed_http": ("send failed: HTTP error", "see the status and body in the event detail"),
    "send_failed_network": ("send failed: connection",
                            "check the endpoint host and port, proxies, and Docker networking"),
    "rejected": ("received but rejected (schema)",
                 "fix the event payload; the reject reason names the field"),
    "acked_not_readable": ("acked by intake but not readable",
                           "ingest or query path lost it; escalate with this event id"),
    "never_tracked": ("never reached the SDK", "the app never called track() for this event"),
    "in_flight": ("in flight", ""),
}

# Where in the pipe each stop point sits, for grouping in reports.
HOP_OF = {
    "landed": "intake", "queue_overflow": "sdk", "never_sent": "sdk", "never_tracked": "app",
    "auth_invalid_key": "auth", "auth_expired_key": "auth", "forbidden": "auth",
    "rate_limited": "intake", "payload_too_large": "intake", "send_timeout": "network",
    "send_failed_http": "intake", "send_failed_network": "network", "rejected": "intake",
    "acked_not_readable": "query", "in_flight": "sdk",
}


def free_port():
    """A localhost port with nothing listening on it."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def valid_event_id(event_id):
    return (isinstance(event_id, str) and 0 < len(event_id) <= MAX_EVENT_ID_LEN
            and "," not in event_id and safe_text(event_id) == event_id)


def read_ledger(path, stats=None):
    """Ledger entries grouped by event id.

    Tolerant by design: the writer may still be appending (partial last line),
    and in attach mode the file is not ours. Lines that are not JSON objects
    with a usable event_id and a known stage are skipped and counted in
    stats["skipped"]. Oversized files or too many ids raise DoctorError.
    """
    stats = stats if stats is not None else {}
    stats.setdefault("skipped", 0)
    by_id = {}
    if not os.path.exists(path):
        return by_id
    if not os.path.isfile(path):
        raise DoctorError(f"ledger {path} is not a regular file")
    if os.path.getsize(path) > MAX_LEDGER_BYTES:
        raise DoctorError(f"ledger {path} is larger than {MAX_LEDGER_BYTES} bytes")
    with open(path, "rb") as f:
        for raw in f:
            if len(raw) > MAX_LEDGER_LINE:
                stats["skipped"] += 1
                continue
            try:
                entry = json.loads(raw.decode("utf-8"))
            except ValueError:  # includes UnicodeDecodeError and a partial last line
                if raw.endswith(b"\n"):
                    stats["skipped"] += 1
                continue
            if (not isinstance(entry, dict) or not valid_event_id(entry.get("event_id"))
                    or entry.get("stage") not in LEDGER_STAGES):
                stats["skipped"] += 1
                continue
            _normalize_entry(entry)
            if entry["event_id"] not in by_id and len(by_id) >= MAX_LEDGER_IDS:
                raise DoctorError(f"ledger {path} has more than {MAX_LEDGER_IDS} event ids")
            by_id.setdefault(entry["event_id"], []).append(entry)
    return by_id


_NUMERIC_FIELDS = {"t": (int, float), "bytes": int, "attempt": int, "attempts": int,
                   "status": int, "queue_size": int, "wait": (int, float)}


def _normalize_entry(entry):
    """Drop numeric fields with the wrong type, so a foreign ledger can't crash classification."""
    for key, kind in _NUMERIC_FIELDS.items():
        if key in entry and (isinstance(entry[key], bool) or not isinstance(entry[key], kind)):
            del entry[key]
    if "t" in entry and not (-1e12 < entry["t"] < 1e12):
        del entry["t"]
    for key in ("kind", "error", "body", "reason"):
        if key in entry and not isinstance(entry[key], str):
            entry[key] = str(entry[key])


def query_intake(url, ids, api_key=DEFAULT_API_KEY, chunk=200, timeout=READ_TIMEOUT_S):
    """Read back what intake holds for ids. Raises DoctorError if intake can't be read.

    Only well-formed entries for ids we asked about are kept, so a misbehaving
    intake cannot inject findings for other events.
    """
    by_id = {}
    ids = list(ids)
    wanted = set(ids)
    for start in range(0, len(ids), chunk):
        query = urllib.parse.urlencode({"ids": ",".join(ids[start:start + chunk])}, safe=",")
        doc = get_json(f"{url}/v1/events?{query}", {"X-Api-Key": api_key}, timeout,
                       secrets=(api_key,))
        entries = doc.get("entries") if isinstance(doc, dict) else None
        if not isinstance(entries, list):
            raise DoctorError("intake read-back response has no 'entries' list")
        for entry in entries:
            if (isinstance(entry, dict) and isinstance(entry.get("event_id"), str)
                    and entry.get("event_id") in wanted
                    and entry.get("status") in ("accepted", "rejected")):
                by_id.setdefault(entry["event_id"], []).append(entry)
    return by_id


def _failure_stop(fail):
    kind, attempts = fail.get("kind"), fail.get("attempts", "?")
    if kind == "connection":
        return "send_failed_network", f"{fail.get('error', 'connection error')} after {attempts} attempts"
    if kind == "timeout":
        return "send_timeout", f"{fail.get('error', 'timed out')} after {attempts} attempts"
    if kind != "http" or not isinstance(fail.get("status"), int):
        return "send_failed_http", f"{fail.get('error', 'unusable response')} after {attempts} attempts"
    status, body = fail["status"], fail.get("body", "")
    body = safe_text(body, limit=500)
    try:
        code = json.loads(body).get("code")
    except (ValueError, AttributeError, TypeError):
        code = None
    if status == 401:
        stop = "auth_expired_key" if code == "key_expired" else "auth_invalid_key"
        return stop, f"HTTP 401 {body}"
    if status == 403:
        return "forbidden", f"HTTP 403 {body}"
    if status == 413:
        return "payload_too_large", f"HTTP 413 {body}"
    if status == 429:
        return "rate_limited", f"HTTP 429 after {attempts} attempts"
    return "send_failed_http", f"HTTP {status} after {attempts} attempts"


def classify(event_id, ledger, server, live=False):
    """Return the per-event finding from client ledger + server log entries.

    With live=True (the emitter is still running) an event that is only
    buffered, or mid-retry, is reported as in_flight rather than lost.
    """
    stages = {}
    for e in ledger:
        stages[e["stage"]] = e  # keep the last entry per stage
    accepted = [e for e in server if e.get("status") == "accepted"]
    rejected = [e for e in server if e.get("status") == "rejected"]
    attempts = [e for e in ledger if e["stage"] == "attempt"]
    http_status = None
    for e in ledger:
        if e["stage"] in ("acked", "rejected"):
            http_status = 200
        elif e["stage"] in ("retry", "failed", "split") and e.get("status"):
            http_status = e["status"]
    hops = {
        "buffered": "buffered" in stages,
        "sent": bool(attempts),
        "http_status": http_status,
        "received": bool(server),
        "landed": bool(accepted),
    }
    size = (stages.get("buffered") or stages.get("dropped") or {}).get("bytes")
    evidence = {"ledger": ledger, "intake": [_slim(e) for e in server]}

    if accepted:
        copies = len(accepted)
        stop, detail = "landed", f"in intake log ({copies} cop{'y' if copies == 1 else 'ies'})"
    elif rejected:
        stop, detail = "rejected", safe_text(rejected[-1].get("reason", "no reason given"), 500)
    elif "dropped" in stages:
        stop = "queue_overflow"
        detail = f"queue already held {stages['dropped'].get('queue_size')} events when track() was called"
    elif "failed" in stages:
        stop, detail = _failure_stop(stages["failed"])
    elif "acked" in stages:
        stop, detail = "acked_not_readable", "SDK got a 200 but the intake log has no record"
    elif live and ledger:
        stop, detail = "in_flight", "buffered or mid-send"
    elif "buffered" in stages:
        stop, detail = "never_sent", "still in SDK buffer when the process exited"
    else:
        stop, detail = "never_tracked", "no SDK ledger entry"

    return {"event_id": event_id, "stop_point": stop, "label": STOP_POINTS[stop][0],
            "hop": HOP_OF[stop], "detail": detail, "bytes": size,
            "attempts": len(attempts), "hops": hops, "evidence": evidence}


def _slim(entry):
    """Intake log entry without the bulky raw payload (attachments can be 1 MB)."""
    entry = dict(entry)
    for key in ("event", "raw"):
        if isinstance(entry.get(key), dict) and len(json.dumps(entry[key])) > 2000:
            entry[key] = {"_omitted": f"{len(json.dumps(entry[key]))} bytes"}
    return entry


def verdict_line(findings):
    total = len(findings)
    counts = Counter(f["stop_point"] for f in findings)
    landed = counts.pop("landed", 0)
    if total == 0:
        return "FAIL: no events were tracked, so nothing was verified."
    if not counts:
        return f"PASS: {landed}/{total} events landed and were read back."
    parts = []
    for stop, n in counts.most_common():
        label, hint = STOP_POINTS[stop]
        if stop == "in_flight":
            parts.append(f"{n} still in flight when watching stopped (not proven either way)")
        else:
            parts.append(f"{n} {label} (fix: {hint})")
    return f"FAIL: {landed}/{total} landed; " + "; ".join(parts) + "."


def emitter_args(fail):
    """Translate a demo failure mode into the sample agent's bug flags."""
    return [] if fail is None else list(FAILURES[fail][1])


def server_knobs(fail):
    return {} if fail is None else dict(FAILURES[fail][2])


def emitter_cmd(endpoint, run_id, ledger_path, fail=None, n=12, gzip=False, extra=()):
    cmd = [sys.executable, "-m", "mock_raindrop.sample_agent",
           "--endpoint", endpoint, "--api-key", DEFAULT_API_KEY,
           "--run-id", run_id, "--n", str(n), "--ledger", ledger_path]
    if gzip:
        cmd.append("--gzip")
    cmd += list(extra)
    cmd += emitter_args(fail)  # argparse keeps the last value, so failure flags override
    return cmd


def build_report(run_id, fail, endpoint, ids, findings, workdir, **extra):
    counts = Counter(f["stop_point"] for f in findings)
    landed = counts.get("landed", 0)
    sizes = [f["bytes"] for f in findings if f["bytes"]]
    return {
        "run_id": run_id,
        "fail_mode": fail,
        "fail_description": FAILURES[fail][0] if fail else None,
        "endpoint": endpoint,
        "created_at": time.time(),
        "total": len(ids),
        "landed": landed,
        "lost": len(ids) - landed,
        "ok": bool(ids) and landed == len(ids),
        "counts": dict(counts),
        "by_hop": dict(Counter(f["hop"] for f in findings if f["stop_point"] != "landed")),
        "attempts": sum(f["attempts"] for f in findings),
        "event_bytes": {"total": sum(sizes), "max": max(sizes, default=0)},
        "verdict": verdict_line(findings),
        **extra,
        "artifacts": {"sdk_ledger": os.path.join(workdir, "sdk_ledger.jsonl"),
                      "intake_log": os.path.join(workdir, "intake_log.jsonl")},
        "events": findings,
    }


def sanitize_report(report, secrets=()):
    """Mask secrets everywhere in the report (evidence, stderr, endpoint) before it is persisted.

    The mock's well-known test keys are masked too, so artifacts never carry
    any key, real or fake.
    """
    secrets = tuple(secrets) + MOCK_KEYS
    clean = redact(report, secrets)
    if isinstance(clean.get("endpoint"), str):
        clean["endpoint"] = redact_url(clean["endpoint"])
    return clean


def write_report(report, workdir, secrets=()):
    """Redact, then write report.json atomically. Mutates report in place to match the file."""
    path = os.path.join(workdir, "report.json")
    report["artifacts"]["report"] = path
    clean = sanitize_report(report, secrets)
    report.clear()
    report.update(clean)
    atomic_write(path, json.dumps(report, indent=2))
    return path


def check_n(n):
    if isinstance(n, bool) or not isinstance(n, int) or not MIN_EVENTS <= n <= MAX_EVENTS:
        raise DoctorError(f"--n must be an integer from {MIN_EVENTS} to {MAX_EVENTS}, got {n!r}")


def run_check(n=12, fail=None, workdir=None, timeout=2.0, gzip=False,
              emitter_timeout=EMITTER_TIMEOUT_S):
    """Run one full check. Returns the JSON-serializable, redacted report.

    Raises DoctorError when the check itself cannot produce a trustworthy
    verdict: bad input, the emitter hung or crashed, or intake could not be
    read back. No report.json is written in that case; the ledger and intake
    log stay in the run directory for inspection.
    """
    if fail not in (None,) + FAIL_MODES:
        raise ValueError(f"unknown fail mode {fail!r}")
    check_n(n)
    run_id = uuid.uuid4().hex[:8]
    workdir = workdir or os.path.join(runs_dir(), run_id)
    private_makedirs(workdir)
    ledger_path = os.path.join(workdir, "sdk_ledger.jsonl")
    ids = event_ids(run_id, n)

    started = time.time()
    with IntakeServer(os.path.join(workdir, "intake_log.jsonl"), **server_knobs(fail)) as server:
        endpoint = f"http://127.0.0.1:{free_port()}" if fail == "network" else server.url
        cmd = emitter_cmd(endpoint, run_id, ledger_path, fail=fail, n=n, gzip=gzip)
        try:
            proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True,
                                  timeout=emitter_timeout)
        except subprocess.TimeoutExpired:
            raise DoctorError(f"canary emitter did not finish within {emitter_timeout:.0f}s; "
                              f"partial ledger kept at {ledger_path}")
        ledger_stats = {}
        ledger = read_ledger(ledger_path, ledger_stats)
        if proc.returncode != 0:
            tail = safe_text(redact(proc.stderr, MOCK_KEYS)[-1000:])
            raise DoctorError(f"canary emitter exited with code {proc.returncode}, so the run "
                              f"proves nothing about delivery. stderr tail: {tail}")

        # Poll only for ids the SDK believes reached the server, so failed ids don't stall us.
        expect = [i for i in ids if any(e["stage"] in ("acked", "rejected") for e in ledger.get(i, []))]
        deadline = time.time() + timeout
        found = query_intake(server.url, ids)
        while any(i not in found for i in expect) and time.time() < deadline:
            time.sleep(0.1)
            found = query_intake(server.url, ids)

    findings = [classify(i, ledger.get(i, []), found.get(i, [])) for i in ids]
    report = build_report(run_id, fail, endpoint, ids, findings, workdir,
                          gzip=gzip, started_at=started, duration_s=round(time.time() - started, 3),
                          emitter_exit_code=proc.returncode,
                          emitter_stderr=safe_text(redact(proc.stderr, MOCK_KEYS)[-2000:]),
                          ledger_lines_skipped=ledger_stats["skipped"])
    write_report(report, workdir)
    return report


def latest_report(directory=None):
    """Path to the most recent report.json under the runs directory, or None."""
    directory = directory or runs_dir()
    if not os.path.isdir(directory):
        return None
    paths = [os.path.join(directory, d, "report.json") for d in os.listdir(directory)]
    paths = [p for p in paths if os.path.isfile(p)]
    return max(paths, key=os.path.getmtime) if paths else None


def render_table(report, verbose=False):
    def yn(v):
        return "-" if v is None else ("yes" if v else "no")

    rows = [("EVENT ID", "BUFFERED", "SENT", "HTTP", "LANDED", "STOP POINT / DETAIL")]
    for f in report["events"]:
        h = f["hops"]
        rows.append((f["event_id"], yn(h["buffered"]), yn(h["sent"]),
                     str(h["http_status"] or "-"), yn(h["landed"]),
                     safe_text(f"{f['label']}: {f['detail']}")))
    widths = [max(len(r[i]) for r in rows) for i in range(5)]
    lines = []
    for i, r in enumerate(rows):
        lines.append("  ".join(c.ljust(w) for c, w in zip(r[:5], widths)) + "  " + r[5])
        if verbose and i > 0:
            lines.extend(evidence_lines(report["events"][i - 1]))
    return "\n".join(lines)


def evidence_lines(finding, indent="    "):
    """One line per ledger and intake entry, oldest first, for --verbose."""
    out = []
    t0 = min((e["t"] for e in finding["evidence"]["ledger"] if "t" in e), default=None)
    for e in finding["evidence"]["ledger"]:
        rel = f"+{(e['t'] - t0) * 1000:6.0f}ms" if t0 is not None and "t" in e else " " * 9
        extra = ", ".join(f"{k}={v}" for k, v in e.items() if k not in ("event_id", "stage", "t"))
        out.append(safe_text(f"{indent}{rel}  sdk     {e['stage']:<9} {extra}"))
    for e in finding["evidence"]["intake"]:
        out.append(safe_text(f"{indent}{'':9}  intake  {e['status']:<9} {e.get('reason', '')}"))
    return out

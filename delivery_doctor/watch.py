"""Watch mode: tail a live emitter and keep reporting delivery health.

Managed mode (default) starts a local intake and a streaming sample agent,
optionally with a failure injected. Attach mode (--ledger and --intake) tails
the ledger of an emitter you started yourself and reads back from its intake.

Every tick re-reads the ledger, reads every id seen so far back from intake,
and classifies each one. Events that are buffered or mid-retry while the
emitter is still running count as in flight, not lost.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from collections import Counter

from delivery_doctor.doctor import (REPO_ROOT, RUNS_DIR, build_report, classify, emitter_cmd,
                                    free_port, query_intake, read_ledger, server_knobs,
                                    write_report, FAIL_MODES)
from mock_raindrop.server import DEFAULT_API_KEY, IntakeServer


def snapshot(ledger_path, intake_url, api_key, live):
    ledger = read_ledger(ledger_path)
    ids = sorted(ledger)
    found = query_intake(intake_url, ids, api_key) if ids else {}
    findings = [classify(i, ledger[i], found.get(i, []), live=live) for i in ids]
    return ids, findings


def summarize(findings, elapsed):
    counts = Counter(f["stop_point"] for f in findings)
    in_flight = counts.get("in_flight", 0)
    landed = counts.get("landed", 0)
    return {"elapsed": round(elapsed, 2), "tracked": len(findings), "landed": landed,
            "in_flight": in_flight, "lost": len(findings) - landed - in_flight,
            "stop_points": dict(counts.most_common())}


def render_tick(tick, width=30):
    head = (f"[{tick['elapsed']:6.1f}s]  tracked {tick['tracked']:<5} landed {tick['landed']:<5} "
            f"lost {tick['lost']:<5} in flight {tick['in_flight']}")
    lines = [head]
    top = max(tick["stop_points"].values(), default=0)
    for stop, n in tick["stop_points"].items():
        bar = "#" * max(1, round(width * n / top)) if n else ""
        lines.append(f"    {stop:<20} {bar} {n}")
    return lines


class Printer:
    """Redraws in place on a terminal, appends blocks otherwise."""

    def __init__(self, out, json_mode):
        self.out, self.json_mode = out, json_mode
        self.tty = hasattr(out, "isatty") and out.isatty() and not json_mode
        self.drawn = 0

    def tick(self, tick):
        if self.json_mode:
            self.out.write(json.dumps({"type": "tick", **tick}) + "\n")
        else:
            lines = render_tick(tick)
            if self.tty and self.drawn:
                self.out.write(f"\x1b[{self.drawn}F\x1b[J")
            self.out.write("\n".join(lines) + "\n")
            self.drawn = len(lines)
        self.out.flush()


def watch(fail=None, duration=10.0, rate=20.0, interval=1.0, gzip=False,
          ledger=None, intake=None, api_key=DEFAULT_API_KEY, out=None, json_mode=False,
          workdir=None):
    """Run watch mode until the emitter exits (managed) or duration passes (attach)."""
    out = out or sys.stdout
    printer = Printer(out, json_mode)
    if fail not in (None,) + FAIL_MODES:
        raise ValueError(f"unknown fail mode {fail!r}")
    run_id = uuid.uuid4().hex[:8]
    workdir = workdir or os.path.join(RUNS_DIR, run_id)
    os.makedirs(workdir, exist_ok=True)
    ticks = []
    start = time.time()

    def loop(ledger_path, intake_url, alive):
        while True:
            live = alive()
            _, findings = snapshot(ledger_path, intake_url, api_key, live)
            tick = summarize(findings, time.time() - start)
            if tick["tracked"] or not live:
                ticks.append(tick)
                printer.tick(tick)
            if not live:
                return
            time.sleep(interval)

    if ledger:  # attach mode: someone else runs the emitter and the intake
        if not intake:
            raise ValueError("attach mode needs --intake as well as --ledger")
        try:
            loop(ledger, intake, lambda: time.time() - start < duration)
        except KeyboardInterrupt:
            pass
        ids, findings = snapshot(ledger, intake, api_key, live=True)
        endpoint = intake
    else:
        ledger = os.path.join(workdir, "sdk_ledger.jsonl")
        with IntakeServer(os.path.join(workdir, "intake_log.jsonl"), **server_knobs(fail)) as server:
            endpoint = f"http://127.0.0.1:{free_port()}" if fail == "network" else server.url
            cmd = emitter_cmd(endpoint, run_id, ledger, fail=fail, gzip=gzip,
                              extra=["--duration", str(duration), "--rate", str(rate)])
            proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL)
            try:
                loop(ledger, server.url, lambda: proc.poll() is None)
            except KeyboardInterrupt:
                proc.terminate()
                proc.wait()
            ids, findings = snapshot(ledger, server.url, api_key, live=False)
            # give acked events a moment to become readable before calling them lost
            deadline = time.time() + 2.0
            while any(f["stop_point"] == "acked_not_readable" for f in findings) and time.time() < deadline:
                time.sleep(0.1)
                ids, findings = snapshot(ledger, server.url, api_key, live=False)

    final = summarize(findings, time.time() - start)
    def same(a, b):
        return {**a, "elapsed": 0} == {**b, "elapsed": 0}

    if not ticks or not same(final, ticks[-1]):
        ticks.append(final)
        printer.tick(final)
    report = build_report(run_id, fail, endpoint, ids, findings, workdir, mode="watch",
                          gzip=gzip, started_at=start, duration_s=round(time.time() - start, 3),
                          ticks=ticks)
    report["artifacts"]["sdk_ledger"] = ledger
    write_report(report, workdir)
    return report

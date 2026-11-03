"""Watch mode: tail a live emitter and keep reporting delivery health.

Managed mode (default) starts a local intake and a streaming sample agent,
optionally with a failure injected. Attach mode (--ledger and --intake) tails
the ledger of an emitter you started yourself and reads back from its intake.

Every tick re-reads the ledger, reads every id seen so far back from intake,
and classifies each one. Events that are buffered or mid-retry while the
emitter is still running count as in flight, not lost.
"""

import json
import math
import os
import subprocess
import sys
import time
import uuid
from collections import Counter

from delivery_doctor.doctor import (MAX_LEDGER_IDS, MOCK_KEYS, REPO_ROOT, build_report, classify,
                                    emitter_cmd, free_port, query_intake, read_ledger, runs_dir,
                                    server_knobs, write_report, FAIL_MODES)
from delivery_doctor.safety import (DoctorError, private_makedirs, redact, safe_text,
                                    validate_intake_url)
from mock_raindrop.server import DEFAULT_API_KEY, IntakeServer

# name -> (min, max); all in seconds or events per second
BOUNDS = {"duration": (0.1, 3600.0), "rate": (0.1, 1000.0), "interval": (0.1, 60.0)}


def check_bounds(**values):
    for name, value in values.items():
        lo, hi = BOUNDS[name]
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or not lo <= value <= hi):
            raise DoctorError(f"--{name} must be between {lo:g} and {hi:g}, got {value!r}")
    if "rate" in values and "duration" in values:
        check_volume(values["rate"], values["duration"])


def check_volume(rate, duration):
    """A managed run must stay under the ledger id cap, or it is bound to end in an error."""
    if rate * duration > MAX_LEDGER_IDS:
        raise DoctorError(f"--rate x --duration would emit about {rate * duration:.0f} events; "
                          f"the limit is {MAX_LEDGER_IDS}")


def stop_emitter(proc, grace=3.0):
    """Terminate the emitter, escalating to kill; never leaves an orphan behind."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def snapshot(ledger_path, intake_url, api_key, live, stats=None):
    local = {}
    ledger = read_ledger(ledger_path, local)
    if stats is not None:
        stats.update(local)  # each snapshot re-reads the whole file, so replace, don't add
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
          workdir=None, allow_remote_intake=False):
    """Run watch mode until the emitter exits (managed) or duration passes (attach).

    Raises DoctorError if the run cannot produce a trustworthy verdict (bad
    bounds, non-loopback intake without opt-in, intake unreadable, ledger
    missing, managed emitter crashed). A managed emitter is always stopped.
    """
    out = out or sys.stdout
    printer = Printer(out, json_mode)
    if fail not in (None,) + FAIL_MODES:
        raise ValueError(f"unknown fail mode {fail!r}")
    check_bounds(duration=duration, interval=interval, **({} if ledger else {"rate": rate}))
    if ledger:
        if not intake:
            raise ValueError("attach mode needs --intake as well as --ledger")
        if fail or gzip:
            raise DoctorError("--fail and --gzip only apply to managed mode, not --ledger/--intake")
        intake = validate_intake_url(intake, allow_remote=allow_remote_intake)
        if os.path.exists(ledger) and not os.path.isfile(ledger):
            raise DoctorError(f"ledger {ledger} is not a regular file")
    run_id = uuid.uuid4().hex[:8]
    workdir = workdir or os.path.join(runs_dir(), run_id)
    private_makedirs(workdir)
    ticks = []
    stats = {}
    start = time.time()

    def loop(ledger_path, intake_url, alive):
        while True:
            live = alive()
            _, findings = snapshot(ledger_path, intake_url, api_key, live, stats)
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
        interrupted = False
        try:
            loop(ledger, intake, lambda: time.time() - start < duration)
        except KeyboardInterrupt:
            interrupted = True
        if not os.path.isfile(ledger):
            raise DoctorError(f"ledger {ledger} never appeared; is the emitter writing to it?")
        ids, findings = snapshot(ledger, intake, api_key, live=True, stats=stats)
        endpoint = intake
        emitter = {"interrupted": interrupted}
    else:
        ledger = os.path.join(workdir, "sdk_ledger.jsonl")
        stderr_path = os.path.join(workdir, "emitter_stderr.txt")
        with IntakeServer(os.path.join(workdir, "intake_log.jsonl"), **server_knobs(fail)) as server, \
                open(stderr_path, "w") as stderr_file:
            endpoint = f"http://127.0.0.1:{free_port()}" if fail == "network" else server.url
            cmd = emitter_cmd(endpoint, run_id, ledger, fail=fail, gzip=gzip,
                              extra=["--duration", str(duration), "--rate", str(rate)])
            proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=subprocess.DEVNULL,
                                    stderr=stderr_file)
            interrupted = False
            try:
                try:
                    loop(ledger, server.url, lambda: proc.poll() is None)
                except KeyboardInterrupt:
                    interrupted = True
            finally:
                stop_emitter(proc)
            if proc.returncode != 0 and not interrupted:
                stderr_file.flush()
                with open(stderr_path, errors="replace") as f:
                    tail = safe_text(redact(f.read(), MOCK_KEYS + (api_key,))[-1000:])
                raise DoctorError(f"emitter exited with code {proc.returncode}, so the run proves "
                                  f"nothing about delivery. stderr tail: {tail}")
            ids, findings = snapshot(ledger, server.url, api_key, live=False, stats=stats)
            # give acked events a moment to become readable before calling them lost
            deadline = time.time() + 2.0
            while any(f["stop_point"] == "acked_not_readable" for f in findings) and time.time() < deadline:
                time.sleep(0.1)
                ids, findings = snapshot(ledger, server.url, api_key, live=False, stats=stats)
        emitter = {"emitter_exit_code": proc.returncode, "interrupted": interrupted}

    final = summarize(findings, time.time() - start)
    def same(a, b):
        return {**a, "elapsed": 0} == {**b, "elapsed": 0}

    if not ticks or not same(final, ticks[-1]):
        ticks.append(final)
        printer.tick(final)
    report = build_report(run_id, fail, endpoint, ids, findings, workdir, mode="watch",
                          gzip=gzip, started_at=start, duration_s=round(time.time() - start, 3),
                          ticks=ticks, ledger_lines_skipped=stats.get("skipped", 0), **emitter)
    report["artifacts"]["sdk_ledger"] = os.path.abspath(ledger)
    if emitter.get("interrupted"):
        report["ok"] = False
        report["verdict"] = "INTERRUPTED (partial, not a verdict): " + report["verdict"]
    write_report(report, workdir, secrets=(api_key,))
    return report

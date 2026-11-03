"""CLI: python -m delivery_doctor {check,watch,report} ...

Exit codes:
  0  every event landed and was read back
  1  at least one event was lost (or nothing could be verified)
  2  usage error (bad flag, out-of-range value, bad path, refused intake URL)
  3  the doctor could not finish (intake unreadable, emitter crashed or hung)
130  interrupted
"""

import argparse
import json
import os
import sys

from delivery_doctor.doctor import (FAILURES, FAIL_MODES, MAX_EVENTS, MIN_EVENTS, MOCK_KEYS,
                                    latest_report, render_table, run_check, runs_dir)
from delivery_doctor.html_report import write_html
from delivery_doctor.safety import (MAX_REPORT_BYTES, RUN_ID_RE, DoctorError, check_output_path,
                                    redact, safe_text, validate_intake_url)
from delivery_doctor.watch import BOUNDS, check_volume

EXIT_OK, EXIT_LOST, EXIT_USAGE, EXIT_ERROR, EXIT_INTERRUPTED = 0, 1, 2, 3, 130
API_KEY_ENV = "DELIVERY_DOCTOR_API_KEY"

FAIL_HELP = "failure modes for --fail:\n" + "\n".join(
    f"  {name:<12} {desc}" for name, (desc, _, _) in FAILURES.items())
EXIT_HELP = ("exit codes: 0 every event landed, 1 losses found, 2 usage error, "
             "3 doctor could not finish, 130 interrupted")


class UsageError(Exception):
    """Bad input discovered after argparse (paths, report files)."""


def rel(path):
    if not path:
        return path
    try:
        r = os.path.relpath(path)
    except ValueError:  # different drive on Windows
        return path
    return path if r.startswith("..") else r


def bounded(kind, lo, hi, name):
    def parse(text):
        try:
            value = kind(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"{name} must be a number, got {text!r}")
        if not (lo <= value <= hi):  # also rejects nan
            raise argparse.ArgumentTypeError(f"{name} must be between {lo:g} and {hi:g}, got {text}")
        return value
    return parse


N_TYPE = bounded(int, MIN_EVENTS, MAX_EVENTS, "--n")


def print_check(report, verbose=False):
    mode = report["fail_mode"] or "none (healthy)"
    print(f"Delivery Doctor run {report['run_id']}  injected failure: {mode}")
    print(f"endpoint: {report['endpoint']}" + ("  (gzip on)" if report.get("gzip") else ""))
    print()
    print(render_table(report, verbose=verbose))
    if verbose:
        print(f"\nsend attempts: {report['attempts']}  "
              f"event bytes: {report['event_bytes']['total']} (largest {report['event_bytes']['max']})  "
              f"run time: {report['duration_s']}s")
        if report.get("emitter_stderr"):
            print("emitter stderr:\n" + safe_text(report["emitter_stderr"]))
    print(f"\nJSON report: {rel(report['artifacts']['report'])}")
    if report["artifacts"].get("html"):
        print(f"HTML report: {rel(report['artifacts']['html'])}")
    print(report["verdict"])


def load_report(source):
    """--from accepts a report.json path, a run directory, or an 8-hex-char run id."""
    candidates = [source, os.path.join(source, "report.json")]
    if RUN_ID_RE.match(source):
        candidates.append(os.path.join(runs_dir(), source, "report.json"))
    for path in candidates:
        if os.path.isfile(path):
            if os.path.getsize(path) > MAX_REPORT_BYTES:
                raise UsageError(f"{path} is larger than {MAX_REPORT_BYTES} bytes")
            try:
                with open(path, encoding="utf-8") as f:
                    report = json.load(f)
            except OSError as e:
                raise UsageError(f"cannot read {path}: {e.strerror or e}")
            except (ValueError, UnicodeDecodeError, RecursionError) as e:
                raise UsageError(f"{path} is not usable JSON: {type(e).__name__}")
            missing = [k for k in ("run_id", "ok", "verdict", "events")
                       if not isinstance(report, dict) or k not in report]
            if missing or not isinstance(report["events"], list):
                raise UsageError(f"{path} is not a Delivery Doctor report "
                                 f"(missing {', '.join(missing) or 'events list'})")
            return report
    raise UsageError(f"no report found for {source!r}")


def build_parser():
    ap = argparse.ArgumentParser(
        prog="python -m delivery_doctor",
        description="Prove that SDK events landed in intake, or name the hop where each one stopped.",
        epilog=EXIT_HELP, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    def add_common(p, fail_help="inject a demo failure (see list below)"):
        p.add_argument("--fail", choices=FAIL_MODES, metavar="MODE", help=fail_help)
        p.add_argument("--gzip", action="store_true", help="have the SDK gzip request bodies")
        p.add_argument("--json", action="store_true", help="machine-readable output only")

    check = sub.add_parser("check", help="emit tagged canaries and prove they landed",
                           epilog=FAIL_HELP + "\n\n" + EXIT_HELP,
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    check.add_argument("--n", type=N_TYPE, default=12,
                       help=f"number of canary events, {MIN_EVENTS}-{MAX_EVENTS} (default 12)")
    add_common(check)
    check.add_argument("--verbose", "-v", action="store_true",
                       help="show every ledger and intake entry under each event")
    check.add_argument("--html", metavar="PATH", help="also write a single-file HTML report")

    watch = sub.add_parser("watch", help="tail a live emitter and report delivery health",
                           epilog=FAIL_HELP + "\n\n" + EXIT_HELP,
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common(watch)
    watch.add_argument("--duration", type=bounded(float, *BOUNDS["duration"], "--duration"),
                       default=10.0,
                       help="seconds the emitter streams for (attach mode: seconds to watch)")
    watch.add_argument("--rate", type=bounded(float, *BOUNDS["rate"], "--rate"), default=20.0,
                       help="events per second (default 20)")
    watch.add_argument("--interval", type=bounded(float, *BOUNDS["interval"], "--interval"),
                       default=1.0, help="seconds between reports")
    watch.add_argument("--ledger", help="attach mode: SDK ledger file of an emitter you started")
    watch.add_argument("--intake", help="attach mode: intake base URL to read events back from "
                                        "(loopback only unless --allow-remote-intake)")
    watch.add_argument("--api-key", default=None,
                       help=f"attach mode: key for reading intake (prefer ${API_KEY_ENV}; "
                            "flags are visible in the process list)")
    watch.add_argument("--allow-remote-intake", action="store_true",
                       help="attach mode: permit a non-loopback https intake (off by default)")
    watch.add_argument("--html", metavar="PATH", help="write a single-file HTML report at the end")

    report = sub.add_parser("report", help="write a shareable single-file HTML report",
                            epilog=FAIL_HELP + "\n\n" + EXIT_HELP,
                            formatter_class=argparse.RawDescriptionHelpFormatter)
    report.add_argument("--html", metavar="PATH", required=True, help="output file")
    report.add_argument("--from", dest="source", metavar="RUN",
                        help="report.json path, run directory, or run id (default: latest run)")
    report.add_argument("--n", type=N_TYPE, default=12, help="canaries, when a fresh check is run")
    add_common(report, fail_help="run a fresh check with this failure injected")
    return ap


def _run(args, ap):
    if getattr(args, "html", None):
        try:
            check_output_path(args.html)  # fail fast, before spending time on a run
        except DoctorError as e:
            raise UsageError(str(e))

    if args.command == "check":
        result = run_check(n=args.n, fail=args.fail, gzip=args.gzip)
        if args.html:
            result["artifacts"]["html"] = write_html(result, args.html)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_check(result, verbose=args.verbose)

    elif args.command == "watch":
        from delivery_doctor.watch import watch as run_watch
        if bool(args.ledger) != bool(args.intake):
            ap.error("attach mode needs both --ledger and --intake")
        if args.ledger and (args.fail or args.gzip):
            ap.error("--fail and --gzip apply to managed mode only, not --ledger/--intake")
        if not args.ledger:
            try:
                check_volume(args.rate, args.duration)
            except DoctorError as e:
                ap.error(str(e))
        if args.allow_remote_intake and not args.intake:
            ap.error("--allow-remote-intake only applies with --ledger/--intake")
        if args.intake:
            try:
                validate_intake_url(args.intake, allow_remote=args.allow_remote_intake)
            except DoctorError as e:
                raise UsageError(str(e))
        api_key = args.api_key or os.environ.get(API_KEY_ENV)
        kwargs = {"api_key": api_key} if api_key else {}
        result = run_watch(fail=args.fail, duration=args.duration, rate=args.rate,
                           interval=args.interval, gzip=args.gzip, ledger=args.ledger,
                           intake=args.intake, json_mode=args.json,
                           allow_remote_intake=args.allow_remote_intake, **kwargs)
        if args.html:
            result["artifacts"]["html"] = write_html(result, args.html)
        if args.json:
            summary = {k: v for k, v in result.items() if k not in ("events", "ticks")}
            print(json.dumps({"type": "report", **summary}))
        else:
            print(f"\nJSON report: {rel(result['artifacts']['report'])}")
            if args.html:
                print(f"HTML report: {rel(result['artifacts']['html'])}")
            print(result["verdict"])
        if result.get("interrupted"):
            return EXIT_INTERRUPTED

    else:  # report
        fresh = args.fail or args.gzip
        if args.source and fresh:
            ap.error("--from cannot be combined with --fail or --gzip")
        # no --from and no runs on disk yet: run a healthy check so this works on a fresh clone
        source = args.source or (None if fresh else latest_report())
        result = load_report(source) if source else run_check(n=args.n, fail=args.fail,
                                                              gzip=args.gzip)
        path = write_html(result, args.html)
        if args.json:
            print(json.dumps({"html": path, "run_id": result["run_id"], "ok": result["ok"],
                              "verdict": result["verdict"]}))
        else:
            print(f"HTML report for run {safe_text(result['run_id'])}: {rel(path)}")
            print(safe_text(result["verdict"]))

    return EXIT_OK if result["ok"] is True else EXIT_LOST


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    secrets = MOCK_KEYS + tuple(k for k in (getattr(args, "api_key", None),
                                            os.environ.get(API_KEY_ENV)) if k)

    def report_error(text, code):
        print("error: " + redact(safe_text(text, 1000), secrets, min_len=1), file=sys.stderr)
        return code

    try:
        return _run(args, ap)
    except UsageError as e:
        return report_error(str(e), EXIT_USAGE)
    except DoctorError as e:
        return report_error(str(e), EXIT_ERROR)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except Exception as e:  # never let a crash masquerade as exit 1 ("events lost")
        return report_error(f"internal error, no verdict: {type(e).__name__}: {e}", EXIT_ERROR)


if __name__ == "__main__":
    sys.exit(main())

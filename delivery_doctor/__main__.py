"""CLI: python -m delivery_doctor {check,watch,report} ...

Exit codes: 0 every event landed, 1 at least one event was lost, 2 usage error.
"""

import argparse
import json
import os
import sys

from delivery_doctor.doctor import FAILURES, FAIL_MODES, latest_report, render_table, run_check
from delivery_doctor.html_report import write_html

FAIL_HELP = "failure modes for --fail:\n" + "\n".join(
    f"  {name:<12} {desc}" for name, (desc, _, _) in FAILURES.items())
EXIT_HELP = "exit codes: 0 every event landed, 1 losses found, 2 usage error"


def rel(path):
    return os.path.relpath(path) if path else path


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
            print("emitter stderr:\n" + report["emitter_stderr"])
    print(f"\nJSON report: {rel(report['artifacts']['report'])}")
    if report["artifacts"].get("html"):
        print(f"HTML report: {report['artifacts']['html']}")
    print(report["verdict"])


def load_report(source):
    """--from accepts a report.json path, a run directory, or a run id."""
    for path in (source, os.path.join(source, "report.json"),
                 os.path.join(".doctor_runs", source, "report.json")):
        if os.path.isfile(path):
            with open(path) as f:
                return json.load(f)
    raise FileNotFoundError(f"no report found for {source!r}")


def main(argv=None):
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
    check.add_argument("--n", type=int, default=12, help="number of canary events (default 12)")
    add_common(check)
    check.add_argument("--verbose", "-v", action="store_true",
                       help="show every ledger and intake entry under each event")
    check.add_argument("--html", metavar="PATH", help="also write a single-file HTML report")

    watch = sub.add_parser("watch", help="tail a live emitter and report delivery health",
                           epilog=FAIL_HELP + "\n\n" + EXIT_HELP,
                           formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common(watch)
    watch.add_argument("--duration", type=float, default=10.0,
                       help="seconds the emitter streams for (attach mode: seconds to watch)")
    watch.add_argument("--rate", type=float, default=20.0, help="events per second (default 20)")
    watch.add_argument("--interval", type=float, default=1.0, help="seconds between reports")
    watch.add_argument("--ledger", help="attach mode: SDK ledger file of an emitter you started")
    watch.add_argument("--intake", help="attach mode: intake base URL to read events back from")
    watch.add_argument("--api-key", default=None, help="attach mode: key for reading intake")
    watch.add_argument("--html", metavar="PATH", help="write a single-file HTML report at the end")

    report = sub.add_parser("report", help="write a shareable single-file HTML report",
                            epilog=FAIL_HELP + "\n\n" + EXIT_HELP,
                            formatter_class=argparse.RawDescriptionHelpFormatter)
    report.add_argument("--html", metavar="PATH", required=True, help="output file")
    report.add_argument("--from", dest="source", metavar="RUN",
                        help="report.json path, run directory, or run id (default: latest run)")
    report.add_argument("--n", type=int, default=12, help="canaries, when a fresh check is run")
    add_common(report, fail_help="run a fresh check with this failure injected")

    args = ap.parse_args(argv)

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
        kwargs = {"api_key": args.api_key} if args.api_key else {}
        result = run_watch(fail=args.fail, duration=args.duration, rate=args.rate,
                           interval=args.interval, gzip=args.gzip, ledger=args.ledger,
                           intake=args.intake, json_mode=args.json, **kwargs)
        if args.html:
            result["artifacts"]["html"] = write_html(result, args.html)
        if args.json:
            summary = {k: v for k, v in result.items() if k not in ("events", "ticks")}
            print(json.dumps({"type": "report", **summary}))
        else:
            print(f"\nJSON report: {rel(result['artifacts']['report'])}")
            if args.html:
                print(f"HTML report: {args.html}")
            print(result["verdict"])

    else:  # report
        fresh = args.fail or args.gzip
        source = args.source or (None if fresh else latest_report())
        if args.source and fresh:
            ap.error("--from cannot be combined with --fail or --gzip")
        try:
            result = load_report(source) if source else run_check(n=args.n, fail=args.fail,
                                                                  gzip=args.gzip)
        except FileNotFoundError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        write_html(result, args.html)
        if args.json:
            print(json.dumps({"html": args.html, "run_id": result["run_id"], "ok": result["ok"],
                              "verdict": result["verdict"]}))
        else:
            print(f"HTML report for run {result['run_id']}: {args.html}")
            print(result["verdict"])

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

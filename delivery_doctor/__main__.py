"""CLI: python -m delivery_doctor check [--n N] [--fail auth|schema|buffer|network] [--json]"""

import argparse
import json
import sys

from delivery_doctor.doctor import FAIL_MODES, render_table, run_check


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m delivery_doctor")
    sub = ap.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="emit tagged canaries and prove they landed")
    check.add_argument("--n", type=int, default=12, help="number of canary events")
    check.add_argument("--fail", choices=FAIL_MODES, help="inject a demo failure")
    check.add_argument("--json", action="store_true", help="print only the JSON report")
    args = ap.parse_args(argv)

    report = run_check(n=args.n, fail=args.fail)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        mode = args.fail or "none (healthy)"
        print(f"Delivery Doctor run {report['run_id']}  injected failure: {mode}")
        print(f"endpoint: {report['endpoint']}\n")
        print(render_table(report))
        print(f"\nJSON report: {report['artifacts']['report']}")
        print(report["verdict"])
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

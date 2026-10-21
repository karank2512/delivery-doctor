"""A sample customer agent that emits tagged events through the mini SDK.

Runs as its own process so "exit before flush" is a real process exit.
The bug flags reproduce real customer mistakes:
  --bad-timestamp-every K  every Kth event sends timestamp as an ISO string
  --oversize-every K       every Kth event carries a ~1 MB attachment
  --skip-flush             return from main without calling flush()
  --no-exit-hook           disable the SDK's flush-on-exit hook
  --max-queue N            cap the SDK's in-memory queue at N events

By default it emits --n events in a burst. With --duration it streams events
at --rate per second for that many seconds instead, which is what
`delivery_doctor watch` tails.
"""

import argparse
import os
import time
from datetime import datetime, timezone

from mock_raindrop.sdk import Client


def event_ids(run_id, n):
    return [f"dd-{run_id}-{i:03d}" for i in range(n)]


def make_event(run_id, i, args):
    ts = time.time()
    every = args.bad_timestamp_every
    if every and i % every == every - 1:
        ts = datetime.now(timezone.utc).isoformat()
    props = {"seq": i, "run_id": run_id}
    every = args.oversize_every
    if every and i % every == every - 1:
        props["attachment"] = os.urandom(512 * 1024).hex()
    return {"event_id": f"dd-{run_id}-{i:03d}", "event": "doctor_canary",
            "user_id": "doctor", "timestamp": ts, "properties": props}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--n", type=int, default=12)
    ap.add_argument("--duration", type=float, help="stream for this many seconds instead of --n")
    ap.add_argument("--rate", type=float, default=20.0, help="events per second when streaming")
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--max-queue", type=int, default=1000)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--gzip", action="store_true")
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--bad-timestamp-every", type=int, default=0)
    ap.add_argument("--oversize-every", type=int, default=0)
    ap.add_argument("--skip-flush", action="store_true")
    ap.add_argument("--no-exit-hook", action="store_true")
    args = ap.parse_args(argv)

    client = Client(args.endpoint, args.api_key, batch_size=args.batch_size,
                    max_queue_size=args.max_queue, timeout=args.timeout, gzip=args.gzip,
                    flush_on_exit=not args.no_exit_hook, ledger_path=args.ledger)
    if args.duration is None:
        for i in range(args.n):
            client.track(make_event(args.run_id, i, args))
    else:
        start, i = time.time(), 0
        while time.time() - start < args.duration:
            client.track(make_event(args.run_id, i, args))
            i += 1
            time.sleep(1.0 / args.rate)
    if not args.skip_flush:
        client.flush()


if __name__ == "__main__":
    main()

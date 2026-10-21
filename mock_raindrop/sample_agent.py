"""A sample customer agent that emits tagged events through the mini SDK.

Runs as its own process so "exit before flush" is a real process exit.
The bug flags reproduce real customer mistakes:
  --bad-timestamp-every K  every Kth event sends timestamp as an ISO string
  --skip-flush             return from main without calling flush()
"""

import argparse
import time
from datetime import datetime, timezone

from mock_raindrop.sdk import Client


def event_ids(run_id, n):
    return [f"dd-{run_id}-{i:03d}" for i in range(n)]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--n", type=int, default=12)
    ap.add_argument("--batch-size", type=int, default=5)
    ap.add_argument("--ledger", required=True)
    ap.add_argument("--bad-timestamp-every", type=int, default=0)
    ap.add_argument("--skip-flush", action="store_true")
    args = ap.parse_args(argv)

    client = Client(args.endpoint, args.api_key, batch_size=args.batch_size,
                    ledger_path=args.ledger)
    for i, event_id in enumerate(event_ids(args.run_id, args.n)):
        ts = time.time()
        if args.bad_timestamp_every and i % args.bad_timestamp_every == args.bad_timestamp_every - 1:
            ts = datetime.now(timezone.utc).isoformat()
        client.track({"event_id": event_id, "event": "doctor_canary",
                      "user_id": "doctor", "timestamp": ts,
                      "properties": {"seq": i, "run_id": args.run_id}})
    if not args.skip_flush:
        client.flush()


if __name__ == "__main__":
    main()

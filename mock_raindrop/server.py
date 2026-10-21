"""Mock Raindrop intake server.

POST /v1/events   body {"events": [...]}, header X-Api-Key, optional
                  Content-Encoding: gzip.
                  401 on an unknown key (code invalid_key) or an expired key
                  (code key_expired), 413 when the body is over the size
                  limit, 429 with Retry-After once a rate limit is hit.
                  Otherwise 200 with per-event accept/reject.
GET  /v1/events?ids=a,b   read back what the log holds for those ids.

Failure knobs for demos and tests: flaky_requests (503s), rate_limit_after
(429 after N accepted requests), max_body_bytes (413), hang_seconds (the
request hangs, then the connection drops without processing, like a proxy
timeout).

Every received event is appended to a JSONL log, accepted or rejected, so the
log is the source of truth for "did it land". Accepted event ids are deduped,
so a retried batch never lands twice.
"""

import argparse
import gzip
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

DEFAULT_API_KEY = "rd_local_test_key"
EXPIRED_API_KEY = "rd_expired_test_key"
DEFAULT_MAX_BODY_BYTES = 256 * 1024

# field name -> required type
SCHEMA = {"event_id": str, "event": str, "user_id": str, "timestamp": (int, float)}


def validate(event):
    """Return None if the event is valid, else a short reject reason."""
    if not isinstance(event, dict):
        return "event is not an object"
    for field, typ in SCHEMA.items():
        if field not in event:
            return f"missing field '{field}'"
        if not isinstance(event[field], typ) or isinstance(event[field], bool):
            return f"field '{field}' has wrong type {type(event[field]).__name__}"
    return None


class EventLog:
    """Append-only JSONL log with an in-memory index for reads."""

    def __init__(self, path):
        self.path = path
        self.entries = []
        self.accepted_ids = set()
        self.lock = threading.Lock()

    def append(self, entry):
        with self.lock:
            if entry["status"] == "accepted":
                if entry["event_id"] in self.accepted_ids:
                    return False
                self.accepted_ids.add(entry["event_id"])
            entry["received_at"] = time.time()
            self.entries.append(entry)
            with open(self.path, "a") as f:
                f.write(json.dumps(entry) + "\n")
            return True

    def lookup(self, ids):
        wanted = set(ids)
        with self.lock:
            return [e for e in self.entries if e.get("event_id") in wanted]


def make_handler(log, api_key, flaky_requests=0, rate_limit_after=None, retry_after=0.05,
                 max_body_bytes=DEFAULT_MAX_BODY_BYTES, hang_seconds=0, expired_keys=()):
    state = {"flaky_left": flaky_requests, "posts_ok": 0}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _reply(self, code, body, headers=None):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _authed(self):
            key = self.headers.get("X-Api-Key")
            if key in expired_keys:
                self._reply(401, {"error": "api key expired", "code": "key_expired"})
                return False
            if key != api_key:
                self._reply(401, {"error": "invalid api key", "code": "invalid_key"})
                return False
            return True

        def do_POST(self):
            if urlparse(self.path).path != "/v1/events":
                return self._reply(404, {"error": "not found"})
            if hang_seconds:
                time.sleep(hang_seconds)
                self.close_connection = True
                return  # drop the connection without a response or processing
            if not self._authed():
                return
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)  # drain first so the client sees the 413, not a reset
            if length > max_body_bytes:
                return self._reply(413, {"error": f"body is {length} bytes, limit is {max_body_bytes}"})
            with lock:
                if state["flaky_left"] > 0:
                    state["flaky_left"] -= 1
                    return self._reply(503, {"error": "temporarily unavailable"})
                if rate_limit_after is not None and state["posts_ok"] >= rate_limit_after:
                    return self._reply(429, {"error": "rate limit exceeded"},
                                       {"Retry-After": str(retry_after)})
                state["posts_ok"] += 1
            try:
                if self.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                events = json.loads(raw)["events"]
            except (OSError, ValueError, KeyError, TypeError):
                return self._reply(400, {"error": "body must be {\"events\": [...]}"})

            accepted, rejected = [], []
            for event in events:
                event_id = event.get("event_id") if isinstance(event, dict) else None
                reason = validate(event)
                if reason:
                    log.append({"status": "rejected", "event_id": event_id,
                                "reason": reason, "raw": event})
                    rejected.append({"event_id": event_id, "reason": reason})
                else:
                    log.append({"status": "accepted", "event_id": event_id, "event": event})
                    accepted.append(event_id)
            self._reply(200, {"accepted": accepted, "rejected": rejected})

        def do_GET(self):
            url = urlparse(self.path)
            if url.path != "/v1/events":
                return self._reply(404, {"error": "not found"})
            if not self._authed():
                return
            ids = parse_qs(url.query).get("ids", [""])[0].split(",")
            self._reply(200, {"entries": log.lookup([i for i in ids if i])})

    return Handler


class IntakeServer:
    """Runs the intake on 127.0.0.1 in a background thread."""

    def __init__(self, log_path, port=0, api_key=DEFAULT_API_KEY, expired_keys=(EXPIRED_API_KEY,),
                 **knobs):
        self.log = EventLog(log_path)
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", port), make_handler(self.log, api_key, expired_keys=expired_keys, **knobs))
        self.port = self.httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


def main():
    ap = argparse.ArgumentParser(description="Run the mock Raindrop intake server.")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--log", default="intake_log.jsonl")
    ap.add_argument("--rate-limit-after", type=int, help="answer 429 after N accepted requests")
    ap.add_argument("--max-body-bytes", type=int, default=DEFAULT_MAX_BODY_BYTES)
    ap.add_argument("--hang-seconds", type=float, default=0, help="hang, then drop every POST")
    args = ap.parse_args()
    with IntakeServer(args.log, port=args.port, rate_limit_after=args.rate_limit_after,
                      max_body_bytes=args.max_body_bytes, hang_seconds=args.hang_seconds) as srv:
        print(f"intake listening on {srv.url}, log at {args.log}, key {DEFAULT_API_KEY}")
        try:
            srv.thread.join()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()

"""Mini Raindrop-style SDK: buffered, batched sends with retry.

Like many real SDKs, track() only buffers. Events go out when a batch fills
(by count or by bytes) or when flush() is called. An atexit hook flushes on a
normal interpreter exit; if that hook is disabled or the process dies hard,
the tail of the buffer is lost silently.

Delivery behavior:
  - batches are capped by event count and by encoded size
  - optional gzip request bodies (Content-Encoding: gzip)
  - retries with exponential backoff and jitter on connection errors,
    timeouts, 429 (honoring Retry-After) and 5xx
  - no retry on other 4xx, so one bad batch cannot stall the queue
  - 413 splits the batch in half and retries each half, so one oversized
    event only takes itself down
  - a bounded in-memory queue; events tracked while it is full are dropped

The one addition is a delivery ledger: a JSONL file the SDK appends to on
every state change (buffered, dropped, attempt, retry, split, acked, rejected,
failed). It is written as things happen, so it survives an abrupt exit and
the doctor can read it.
"""

import atexit
import gzip
import json
import random
import socket
import time
import urllib.error
import urllib.request


def event_size(event):
    """Encoded size of one event in bytes, as it would appear in a batch."""
    return len(json.dumps(event).encode())


class Client:
    def __init__(self, endpoint, api_key, batch_size=5, max_batch_bytes=512 * 1024,
                 max_queue_size=1000, max_retries=3, backoff=0.05, max_backoff=1.0,
                 timeout=2.0, gzip=False, flush_on_exit=True, ledger_path=None):
        self.endpoint = endpoint.rstrip("/") + "/v1/events"
        self.api_key = api_key
        self.batch_size = batch_size
        self.max_batch_bytes = max_batch_bytes
        self.max_queue_size = max_queue_size
        self.max_retries = max_retries
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.timeout = timeout
        self.gzip = gzip
        self.ledger_path = ledger_path
        self.buffer = []
        self.counters = {"buffered": 0, "dropped": 0, "sent": 0, "acked": 0,
                         "rejected": 0, "failed": 0, "retries": 0, "bytes_sent": 0}
        if flush_on_exit:
            atexit.register(self.flush)

    def _record(self, stage, event_ids, **detail):
        if not self.ledger_path:
            return
        now = time.time()
        with open(self.ledger_path, "a") as f:
            for event_id in event_ids:
                f.write(json.dumps({"event_id": event_id, "stage": stage, "t": now, **detail}) + "\n")

    def track(self, event):
        event_id = event.get("event_id")
        size = event_size(event)
        if len(self.buffer) >= self.max_queue_size:
            self.counters["dropped"] += 1
            self._record("dropped", [event_id], reason="queue_overflow",
                         queue_size=self.max_queue_size, bytes=size)
            return False
        self.buffer.append(event)
        self.counters["buffered"] += 1
        self._record("buffered", [event_id], bytes=size)
        if len(self.buffer) >= self.batch_size or self._buffer_bytes() >= self.max_batch_bytes:
            self.flush()
        return True

    def _buffer_bytes(self):
        return sum(event_size(e) for e in self.buffer)

    def _next_batch(self):
        batch, size = [], 0
        for event in self.buffer:
            n = event_size(event)
            if batch and (len(batch) >= self.batch_size or size + n > self.max_batch_bytes):
                break
            batch.append(event)
            size += n
        self.buffer = self.buffer[len(batch):]
        return batch

    def flush(self):
        while self.buffer:
            self._send(self._next_batch())

    def _wait(self, attempt, retry_after=None):
        if retry_after is not None:
            return min(retry_after, self.max_backoff)
        base = min(self.backoff * 2 ** (attempt - 1), self.max_backoff)
        return base * (0.5 + random.random() / 2)

    def _encode(self, batch):
        body = json.dumps({"events": batch}).encode()
        headers = {"Content-Type": "application/json", "X-Api-Key": self.api_key}
        if self.gzip:
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"
        return body, headers

    def _send(self, batch):
        ids = [e.get("event_id") for e in batch]
        body, headers = self._encode(batch)
        error = None
        for attempt in range(1, self.max_retries + 1):
            self._record("attempt", ids, attempt=attempt, bytes=len(body), gzip=self.gzip)
            req = urllib.request.Request(self.endpoint, data=body, method="POST", headers=headers)
            retry_after = None
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    result = json.loads(resp.read())
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")
                error = {"kind": "http", "status": e.code, "body": text}
                if e.code == 413 and len(batch) > 1:
                    half = len(batch) // 2
                    self._record("split", ids, status=413, into=[half, len(batch) - half])
                    self._send(batch[:half])
                    self._send(batch[half:])
                    return
                if e.code == 429:
                    try:
                        retry_after = float(e.headers.get("Retry-After", ""))
                    except ValueError:
                        retry_after = None
                elif e.code < 500:
                    break  # 4xx will not get better on retry, so do not stall the queue
            except (socket.timeout, TimeoutError) as e:
                error = {"kind": "timeout", "error": f"no response within {self.timeout}s"}
            except (urllib.error.URLError, OSError) as e:
                reason = getattr(e, "reason", e)
                if isinstance(reason, (socket.timeout, TimeoutError)):
                    error = {"kind": "timeout", "error": f"no response within {self.timeout}s"}
                else:
                    error = {"kind": "connection", "error": str(reason)}
            else:
                self.counters["sent"] += len(batch)
                self.counters["bytes_sent"] += len(body)
                rejected = {r["event_id"]: r["reason"] for r in result.get("rejected", [])}
                for event_id in ids:
                    if event_id in rejected:
                        self.counters["rejected"] += 1
                        self._record("rejected", [event_id], status=200, reason=rejected[event_id])
                    else:
                        self.counters["acked"] += 1
                        self._record("acked", [event_id], status=200)
                return
            if attempt < self.max_retries:
                wait = self._wait(attempt, retry_after)
                self.counters["retries"] += 1
                self._record("retry", ids, attempt=attempt, wait=round(wait, 3),
                             **{k: v for k, v in error.items() if k != "body"})
                time.sleep(wait)
        self.counters["failed"] += len(batch)
        self._record("failed", ids, attempts=attempt, **error)

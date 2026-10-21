"""Mini Raindrop-style SDK: buffered, batched sends with retry.

Like many real SDKs, track() only buffers. Events go out when a batch fills
or when flush() is called. If the process exits before flush(), the tail of
the buffer is lost silently (proposal pain 1).

The one addition is a delivery ledger: a JSONL file the SDK appends to on
every state change (buffered, attempt, acked, rejected, failed). It is written
as things happen, so it survives an abrupt exit and the doctor can read it.
"""

import json
import time
import urllib.error
import urllib.request


class Client:
    def __init__(self, endpoint, api_key, batch_size=5, max_retries=3,
                 backoff=0.05, timeout=2.0, ledger_path=None):
        self.endpoint = endpoint.rstrip("/") + "/v1/events"
        self.api_key = api_key
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.backoff = backoff
        self.timeout = timeout
        self.ledger_path = ledger_path
        self.buffer = []
        self.counters = {"buffered": 0, "sent": 0, "acked": 0,
                         "rejected": 0, "failed": 0}

    def _record(self, stage, event_ids, **detail):
        if not self.ledger_path:
            return
        with open(self.ledger_path, "a") as f:
            for event_id in event_ids:
                f.write(json.dumps({"event_id": event_id, "stage": stage, **detail}) + "\n")

    def track(self, event):
        self.buffer.append(event)
        self.counters["buffered"] += 1
        self._record("buffered", [event.get("event_id")])
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        while self.buffer:
            batch, self.buffer = self.buffer[:self.batch_size], self.buffer[self.batch_size:]
            self._send(batch)

    def _send(self, batch):
        ids = [e.get("event_id") for e in batch]
        body = json.dumps({"events": batch}).encode()
        error = None
        for attempt in range(1, self.max_retries + 1):
            self._record("attempt", ids, attempt=attempt)
            req = urllib.request.Request(
                self.endpoint, data=body, method="POST",
                headers={"Content-Type": "application/json", "X-Api-Key": self.api_key})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    result = json.loads(resp.read())
            except urllib.error.HTTPError as e:
                error = {"kind": "http", "status": e.code, "body": e.read().decode(errors="replace")}
                if e.code < 500:
                    break  # 4xx will not get better on retry, so do not stall the queue
            except (urllib.error.URLError, OSError) as e:
                error = {"kind": "connection", "error": str(getattr(e, "reason", e))}
            else:
                self.counters["sent"] += len(batch)
                rejected = {r["event_id"]: r["reason"] for r in result.get("rejected", [])}
                for event_id in ids:
                    if event_id in rejected:
                        self.counters["rejected"] += 1
                        self._record("rejected", [event_id], status=200, reason=rejected[event_id])
                    else:
                        self.counters["acked"] += 1
                        self._record("acked", [event_id], status=200)
                return
            time.sleep(self.backoff * attempt)
        self.counters["failed"] += len(batch)
        self._record("failed", ids, attempts=attempt, **error)

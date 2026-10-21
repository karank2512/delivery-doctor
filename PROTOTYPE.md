# Delivery Doctor: prototype

A runnable, local-only version of the Delivery Doctor feature from the proposal in `README.md`. It proves that events emitted by an SDK actually landed in intake, and when they did not, it names the exact hop where each one stopped.

Everything runs on `127.0.0.1` with sample data. There is no Raindrop account, no real API key, and no network access beyond localhost. Python 3 stdlib only (pytest for the tests).

## What is in here

| Path | What it is |
|---|---|
| `mock_raindrop/server.py` | Local intake server. `POST /v1/events` checks the `X-Api-Key` header (401 on a bad key), validates each event against a schema, and appends every event, accepted or rejected, to an append-only JSONL log. Accepted ids are deduped so retries never double count. `GET /v1/events?ids=...` reads events back. |
| `mock_raindrop/sdk.py` | Mini SDK like a customer agent would use: `track()` buffers, batches go out when full or on `flush()`, retries with backoff on connection errors and 5xx, no retry on 4xx (so one bad batch cannot stall the queue). It also writes a delivery ledger (buffered, attempt, acked, rejected, failed) as each state change happens. |
| `mock_raindrop/sample_agent.py` | A sample customer app that emits N canary events with known ids (`dd-<run_id>-NNN`). Runs as a separate process, so "exit before flush" is a real process exit. |
| `delivery_doctor/` | The CLI. Starts a local intake, runs the sample agent, reads the SDK ledger, polls intake for every id, and classifies each event. Prints a hop-by-hop table, writes a JSON report, and ends with a one-line verdict. Exit code 0 means every event landed. |
| `tests/test_doctor.py` | Pytest suite: a healthy run lands everything, and each failure mode is pinned to the right stop point. |

## How it decides where an event stopped

For each canary id the doctor joins two independent sources of evidence: what the client says it did (the SDK ledger) and what the server actually holds (the intake log, read back over HTTP). It walks the hops in order and reports the first one that failed:

| Stop point | Evidence |
|---|---|
| `landed` | Intake log has an accepted record for the id |
| `rejected` (schema) | Intake log has a rejected record; detail is the server's reject reason |
| `send_failed_auth` | SDK ledger shows the batch failed with HTTP 401/403 |
| `send_failed_network` | SDK ledger shows connection errors after all retries |
| `never_sent` (buffer drop) | SDK ledger shows the event was buffered but never attempted |
| `acked_not_readable` | SDK got a 200 but intake has no record (ingest or query loss) |
| `never_tracked` | No ledger entry at all; the app never called `track()` |

Every finding in the JSON report carries its raw evidence (ledger lines and intake log entries), and each run's artifacts are kept under `.doctor_runs/<run_id>/`.

## Two-minute demo

```bash
python3 -m delivery_doctor check                  # healthy: PASS, exit 0
python3 -m delivery_doctor check --fail auth      # SDK uses a wrong API key
python3 -m delivery_doctor check --fail schema    # every 4th event sends timestamp as a string
python3 -m delivery_doctor check --fail buffer    # app exits without calling flush()
python3 -m delivery_doctor check --fail network   # SDK points at a port with nothing listening
python3 -m delivery_doctor check --json           # JSON report only, for CI or scripts
```

Options: `--n N` sets the number of canaries (default 12). To run the intake on its own: `python3 -m mock_raindrop.server --port 8787`.

Tests:

```bash
python3 -m pip install pytest
python3 -m pytest -q
```

## Sample output

`--fail buffer` (the proposal's pain 1: the process exits before the buffer is flushed):

```
Delivery Doctor run 15cd578d  injected failure: buffer
endpoint: http://127.0.0.1:56768

EVENT ID         BUFFERED  SENT  HTTP  LANDED  STOP POINT / DETAIL
dd-15cd578d-000  yes       yes   200   yes     landed: in intake log (1 copy)
...
dd-15cd578d-009  yes       yes   200   yes     landed: in intake log (1 copy)
dd-15cd578d-010  yes       no    -     no      never sent by client (buffer drop): still in SDK buffer when the process exited
dd-15cd578d-011  yes       no    -     no      never sent by client (buffer drop): still in SDK buffer when the process exited

JSON report: .doctor_runs/15cd578d/report.json
FAIL: 10/12 landed; 2 never sent by client (buffer drop) (fix: call flush() before the process exits).
```

Verdict lines for the other modes:

```
PASS: 12/12 events landed and were read back.
FAIL: 0/12 landed; 12 send failed: auth rejected (fix: check the API key / write key the SDK is configured with).
FAIL: 9/12 landed; 3 received but rejected (schema) (fix: fix the event payload; the reject reason names the field).
FAIL: 0/12 landed; 12 send failed: connection (fix: check the endpoint host and port, proxies, and Docker networking).
```

Top of the JSON report (`--json`), followed by an `events` array with per-event hops and evidence:

```json
{
  "run_id": "12878c2d",
  "fail_mode": null,
  "total": 12,
  "landed": 12,
  "ok": true,
  "counts": {"landed": 12},
  "verdict": "PASS: 12/12 events landed and were read back."
}
```

## Mapping to the proposal

| Proposal | Prototype |
|---|---|
| "`raindrop doctor` sends a canary ... tagged with a unique ID. It then polls ingest until it can read each one back." | `python -m delivery_doctor check` emits tagged canaries and polls `GET /v1/events?ids=...` until each expected id is readable |
| "Every hop gets a pass or fail: SDK buffer, flush on exit, network or Host check, auth, ingest, and query" | Per-event BUFFERED / SENT / HTTP / LANDED columns and a single stop point per event |
| Pain 1: events dropped because the process exited before the POSTs went out | `--fail buffer` reproduces it; reported as `never_sent` with the exact ids lost |
| Pain 2: Docker traffic rejected by a Host check, telemetry silently never appears | `--fail network` covers the "cannot reach intake" side; `send_failed_auth` treats 401 and 403 alike, so a Host check 403 lands in the send-failed bucket with the server's response body |
| Pain 3: one permanently rejected batch stalled the Go SDK queue | The SDK never retries a 4xx, per-event schema rejects come back as `rejected` with the reason, and the server dedupes by id so retries never duplicate (tested with a forced 503) |
| "Each SDK keeps per-process counters for sent, acked, rejected ... and dropped on exit" | `Client.counters` plus the append-only delivery ledger, which survives an abrupt exit |
| "Every pass/fail result links to raw evidence" | Each event in `report.json` includes its ledger lines and intake log entries |

## Not in this prototype

The dashboard "Pipeline health" panel, the "went quiet" alert, and the dependency-mode check (pain 4) from the proposal. This also runs against a mock intake, not the real `raindrop-ai` SDK and Workshop; the proposal's Day 1 plan swaps those in.

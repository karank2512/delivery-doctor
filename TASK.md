# Build prototype: Raindrop "Delivery Doctor"

You are working in a fresh clone of the PRIVATE repo karank2512/yc-proposal-raindrop on branch `prototype`. The README is Karan's proposal - read it first; it defines the feature (a delivery check that proves SDK events actually landed, and says exactly where they stopped).

## What to build
A self-contained Python prototype using ONLY local components and sample data. No real Raindrop account, no API keys, no network beyond localhost.

1. `mock_raindrop/`: a local event intake server (localhost HTTP) with an append-only event log, plus a mini SDK emitter like a customer's agent would use (batched sends, retry, api-key header).
2. `delivery_doctor/`: a CLI (`python -m delivery_doctor check`) that:
   - runs a sample emitter that emits N tagged events with known ids,
   - queries the intake log for those ids,
   - reports per event: landed, or exactly where it stopped - never sent by client (buffer drop), send failed (auth 401 / connection), received but rejected (schema), or landed.
   - ends with a one-line verdict and a machine-readable JSON report.
3. Include failure-mode demo flags so a founder can watch it catch each stop point: `--fail auth`, `--fail schema`, `--fail buffer`, `--fail network`.
4. `tests/`: pytest suite proving each failure mode is localized to the right stop point and a healthy run reports all landed.
5. `PROTOTYPE.md`: what it is, demo commands, sample output, mapping to the proposal.

## Rules
- Python 3 stdlib + pytest only. No paid services.
- Do NOT modify README.md. Add new files only.
- No em dashes in any prose you write.
- Small and readable; demo a founder can run in 2 minutes.
- When done: run the full test suite, paste output at the end of your final message, then `git add -A && git commit`. Do NOT push.

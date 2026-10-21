# Raindrop: feature proposal

- **Company:** Raindrop (raindrop.ai), monitoring for AI agents in production. Raindrop 2.0 adds "Self-Healing Agents", where a coding agent fixes the issue and the fix becomes an eval.
- **Role:** Forward Deployed Engineer (FDE), San Francisco, in person. Focus: get customers running on Raindrop, own their problems end to end across their agents and Raindrop's platform, and turn field learnings into product feedback.
- **Posting:** https://jobs.ashbyhq.com/Raindrop/2ae60d74-621b-4bbd-b92b-2f64a2979cc9 (published 2026-08-13 per Ashby's public posting API)
- **Date researched:** 2026-10-21

## Pain points (public sources)

Raindrop has no G2 reviews, Reddit threads, or Hacker News threads about it that I could find. Its public GitHub repos are the best first-hand source. The pattern across them: **telemetry that never arrives, with no error to say so.** For a product built to catch silent failures, a silent failure in its own pipe is the worst kind.

1. **Local-only mode dropped every event.** The Claude Code plugin sent its POSTs to Workshop as fire-and-forget calls, then the process exited before they went out, so Workshop "stays empty forever". Having a cloud key hid the bug. Reported by a user on 2026-08-22, now closed.
   https://github.com/raindrop-ai/claude-code-plugin/issues/1
2. **Docker setups get a 403 and see nothing.** Since Workshop v0.1.10, SDK traffic from apps running in Docker (sent to `host.docker.internal`) is rejected by a Host header check, "and telemetry silently never appears in Workshop". It worked in v0.1.8 or earlier, and there is no escape hatch. Opened 2026-07-07, still open.
   https://github.com/raindrop-ai/workshop/issues/33
3. **One rejected batch stalled the Go SDK queue.** If the server permanently rejected a trace batch, the SDK resent it on every flush, and "No later spans were sent until the queue overflowed." Fixed in a PR merged 2026-09-30.
   https://github.com/raindrop-ai/go/pull/12
4. **The install footprint blocks lean setups.** `raindrop-ai` eagerly imports the Traceloop SDK and about nine provider instrumentation packages even with `useExternalOtel: true`. Installing with `--omit=optional` makes the base package fail to load. Opened 2026-07-17, still open.
   https://github.com/raindrop-ai/workshop/issues/34
5. **Install-time outage and Windows failures.** `curl -fsSL https://raindrop.sh/install` could not connect (2026-05-16, closed). Separately, Workshop compiles on Windows/WSL but then errors out (2026-05-25, still open).
   https://github.com/raindrop-ai/workshop/issues/10 and https://github.com/raindrop-ai/workshop/issues/18

## Proposed feature: Delivery Doctor

**One thing:** a delivery check, run from the CLI and the dashboard, that proves the events an SDK emits actually landed. When they don't, it says exactly where they stopped.

- `raindrop doctor` sends a canary event, a trace, and a tool span, each tagged with a unique ID. It then polls ingest until it can read each one back. Every hop gets a pass or fail: SDK buffer, flush on exit, network or Host check, auth, ingest, and query. This would have caught pains 1, 2 and 3 in the first minute of setup, not after a user went digging in source.
- Each SDK keeps per-process counters for sent, acked, rejected (with the 4xx reason), and dropped on exit. Raindrop shows these on a "Pipeline health" panel per project.
- An alert fires when a project that was sending events goes quiet, or when its reject rate jumps. Raindrop already alerts on agent failures, and this alerts on its own data path.
- `doctor` also prints the resolved dependency mode (Traceloop or direct shipping), so pain 4 shows up as a clear diagnosis, not a load error.

**Why this role:** the FDE job is to get customers running and debug "across their agents and our platform". Today the first step of that debugging is proving data arrived at all. Delivery Doctor makes that one command, and its failure reports become the product feedback the posting asks for.

## How Karan would build it (each claim mapped to his work)

| Piece of Delivery Doctor | What Karan has done that maps to it | Project |
|---|---|---|
| Canary events with unique IDs, traced hop by hop and read back | Built approve/deny traces tied to tool call IDs, and evals that score every LLM output 3 ways (fully tested MVP in simulated mode) | Foreman |
| Retries that never duplicate or stall the queue (pain 3) | Built 6 idempotent collectors tracking 907 companies and 10,585 open postings, with 97.8% run success over 321 runs | tell |
| Every pass/fail result links to raw evidence (the request, the response, the reject reason) | In tell, every score links to its public-record receipt | tell |
| Acked writes counted once, and doubtful states held back and not guessed | Exactly-once HubSpot writes, with low-confidence writes held back for a human | RelayIQ |
| Flush on exit and a durable local buffer, so events survive the process ending (pain 1) | Owns the storage and session-restore layer (team results: 340s to 0.88s median fork, 14.3 GB/s restore) | thaw |
| Embedding with users and proving the system works against a known baseline | Production retrieval assistant used daily by 60+ staff, cut manual lookup time 40%, evaluated against a golden set | UW-Madison IT |

## Rough demo plan (a few days)

- **Day 1:** Write a small agent app that uses the public `raindrop-ai` TypeScript SDK and sends to a local Workshop. Reproduce two known failures on purpose: exit right after tracking (pain 1), and run the app in Docker against `host.docker.internal` (pain 2).
- **Day 2:** Write `doctor` as a standalone script. It sends tagged canaries, reads them back from local Workshop, and prints a hop-by-hop table: buffered, flushed, HTTP status, readable.
- **Day 3:** Add sent/acked/rejected/dropped counters by wrapping the SDK's transport. Add a "went quiet" check over a sliding window, and show the results on a one-page HTML panel.
- **Day 4:** Record a 2-minute walkthrough. Start in a broken state, run `doctor`, read the diagnosis, apply the fix, run it again, and get all green.
- **Scope note:** this runs only against local, open-source Workshop. It needs no Raindrop account, no write key, and no paid service.

## LinkedIn note (under 300 characters)

> Hi, I'm Karan. I built tell: 6 idempotent collectors, 97.8% success over 321 runs. Saw Workshop issues where events drop silently, so I'd love to prototype a check that proves every SDK event landed. Up for a quick chat about the FDE role?

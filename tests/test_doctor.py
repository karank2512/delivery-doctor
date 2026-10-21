import html
import io
import json
import re
import subprocess
import sys

import pytest

from delivery_doctor.__main__ import main
from delivery_doctor.doctor import build_report, classify, read_ledger, run_check
from delivery_doctor.html_report import render_html
from delivery_doctor.watch import watch
from mock_raindrop.sdk import Client
from mock_raindrop.server import DEFAULT_API_KEY, IntakeServer


def stops(report):
    return [e["stop_point"] for e in report["events"]]


def test_healthy_run_reports_all_landed(tmp_path):
    report = run_check(n=12, workdir=str(tmp_path))
    assert report["ok"] is True
    assert stops(report) == ["landed"] * 12
    assert report["verdict"].startswith("PASS: 12/12")
    for e in report["events"]:
        assert e["hops"] == {"buffered": True, "sent": True, "http_status": 200,
                             "received": True, "landed": True}


def test_auth_failure_stops_at_send_with_401(tmp_path):
    report = run_check(n=12, fail="auth", workdir=str(tmp_path))
    assert stops(report) == ["auth_invalid_key"] * 12
    assert all(e["hops"]["http_status"] == 401 for e in report["events"])
    assert all(not e["hops"]["received"] for e in report["events"])
    assert report["verdict"].startswith("FAIL: 0/12")


def test_schema_failure_flags_only_bad_events_as_rejected(tmp_path):
    report = run_check(n=12, fail="schema", workdir=str(tmp_path))
    # sample agent breaks every 4th event (seq 3, 7, 11)
    expected = ["rejected" if i % 4 == 3 else "landed" for i in range(12)]
    assert stops(report) == expected
    rejected = [e for e in report["events"] if e["stop_point"] == "rejected"]
    assert all("timestamp" in e["detail"] for e in rejected)
    assert all(e["hops"]["received"] and not e["hops"]["landed"] for e in rejected)


def test_buffer_failure_flags_unflushed_tail_as_never_sent(tmp_path):
    report = run_check(n=12, fail="buffer", workdir=str(tmp_path))
    # batch size 5: two full batches go out, the last 2 events sit in the buffer at exit
    assert stops(report) == ["landed"] * 10 + ["never_sent"] * 2
    tail = report["events"][-1]["hops"]
    assert tail["buffered"] is True and tail["sent"] is False


def test_network_failure_stops_at_connection(tmp_path):
    report = run_check(n=12, fail="network", workdir=str(tmp_path))
    assert stops(report) == ["send_failed_network"] * 12
    assert all("after 3 attempts" in e["detail"] for e in report["events"])
    assert all(e["hops"]["http_status"] is None for e in report["events"])


def test_retry_after_503_lands_each_event_exactly_once(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    with IntakeServer(str(tmp_path / "log.jsonl"), flaky_requests=1) as srv:
        client = Client(srv.url, DEFAULT_API_KEY, batch_size=3, ledger_path=str(ledger))
        for i in range(3):
            client.track({"event_id": f"e{i}", "event": "x", "user_id": "u", "timestamp": 1.0})
        client.flush()
        # resend the same batch: server dedupes by event_id
        client.buffer = [{"event_id": "e0", "event": "x", "user_id": "u", "timestamp": 1.0}]
        client.flush()
        ids = [e["event_id"] for e in srv.log.entries]
    assert sorted(ids) == ["e0", "e1", "e2"]
    attempts = [e for e in read_ledger(str(ledger))["e1"] if e["stage"] == "attempt"]
    assert len(attempts) == 2


def test_acked_but_missing_from_intake_is_its_own_stop_point():
    ledger = [{"event_id": "x", "stage": "buffered"},
              {"event_id": "x", "stage": "attempt", "attempt": 1},
              {"event_id": "x", "stage": "acked", "status": 200}]
    assert classify("x", ledger, [])["stop_point"] == "acked_not_readable"
    assert classify("x", [], [])["stop_point"] == "never_tracked"


@pytest.mark.parametrize("fail,code", [(None, 0), ("buffer", 1)])
def test_cli_json_output_and_exit_code(capsys, fail, code):
    argv = ["check", "--n", "6", "--json"] + (["--fail", fail] if fail else [])
    assert main(argv) == code
    report = json.loads(capsys.readouterr().out)
    assert report["total"] == 6
    assert report["ok"] is (code == 0)


def test_expired_key_is_distinct_from_invalid_key(tmp_path):
    report = run_check(n=6, fail="expired-key", workdir=str(tmp_path))
    assert stops(report) == ["auth_expired_key"] * 6
    assert "key_expired" in report["events"][0]["detail"]
    assert "rotate" in report["verdict"]


def test_rate_limit_lands_first_batch_then_reports_429(tmp_path):
    report = run_check(n=12, fail="rate-limit", workdir=str(tmp_path))
    assert stops(report) == ["landed"] * 5 + ["rate_limited"] * 7
    lost = report["events"][-1]
    assert lost["hops"]["http_status"] == 429
    assert lost["attempts"] == 3
    # the SDK honored the server's Retry-After instead of its own backoff
    waits = [e["wait"] for e in lost["evidence"]["ledger"] if e["stage"] == "retry"]
    assert waits == [0.05, 0.05]


def test_payload_too_large_only_takes_down_the_oversized_events(tmp_path):
    report = run_check(n=12, fail="too-large", workdir=str(tmp_path))
    expected = ["payload_too_large" if i % 6 == 5 else "landed" for i in range(12)]
    assert stops(report) == expected
    big = report["events"][5]
    assert big["bytes"] > 1_000_000 and big["hops"]["http_status"] == 413
    assert report["event_bytes"]["max"] == max(e["bytes"] for e in report["events"])


def test_timeout_is_its_own_stop_point(tmp_path):
    report = run_check(n=5, fail="timeout", workdir=str(tmp_path))
    assert stops(report) == ["send_timeout"] * 5
    assert all("after 3 attempts" in e["detail"] for e in report["events"])


def test_queue_overflow_drops_the_burst_tail(tmp_path):
    report = run_check(n=12, fail="overflow", workdir=str(tmp_path))
    assert stops(report) == ["landed"] * 8 + ["queue_overflow"] * 4
    tail = report["events"][-1]
    assert tail["hops"]["buffered"] is False and tail["hop"] == "sdk"


def test_gzip_run_lands_everything_with_smaller_bodies(tmp_path):
    plain = run_check(n=10, workdir=str(tmp_path / "plain"))
    zipped = run_check(n=10, gzip=True, workdir=str(tmp_path / "gzip"))
    assert zipped["ok"] and zipped["gzip"] is True

    def first_attempt(report):
        return next(e for e in report["events"][0]["evidence"]["ledger"] if e["stage"] == "attempt")

    assert first_attempt(zipped)["gzip"] is True
    assert first_attempt(zipped)["bytes"] < first_attempt(plain)["bytes"]


def test_exit_hook_flushes_when_app_forgets_to(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    with IntakeServer(str(tmp_path / "log.jsonl")) as srv:
        subprocess.run([sys.executable, "-m", "mock_raindrop.sample_agent", "--endpoint", srv.url,
                        "--api-key", DEFAULT_API_KEY, "--run-id", "hook", "--n", "7",
                        "--ledger", str(ledger), "--skip-flush"], check=True, timeout=30)
        assert len({e["event_id"] for e in srv.log.entries}) == 7


def test_sdk_batches_by_bytes_and_splits_on_413(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    with IntakeServer(str(tmp_path / "log.jsonl"), max_body_bytes=4000) as srv:
        client = Client(srv.url, DEFAULT_API_KEY, batch_size=10, max_batch_bytes=20_000,
                        ledger_path=str(ledger), flush_on_exit=False)
        for i in range(4):
            pad = "x" * (5000 if i == 2 else 10)
            client.track({"event_id": f"e{i}", "event": "x", "user_id": "u", "timestamp": 1.0,
                          "properties": {"pad": pad}})
        client.flush()
        landed = sorted(e["event_id"] for e in srv.log.entries)
    assert landed == ["e0", "e1", "e3"]
    entries = read_ledger(str(ledger))
    assert any(e["stage"] == "split" for e in entries["e2"])
    assert classify("e2", entries["e2"], [])["stop_point"] == "payload_too_large"
    assert client.counters["failed"] == 1 and client.counters["acked"] == 3


def test_sdk_drops_when_queue_is_full():
    client = Client("http://127.0.0.1:9", DEFAULT_API_KEY, batch_size=100, max_queue_size=2,
                    flush_on_exit=False)
    results = [client.track({"event_id": f"e{i}"}) for i in range(4)]
    assert results == [True, True, False, False]
    assert client.counters["dropped"] == 2


def test_live_classification_keeps_unsent_events_in_flight():
    ledger = [{"event_id": "x", "stage": "buffered", "t": 1.0}]
    assert classify("x", ledger, [], live=True)["stop_point"] == "in_flight"
    assert classify("x", ledger, [], live=False)["stop_point"] == "never_sent"


def test_forbidden_403_is_reported_separately():
    ledger = [{"event_id": "x", "stage": "buffered"},
              {"event_id": "x", "stage": "failed", "kind": "http", "status": 403,
               "body": '{"error": "host not allowed"}', "attempts": 1}]
    finding = classify("x", ledger, [])
    assert finding["stop_point"] == "forbidden" and "host not allowed" in finding["detail"]


def test_watch_streams_ticks_and_ends_with_a_report(tmp_path):
    out = io.StringIO()
    report = watch(duration=1.2, rate=20, interval=0.3, out=out, json_mode=True,
                   workdir=str(tmp_path))
    ticks = [json.loads(line) for line in out.getvalue().splitlines()]
    assert len(ticks) >= 2 and all(t["type"] == "tick" for t in ticks)
    assert ticks[-1]["tracked"] == report["total"] > 5
    assert report["ok"] and report["mode"] == "watch"
    assert ticks[-1]["lost"] == 0 and ticks[-1]["in_flight"] == 0


def test_watch_reports_losses_live_and_at_the_end(tmp_path):
    out = io.StringIO()
    report = watch(fail="auth", duration=1.0, rate=20, interval=0.3, out=out,
                   workdir=str(tmp_path))
    text = out.getvalue()
    assert "auth_invalid_key" in text and "lost" in text
    assert not report["ok"] and report["landed"] == 0
    assert set(report["counts"]) == {"auth_invalid_key"}


def test_watch_attach_mode_tails_an_external_emitter(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    with IntakeServer(str(tmp_path / "log.jsonl")) as srv:
        proc = subprocess.Popen([sys.executable, "-m", "mock_raindrop.sample_agent",
                                 "--endpoint", srv.url, "--api-key", DEFAULT_API_KEY,
                                 "--run-id", "ext", "--duration", "0.6", "--rate", "20",
                                 "--ledger", str(ledger)])
        proc.wait(timeout=30)
        report = watch(ledger=str(ledger), intake=srv.url, duration=0.5, interval=0.2,
                       out=io.StringIO(), workdir=str(tmp_path / "w"))
    assert report["ok"] and report["total"] > 5


def test_html_report_is_self_contained(tmp_path):
    report = run_check(n=12, fail="schema", workdir=str(tmp_path))
    page = render_html(report)
    assert report["run_id"] in page and html.escape(report["verdict"]) in page
    assert not re.search(r'(src|href)="(https?:)?//', page)
    data = re.search(r'<script type="application/json" id="report-data">(.*?)</script>', page, re.S)
    assert json.loads(data.group(1))["counts"] == report["counts"]
    assert page.count('class="track"') == 12
    assert "rejected" in page and "timestamp" in page


def test_html_report_escapes_hostile_strings():
    report = build_report(
        "r1", None, "http://x", ["</script><b>"],
        [classify("</script><b>", [], [])], "/tmp")
    page = render_html(report)
    assert "</script><b>" not in page.split('id="report-data">')[0]
    data = re.search(r'id="report-data">(.*?)</script>', page, re.S).group(1)
    assert json.loads(data)["events"][0]["event_id"] == "</script><b>"


def test_cli_report_writes_html_from_latest_and_from_path(tmp_path, capsys):
    out = tmp_path / "fresh.html"
    assert main(["report", "--html", str(out), "--fail", "buffer", "--n", "6"]) == 1
    assert out.read_text().startswith("<!doctype html>")
    capsys.readouterr()
    again = tmp_path / "again.html"
    assert main(["report", "--html", str(again), "--json"]) == 1  # latest run is the buffer one
    assert json.loads(capsys.readouterr().out)["ok"] is False
    healthy = run_check(n=4, workdir=str(tmp_path / "h"))
    assert main(["report", "--html", str(again), "--from", healthy["artifacts"]["report"]]) == 0
    assert main(["report", "--html", str(again), "--from", "no-such-run"]) == 2


def test_cli_check_verbose_and_html(tmp_path, capsys):
    out = tmp_path / "r.html"
    assert main(["check", "--n", "6", "--fail", "rate-limit", "--verbose", "--html", str(out)]) == 1
    text = capsys.readouterr().out
    assert "sdk     retry" in text and "intake  accepted" in text
    assert "HTML report:" in text and out.exists()
    assert text.strip().splitlines()[-1].startswith("FAIL: 5/6 landed")


def test_cli_watch_json_ends_with_report_line(capsys):
    code = main(["watch", "--duration", "0.6", "--interval", "0.2", "--json"])
    lines = [json.loads(l) for l in capsys.readouterr().out.splitlines()]
    assert code == 0 and lines[-1]["type"] == "report" and lines[-1]["ok"] is True


def test_cli_rejects_unknown_fail_mode():
    with pytest.raises(SystemExit) as e:
        main(["check", "--fail", "nope"])
    assert e.value.code == 2

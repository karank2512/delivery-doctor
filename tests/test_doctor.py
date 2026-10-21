import json

import pytest

from delivery_doctor.__main__ import main
from delivery_doctor.doctor import classify, read_ledger, run_check
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
    assert stops(report) == ["send_failed_auth"] * 12
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

"""Hardening tests: input bounds, error exits, transport edge cases, redaction,
HTML safety, path handling, and process cleanup. Stdlib only; every server
here is a local fake on 127.0.0.1, nothing leaves the machine."""

import contextlib
import gzip
import http.client
import io
import json
import os
import re
import socket
import stat
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from delivery_doctor import doctor, safety
from delivery_doctor import watch as watch_mod
from delivery_doctor.doctor import (REPO_ROOT, WRONG_API_KEY, build_report, classify,
                                    query_intake, read_ledger, render_table, run_check,
                                    verdict_line)
from delivery_doctor.html_report import CSP, JS, render_html
from delivery_doctor.safety import DoctorError, get_json, validate_intake_url
from mock_raindrop.sdk import Client
from mock_raindrop.server import (DEFAULT_API_KEY, EXPIRED_API_KEY, MAX_GET_IDS, IntakeServer)
from tests.helpers import TempCase

EVENT = {"event": "x", "user_id": "u", "timestamp": 1.0}


@contextlib.contextmanager
def fake_server(handler_cls):
    """A throwaway loopback HTTP server running handler_cls."""
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        yield f"http://127.0.0.1:{srv.server_address[1]}", srv
    finally:
        srv.shutdown()
        srv.server_close()


class Quiet(BaseHTTPRequestHandler):
    seen = None  # list of (method, path, headers) shared per subclass

    def log_message(self, *a):
        pass

    def _drain(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n:
            self.rfile.read(n)

    def _send(self, code, body=b"", headers=()):
        self.send_response(code)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def recording_handler():
    class Rec(Quiet):
        seen = []

        def do_GET(self):
            self.seen.append(("GET", self.path, dict(self.headers)))
            self._send(200, b'{"entries": []}')

        do_POST = do_GET
    return Rec


class InputBoundsTests(TempCase):
    def test_check_rejects_out_of_range_n_instead_of_vacuous_pass(self):
        for n in ("0", "-3", "5001", "abc"):
            with self.subTest(n=n):
                code, out, err = self.run_cli(["check", "--n", n])
                self.assertEqual(code, 2)
                self.assertNotIn("PASS", out)

    def test_run_check_api_rejects_bad_n(self):
        for n in (0, -1, 10**6, 2.5, True):
            with self.subTest(n=n), self.assertRaises(DoctorError):
                run_check(n=n, workdir=str(self.tmp / "x"))

    def test_watch_rejects_bad_rate_interval_duration(self):
        for flag, value in (("--rate", "0"), ("--rate", "-1"), ("--interval", "0"),
                            ("--duration", "nan"), ("--duration", "1e9"), ("--interval", "inf")):
            with self.subTest(flag=flag, value=value):
                self.assertEqual(self.run_cli(["watch", flag, value])[0], 2)

    def test_watch_api_rejects_bad_bounds(self):
        with self.assertRaises(DoctorError):
            watch_mod.watch(rate=0, duration=1, out=io.StringIO(), workdir=str(self.tmp / "w"))

    def test_zero_events_is_never_a_pass(self):
        self.assertTrue(verdict_line([]).startswith("FAIL"))
        self.assertFalse(build_report("r", None, "http://x", [], [], "/tmp")["ok"])


class AttachModeSafetyTests(TempCase):
    def test_non_http_scheme_is_refused(self):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text("")
        code, out, err = self.run_cli(["watch", "--ledger", str(ledger), "--intake",
                                       "file:///etc/passwd", "--duration", "0.2"])
        self.assertEqual(code, 2)
        self.assertIn("http", err)

    def test_remote_intake_is_off_by_default_and_https_only_when_allowed(self):
        with self.assertRaises(DoctorError):
            validate_intake_url("http://intake.example.com")
        with self.assertRaises(DoctorError):
            validate_intake_url("http://intake.example.com", allow_remote=True)
        self.assertEqual(validate_intake_url("https://intake.example.com/", allow_remote=True),
                         "https://intake.example.com")
        for url in ("http://u:p@127.0.0.1:1", "http://127.0.0.1:1/?x=1", "http://127.0.0.1:99999",
                    "http:///nohost", ""):
            with self.subTest(url=url), self.assertRaises(DoctorError):
                validate_intake_url(url)
        for url in ("http://127.0.0.1:8787", "http://localhost:8787", "http://[::1]:8787"):
            validate_intake_url(url)

    def test_allow_remote_flag_requires_attach_mode(self):
        self.assertEqual(self.run_cli(["watch", "--allow-remote-intake"])[0], 2)

    def test_remote_intake_refused_by_cli_before_any_request(self):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text("")
        with mock.patch.object(doctor, "get_json", side_effect=AssertionError("network used")):
            code, _, err = self.run_cli(["watch", "--ledger", str(ledger), "--intake",
                                         "http://10.0.0.5:8787", "--duration", "0.2"])
        self.assertEqual(code, 2)
        self.assertIn("not loopback", err)

    def test_unreachable_intake_is_an_error_not_a_pass(self):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text(json.dumps({"event_id": "e1", "stage": "buffered", "t": 1}) + "\n")
        code, out, err = self.run_cli(["watch", "--ledger", str(ledger), "--intake",
                                       f"http://127.0.0.1:{doctor.free_port()}",
                                       "--duration", "0.2", "--interval", "0.1"])
        self.assertEqual(code, 3)
        self.assertIn("intake read-back failed", err)
        self.assertNotIn("PASS", out)

    def test_missing_ledger_is_an_error(self):
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv:
            code, _, err = self.run_cli(["watch", "--ledger", str(self.tmp / "nope.jsonl"),
                                         "--intake", srv.url, "--duration", "0.2",
                                         "--interval", "0.1"])
        self.assertEqual(code, 3)
        self.assertIn("never appeared", err)

    def test_garbage_ledger_tracks_nothing_and_fails(self):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text("127.0.0.1 localhost\n[1,2]\n" + json.dumps({"event_id": 5}) + "\n")
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv:
            code, out, _ = self.run_cli(["watch", "--ledger", str(ledger), "--intake", srv.url,
                                         "--duration", "0.2", "--interval", "0.1", "--json"])
        self.assertEqual(code, 1)
        final = json.loads(out.splitlines()[-1])
        self.assertIn("no events were tracked", final["verdict"])
        self.assertEqual(final["ledger_lines_skipped"], 3)

    def test_wrong_read_key_is_an_error_naming_the_flag(self):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text(json.dumps({"event_id": "e1", "stage": "buffered", "t": 1}) + "\n")
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv, \
                mock.patch.dict(os.environ, {"DELIVERY_DOCTOR_API_KEY": "rd_nope"}):
            code, _, err = self.run_cli(["watch", "--ledger", str(ledger), "--intake", srv.url,
                                         "--duration", "0.2", "--interval", "0.1"])
        self.assertEqual(code, 3)
        self.assertIn("401", err)
        self.assertIn("--api-key", err)


class ReadBackTransportTests(TempCase):
    def test_redirects_are_refused_so_the_key_is_not_forwarded(self):
        Rec = recording_handler()
        with fake_server(Rec) as (target, _):
            class Redirect(Quiet):
                def do_GET(self):
                    self._send(302, b"", [("Location", target + "/v1/events")])
            with fake_server(Redirect) as (url, _):
                with self.assertRaises(DoctorError) as ctx:
                    query_intake(url, ["e1"], api_key="secret-key")
        self.assertIn("302", str(ctx.exception))
        self.assertEqual(Rec.seen, [])

    def test_loopback_reads_and_sdk_sends_bypass_http_proxy(self):
        Proxy = recording_handler()
        with fake_server(Proxy) as (proxy_url, _), IntakeServer(str(self.tmp / "log.jsonl")) as srv, \
                mock.patch.dict(os.environ, {"http_proxy": proxy_url, "HTTP_PROXY": proxy_url,
                                             "no_proxy": "", "NO_PROXY": ""}):
            client = Client(srv.url, DEFAULT_API_KEY, flush_on_exit=False)
            client.track({"event_id": "e1", **EVENT})
            client.flush()
            found = query_intake(srv.url, ["e1"])
        self.assertEqual(Proxy.seen, [])
        self.assertEqual(found["e1"][0]["status"], "accepted")

    def test_oversized_and_malformed_read_back_bodies_are_errors(self):
        class Big(Quiet):
            def do_GET(self):
                self._send(200, b'{"entries": [' + b'1,' * 5000 + b'1]}')
        with fake_server(Big) as (url, _):
            with self.assertRaises(DoctorError):
                get_json(url + "/v1/events", {}, timeout=2, limit=1000)

        class NotJson(Quiet):
            def do_GET(self):
                self._send(200, b"<html>proxy login</html>")
        with fake_server(NotJson) as (url, _), self.assertRaises(DoctorError):
            query_intake(url, ["e1"])

    def test_read_back_ignores_entries_it_did_not_ask_for(self):
        class Noisy(Quiet):
            def do_GET(self):
                body = {"entries": [{"event_id": "other", "status": "accepted"}, "junk",
                                    {"event_id": "e1", "status": "weird"},
                                    {"event_id": "e1", "status": "accepted"}]}
                self._send(200, json.dumps(body).encode())
        with fake_server(Noisy) as (url, _):
            found = query_intake(url, ["e1"])
        self.assertEqual(list(found), ["e1"])
        self.assertEqual(len(found["e1"]), 1)

    def test_read_back_times_out_instead_of_hanging(self):
        class Slow(Quiet):
            def do_GET(self):
                time.sleep(1.5)
                self._send(200, b'{"entries": []}')
        with fake_server(Slow) as (url, _):
            start = time.time()
            with self.assertRaises(DoctorError):
                query_intake(url, ["e1"], timeout=0.3)
            self.assertLess(time.time() - start, 1.4)


class SdkNeverCrashesTests(TempCase):
    def _client(self, url, **kw):
        return Client(url, DEFAULT_API_KEY, flush_on_exit=False, backoff=0.01,
                      ledger_path=str(self.tmp / "l.jsonl"), **kw)

    def test_bogus_retry_after_values_do_not_crash_the_app(self):
        for value in ("-1", "nan", "inf", "soon", "-inf"):
            class RL(Quiet):
                def do_POST(self, v=value):
                    self._drain()
                    self._send(429, b"{}", [("Retry-After", v)])
            with self.subTest(value=value), fake_server(RL) as (url, _):
                client = self._client(url)
                client.track({"event_id": "e1", **EVENT})
                start = time.time()
                client.flush()  # must not raise
                self.assertLess(time.time() - start, 5)
                f = classify("e1", read_ledger(str(self.tmp / "l.jsonl"))["e1"], [])
                self.assertEqual(f["stop_point"], "rate_limited")
            os.unlink(self.tmp / "l.jsonl")

    def test_unusable_200_bodies_are_recorded_not_raised(self):
        for body in (b"ok", b"[1,2]", b'{"rejected": "nope"}'):
            class Bad(Quiet):
                def do_POST(self, b=body):
                    self._drain()
                    self._send(200, b)
            with self.subTest(body=body), fake_server(Bad) as (url, _):
                client = self._client(url)
                client.track({"event_id": "e1", **EVENT})
                client.flush()
                f = classify("e1", read_ledger(str(self.tmp / "l.jsonl"))["e1"], [])
                self.assertEqual(f["stop_point"], "send_failed_http")
                self.assertIn("unusable 200", f["detail"])
            os.unlink(self.tmp / "l.jsonl")

    def test_garbage_status_line_is_a_network_failure_not_a_crash(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        stop = threading.Event()

        def serve():
            srv.settimeout(0.2)
            while not stop.is_set():
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                with conn:
                    conn.recv(65536)
                    conn.sendall(b"NOT HTTP AT ALL\r\n\r\n")
        t = threading.Thread(target=serve, daemon=True)
        t.start()
        try:
            client = self._client(f"http://127.0.0.1:{srv.getsockname()[1]}")
            client.track({"event_id": "e1", **EVENT})
            client.flush()
        finally:
            stop.set()
            t.join()
            srv.close()
        f = classify("e1", read_ledger(str(self.tmp / "l.jsonl"))["e1"], [])
        self.assertEqual(f["stop_point"], "send_failed_network")

    def test_unwritable_ledger_does_not_crash_the_app(self):
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv:
            client = Client(srv.url, DEFAULT_API_KEY, flush_on_exit=False,
                            ledger_path=str(self.tmp / "missing-dir" / "l.jsonl"))
            self.assertTrue(client.track({"event_id": "e1", **EVENT}))
            client.flush()
            self.assertGreater(client.ledger_errors, 0)
            self.assertEqual(client.counters["acked"], 1)


class IntakeServerBoundsTests(TempCase):
    def _post(self, url, body, headers):
        host, port = url.split("//")[1].split(":")
        conn = http.client.HTTPConnection(host, int(port), timeout=5)
        try:
            conn.putrequest("POST", "/v1/events")
            for k, v in headers.items():
                conn.putheader(k, v)
            conn.endheaders()
            if body:
                conn.send(body)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()

    def test_binds_loopback_only_and_has_socket_timeout(self):
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv:
            self.assertEqual(srv.httpd.server_address[0], "127.0.0.1")
            self.assertTrue(srv.httpd.RequestHandlerClass.timeout)

    def test_bad_content_length_gzip_bomb_and_id_flood(self):
        key = {"X-Api-Key": DEFAULT_API_KEY}
        with IntakeServer(str(self.tmp / "log.jsonl")) as srv:
            self.assertEqual(self._post(srv.url, b"", {**key, "Content-Length": "abc"})[0], 411)
            self.assertEqual(self._post(srv.url, b"", {**key, "Content-Length": "999999999"})[0], 413)
            bomb = gzip.compress(b'{"events": [' + b" " * (20 * 1024 * 1024) + b"]}")
            status, _ = self._post(srv.url, bomb, {**key, "Content-Encoding": "gzip",
                                                   "Content-Length": str(len(bomb))})
            self.assertEqual(status, 413)
            ok = json.dumps({"events": "notalist"}).encode()
            self.assertEqual(self._post(srv.url, ok, {**key, "Content-Length": str(len(ok))})[0], 400)
            ids = ",".join(f"e{i}" for i in range(MAX_GET_IDS + 1))
            with self.assertRaises(DoctorError) as ctx:
                get_json(f"{srv.url}/v1/events?ids={ids}", key, timeout=2)
            self.assertIn("400", str(ctx.exception))


class LedgerParsingTests(TempCase):
    def test_malformed_lines_are_skipped_and_counted(self):
        path = self.tmp / "l.jsonl"
        good = {"event_id": "e1", "stage": "buffered", "t": 1}
        lines = [json.dumps(good), "not json", "[]", json.dumps({"event_id": 3, "stage": "acked"}),
                 json.dumps({"event_id": "a,b", "stage": "acked"}),
                 json.dumps({"event_id": "e\x1b[2J", "stage": "acked"}),
                 json.dumps({"event_id": "e2", "stage": "teleported"}),
                 json.dumps({"event_id": "x" * 500, "stage": "acked"})]
        path.write_bytes(("\n".join(lines) + "\n").encode() + b'{"event_id": "e3", "sta')
        stats = {}
        ledger = read_ledger(str(path), stats)
        self.assertEqual(list(ledger), ["e1"])
        self.assertEqual(stats["skipped"], 7)  # partial last line is not counted

    def test_directory_as_ledger_is_an_error(self):
        with self.assertRaises(DoctorError):
            read_ledger(str(self.tmp))


class FailureSurfacingTests(TempCase):
    def test_crashed_emitter_is_exit_3_not_a_delivery_verdict(self):
        crash = [sys.executable, "-c", "import sys; sys.stderr.write('boom rd_local_test_key'); sys.exit(4)"]
        with mock.patch.object(doctor, "emitter_cmd", return_value=crash):
            code, out, err = self.run_cli(["check", "--n", "3"])
        self.assertEqual(code, 3)
        self.assertIn("exited with code 4", err)
        self.assertIn("boom", err)
        self.assertNotIn(DEFAULT_API_KEY, err)

    def test_hung_emitter_times_out_cleanly(self):
        hang = [sys.executable, "-c", "import time; time.sleep(30)"]
        with mock.patch.object(doctor, "emitter_cmd", return_value=hang):
            start = time.time()
            with self.assertRaises(DoctorError):
                run_check(n=2, workdir=str(self.tmp / "w"), emitter_timeout=0.5)
            self.assertLess(time.time() - start, 10)

    def test_managed_watch_always_stops_its_emitter(self):
        procs = []
        real_popen = subprocess.Popen

        def spy(*a, **kw):
            p = real_popen(*a, **kw)
            procs.append(p)
            return p

        def broken(*a, **kw):
            raise DoctorError("intake read-back failed: simulated")

        with mock.patch.object(watch_mod.subprocess, "Popen", side_effect=spy), \
                mock.patch.object(watch_mod, "query_intake", side_effect=broken):
            with self.assertRaises(DoctorError):
                watch_mod.watch(duration=30, rate=5, interval=0.1, out=io.StringIO(),
                                workdir=str(self.tmp / "w"))
        self.assertEqual(len(procs), 1)
        self.assertIsNotNone(procs[0].poll(), "emitter left running")

    def test_crashed_managed_watch_emitter_is_an_error(self):
        crash = [sys.executable, "-c", "import sys; sys.exit(5)"]
        with mock.patch.object(watch_mod, "emitter_cmd", return_value=crash):
            with self.assertRaises(DoctorError) as ctx:
                watch_mod.watch(duration=1, interval=0.1, out=io.StringIO(),
                                workdir=str(self.tmp / "w"))
        self.assertIn("code 5", str(ctx.exception))


class ArtifactPrivacyTests(TempCase):
    def test_no_api_keys_in_report_json_or_html(self):
        for fail in ("auth", "expired-key", None):
            with self.subTest(fail=fail):
                out = self.tmp / f"{fail}.html"
                code, _, _ = self.run_cli(["check", "--n", "3", "--html", str(out)]
                                          + (["--fail", fail] if fail else []))
                self.assertEqual(code, 1 if fail else 0)
                run = max((self.tmp / "runs").iterdir(), key=lambda p: p.stat().st_mtime)
                for text in ((run / "report.json").read_text(), out.read_text()):
                    for key in (DEFAULT_API_KEY, EXPIRED_API_KEY, WRONG_API_KEY):
                        self.assertNotIn(key, text)
                self.assertNotIn(str(self.tmp), out.read_text())  # no local paths in shared HTML

    def test_secret_named_fields_and_url_credentials_are_redacted(self):
        report = build_report("r1", None, "http://user:pw@127.0.0.1:1", ["e1"], [classify(
            "e1", [{"event_id": "e1", "stage": "buffered", "t": 1.0}],
            [{"event_id": "e1", "status": "accepted",
              "event": {"properties": {"api_key": "sk-live-123", "Authorization": "Bearer x",
                                       "note": "token is my-secret-value"}}}])],
            str(self.tmp))
        doctor.write_report(report, str(self.tmp), secrets=("my-secret-value",))
        text = (self.tmp / "report.json").read_text()
        for leaked in ("sk-live-123", "Bearer x", "my-secret-value", "user:pw"):
            self.assertNotIn(leaked, text)
        self.assertIn("[redacted]", text)

    @unittest.skipIf(os.name != "posix", "POSIX permissions")
    def test_run_artifacts_are_owner_only(self):
        report = run_check(n=2, workdir=str(self.tmp / "runs" / "abcd1234"))
        self.assertEqual(stat.S_IMODE(os.stat(self.tmp / "runs" / "abcd1234").st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(os.stat(report["artifacts"]["report"]).st_mode), 0o600)

    def test_terminal_output_strips_control_characters(self):
        finding = classify("e1", [{"event_id": "e1", "stage": "buffered"}],
                           [{"event_id": "e1", "status": "rejected", "reason": "bad\x1b[2Jfield"}])
        table = render_table({"events": [finding]}, verbose=True)
        self.assertNotIn("\x1b", table)


class HtmlSafetyTests(TempCase):
    def hostile(self):
        return {
            "run_id": "<script>alert(1)</script>", "ok": "yes", "verdict": "<img src=x onerror=1>",
            "created_at": "nan", "total": "<b>", "landed": None, "attempts": {"x": 1},
            "duration_s": "<i>", "event_bytes": "<u>", "endpoint": "javascript:alert(1)",
            "counts": {"<svg onload=1>": "<b>", "landed": float("inf")},
            "fail_mode": "<x>", "fail_description": "</title><script>",
            "events": ["junk", {"event_id": "<e>", "stop_point": "<s>", "detail": "<d>",
                                "attempts": "<img src=x onerror=alert(1)>", "bytes": "<b>",
                                "evidence": {"ledger": [{"stage": "<st>", "t": "<t>"}, 7],
                                             "intake": "nope"}}],
        }

    def test_hostile_report_renders_without_markup_injection(self):
        page = render_html(self.hostile())
        body = page.split('id="report-data">')[0]
        self.assertIsNone(re.search(r"<(img|svg|e|s|d|u|x|t|st)[\s>]", body))
        self.assertEqual(body.count("<b>"), 6)  # only the six stat cards' own <b> tags
        self.assertNotIn("<script>alert", page)
        data = re.search(r'id="report-data">(.*?)</script>', page, re.S).group(1)
        self.assertNotIn("<", data)
        self.assertEqual(json.loads(data)["run_id"], "<script>alert(1)</script>")
        self.assertIn('class="verdict bad"', page)  # "yes" is not True

    def test_csp_blocks_network_and_pins_the_inline_script(self):
        page = render_html(self.hostile())
        self.assertIn("Content-Security-Policy", page)
        self.assertIn("default-src 'none'", CSP)
        scripts = re.findall(r"<script>(.*?)</script>", page, re.S)
        self.assertEqual(scripts, [JS])
        import base64
        import hashlib
        digest = base64.b64encode(hashlib.sha256(JS.encode()).digest()).decode()
        self.assertIn(f"'sha256-{digest}'", CSP)

    def test_report_from_hostile_file_via_cli(self):
        src = self.tmp / "evil.json"
        src.write_text(json.dumps(self.hostile()))
        out = self.tmp / "evil.html"
        code, stdout, _ = self.run_cli(["report", "--html", str(out), "--from", str(src)])
        self.assertEqual(code, 1)
        self.assertNotIn("<img src=x", out.read_text().split('id="report-data">')[0])
        self.assertNotIn("\x1b", stdout)


class ReportPathTests(TempCase):
    def test_from_run_id_works_from_any_cwd(self):
        report = run_check(n=2, workdir=str(self.tmp / "runs" / "abcd1234"))
        self.assertTrue(report["ok"])
        old = os.getcwd()
        os.chdir(self.tmp)
        try:
            code, _, _ = self.run_cli(["report", "--html", str(self.tmp / "o.html"),
                                       "--from", "abcd1234"])
        finally:
            os.chdir(old)
        self.assertEqual(code, 0)

    def test_bad_from_sources_are_usage_errors(self):
        (self.tmp / "notjson.json").write_text("{nope")
        (self.tmp / "other.json").write_text('{"hello": 1}')
        for source in ("../../etc", str(self.tmp / "notjson.json"), str(self.tmp / "other.json"),
                       "zzzzzzzz"):
            with self.subTest(source=source):
                code, _, err = self.run_cli(["report", "--html", str(self.tmp / "o.html"),
                                             "--from", source])
                self.assertEqual(code, 2)
                self.assertIn("error:", err)

    def test_bad_output_paths_fail_fast_before_running(self):
        with mock.patch.object(doctor, "emitter_cmd", side_effect=AssertionError("ran")):
            for path in (str(self.tmp / "no" / "such" / "dir.html"), str(self.tmp)):
                with self.subTest(path=path):
                    code, _, err = self.run_cli(["check", "--html", path])
                    self.assertEqual(code, 2)
                    self.assertIn("error:", err)

    def test_html_write_is_atomic_and_leaves_no_temp_files(self):
        out = self.tmp / "r.html"
        report = run_check(n=2, workdir=str(self.tmp / "w"))
        from delivery_doctor.html_report import write_html
        write_html(report, str(out))
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir()), ["r.html", "runs", "w"]
                         if (self.tmp / "runs").exists() else ["r.html", "w"])


class QuickstartSmokeTests(TempCase):
    """The README quickstart, as subprocesses, exactly as a user would type it."""

    def cli(self, *args):
        env = dict(os.environ, DELIVERY_DOCTOR_RUNS_DIR=str(self.tmp / "runs"))
        return subprocess.run([sys.executable, "-m", "delivery_doctor", *args], cwd=REPO_ROOT,
                              env=env, capture_output=True, text=True, timeout=120)

    def test_healthy_injected_and_usage_exit_codes(self):
        healthy = self.cli("check")
        self.assertEqual(healthy.returncode, 0, healthy.stderr)
        self.assertIn("PASS: 12/12 events landed and were read back.", healthy.stdout)
        broken = self.cli("check", "--fail", "buffer")
        self.assertEqual(broken.returncode, 1)
        self.assertIn("FAIL: 10/12 landed; 2 never sent by client (buffer drop)", broken.stdout)
        self.assertEqual(self.cli("check", "--n", "0").returncode, 2)
        self.assertEqual(self.cli("nonsense").returncode, 2)


class ReviewRegressionTests(TempCase):
    """Defects found by independent review of the first hardening pass."""

    def attach(self, ledger_lines, handler=None, extra=()):
        ledger = self.tmp / "l.jsonl"
        ledger.write_text("".join(l + "\n" for l in ledger_lines))
        ctx = fake_server(handler) if handler else IntakeServer(str(self.tmp / "log.jsonl"))
        with ctx as srv:
            url = srv[0] if handler else srv.url
            return self.run_cli(["watch", "--ledger", str(ledger), "--intake", url,
                                 "--duration", "0.2", "--interval", "0.1", *extra])

    def test_hostile_intake_responses_exit_3_not_traceback(self):
        def raw_reply(payload):
            class H(Quiet):
                def do_GET(self):
                    self._send(200, payload)
            return H

        class Garbage(Quiet):
            def do_GET(self):
                self.wfile.write(b"NOT HTTP\r\n\r\n")
        line = json.dumps({"event_id": "e1", "stage": "buffered", "t": 1})
        for name, handler in (("status line", Garbage),
                              ("deep json", raw_reply(b"[" * 100000)),
                              ("list id", raw_reply(b'{"entries":[{"event_id":["x"],"status":"accepted"}]}'))):
            with self.subTest(name):
                code, out, err = self.attach([line], handler)
                self.assertNotIn("Traceback", err)
                if name == "list id":
                    self.assertEqual(code, 1)  # bogus entry ignored; e1 just did not land
                else:
                    self.assertEqual(code, 3)

    def test_ledger_with_wrong_field_types_does_not_crash(self):
        lines = [json.dumps({"event_id": "e1", "stage": "failed"}),
                 json.dumps({"event_id": "e2", "stage": "failed", "kind": "connection"}),
                 json.dumps({"event_id": "e3", "stage": "buffered", "bytes": "big", "t": "x"}),
                 json.dumps({"event_id": "e4", "stage": "buffered", "t": 1e300}),
                 json.dumps({"event_id": "e5", "stage": "failed", "kind": "http", "status": "401",
                             "attempts": [1]})]
        out_html = self.tmp / "r.html"
        code, out, err = self.attach(lines, extra=["--json", "--html", str(out_html)])
        self.assertEqual(code, 1, err)
        self.assertNotIn("Traceback", err)
        self.assertTrue(out_html.exists())

    def test_overflowing_numbers_and_unreadable_reports(self):
        src = self.tmp / "big.json"
        src.write_text('{"run_id":"r","ok":false,"verdict":"v","total":1' + "0" * 400 + ',"events":'
                       '[{"event_id":"e","evidence":{"ledger":[{"t":1' + "0" * 400 + '}],"intake":[]}}]}')
        self.assertEqual(self.run_cli(["report", "--html", str(self.tmp / "a.html"),
                                       "--from", str(src)])[0], 1)
        deep = self.tmp / "deep.json"
        deep.write_text("[" * 100000)
        self.assertEqual(self.run_cli(["report", "--html", str(self.tmp / "b.html"),
                                       "--from", str(deep)])[0], 2)
        if os.name == "posix" and os.getuid() != 0:
            locked = self.tmp / "locked.json"
            locked.write_text("{}")
            locked.chmod(0)
            code, _, err = self.run_cli(["report", "--html", str(self.tmp / "c.html"),
                                         "--from", str(locked)])
            self.assertEqual(code, 2)
            self.assertIn("cannot read", err)

    def test_unexpected_exception_maps_to_exit_3(self):
        with mock.patch.object(doctor, "emitter_cmd", side_effect=RuntimeError("kaboom")):
            code, _, err = self.run_cli(["check", "--n", "2"])
        self.assertEqual(code, 3)
        self.assertIn("internal error", err)

    def test_interrupted_watch_exits_130_and_does_not_pass(self):
        calls = {"n": 0}
        real = watch_mod.snapshot

        def interrupt_once(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 2:
                raise KeyboardInterrupt
            return real(*a, **kw)

        with mock.patch.object(watch_mod, "snapshot", side_effect=interrupt_once):
            code, out, _ = self.run_cli(["watch", "--duration", "5", "--interval", "0.2"])
        self.assertEqual(code, 130)
        self.assertIn("INTERRUPTED", out)
        self.assertNotIn("\nPASS", out)

    def test_report_from_is_redacted_too(self):
        src = self.tmp / "leaky.json"
        src.write_text(json.dumps({"run_id": "r", "ok": False, "verdict": "v", "events": [
            {"event_id": "e", "detail": f"body had {DEFAULT_API_KEY}",
             "evidence": {"ledger": [], "intake": [{"event": {"api_key": "sk-live-AAA"}}]}}]}))
        out = self.tmp / "o.html"
        self.run_cli(["report", "--html", str(out), "--from", str(src)])
        page = out.read_text()
        self.assertNotIn("sk-live-AAA", page)
        self.assertNotIn(DEFAULT_API_KEY, page)

    def test_slow_drip_read_back_hits_overall_deadline(self):
        class Drip(Quiet):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", "1000")
                self.end_headers()
                try:
                    for _ in range(1000):
                        self.wfile.write(b" ")
                        self.wfile.flush()
                        time.sleep(0.2)
                except OSError:
                    pass
        with fake_server(Drip) as (url, _):
            start = time.time()
            with self.assertRaises(DoctorError):
                get_json(url, {}, timeout=0.5)
            self.assertLess(time.time() - start, 2.5)  # 2 * timeout + one read

    def test_error_text_masks_the_read_key(self):
        class Echo(Quiet):
            def do_GET(self):
                self._send(401, json.dumps({"error": "bad key " + self.headers["X-Api-Key"]}).encode())
        line = json.dumps({"event_id": "e1", "stage": "buffered", "t": 1})
        with mock.patch.dict(os.environ, {"DELIVERY_DOCTOR_API_KEY": "secret-READKEY-123"}):
            code, _, err = self.attach([line], Echo)
        self.assertEqual(code, 3)
        self.assertNotIn("secret-READKEY-123", err)
        self.assertIn("[redacted]", err)

    def test_key_cut_at_truncation_boundary_is_still_masked(self):
        noise = "x" * 995
        crash = [sys.executable, "-c",
                 f"import sys; sys.stderr.write('{DEFAULT_API_KEY}' + '{noise}'); sys.exit(4)"]
        with mock.patch.object(doctor, "emitter_cmd", return_value=crash):
            _, _, err = self.run_cli(["check", "--n", "2"])
        self.assertNotIn(DEFAULT_API_KEY[-6:] + "x", err)

    def test_redaction_key_names_and_short_secrets(self):
        out = safety.redact({"prompt_tokens": 5, "access_token": "a", "private_key": "b",
                             "auth": "c", "session": "d", "note": "e e e"}, secrets=("e",))
        self.assertEqual(out["prompt_tokens"], 5)
        self.assertEqual(out["note"], "e e e")  # 1-char "secret" is not substring-masked
        for k in ("access_token", "private_key", "auth", "session"):
            self.assertEqual(out[k], "[redacted]")

    def test_camel_case_and_word_split_secret_keys(self):
        for k in ("authToken", "accessToken", "idToken", "sessionToken", "session_id",
                  "auth_header", "credentials", "jwt", "X-Api-Key", "client-secret"):
            self.assertTrue(safety.is_secret_key(k), k)
        for k in ("prompt_tokens", "completion_tokens", "author", "tokenizer", "event_id"):
            self.assertFalse(safety.is_secret_key(k), k)

    def test_short_read_key_and_control_chars_masked_in_errors(self):
        class Echo(Quiet):
            def do_GET(self):
                self._send(401, json.dumps({"error": "bad key " + self.headers["X-Api-Key"]
                                            + " \x1b[2J"}).encode())
        line = json.dumps({"event_id": "e1", "stage": "buffered", "t": 1})
        with mock.patch.dict(os.environ, {"DELIVERY_DOCTOR_API_KEY": "abc1234"}):
            code, _, err = self.attach([line], Echo)
        self.assertEqual(code, 3)
        self.assertNotIn("abc1234", err)
        self.assertNotIn("\x1b", err)

    def test_internal_error_text_is_redacted(self):
        boom = RuntimeError(f"exploded near {DEFAULT_API_KEY}\x1b[2J")
        with mock.patch.object(doctor, "emitter_cmd", side_effect=boom):
            code, _, err = self.run_cli(["check", "--n", "2"])
        self.assertEqual(code, 3)
        self.assertNotIn(DEFAULT_API_KEY, err)
        self.assertNotIn("\x1b", err)

    def test_watch_volume_cap_is_a_usage_error(self):
        self.assertEqual(self.run_cli(["watch", "--rate", "1000", "--duration", "3600"])[0], 2)

    def test_ipv4_mapped_loopback_is_consistent(self):
        self.assertTrue(safety.is_loopback_host("::ffff:127.0.0.1"))
        self.assertFalse(safety.is_loopback_host("::ffff:10.0.0.1"))


if __name__ == "__main__":
    unittest.main()

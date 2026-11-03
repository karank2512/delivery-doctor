"""Guard rails shared by the CLI, check, watch, and the HTML report.

- intake URLs: http(s) only, loopback only unless the caller opts in, and a
  remote intake must use https so the API key never crosses a network in clear
- HTTP reads: no proxies for loopback, no redirects (urllib would forward the
  X-Api-Key header to wherever a redirect points), bounded timeouts and bodies
- redaction: secret-looking keys and the configured API key are masked before
  anything is written to report.json, the HTML report, or the terminal
- artifacts: written atomically, run directories created owner-only
"""

import http.client
import ipaddress
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

MAX_RESPONSE_BYTES = 32 * 1024 * 1024  # intake read-back body
MAX_REPORT_BYTES = 64 * 1024 * 1024  # report.json accepted by `report --from`
RUN_ID_RE = re.compile(r"^[0-9a-f]{8}$")
REDACTED = "[redacted]"
# Field names whose values are masked. Names are first split into words
# (camelCase and kebab-case become snake_case), then matched word by word, so
# authToken / access-token / session_id are caught while prompt_tokens is not.
SECRET_KEY_RE = re.compile(r"(api_?key|access_?key|private_?key|secret|passw(or)?d|authorization|"
                           r"cookie|credentials?|(^|_)(auth|token|session|jwt)(_|$))")
MIN_SECRET_LEN = 8  # shorter "secrets" would rewrite ordinary text, so they are not substring-masked
CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


class DoctorError(Exception):
    """The doctor itself could not finish (bad input, intake unreadable, emitter stuck).

    Distinct from "events were lost": the CLI maps it to exit code 3 so a
    broken harness is never reported as a delivery verdict.
    """


def is_loopback_host(host):
    if not host:
        return False
    if host.lower() == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return False
    mapped = getattr(ip, "ipv4_mapped", None)  # same answer on every Python version
    return (mapped or ip).is_loopback


def validate_intake_url(url, allow_remote=False):
    """Return a normalized base URL or raise DoctorError.

    Loopback http(s) is always allowed. Anything else needs allow_remote=True
    and https. Credentials in the URL, query strings, and fragments are refused.
    """
    if not isinstance(url, str) or not url or len(url) > 2048:
        raise DoctorError("intake URL is empty or too long")
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise DoctorError(f"intake URL must be http:// or https://, got {parts.scheme or 'none'}://")
    if parts.username or parts.password:
        raise DoctorError("intake URL must not embed credentials; use --api-key or "
                          "DELIVERY_DOCTOR_API_KEY")
    if parts.query or parts.fragment:
        raise DoctorError("intake URL must be a base URL without query or fragment")
    try:
        host, port = parts.hostname, parts.port
    except ValueError as e:
        raise DoctorError(f"intake URL has an invalid port: {e}")
    if not host:
        raise DoctorError("intake URL has no host")
    if not is_loopback_host(host):
        if not allow_remote:
            raise DoctorError(
                f"intake host {host!r} is not loopback; remote intakes are off by default. "
                "Pass --allow-remote-intake (https only) if you really mean it.")
        if parts.scheme != "https":
            raise DoctorError("a remote intake must use https so the API key is not sent in clear")
    netloc = parts.netloc
    return urllib.parse.urlunsplit((parts.scheme, netloc, parts.path.rstrip("/"), "", ""))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code,
                                     f"redirect to {newurl} refused (would forward the API key)",
                                     headers, fp)


def build_opener(url):
    """Opener with redirects off; proxies off for loopback targets."""
    handlers = [_NoRedirect()]
    if is_loopback_host(urllib.parse.urlsplit(url).hostname):
        handlers.append(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(*handlers)


def read_bounded(resp, limit=MAX_RESPONSE_BYTES, deadline=None):
    """Read at most limit bytes, in chunks, giving up once deadline (monotonic) passes.

    Each chunk read is still bounded by the socket timeout, so the worst case
    is deadline plus one socket timeout.
    """
    chunks, size = [], 0
    while True:
        if deadline is not None and time.monotonic() > deadline:
            raise DoctorError("intake response took too long to arrive")
        # read1 returns what has arrived; read(n) would block until n bytes, so a
        # server dripping one byte at a time could hold it far past the deadline
        reader = getattr(resp, "read1", resp.read)
        chunk = reader(min(65536, limit + 1 - size))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        size += len(chunk)
        if size > limit:
            raise DoctorError(f"intake response is larger than {limit} bytes")


def get_json(url, headers, timeout, limit=MAX_RESPONSE_BYTES, secrets=()):
    """GET a JSON document with the guards above. Raises DoctorError on any failure.

    timeout is the per-read socket timeout; the whole request must also finish
    within 2 * timeout. Error text is redacted with secrets before it is raised.
    """
    def fail(msg):
        text = safe_text(f"intake read-back failed: {msg}", 1000)
        return DoctorError(redact(text, secrets, min_len=1))

    req = urllib.request.Request(url, headers=headers)
    deadline = time.monotonic() + 2 * timeout
    try:
        with build_opener(url).open(req, timeout=timeout) as resp:
            body = read_bounded(resp, limit, deadline)
    except urllib.error.HTTPError as e:
        try:
            detail = e.read(500).decode("utf-8", "replace") if e.fp else ""
        except (OSError, http.client.HTTPException):
            detail = ""
        hint = " (check --api-key)" if e.code in (401, 403) else ""
        raise fail(f"HTTP {e.code}{hint} {safe_text(detail)}".rstrip())
    except (urllib.error.URLError, OSError, ValueError, http.client.HTTPException) as e:
        raise fail(getattr(e, "reason", None) or f"{type(e).__name__}: {e}")
    try:
        return json.loads(body)
    except (ValueError, RecursionError):
        raise fail("response is not usable JSON")


def safe_text(value, limit=None):
    """Printable single-line-safe text: control characters (terminal escapes) removed."""
    text = CONTROL_RE.sub("?", str(value))
    if limit and len(text) > limit:
        text = text[:limit] + f"...[{len(text) - limit} more chars]"
    return text


def is_secret_key(name):
    if not isinstance(name, str):
        return False
    words = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).replace("-", "_").lower()
    return bool(SECRET_KEY_RE.search(words))


def redact(value, secrets=(), _depth=0, min_len=MIN_SECRET_LEN):
    """Copy of value with secret-named keys masked and known secret strings replaced.

    In bulk data, secrets shorter than min_len are not substring-masked (a
    1-char "key" would rewrite everything). Error messages pass min_len=1.
    """
    secrets = tuple(s for s in secrets if isinstance(s, str) and len(s) >= max(min_len, 1))
    if _depth > 50:
        return REDACTED
    if isinstance(value, dict):
        return {k: (REDACTED if is_secret_key(k)
                    else redact(v, secrets, _depth + 1, min_len)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, secrets, _depth + 1, min_len) for v in value]
    if isinstance(value, str):
        for s in secrets:
            value = value.replace(s, REDACTED)
        return value
    return value


def redact_url(url):
    """Drop userinfo from a URL for display."""
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return REDACTED
    if parts.username or parts.password:
        netloc = parts.hostname or ""
        if parts.port:
            netloc += f":{parts.port}"
        return urllib.parse.urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return url


def private_makedirs(path):
    """mkdir -p with owner-only permissions on the directories it creates."""
    os.makedirs(path, mode=0o700, exist_ok=True)


def check_output_path(path):
    """Validate a user-chosen output file path; return its absolute form."""
    if not path or not isinstance(path, str):
        raise DoctorError("output path is empty")
    path = os.path.abspath(path)
    if os.path.isdir(path):
        raise DoctorError(f"output path {path} is a directory")
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        raise DoctorError(f"output directory {parent} does not exist")
    return path


def atomic_write(path, text):
    """Write text to path via a temp file + rename, so readers never see half a file."""
    path = check_output_path(path)
    fd, tmp = tempfile.mkstemp(prefix=".dd-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(tmp, 0o600 if os.path.basename(path) == "report.json" else 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path

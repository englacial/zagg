"""Tests for the orchestrator Earthdata auth helpers (issue #137).

The login retry/backoff wraps ``earthaccess.login()`` so a transient network
failure on the orchestrator/CI (an IPv6 no-route ``OSError: [Errno 101]`` or a
``requests.exceptions.ConnectionError``, which subclasses ``OSError``) self-heals
instead of failing the whole run on the first attempt.
"""

import pytest

import zagg.auth as auth


class _FakeAuth:
    """Stand-in for the object ``earthaccess.login()`` returns."""

    def __init__(self):
        self.token = {"access_token": "tok-123"}

    def get_s3_credentials(self, daac):
        return {"accessKeyId": "AK", "secretAccessKey": "SK", "daac": daac}


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Never actually sleep during the backoff (keeps the tests instant)."""
    monkeypatch.setattr(auth.time, "sleep", lambda _s: None)


def _login_seq(monkeypatch, *outcomes):
    """Patch ``earthaccess.login`` to yield ``outcomes`` (exception -> raise,
    else -> return) in order, recording the call count."""
    calls = {"n": 0}

    def fake_login():
        calls["n"] += 1
        result = outcomes[calls["n"] - 1]
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr(auth.earthaccess, "login", fake_login)
    return calls


def test_login_retries_then_succeeds(monkeypatch):
    """A transient OSError on the first attempt is retried and the second wins."""
    fake = _FakeAuth()
    calls = _login_seq(monkeypatch, OSError(101, "Network is unreachable"), fake)
    sleeps = []
    monkeypatch.setattr(auth.time, "sleep", lambda s: sleeps.append(s))

    assert auth._login_with_retry() is fake
    assert calls["n"] == 2  # failed once, then succeeded
    assert sleeps == [auth._LOGIN_BACKOFF_BASE_S]  # one backoff, base delay


def test_login_gives_up_after_max_attempts(monkeypatch):
    """All attempts failing -> the last exception propagates, after N tries."""
    err = OSError(101, "Network is unreachable")
    calls = _login_seq(monkeypatch, *([err] * auth._LOGIN_MAX_ATTEMPTS))
    sleeps = []
    monkeypatch.setattr(auth.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(OSError, match="Network is unreachable"):
        auth._login_with_retry()
    assert calls["n"] == auth._LOGIN_MAX_ATTEMPTS
    # Backoff between attempts only (not after the final failure): 2s, 4s, ...
    assert sleeps == [
        auth._LOGIN_BACKOFF_BASE_S * (2**i) for i in range(auth._LOGIN_MAX_ATTEMPTS - 1)
    ]


def test_login_does_not_retry_non_oserror(monkeypatch):
    """A non-transient error (e.g. bad credentials) is not retried -- fail fast."""
    calls = _login_seq(monkeypatch, ValueError("bad token"))
    with pytest.raises(ValueError, match="bad token"):
        auth._login_with_retry()
    assert calls["n"] == 1  # no retry


def test_connection_error_is_retried(monkeypatch):
    """``requests.exceptions.ConnectionError`` (an OSError subclass) is retried too."""
    requests_exceptions = pytest.importorskip("requests.exceptions")
    assert issubclass(requests_exceptions.ConnectionError, OSError)
    fake = _FakeAuth()
    calls = _login_seq(monkeypatch, requests_exceptions.ConnectionError("no route"), fake)
    assert auth._login_with_retry() is fake
    assert calls["n"] == 2


def test_get_edl_token_uses_retry(monkeypatch):
    """The public HTTPS-token helper routes through the retry wrapper."""
    fake = _FakeAuth()
    _login_seq(monkeypatch, OSError(101, "unreachable"), fake)
    assert auth.get_edl_token() == "tok-123"


def test_get_nsidc_s3_credentials_uses_retry(monkeypatch):
    """The public S3-creds helper routes through the retry wrapper."""
    fake = _FakeAuth()
    _login_seq(monkeypatch, OSError(101, "unreachable"), fake)
    creds = auth.get_nsidc_s3_credentials()
    assert creds["accessKeyId"] == "AK"
    assert creds["daac"] == "NSIDC"  # login().get_s3_credentials(daac="NSIDC")

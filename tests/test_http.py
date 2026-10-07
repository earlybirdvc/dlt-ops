"""Tests for the raise_for_status response hook."""

import pytest
import requests
from requests import HTTPError

from dlt_ops import raise_for_status

# The error-body limit these tests pin. The `show_error_body` fixture exports it,
# so no assertion depends on dlt's own default.
BODY_LIMIT = 64


def _response(
    status: int, body: str = "", *, url: str = "https://api.example.com/v1/things?key=SECRET"
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}.get(status, "")
    response._content = body.encode("utf-8")
    response.url = url
    response.request = requests.Request(method="POST", url=url).prepare()
    return response


@pytest.fixture
def show_error_body(monkeypatch: pytest.MonkeyPatch):
    """Turn dlt's error-body setting on, as a project's [runtime] table would.

    `resolve_configuration` reads the environment on every call, so setting the
    variable is enough — there is no resolved-config cache to reset.
    """
    monkeypatch.setenv("RUNTIME__HTTP_SHOW_ERROR_BODY", "true")
    monkeypatch.setenv("RUNTIME__HTTP_MAX_ERROR_BODY_LENGTH", str(BODY_LIMIT))


@pytest.mark.parametrize("status", [200, 201, 204, 302, 399])
def test_non_error_statuses_pass_through(status):
    assert raise_for_status(_response(status, '{"items": []}')) is None


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500, 503])
def test_error_statuses_raise(status):
    with pytest.raises(HTTPError):
        raise_for_status(_response(status, "boom"))


def test_client_vs_server_error_labelling():
    with pytest.raises(HTTPError, match="Client Error"):
        raise_for_status(_response(404, "gone"))
    with pytest.raises(HTTPError, match="Server Error"):
        raise_for_status(_response(500, "oops"))


def test_response_is_attached_to_the_error():
    response = _response(400, "nope")
    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(response)
    assert excinfo.value.response is response


def test_query_string_is_not_echoed_into_the_error(show_error_body):
    """Providers that authenticate by query param must not leak the key into logs."""
    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(401, "unauthorized", url="https://api.example.com/v1/things?api_key=SECRET"))
    message = str(excinfo.value)
    assert "SECRET" not in message
    assert "https://api.example.com/v1/things" in message


def test_userinfo_is_not_echoed_into_the_error(show_error_body):
    """Credentials in the URL are dropped, not masked."""
    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(401, "unauthorized", url="https://user:pa55word@api.example.com/v1/things"))
    message = str(excinfo.value)
    assert "pa55word" not in message
    assert "user" not in message
    assert "https://api.example.com/v1/things" in message


def test_accepts_extra_hook_args():
    """requests invokes response hooks as hook(response, *args, **kwargs)."""
    assert raise_for_status(_response(200), object(), key="value") is None


def test_empty_body_still_raises(show_error_body):
    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(400, ""))
    message = str(excinfo.value)
    assert "400 Client Error" in message
    assert "Response:" not in message


def test_body_is_omitted_under_dlt_defaults():
    """dlt ships http_show_error_body off, and the hook honours that."""
    body = '{"error": true, "message": "Invalid field `some_field` provided"}'

    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(400, body))

    message = str(excinfo.value)
    assert "400 Client Error: Bad Request for url: https://api.example.com/v1/things" in message
    assert "Response:" not in message
    assert "some_field" not in message


def test_provider_reason_surfaces_when_error_body_is_enabled(show_error_body):
    """The provider's own message appears nowhere but the response body."""
    body = '{"error": true, "message": "Invalid field"}'
    assert len(body) <= BODY_LIMIT, "the body must fit the limit — truncation is another test's subject"

    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(400, body))

    message = str(excinfo.value)
    assert "400 Client Error" in message
    assert f"Response: {body}" in message
    assert "truncated" not in message


def test_long_body_is_truncated_at_the_configured_length(show_error_body):
    with pytest.raises(HTTPError) as excinfo:
        raise_for_status(_response(400, "x" * (BODY_LIMIT * 4)))

    message = str(excinfo.value)
    assert "… (truncated)" in message
    assert f"Response: {'x' * BODY_LIMIT}… (truncated)" in message

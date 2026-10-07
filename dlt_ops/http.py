"""HTTP response hooks for dlt REST sources.

``RESTClient`` installs its own error-raising response handler only when the
caller passes no ``response`` hook, and it builds its session with
``raise_for_status=False``. A resource that passes any ``response`` hook of its
own removes the only thing that turns a 4xx/5xx into an exception, and the HTTP
error can then pass for an ordinary empty page. A custom session does not
restore the behaviour either: ``RESTClient`` honours ``raise_for_status`` only
on sessions that carry that attribute.

Prepend :func:`raise_for_status` to every custom ``response`` hook list, so the
error surfaces before any other hook interprets the body::

    hooks={"response": [raise_for_status, my_progress_hook]}

Ordering matters: raising first stops a logging hook from reporting a
"0 records" page for what is actually an error response. The
``response_hook_raises_http_errors`` rule checks this ordering statically.

Endpoints where a specific status legitimately means "nothing here" — 404 on a
deleted entity, 410 on an expired one — should not use this hook. Issue a
direct request and handle those statuses explicitly instead.
"""

from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import RuntimeConfiguration
from requests import HTTPError, Response

__all__ = ["raise_for_status"]


def _sanitize_url(url: str) -> str:
    """Drop query, fragment and userinfo.

    Dropped rather than masked: providers that authenticate by query parameter
    would otherwise leak the key into every error log line.
    """
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def raise_for_status(response: Response, *args: Any, **kwargs: Any) -> None:
    """Raise ``HTTPError`` on a 4xx/5xx response. Drop-in ``requests`` response hook.

    Mirrors the message shape of dlt's own internal handler — status, reason,
    sanitized url, optional truncated body — so logs read the same whether or
    not a source overrides the hook list. The body is governed by dlt's
    ``RuntimeConfiguration``: ``http_show_error_body`` (off by default) decides
    whether it appears at all, and ``http_max_error_body_length`` caps it.

    ``requests.Response.raise_for_status`` is deliberately not reused: it omits
    the response body, which is where providers put the actual reason.
    """
    if response.status_code < 400:
        return

    config = resolve_configuration(RuntimeConfiguration())

    reason = response.reason
    if isinstance(reason, bytes):
        try:
            reason = reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = reason.decode("iso-8859-1")

    error_type = "Client" if response.status_code < 500 else "Server"
    msg = f"{response.status_code} {error_type} Error: {reason} for url: {_sanitize_url(response.url or '')}"

    if config.http_show_error_body and response.text:
        body = response.text
        if len(body) > config.http_max_error_body_length:
            body = body[: config.http_max_error_body_length] + "… (truncated)"
        msg += f"\nResponse: {body}"

    raise HTTPError(msg, response=response)

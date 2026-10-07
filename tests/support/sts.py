"""A loopback STS that answers ``AssumeRole``, for the assume-role source, marketlake #737.

``bucket_credentials: assume_role`` and the token parameter's put sign with credentials
the command key gets from STS. The STS client is private to
``lake.aws_session.build_client``, and each botocore client has its own event emitter, so
a ``before-send`` hook on the S3 or SSM client cannot answer the assume. This server
does, on ``127.0.0.1``, which the suite's network guard allows. A test reaches it through
the ``sts`` fixture in ``tests/conftest.py``, which points ``aws_session.STS_ENDPOINT_URL``
at it. The fixture is applied test by test or file by file, never to the whole suite, so
a test can still show that no STS call happens.

The server answers each ``AssumeRole`` with a numbered ``ASIA`` key, a secret and a
session token, so a test can tell the first session from the second. Its expiry is an
hour away unless a test sets ``expires_in`` or ``expiration`` itself. A test told to
refuse sets ``status`` and ``code``, and the refusal's message names the fixture's
account id and principal ARN, the way STS's own does, so a test can show neither reaches
an output. ``script`` queues one-off answers ahead of the standing one. Every request is
recorded with its headers and its form fields.

Nothing here is a real account, key or role.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

# The fixture's account and the principal STS names in a refusal. AWS's documentation
# account id, so it is no one's.
ACCOUNT_ID = "111122223333"
PRINCIPAL_ARN = f"arn:aws:iam::{ACCOUNT_ID}:user/marketlake-command"
BUCKET_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/marketlake-backup"
TOKEN_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/marketlake-token-writer"

# The command key, the one key the laptop holds.
COMMAND_KEY_ID = "AKIDCOMMANDKEY"
COMMAND_SECRET = "command-secret-value"


def session_key_id(number: int) -> str:
    """The access key id of the ``number``-th session the server hands out, from 1."""
    return f"ASIASESSION{number:04d}"


def session_token(number: int) -> str:
    """The session token of the ``number``-th session the server hands out, from 1."""
    return f"session-token-{number:04d}"


@dataclass(frozen=True)
class Sent:
    """One request the server received: its headers and its form fields."""

    headers: dict[str, str]
    params: dict[str, str]

    @property
    def authorization(self) -> str:
        return self.headers.get("Authorization", "")

    @property
    def key_id(self) -> str:
        """The access key id the request was signed with."""
        return self.authorization.split("Credential=", 1)[1].split("/", 1)[0]

    @property
    def scope_region(self) -> str:
        """The region in the signature's credential scope."""
        return self.authorization.split("Credential=", 1)[1].split("/")[2]


@dataclass
class Answer:
    """One answer: a status and an error code, or credentials with this expiry text."""

    status: int = 200
    code: str | None = None
    expiration: str | None = None


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class StsServer:
    """The loopback STS. ``reset`` puts every setting back between tests."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the stdlib's name
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8")
                params = {key: values[0] for key, values in parse_qs(body).items()}
                status, text = server._respond(Sent(dict(self.headers), params))
                data = text.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/xml")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args) -> None:
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def reset(self) -> None:
        with self.lock:
            self.requests: list[Sent] = []
            self.status = 200
            self.code: str | None = None
            self.expires_in = timedelta(hours=1)
            self.expiration: str | None = None
            self.script: deque[Answer] = deque()
            self.issued = 0

    def refuse(self, code: str, status: int = 403) -> None:
        """Answer every later request with this error."""
        self.status, self.code = status, code

    def _respond(self, sent: Sent) -> tuple[int, str]:
        with self.lock:
            self.requests.append(sent)
            if self.script:
                answer = self.script.popleft()
            else:
                answer = Answer(self.status, self.code, self.expiration)
            if answer.status != 200:
                return answer.status, _error(answer.code or "InternalFailure")
            self.issued += 1
            number = self.issued
            expiration = answer.expiration or _iso(datetime.now(UTC) + self.expires_in)
        return 200, _credentials(number, expiration, sent.params.get("RoleSessionName", ""))

    def __enter__(self) -> StsServer:
        threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def _credentials(number: int, expiration: str, session: str) -> str:
    return (
        '<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<AssumeRoleResult><Credentials>"
        f"<AccessKeyId>{session_key_id(number)}</AccessKeyId>"
        f"<SecretAccessKey>session-secret-{number:04d}</SecretAccessKey>"
        f"<SessionToken>{session_token(number)}</SessionToken>"
        f"<Expiration>{expiration}</Expiration>"
        "</Credentials><AssumedRoleUser>"
        f"<AssumedRoleId>AROAFIXTURE:{session}</AssumedRoleId>"
        f"<Arn>arn:aws:sts::{ACCOUNT_ID}:assumed-role/marketlake-backup/{session}</Arn>"
        "</AssumedRoleUser></AssumeRoleResult>"
        "<ResponseMetadata><RequestId>fixture</RequestId></ResponseMetadata>"
        "</AssumeRoleResponse>"
    )


def _error(code: str) -> str:
    # STS's own refusal names the caller and the role, which is what must never surface.
    message = (
        f"User: {PRINCIPAL_ARN} is not authorized to perform: sts:AssumeRole on resource: "
        f"{BUCKET_ROLE_ARN}"
    )
    return (
        '<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        f"<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>"
        "<RequestId>fixture</RequestId></ErrorResponse>"
    )

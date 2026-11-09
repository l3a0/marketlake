"""Print when a Schwab token.json was minted and when its refresh token dies.

The refresh token dies seven days after the browser login that minted it, and
``creation_timestamp`` in token.json is that login's epoch second. This prints
the mint time, the expiry and the hours left, and nothing else from the file.
The token itself is a full brokerage credential, so no byte of it is printed.

It uses the standard library alone, so it also runs on the VM fed through ssh:

    ssh ... python3 - < token_mint.py

Usage: python3 token_mint.py [path]. The default path is
~/.config/marketlake/token.json.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

LIFETIME = timedelta(days=7)
EASTERN = ZoneInfo("America/New_York")
# timezone.utc rather than datetime.UTC, which arrived in Python 3.11. The laptop's
# system python3 can be 3.9.
UTC_ZONE = timezone.utc  # noqa: UP017


def main(argv: list[str]) -> int:
    path = Path(argv[1]) if len(argv) > 1 else Path.home() / ".config/marketlake/token.json"
    try:
        stamp = json.loads(path.read_text(encoding="utf-8"))["creation_timestamp"]
        minted = datetime.fromtimestamp(int(stamp), UTC_ZONE)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        # The type name alone: a JSON error's message can quote the file's text.
        print(f"token_mint: cannot read the mint time of {path}: {type(exc).__name__}")
        return 1
    expires = minted + LIFETIME
    left = (expires - datetime.now(UTC_ZONE)).total_seconds() / 3600
    fmt = "%Y-%m-%d %H:%M %Z"
    print(f"token_mint: {path}")
    print(f"  minted:  {minted:{fmt}} ({minted.astimezone(EASTERN):{fmt}})")
    print(f"  expires: {expires:{fmt}} ({expires.astimezone(EASTERN):{fmt}})")
    print(f"  left:    {left:.1f} hours" if left > 0 else "  left:    expired")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

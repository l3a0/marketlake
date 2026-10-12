"""Reads the build board page's icon link the way a browser does (#884).

A search of the raw markup also finds a link a browser never loads, one written inside
an HTML comment or a ``noscript`` block. So the page's head is parsed with the standard
library's ``html.parser``. The parser hands a comment to ``handle_comment`` rather than
to ``handle_starttag``, so a link inside one is never seen, and a link inside
``noscript`` is skipped by counting how deep the parser is inside that element. Only
links inside ``head`` count.
"""

from __future__ import annotations

from html.parser import HTMLParser


class _HeadLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_head = False
        self.noscript = 0
        self.links: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "head":
            self.in_head = True
        elif tag == "noscript":
            self.noscript += 1
        elif tag == "link" and self.in_head and not self.noscript:
            self.links.append({name: value or "" for name, value in attrs})

    def handle_endtag(self, tag: str) -> None:
        if tag == "head":
            self.in_head = False
        elif tag == "noscript" and self.noscript:
            self.noscript -= 1


def icon_links(markup: str) -> list[dict[str, str]]:
    """Return the attributes of each ``link`` in the head whose ``rel`` names ``icon``."""
    parser = _HeadLinks()
    parser.feed(markup)
    parser.close()
    return [link for link in parser.links if "icon" in link.get("rel", "").lower().split()]

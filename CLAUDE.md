# CLAUDE.md — Marketlake

Marketlake is a capture-first market data lake. It records full option chains and equity quotes at one-minute cadence from the Schwab Trader API. The design doc at [docs/design.md](docs/design.md) is the source of truth. Read it before proposing any change. Status: design complete, no code yet. Build order and slices are defined at the end of the doc.

## Writing style (owner directive, 2026-08-26)

Use short, complete sentences. One idea per sentence. Avoid em dashes and semicolons. Break a long sentence into two. This applies to every prose surface: this file, the design doc, commit messages, PR bodies, and chat replies. Use plain language. Give the intuition first. Put the precise rule right behind it.

**Explain every concept on first use.** This covers coined vocabulary (surface, segment, gap-marking), borrowed tools (`flock`, `pmset`, Arrow IPC), and behaviors (sleep-missed jobs, dead-man checks). The test: if a reader must ask "what is X," the doc failed at X's first appearance. Add the gloss there, not in a glossary. The design doc's existing glosses are the pattern to follow.

## The design doc is review-hardened

The doc survived three adversarial review batteries. That was 89+ agents and 58 verified findings, with zero findings refuted. It also survived the owner's own Socratic passes. Respect two conventions it carries:

- **The considered-and-rejected register.** Cut machinery is pinned in the doc with its rationale. Examples: backfill, the streamer, the Saturday OI wake, the entire morning-OI job, extra-account quota farming, `pmset disablesleep`, the pause API, static dashboard rendering. Do not re-propose these. When something new is cut, pin it the same way.
- **Pinned vocabulary.** Terms like *surface*, *segment*, *canonical*, *spot_close*, *comparable set*, and *capture_start* have exact definitions in the doc. Reuse them. Do not coin synonyms.

One lesson from the review campaign is worth keeping in view. Reviews armor what exists. They rarely ask whether it should exist. The morning-OI job survived two hardening rounds before one first-principles question deleted it. Ask "why is this needed" before "is this correct."

## Markdown hygiene

Every `.md` file must pass markdownlint. The rules that bite most: use real headings, never a bold line as a heading (MD036). No trailing whitespace (MD009). No stacked blank lines (MD012). End the file with exactly one newline (MD047). Table delimiter rows use single-space padding, so `| --- |` and never `|---|` (MD060). Escape an "approximately" tilde in prose as `\~`. Code fences are exempt. After any edit, sweep:

```bash
rg -n --pcre2 '(?<![\s~\\`<])~' *.md docs/*.md
rg -n '\|-{1,}\|' *.md docs/*.md
```

When a heading changes, verify the Contents anchors still resolve.

## Secrets and machine paths

This repo is public. Tracked files never carry secrets or machine-specific paths. The Schwab token lives at `~/.config/marketlake/token.json`. Machine-local config lives at `~/.config/marketlake/config.yaml`. Healthchecks ping URLs and the ntfy topic are secrets. The design doc's Configuration section states the full rules. Sweep for leaks before any publish.

## Committing

Do not commit or push without explicit per-change review. Each commit instruction authorizes exactly the changes summarized in the immediately prior turn. Once that commit lands, the authorization is spent. The next change starts fresh. The pattern: make the change, summarize it, wait, then commit exactly what was summarized. If new changes appear between summary and commit, re-summarize and re-confirm. Direct commits to `main` are current practice for this doc-only repo, each one owner-authorized. Revisit that (branch plus PR) when code lands.

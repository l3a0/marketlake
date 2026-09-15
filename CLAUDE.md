# CLAUDE.md — Marketlake

Marketlake is a capture-first market data lake. It records full option chains and equity quotes at one-minute cadence from the Schwab Trader API. Status: implementation is underway. Code lives in `src/lake`, with tests under `tests/`.

**The tracker is authoritative for scope (owner directive, 2026-09-13).** An unbuilt deliverable's issue is the source of truth for what it is and what it must do. The design doc at [docs/design.md](docs/design.md) carries the reasoning, the premise, and the considered-and-rejected register, and it links to the issue rather than competing with it. [docs/build-plan.md](docs/build-plan.md) carries the slicing rule, the build order, and each slice's test surface, and its deliverable entries are links. Read an unbuilt deliverable's issue before proposing a change to it. Read the design doc for everything else, which includes every built deliverable and the reasoning behind all of them.

The price of this is named rather than hidden: the same substance now exists in an issue and in the doc that reasons about it, so the two can drift. The issue wins. When they disagree, the doc is what gets corrected.

**Rank work by what makes the product usable (owner directive, 2026-09-14).** Capture reliability still ranks first, as the premise in [docs/design.md](docs/design.md) states. This directive decides what comes next, and what comes next is not severity. Before proposing an order, name what is missing from the shortest path to a product someone can use, and put that first.

Then ship it, use it, and let what breaks set the order after that. Evidence from real use outranks any ranking made in advance, including this one.

For everything else in the product's own code, ask how many times the path has run, and give the count. Zero means defer the work and say that in the proposal.

Two exceptions come from the design's own reasoning.

1. A path that can lose or corrupt a captured minute is never deferred. That minute is gone forever, while everything computed downstream is regenerable. The design already defers on that axis rather than on this one.
2. An alarm reads zero while it is healthy, so the count says nothing about it. The dead-man, the watchdog, the Sunday canary, and the backup sit outside this rule. Count the cycles they watched, not the times they fired.

Three measurements produced this rule.

1. Slice 2, the daemon, stood at 43 issues closed and 32 open. Slice 4, the read layer, stood at 0 closed.
2. The lake held 9,839,816 chain rows captured on 2026-09-14 and no supported way to read any of them. There is no loader in `src/lake`.
3. Several rounds of work hardened the `extra` overflow column. That column was non-null on zero of the lake's 9,846,266 sealed rows.

Ranking by severity never runs out of work, because any path with no test behind it can be called a failure waiting to happen. That is how three rounds of hardening reached the capture path while the lake stayed unreadable.

Cut a large deliverable down to the part that runs against data the lake already holds. Deliverable 17 in [docs/build-plan.md](docs/build-plan.md) is the example. Its chain loader reads partitions that exist today, while its bars loader waits on deliverable 16, which slice 3 has not built. A cut like that obeys the closing rule below: the shipping pull request writes `Part of`, and the issue keeps what is left.

The price of this is named rather than hidden. Shipping the usable path first leaves gaps open on paths nothing has exercised, and one of them will eventually cost something. That is accepted on purpose, inside the two exceptions above. A lake nobody can read produces no evidence about which hardening mattered, so the deferred work is also the work with the least evidence behind it.

## Writing style (owner directive, 2026-08-26)

Clarity comes first. Write plain sentences a reader understands on one read. Prefer short, complete sentences, but never at the cost of clarity. Do not chop an idea into cryptic one-idea fragments. When a short sentence turns hard to parse, write the clear sentence instead, even if it runs a little longer. Explain as you go, like teaching, so the reader follows without backtracking. Avoid em dashes and semicolons. Break a genuinely long sentence into two when that reads better. This applies to every prose surface: this file, the design doc, commit messages, PR bodies, and chat replies. Use plain language. Give the intuition first. Put the precise rule right behind it.

**Clarity first (owner directive, 2026-09-11).** The earlier "short sentences, one idea each" rule was read too literally and produced terse, cryptic fragments that were hard to follow. The fix is the paragraph above: clarity is the first goal, shortness the second. A sentence that is short but cryptic is worse than a slightly longer sentence that is clear. This does not loosen the other rules. Keep dropping jargon, leading with why, the impersonal voice, and listing counted sets.

**Impersonal voice (owner directive, 2026-08-31).** Write without the first person. No "I," "my," or "mine." The subject is the process, the mechanism, or the finding, not the author. "Chunking the fetch keeps the response under the body limit" beats "I chunk the fetch to stay under the body limit." Keep it active, not passive: "The recompute reads the captured rows," never "the captured rows are read." Direct advice stays as an imperative. Drop the explicit "you" and "your" where it reads cleaner. Keep "you" only where removing it forces an awkward passive. This holds on every prose surface named above. The design doc is the exemplar, with no first person in it. This matches the sibling `trading-strategies` repo.

**Explain every concept on first use.** This covers coined vocabulary (surface, segment, gap-marking), borrowed tools (`flock`, `pmset`, Arrow IPC), and behaviors (sleep-missed jobs, dead-man checks). The test: if a reader must ask "what is X," the doc failed at X's first appearance. Add the gloss there, not in a glossary. The design doc's existing glosses are the pattern to follow.

**Drop the jargon (owner directive, 2026-09-10).** Given the choice between glossing an in-group term and deleting it, delete it. The test: when a sentence names a concept where it could say what happens, say what happens. "No test covers it" beats "it is unheld." "Nothing fails when the daemon's hook wiring breaks" beats "that wiring is held by nothing." A gloss works once, at first use, while the term keeps reappearing and costs the reader attention every time. Being native to this repo does not save a term. *Unheld* was native here before it was cut, and so was the testing sense of *pin*, as in "a test pins the contract." Cut the whole family in one pass. The first sweep took *hold* and left *pin*, which is the same idiom in the same places, and a review had to catch it. One exemption: the design doc's pinned vocabulary, named under the review-hardened section below, carries exact definitions and is reused on purpose. Keep any word where it is ordinary English, as in "`config.yaml` holds four secrets" or "the daemon holds no expiration state."

**List a counted set. Do not inline it.** When a sentence names a count of items, like "four seams" or "three tests," the items follow as a list, not a run-on of sentences. Number the list when the prose states the count. Use a bulleted list for an unordered set with no count.

## The design doc is review-hardened

The doc survived three adversarial review batteries. That was 89+ agents and 58 verified findings, with zero findings refuted. It also survived the owner's own Socratic passes. Respect two conventions it carries:

- **The considered-and-rejected register.** Cut machinery is pinned in the doc with its rationale. Examples: backfill, the streamer, the Saturday OI wake, the entire morning-OI job, extra-account quota farming, `pmset disablesleep`, the pause API, static dashboard rendering. Do not re-propose these. When something new is cut, pin it the same way.
- **Pinned vocabulary.** Terms like *surface*, *segment*, *option_close*, *spot_close*, *comparable set*, and *capture_start* have exact definitions in the doc. Reuse them. Do not coin synonyms.

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

Do not commit or push without explicit per-change review. Each commit instruction authorizes exactly the changes summarized in the immediately prior turn. Once that commit lands, the authorization is spent. The next change starts fresh. The pattern: make the change, summarize it, wait, then commit exactly what was summarized. If new changes appear between summary and commit, re-summarize and re-confirm. `main` requires a pull request. An active repository ruleset enforces it. Owners can bypass that rule, but do not: branch, push, and open a PR, even for a one-line docs change.

## Pull requests

**Review every PR before the owner does (owner directive, 2026-09-06).** A PR the owner has not seen reviewed is not finished work. This holds whether the PR is yours or someone else's, whether it is one line or a thousand, and whether or not a review was asked for. The review runs before the PR is handed over, not after.

The gate is mechanical, because the rule is easy to hold in principle and easy to miss in practice. Opening a PR is not finishing it. A PR link and the result of its review go to the owner in the same message, or neither goes. Reporting the link first puts the review after the handover by construction, which is the failure this rule exists to stop.

Review by fanning out independent lenses, then verifying each finding adversarially. Several reviewers in parallel, each with one lens and no sight of the others, produce the findings. Verifiers then try to refute each one, and only what survives is acted on. Point one lens at completeness and one at over-reach, which catch the two failures that recur:

1. Fixing the instance rather than the class, such as a false claim corrected in one file while it still stands in three more.
2. Fixing past the class, such as generalising a change into places it does not belong.

Verify by executing, not by reading. Mutate the code and confirm a test fails. A test that still passes under mutation does not cover what it claims to cover. Say plainly what the review found and what it refuted, including when it found nothing.

**A filed issue carries its milestone and its labels (owner directive, 2026-09-13).** Filing is not finished when the issue exists. An issue with no milestone appears in no slice view and no view scoped by kind, so only a sweep for nulls finds it, and nothing brings it back on its own. Three arrived that way in a single day, each from a session told to file what it found and nothing further: #143, then #145 and #146. The sessions did exactly what was asked, which is why the rule belongs here rather than in a reminder.

So a filed issue is finished when it says three things.

1. A milestone says which slice owns it.
2. A label says what kind of work it is.
3. A dependency says what it waits on, where it waits on anything.

No automation supplies the first two. A project's auto-add makes an untriaged issue visible and does not triage it, and nothing infers which slice a gap belongs to.

The same applies to an issue a spawned session is told it may file. The instruction to file carries the instruction to triage, or the work lands where nothing will look for it.

**Close an issue only when nothing is left in it (owner directive, 2026-09-12).** Before a PR closes an issue, move whatever that PR does not do into its own issue. A piece described only inside a body goes when the body closes, and nothing surfaces it again. Two issues have already gone that way, and in both the closing PR's own text named the work it was leaving. #101 was closed by the PR that did half of it. #85 opened with `Closes #77.` and then said a later PR would drop the column, whose remainder survives only because #96 was filed for it afterwards.

While a piece is outstanding, a PR writes `Part of #NN` and the closing keyword waits for the PR that leaves nothing. GitHub reads the keyword only when the number follows it immediately, so `Closes #101` closes and `Closes the second half of #101` closes nothing at all. #109 wrote the second form and linked no issue.

An issue whose pieces have all been split has no finishing PR left, so close it by hand and name where each piece went. Do the same when two PRs are open against one issue, because merge order decides which lands last and neither body can know it. The split is the guard that matters here. The keyword discipline only keeps the issue open long enough to make the split.

A split leaves code comments pointing at the parent for work that moved, so repoint those in the PR that splits. A comment naming a closed issue in the past tense records what happened rather than pointing anywhere, and it stays. An unstarted piece goes to an issue, not to the build plan's unowned register. The issue carries that piece's scope and its status together, per the directive at the top of this file. The plan names the issue rather than restating it, so a plan entry cannot go stale the moment work lands.

PR titles use a Conventional Commits prefix. The form is `type(scope): summary`. Types in use: `docs`, `feat`, `fix`, `refactor`, `chore`, `ci`, `perf`. Add a scope in parens when it sharpens the title, like `docs(CLAUDE.md)`. Drop it when none does, like a plain `docs:` for a whole-doc change. The form and the scope rule match the sibling `trading-strategies` repo. Its list carries every type but `refactor`, which this repo uses and that one does not.

PR bodies use Markdown section headings, not a wall of prose. Lead with `## Why`, then `## What`. Add situational sections after as the change needs them, like `## Scope`, `## Notes`, or `## Evidence`. The body's prose obeys the writing-style rules above. So clear, short sentences and no em dashes, even though the sibling repo allows them. End every body with the footer line: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

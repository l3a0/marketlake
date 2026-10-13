---
name: board-markers
description: What a Marketlake session sets on GitHub so the build board shows it, and when. Use when a plan loop starts or exits on an issue, when a build session starts on an issue or hands its pull request over, and when ranking issues for the board. It covers the working:build and working:decompose labels, the "Board verdict:" comment line, and the "Board order:" and "Board after:" body lines.
---

# Board markers

The build board at <https://l3a0.github.io/marketlake/> says which cards a
session is on, which plans are finished, and what to take next. No session
writes the board. A GitHub Action runs
`.claude/skills/sync-prune-next/build_board.py`, which reads GitHub and
publishes `board.json`. So the only way a session's judgement reaches the board
is through something it sets on GitHub. A session that reaches one of the
moments below and sets nothing leaves the board wrong, and nothing else
notices.

A label, a comment and a body edit each start a run of the Action, so the page
changes within a few minutes.

## The markers

Three kinds of marker carry a session's judgement.

1. **Labels.** `working:build` says a build session holds the card.
   `working:decompose` says a plan loop is running on it.
2. **A verdict line in a comment.** A plan loop that exits posts a comment
   carrying one of these lines.

   ```text
   Board verdict: build after 11 passes
   Board verdict: decide after 4 passes
   ```

   `build` means the plan is ready for a builder. `decide` means the loop
   ended on a question only the owner can answer.
3. **Order and after lines in the issue body.** `Board order:` ranks the card,
   and `Board after:` names the issues it should land after, where nothing
   enforces that order.

   ```text
   Board order: 3
   Board after: #638, #759
   ```

Each line follows the same grammar.

- It starts its line, with nothing before it but spaces or tabs. A list marker
  such as `- ` or `1. `, or a quote marker `> `, stops it being read. An order or verdict
  line also has nothing after it but spaces or tabs.
- The words are exactly as shown, in that case, with one ordinary space after
  the colon. `Board after:` takes any spacing after its colon.
- An order is an optional minus sign, then digits, then optionally a decimal
  point followed by at least one digit, with at most 15 digits in all. So
  `Board order: 2.5` slots a card between 2 and 3 without renumbering them,
  while `+2`, `.5`, `2.` and `1e3` are not read. A pass count is a whole
  number of up to 15 digits. `pass` and `passes` both read, and a trailing
  full stop is allowed.
- `Board after:` reads each `#N` on the line, and the link form
  `[#N](https://github.com/l3a0/marketlake/issues/N)`, which may carry an
  anchor or a query after the number, such as `#issuecomment-5`. A reference
  to another repository, written `owner/name#N` or as a link to another
  repository, is refused rather than read as this repository's issue. So is a
  link whose text and address name different numbers.
- A line inside a fenced code block is not read, so a fence is how to quote the
  format. A line inside a code span is not one either, because it does not
  start the line.

Only the owner's text counts, because anyone can comment on or open an issue
in this public repository. Sessions act as the owner, so their text counts.

- A verdict counts only in a comment whose author is the owner.
- An order or after line counts only in the body of an issue the owner opened.
  A line the owner adds to someone else's issue is refused.
- A label needs triage access, so a label counts whoever set it.

When an issue's comments carry several verdicts, the newest wins. When a body
carries several order lines, the first wins. A line that looks meant as a marker
and is not read, such as `board order: 3`, `Board order 3` or
`1. Board order: 3`, never stops the Action. The generator lists it in its
summary as unread, along with conflicts, refused lines and dropped markers, and
a sync round reports that list. The summary names the issue and the marker's
kind, never the line. A line counts as meant for a marker when the kind ends
it, or is followed, after optional emphasis and spaces, by a colon, a digit or
a `#`. Prose that only starts with the words, such as "Board orders form one
sequence", is not listed.

## When to set them

Items 1 to 5 below are the moments where the board's answer changes and
nothing GitHub records on its own says so. Items 6 and 7 are rules for what
those moments set on particular cards and verdicts. `<n>` is the issue's
number.

1. **A plan loop starts.** Add `working:decompose`.

   ```bash
   gh issue edit <n> --repo l3a0/marketlake --add-label working:decompose
   ```

   Each pass gets its own comment headed `## Plan audit, pass N`. A combined
   heading such as `## Plan audit, passes 7 and 8` is not counted, so a sync
   round cannot tell that pass happened. A later loop on the same issue
   continues the numbering, and its verdict counts its own passes. A
   `working:` label already on the issue is no lock. Check it against the
   stale-label rule in step 3 of the `sync-prune-next` skill, and ask the
   session that owns it before taking over.
2. **A plan loop exits.** Post the verdict as a comment carrying one
   `Board verdict:` line, then remove `working:decompose`.

   ```bash
   gh issue comment <n> --repo l3a0/marketlake --body-file <file>
   gh issue edit <n> --repo l3a0/marketlake --remove-label working:decompose
   ```

   A sync round lists an issue whose newest owner `## Plan audit, pass N`
   heading comes after its newest verdict, in a later comment or lower in the
   same one, so a loop that exits without a verdict is reported. An exit
   comment headed as the last pass and ending with the verdict is not listed.

   In the same step, post the loop's cost as a separate comment starting with
   the line `## Cost`, per `CLAUDE.md`'s "Cost on the issue".
3. **A build starts.** Add `working:build`, after the same check as a plan
   loop.

   ```bash
   gh issue edit <n> --repo l3a0/marketlake --add-label working:build
   ```

   If the label does not exist yet, `gh issue edit` fails. Report that and
   carry on. Never run `gh label create`, because creating labels is the
   owner's.
4. **A build hands over.** Remove `working:build` once the `## Review complete`
   comment is posted on the pull request, not when the pull request opens.
   Until then the label keeps the pull request out of the board's "Waiting on
   your review" column, which is its job while the review runs.

   ```bash
   gh issue edit <n> --repo l3a0/marketlake --remove-label working:build
   ```

   In the same step, post the build's and the review's cost on the issue as a
   comment starting with the line `## Cost`, per `CLAUDE.md`'s "Cost on the
   issue".

   A `Part of` pull request leaves its issue open. Once it merges, the card
   drops back to "Planned, no builder" with its old verdict reading "ready to
   build", because the board shows only open pull requests. So that hand-over
   also settles the verdict for the remainder. A build session runs no plan
   passes of its own, so it does one of two things.

   - If the remainder already has a plan that still holds, post
     `Board verdict: build after N passes`, with N that plan's pass count.
   - If it does not, edit the comment carrying the old verdict and remove its
     `Board verdict:` line, so the card stops reading ready. Sessions act as
     the owner, so they can edit the owner's comment.

   ```bash
   gh api --method PATCH repos/l3a0/marketlake/issues/comments/<id> -F body=@<file>
   ```

   A verdict is otherwise withdrawn only by a newer one, or by editing or
   deleting its comment.
5. **Ranking changes.** Write `Board order:` and `Board after:` lines in the
   issue bodies. Once any card in a milestone that is not deferred carries an
   order, the page hides that milestone's unranked cards behind "Show all". So
   rank a milestone's open issues that are not parents in one pass, rather
   than one card at a time. A deferred card's order hides nothing, because the
   page ignores it. Orders form one sequence across every milestone, so do not
   restart the numbering at 1 in each.

   ```bash
   gh issue view <n> --repo l3a0/marketlake --json body --jq .body > <file>
   gh issue edit <n> --repo l3a0/marketlake --body-file <file>
   ```

   A leftover order line hides every unranked card in its milestone, and
   nothing reports it. Remove an order line that no longer means anything.
6. **A parent or a deferred card.** A parent carries no verdict and no order,
   because it builds nothing, and its children carry them. A `deferred` card
   carries no verdict, because the board would otherwise offer it as free to
   pick up. The generator drops a verdict or an order on a parent, and a
   verdict on a deferred card, and lists each drop. Removing `deferred` brings
   the verdict back on the next run. A deferred card keeps its order, and the
   page and the sync round both skip it.
7. **A `decide` verdict.** Its comment names the question, because the sync
   round lists the card and not the question. Once the owner answers, a loop
   posts a fresh verdict. An answer carries no verdict line, so without one the
   card reads "waits on an owner call" forever.

## The labels

| Label | Colour | Description |
| --- | --- | --- |
| `working:build` | `fbca04` | A build session holds this card until it hands its pull request over |
| `working:decompose` | `fbca04` | A decompose loop is planning this card |

The owner creates them. No `band:N` label exists here, because the order line
alone ranks a card.

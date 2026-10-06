---
name: sync-prune-next
description: Sync with origin, prune the worktrees and branches whose work is finished, refresh the Marketlake build board, and recommend what to take next on the current MVP milestone. Use when asked to "git sync prune", to clean up worktrees or branches, to "update the build board and say what's next", or for any status round of this repo's sessions and pull requests.
---

# Sync, prune, and say what's next

Many sessions work on this repo at once, each in its own worktree, so the
checkout fills with worktrees and branches whose work has merged. Removing them
keeps the list readable. Removing the wrong one loses work nobody pushed, and
that cannot be recovered. So most of this skill is about telling the two apart
from evidence rather than from a branch name or a session title.

The round has four steps, and they run in this order because each feeds the
next.

1. Fetch and prune, so every later check reads the current `main`.
2. Remove what is finished, and only that.
3. Update the build board.
4. Recommend what to take next.

## 1. Fetch and prune

```bash
git fetch --prune origin && git log --oneline -6 origin/main
git worktree list
```

The log shows what merged since the last round, which is what moves the board.

`--prune` drops a remote-tracking ref only when its branch was deleted on
GitHub. This repo deletes a branch when its pull request merges, so a merged
branch's remote ref goes at this step. That also means check 3 below rarely
finds a merged commit on a remote branch, and the merged pull request's
`headRefOid` is what proves it safe. A branch whose pull request closed
unmerged stays on GitHub. Deleting it is the owner's call, and this skill
leaves it alone.

Fast-forward the main checkout only when it is clean and on `main`, since a
session may be using it. `git -C` keeps the shell where it is, which matters
because the Bash tool keeps a `cd` for every later command.

```bash
M=$(git worktree list | head -1 | cut -d' ' -f1)
[ "$(git -C "$M" branch --show-current)" = main ] && [ -z "$(git -C "$M" status --porcelain)" ] && git -C "$M" merge --ff-only origin/main
```

In this repo the main checkout is also the code the daemon runs. A deploy is a
fast-forward there followed by a restart, so a fast-forward on its own is half a
deploy. A calendar job starts fresh and runs the new code, while a resident job
keeps the code it imported at start. The restart runs the restart script that
`python -m lake.control_plane render --out <dir>` writes. That script needs
root, so running it is the owner's step and never the round's. Say in the
report that the checkout moved, so the owner knows a restart is owed.

## 2. Remove what is finished, and only that

A worktree is removable only when all five of these hold.

1. **It is not the worktree this round runs in.** `list_sessions` leaves out
   the current session, so nothing else protects it, and git removes a worktree
   from inside it without complaint. Compare against
   `git rev-parse --show-toplevel`.
2. **No unarchived session has it as its `cwd`.** Read `cwd`, not `branch`.
   `branch` is the branch a session started on, and a session that switched
   branches reports one it no longer uses. Read `isRunning` as no evidence
   either way. An open session between turns reports `false`, and so does one
   waiting on its own background sub-agents. Pass a `limit` above the default
   of 20 so no session is missed.
3. **Its `HEAD` commit is safe.** Test the worktree's `HEAD` itself, not only
   its branch, because a worktree with no branch checked out holds commits that
   no branch names. The commit is safe when `origin/main` contains it, when a
   remote branch contains it (`git branch -r --contains`), or when it equals
   the `headRefOid` of a merged pull request. Squash merges make the last case
   common, because the branch's commit never reaches `main`. Test that last case
   by exact comparison against the list of merged heads. A
   `gh pr list --search <sha>` matches any commit inside a pull request, not
   only its head, so it does not test what this check states. `60ef88e`
   matched [PR #646](https://github.com/l3a0/marketlake/pull/646), whose head
   is `f1ecb4c`.
4. **It holds no uncommitted changes.** `git status --porcelain` is empty.
   That listing omits ignored files, which `git worktree remove` deletes
   silently. `--ignored` shows them. Caches such as `.venv/` and
   `.pytest_cache/` are expected, and anything else is a reason to keep the
   worktree.
5. **It is not locked.** `git worktree list --porcelain` prints `locked` with a
   reason for a sub-agent's worktree. The pid in that reason belongs to the
   host session's `claude` process, not to the agent, so a live pid shows only
   that the host is alive. Leave a locked worktree alone. If its pid is gone and
   the other checks pass, `git worktree unlock` it first, because
   `git worktree remove` refuses any locked worktree.

The `isRunning` lesson came from the sibling repository, `quantitative-trading`,
where it cost something once already. On 2026-10-05 UTC the session building
[quantitative-trading#335](https://github.com/l3a0/quantitative-trading/issues/335)
reported `isRunning: false`, and its branch sat at `main` with no commits of
its own. Its worktree held seven modified files. Minutes later it opened
[quantitative-trading PR #369](https://github.com/l3a0/quantitative-trading/pull/369)
from that branch, and the files turned out to be review fixes in progress.
Check 4 is what kept the worktree.

The same evidence does not show that work has stalled. Before reporting a
session as stuck, re-run `gh pr list --state open --limit 1000` and read its
latest events with `list_events`, because a pull request may have opened since
the worktree was classified.

Check 3's last case needs the heads of the merged pull requests. Fetch them
once into a file, so both loops below read the same list. A shell variable
would not reach the second loop, because each Bash tool call starts a new
shell. Replace `<scratch>` with the scratch directory in this block and the two
after it.

```bash
MERGED="<scratch>/merged-heads.txt"
gh pr list --repo l3a0/marketlake --state merged --limit 1000 --json headRefOid --jq '.[].headRefOid' > "$MERGED"
wc -l < "$MERGED"
```

A count of exactly 1000 means `gh` cut the list, so raise the limit and fetch
again.

This prints what checks 3 to 5 need for every worktree.

```bash
MERGED="<scratch>/merged-heads.txt"
git worktree list --porcelain | sed -n 's/^worktree //p' | while read -r w; do
  if ! h=$(git -C "$w" rev-parse HEAD 2>/dev/null) || [ "$(git -C "$w" rev-parse --show-toplevel 2>/dev/null)" != "$w" ]; then
    echo "$w unreadable, skipped"; continue
  fi
  safe=$( { git merge-base --is-ancestor "$h" origin/main && echo main; } || git branch -r --contains "$h" | head -1 | tr -d ' ')
  grep -qxF "$h" "$MERGED" && safe="$safe merged-pr-head"
  echo "$w head=${h:0:7} branch=$(git -C "$w" branch --show-current) safe=[${safe}] dirty=$(git -C "$w" status --porcelain | wc -l | tr -d ' ') ignored=$(git -C "$w" status --porcelain --ignored | grep -c '^!!')"
done
git worktree list --porcelain | grep -B3 '^locked'
```

A worktree printed as unreadable has a directory that is gone or a `.git` file
that is broken. Its `HEAD` cannot be read, so none of the tests can pass for
it, and the loop skips it rather than test an empty commit. Report it as kept.
The `--show-toplevel` comparison catches a quieter case. The worktrees sit
inside the main checkout, so a worktree directory with no `.git` file at all
resolves to the main checkout, and without the comparison it reports the main
checkout's `HEAD` as its own.

A branch checked out in no worktree is removable when check 3 holds for its
head. Git refuses to delete a branch that a worktree has checked out, which
protects every branch in use. This loop runs the same three tests as the one
above.

```bash
MERGED="<scratch>/merged-heads.txt"
git branch --format='%(refname:short)' | grep -v -e '^main$' -e '^(HEAD' -e '^(no branch' | while read -r b; do
  h=$(git rev-parse "$b")
  safe=$( { git merge-base --is-ancestor "$h" origin/main && echo main; } || git branch -r --contains "$h" | head -1 | tr -d ' ')
  grep -qxF "$h" "$MERGED" && safe="$safe merged-pr-head"
  echo "$b ${h:0:7} ahead=$(git rev-list --count origin/main.."$h") safe=[${safe}]"
done
```

When the loop runs from a worktree with no branch checked out, `git branch`
adds a line for that worktree's `HEAD`. The line usually reads
`(HEAD detached at <sha>)`. It reads `(no branch)` instead when the `HEAD`
reflog does not record the detach, and `(no branch, rebasing <branch>)` during
a rebase. The `^(HEAD` pattern misses both, so the third pattern removes them.

Write one `-e` per pattern rather than joining them with `\|`. macOS's
`/usr/bin/grep` reads the `$` before `\|` as a literal character, so the joined
form keeps `main` in the list, and `main` then reads as safe to delete.

Then remove with the commands that refuse to lose work. `git worktree remove`
without `--force` refuses a worktree holding modified or untracked files, and
that refusal is a check worth keeping. Use `git branch -D` only on a branch
whose head passed check 3.

```bash
git worktree remove <path>
git branch -D <branch>
git worktree prune
```

Archive a finished session only when the owner has asked for it in this
conversation. Archiving stops the session and is the owner's call, even when the
board says its loop exited.

Report what was removed, and what was kept with the reason for each. Include
stash entries from `git stash list`, which survive every removal above and
which nothing else surfaces. The kept list is where the owner learns about work
in progress that nothing else announces, such as the worktree above.

## 3. Update the build board

The board is the artifact at
`https://claude.ai/artifact/83eeHmHAA19A6hy8kKGJBw`. It cannot read the disk or
poll GitHub, so every figure on it was measured by a session and written by
hand. A round that changes what the board should show and leaves without
writing has made it wrong, and nothing else notices.

### Read, edit, and write one document

The page reads one database document, `data` in the `board` collection, and
redraws on every write. Read it into a scratch directory rather than the repo,
because nothing read here belongs in a commit.

```text
ArtifactData action="get" url="https://claude.ai/artifact/83eeHmHAA19A6hy8kKGJBw"
             collection="board" doc_id="data" out_dir="<scratch>/readback"
```

That saves `readback/board/data.json`, and the result names the document's
`version`. Edit the file, then write the whole document back with `set`,
pinned to that version.

```text
ArtifactData action="set" url="https://claude.ai/artifact/83eeHmHAA19A6hy8kKGJBw"
             collection="board" doc_id="data"
             file_path="<scratch>/readback/board/data.json" if_version=<read version>
```

The pin is what stops two sessions from losing each other's work. A write based
on an old version is refused and writes nothing, rather than overwriting
whatever another session wrote since the read. On a refusal, read the document
again, redo the edit on what it holds now, and write again. Never drop the pin
to get a write through.

Never issue the local edit of the JSON file and the `set` in the same parallel
batch of tool calls. Calls in one batch run in no promised order, so the `set`
can send the file before the edit lands. The write then succeeds, stores the
old content under a new version, and looks exactly like a good write.

### The document's shape

```text
{ schema: 1, state, prs, working, planned, next, untracked, tracker }
```

- **`state`** carries `main`, `deployed`, `updatedAt`, `lake` (with
  `sessions`, `chainRows`, `first` and `last`), `suite.tests`, and `issues`
  (with `open` and `deferred`). The page no longer draws `lake` or `suite`, but
  its `usable()` check refuses a document without them, so keep both.
  `state.deployed` changes only when the owner's restart lands. A fast-forward
  of the main checkout leaves it as it was.
- **`prs`** has one entry per open pull request, written as
  `{issue, pr, state, review, linked, reviewed, rollup}` and keyed by `issue`.
  An entry keyed `n` drops its card without any error. `linked` comes from the
  pull request's `closingIssuesReferences`. A reviewed, green card lands in the
  owner's queue, so `reviewed` is true only once the review is complete. Any
  review comment is not enough, because a session can post some lenses' results
  while another lens still runs. That sent
  [PR #713](https://github.com/l3a0/marketlake/pull/713) to the owner while its
  mutation lens was running. A review is complete when the pull request carries
  a comment or review whose whole first line is `## Review complete`, posted
  after the head commit's `committedDate`. A heading that only starts with
  those words, like `## Review completeness check`, does not count. Neither
  does a marker from an earlier round, because the commits pushed since then
  are code that review never saw. Count the markers with
  `gh pr view <n> --json comments,reviews,commits --jq '.commits[-1].committedDate as $head | [.comments[] | select(.createdAt > $head) | .body] + [.reviews[] | select(.submittedAt > $head) | .body] | map(select(test("\\A## Review complete\\r?(\\n|\\z)"))) | length'`,
  and set `reviewed` only when the count is above zero. A pull request whose
  final review was posted before this rule carries no marker, so it reads
  `reviewed: false` until a session posts one, and the sync reports it rather
  than guessing whether its review finished. GitHub's review
  decision cannot tell whether the review ran, so it does not decide
  `reviewed`. A session posting review results puts that heading only on the
  comment that closes the review, after every lens has reported. A partial
  comment leaves `reviewed` false, and the entry's `review` text says which
  lens is still running. `rollup` is a list
  of `[name, conclusion]` pairs, where the conclusion is one of `"success"`,
  `"failure"`, `"running"` or `"neutral"`.
- **`working`** marks a card a session is on right now, as `{n, kind, what}`.
  An entry with `kind: "build"` draws under Building, and any other `kind`
  draws under Being planned.
- **`planned`** holds the cards whose plan is finished. Each entry is keyed by
  `n` and carries `passes`, `note`, and a `ready` of `build` or `decide`.
- **`next`** is the ranking. Each entry is keyed by `issue` and carries `band`,
  `order`, `ready` and `why`. Its `ready` takes `build`, `decide` or `plan`,
  and the page ranks them in that order. It leaves out every issue labeled
  `deferred`.
- **`untracked`** carries over as read unless the round has reason to change
  it.
- **`tracker`** is every open issue, read from GitHub's GraphQL API. Its `ms`
  is written as `"MVP 2"` or `"no milestone"`. Its `kind` is one of `ready`,
  `deferred` or `parent` in the data so far, and the page also draws
  `decision` and `data`. An issue with open sub-issues gets `kind: "parent"`.
  Its `needs` comes from the issue's native `blockedBy` links plus the
  dependency sections of issue bodies.

A `next` or `planned` entry under the wrong key drops its card in silence, the
same way a `prs` entry keyed `n` does.

The page filters cards by milestone, with a selector that defaults to "MVP 2".
So a card's milestone comes only from `tracker`, and an issue missing from
`tracker` never draws, whatever `next` or `prs` say about it.

### What a round usually changes

- `prs` takes each open pull request's checks at its current head and its
  review status, and drops pull requests that merged or closed.
- `tracker` and `state.issues` take the issues filed, closed or relabeled since
  the last round. `planned` and `next` drop entries for closed issues.
- `state.main` takes the head the fetch printed, and `state.updatedAt` takes
  the time of the write.
- `working` entries are owed a removal by whoever added them, so report a stale
  one rather than deleting another session's entry. A build session keeps its
  entry until it hands its pull request over. Until that session removes it, a
  `working` entry of `kind: "build"` keeps the card out of "Waiting on your
  review", even when its pull request is reviewed and green. An entry of any
  other `kind` does not hold the card, because a planning loop on leftover
  scope should not hide a finished pull request. Ask whoever added an entry
  before calling it stale.

Pass `--limit 1000` to every `gh` list command that feeds the board, because
`gh` stops at its limit without a warning and its default is 30. A cut list
prints the same way a complete one does.

### Confirm the page drew it

A `set` that succeeded proves the database took the document. It does not prove
the page drew it, because the page refuses a document its `usable()` check
rejects, and an entry keyed wrong drops its card in silence. So after writing,
look at the page and confirm every card this round added or moved.

1. **Look under both selectors.** Check the cards under "MVP 2", then again
   under "All". The selector defaults to "MVP 2", and bugs that can lose a
   captured minute and bugs in an alarm carry no milestone. Their cards often
   rank first in `next`, and they never draw under the default.
2. **View the page in a browser.** The `Artifact` tool's `open` action shows
   the page to the owner and shows the session nothing. Open the page in a
   browser signed in to claude.ai, through Claude in Chrome, and read a
   screenshot. Text extraction cannot read the page, because it renders inside
   a frame.

### Changing the page

The page was republished 54 times from sessions' scratch copies, with no review
and no history. On 2026-10-06 a bug in its column logic sent an unfinished pull
request to the owner's queue. So the page's source now lives in the repository
as `board.html`, beside this skill, and a change to it is a change to code.

1. Edit `board.html` on a branch and open a pull request. It gets the same
   review as any other change, per `CLAUDE.md`.
2. After the pull request merges, publish the file from a checkout of `main`
   fast-forwarded to `origin/main`. Use the `Artifact` tool's publish with
   `url` set to the board's URL and `file_path` set to `board.html`. Read the
   board with `action: "read"` first, because the tool refuses a publish to an
   artifact the session has not read. Leave `capabilities` out of the call,
   which keeps the page's database access as it is.
3. Never republish from a scratch copy, an unmerged branch, or a copy read back
   from the live page. Each of those can carry a change nobody reviewed, or undo
   one that merged.

The file carries no board data. Its `FALLBACK` is a stub that `usable()`
accepts, so a view that cannot reach the database says it has no board data
rather than showing an old copy. Data writes stay as they are. A round writes
the `data` document as described above, and never republishes the page to
change what it shows.

## 4. Recommend what to take next

`CLAUDE.md`'s ranking directive and its exceptions decide the order. Read them
there rather than from a copy here. The board's `next` stores that order, so
read `next` rather than re-deriving it.

The open MVP milestone tracks the work
([MVP 2, capture on a hosted VM](https://github.com/l3a0/marketlake/milestone/5),
when this was written). An issue off that path carries no milestone and the
`deferred` label. Bugs that can lose a captured minute, and bugs in an alarm,
are never deferred, so they carry neither and stay candidates even without the
milestone. That is why the page check above looks under "All" as well.

Collect candidates from three places, and give the evidence for each one.

1. **The owner's queue.** Nothing moves until the owner answers, so these come
   first.
   - Pull requests that are reviewed, green at the current head, and carry no
     `working` entry of `kind: "build"` on their card.
   - `planned` entries with `ready` of `decide`.
   - Questions a session handed back.
2. **Plans ready to build with no builder.** These are `planned` entries with
   `ready` of `build`, with no `working` entry and no open pull request.
3. **The MVP milestone's remaining issues**, in `next`'s order, each with the
   blockers its `needs` names. A blocked issue is not a candidate until its
   blockers close, so name what it waits on rather than recommending it.

Link every issue and pull request number, as `[#NN](...)` for an issue and
`[PR #NN](...)` for a pull request. Say which candidates only the owner can act
on, and which a new session can start without them.

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

`--prune` deletes every ref under `refs/remotes/origin/` that has no branch
behind it on GitHub, including one made there by hand. So after this step,
every ref under `refs/remotes/origin/` names a branch GitHub holds. This repo
deletes a branch when its pull request merges, so a merged branch's remote ref
goes at this step. That also means check 3 below rarely finds a merged commit
on a remote branch, and the merged pull request's head ref is what proves it
safe. A branch whose pull request closed unmerged stays on GitHub. Deleting it
is the owner's call, and this skill leaves it alone.

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
   branch on `origin` contains it, or when a merged pull request's head ref,
   `refs/pull/N/head`, contains it. The text just before the loops below says
   why each case counts and which refs do not.
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

Check 3 counts only refs that GitHub backs. After step 1, every ref under
`refs/remotes/origin/` names a branch on GitHub. A ref elsewhere under
`refs/remotes/` was made locally, for example by
`git fetch origin pull/N/head:refs/remotes/pr/N`, and proves nothing. Yet
`git branch -r --contains` lists it like any remote branch, so both loops keep
only that command's `origin/` lines. On 2026-10-07 this checkout held
`refs/remotes/pr/492`, `refs/remotes/pr652` and `refs/remotes/audit/pr697`,
and [#758](https://github.com/l3a0/marketlake/issues/758) found 34 clean
worktrees whose only evidence was a ref like these.

Squash merges make the merged-pull-request case common, because the branch's
commits never reach `main`. GitHub keeps `refs/pull/N/head` for every pull
request, through the merge and the deletion of its branch. A ref keeps every
commit it reaches, so removing a worktree on any of them loses nothing. Two
cases still fail, and must.

1. A commit force-pushed out of a pull request. The final head ref does not
   reach it, and GitHub may collect it.
2. A commit made on a worktree after the pull request's last push. Nothing on
   GitHub holds it.

Check 4 still catches uncommitted files.

The test is ancestry against the head ref, not a pull request's commit list
and not a search. `gh pr list --search 60ef88e` returns
[PR #646](https://github.com/l3a0/marketlake/pull/646), whose head ref reaches
that commit, and [PR #661](https://github.com/l3a0/marketlake/pull/661), whose
head ref does not. The second matches only because a comment on it quotes the
hash. A search reads text, while ancestry asks whether a ref GitHub keeps still
reaches the commit.

The last case needs the numbers and head refs of the merged pull requests.
Fetch them once, so both loops below read the same ones. A shell variable would
not reach the second loop, because each Bash tool call starts a new shell.
Replace `<scratch>` with the scratch directory in this block and the two after
it.

```bash
MERGED="<scratch>/merged-prs.txt"
gh pr list --repo l3a0/marketlake --state merged --limit 1000 --json number --jq '.[].number' > "$MERGED"
wc -l < "$MERGED"
git for-each-ref --format='delete %(refname)' refs/pr-heads/ | git update-ref --stdin
sed 's#.*#+refs/pull/&/head:refs/pr-heads/&#' "$MERGED" | git fetch --quiet --stdin origin || echo "FETCH FAILED: stop the round"
```

A count of exactly 1000 means `gh` cut the list, so raise the limit and fetch
again.

The fetch writes each head to `refs/pr-heads/N`, outside `refs/remotes/`.
`git branch -r` lists everything under `refs/remotes/`, so heads fetched there
would read as remote branches and rebuild the trap above.

The block deletes every ref under `refs/pr-heads/` before it fetches, because
a head left by an earlier fetch can be stale. One fetched while its pull
request was open still reaches any commit a later force-push dropped, and the
loops' merged-number filter accepts it once the pull request merges. One
missing ref on GitHub aborts the whole fetch, which then writes nothing, so
without the delete every stale head would survive a failed fetch. With it, a
failed fetch leaves no heads, and the merged-pull-request case finds nothing
rather than something false. The `+` on each refspec is a second guard. It
lets the fetch overwrite a head the delete missed, where git would otherwise
refuse to move a ref to a commit that does not descend from it.

If the fetch fails, stop the round. The loops would report no worktree safe
through a merged pull request, which loses nothing but tells the owner nothing.
Find why the fetch failed and rerun this block.

This prints what checks 3 to 5 need for every worktree.

```bash
MERGED="<scratch>/merged-prs.txt"
git worktree list --porcelain | sed -n 's/^worktree //p' | while read -r w; do
  if ! h=$(git -C "$w" rev-parse HEAD 2>/dev/null) || [ "$(git -C "$w" rev-parse --show-toplevel 2>/dev/null)" != "$w" ]; then
    echo "$w unreadable, skipped"; continue
  fi
  safe=$( { git merge-base --is-ancestor "$h" origin/main && echo main; } || git branch -r --contains "$h" | grep -e '^ *origin/' | head -1 | tr -d ' ')
  pr=$(git for-each-ref --contains "$h" --format='%(refname:lstrip=2)' refs/pr-heads/ | grep -xF -f "$MERGED" | head -1)
  [ -n "$pr" ] && safe="$safe merged-pr/$pr"
  echo "$w head=${h:0:7} branch=$(git -C "$w" branch --show-current) safe=[${safe}] dirty=$(git -C "$w" status --porcelain | wc -l | tr -d ' ') ignored=$(git -C "$w" status --porcelain --ignored | grep -c '^!!')"
done
git worktree list --porcelain | grep -B3 '^locked'
```

`git for-each-ref --contains` runs the ancestry test of
`git merge-base --is-ancestor` against every fetched head in one call, rather
than one process per head. The `grep -xF -f "$MERGED"` keeps only the merged
numbers, so an open pull request's head that someone fetched into
`refs/pr-heads/` does not count.

A worktree printed as unreadable has a directory that is gone or a `.git` file
that is broken. Its `HEAD` cannot be read, so none of the tests can pass for
it, and the loop skips it rather than test an empty commit. Report it as kept.
The `--show-toplevel` comparison catches a quieter case. The worktrees sit
inside the main checkout, so a worktree directory with no `.git` file at all
resolves to the main checkout, and without the comparison it reports the main
checkout's `HEAD` as its own.

A branch checked out in no worktree is removable when check 3 holds for its
head. Git refuses to delete a branch that a worktree has checked out, which
protects every branch in use. The loop also lists, by full name, each ref under
`refs/remotes/` outside `origin/`, such as `refs/remotes/pr/492`, since nothing
on GitHub backs it. This loop runs the same three tests as the one above.

```bash
MERGED="<scratch>/merged-prs.txt"
{ git branch --format='%(refname:short)' | grep -v -e '^main$' -e '^(HEAD' -e '^(no branch'
  git for-each-ref --format='%(refname)' refs/remotes/ | grep -v -e '^refs/remotes/origin/'
} | while read -r b; do
  h=$(git rev-parse "$b")
  safe=$( { git merge-base --is-ancestor "$h" origin/main && echo main; } || git branch -r --contains "$h" | grep -e '^ *origin/' | head -1 | tr -d ' ')
  pr=$(git for-each-ref --contains "$h" --format='%(refname:lstrip=2)' refs/pr-heads/ | grep -xF -f "$MERGED" | head -1)
  [ -n "$pr" ] && safe="$safe merged-pr/$pr"
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
whose head passed check 3. A name the branch loop printed with the
`refs/remotes/` prefix is a ref, not a branch. Delete it with
`git update-ref -d` on the same evidence, and keep and report one whose commit
fails check 3.

```bash
git worktree remove <path>
git branch -D <branch>
git update-ref -d <refs/remotes/...>
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
  of the main checkout leaves it as it was. `state.page` is the full sha of the
  last commit to the page's source that the live page carries, per "Record
  what is live" below. The page itself never reads it.
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
  `reviewed`. The rule for writing the heading lives in `CLAUDE.md`, under
  "Pull requests", where review sessions read it. A partial comment leaves
  `reviewed` false, and the entry's `review` text says which lens is still
  running. `rollup` is a list of `[name, conclusion]` pairs, and the
  conclusion is one of five words.

  1. `"success"` is a check that ran and passed. Write it for a check run's
     SUCCESS and a status context's SUCCESS.
  2. `"skipped"` is GitHub's SKIPPED, a job whose condition kept it from
     running. It counts as settled. The infra workflow's `tofu apply (live)`
     job is skipped on every pull request, because it runs only on main.
  3. `"failure"` is a check that failed, including one that timed out or
     could not start. Write it for a check run's FAILURE, TIMED_OUT and
     STARTUP_FAILURE, and for a status context's FAILURE and ERROR.
  4. `"running"` is a check that has not concluded. Write it for a check run
     with no conclusion yet, and for a status context's PENDING and EXPECTED.
  5. `"neutral"` is a check that concluded without passing or failing. Write
     it for a check run's NEUTRAL, CANCELLED, STALE and ACTION_REQUIRED.

  Those cover every value of GitHub's `CheckConclusionState` and
  `StatusState`, so no raw value goes into `rollup` unmapped. A value GitHub
  adds later goes in as `"neutral"` until this list names it.

  The page reads a rollup as green when every entry is `"success"` or
  `"skipped"` and at least one is `"success"`. It reads any word outside
  these five as unsettled, the same as `"neutral"`, so a typo keeps a card out
  of the owner's queue rather than drawing a pass.
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
- The round compares `state.page` with `main` and publishes the page when they
  differ, per "Record what is live" below.

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

A page republished from a session's scratch copy has no review and no history.
Fifty-four versions of the board went out that way, and on 2026-10-06 a bug in
the column logic of one of them sent an unfinished pull request to the owner's
queue. So the page's source is `.claude/skills/sync-prune-next/board.html`,
and a change to it goes through a pull request and the same review as any
other code, per `CLAUDE.md`.

One rule decides every publish: the bytes published to the board equal
`main`'s copy of that file. Where the file sits on disk does not matter.

A round changes what the board shows by writing the `data` document, as above.
It publishes the page only to bring it level with `main`.

#### Test the page before the pull request

Nothing in CI runs the page, so a change that throws or draws the wrong thing
reaches every viewer unless its author runs it first. `board-harness.js`,
beside this skill, runs the page's inline script against a stub DOM and prints
what each element drew, as JSON. It runs under `osascript -l JavaScript`,
because macOS ships that and node may be absent.

```bash
osascript -l JavaScript .claude/skills/sync-prune-next/board-harness.js \
  .claude/skills/sync-prune-next/board.html live - "MVP 2" <scratch>/readback/board/data.json
```

The arguments, in order, are these five.

1. The page file.
2. How the database answers: `none`, `loading`, `empty`, `shape`, `stopped`,
   `live` or `dropped`. The header of `board-harness.js` says what each one
   delivers.
3. The saved "Show all" choice, as `1` or `0`, or `-` for none saved.
4. The saved milestone choice, or `-` for none saved.
5. The data file, read as described above. Only `live` and `dropped` use it.

In the output, `errors` lists anything that threw, and `visible` maps each
drawn element to the text a reader would see. Run `main`'s copy and the changed
copy over the same modes, choices and data, and compare the two outputs. Every
difference should be one the change intends. Run each mode at least once,
because the modes with no data take a different path through the page.

#### A change to what the page requires

A page change that makes `usable()` or a render path require a new field ships
in this order after its pull request merges.

1. Write the field into the `data` document. The live page ignores a field it
   does not read.
2. Publish the page.

In the other order the new page refuses the old document, and the board shows
no data until a round writes the field. The same pull request updates "The
document's shape" above.

#### Publish from main

1. Fetch with `git fetch origin`.
2. Write the published file to a scratch directory with
   `git show origin/main:.claude/skills/sync-prune-next/board.html > <scratch>/board.html`.
3. Check that `git hash-object <scratch>/board.html` prints the same id as
   `git rev-parse origin/main:.claude/skills/sync-prune-next/board.html`. Stop
   if they differ.
4. Run the `Artifact` tool's `read` action on the board's URL. The only reason
   for this step is that the tool refuses a publish to an artifact the session
   has not read. The file the read saves is never a source for the publish.
5. Publish with the `Artifact` tool, with `url` set to the board's URL and
   `file_path` set to `<scratch>/board.html`. Set `label` to the short sha of
   the last commit that touched the page's source, which
   `git log -1 --format=%h origin/main -- .claude/skills/sync-prune-next/board.html`
   prints.
6. Leave `capabilities` and `contract` out of the call. Leaving them out keeps
   the page's database access and its runtime version as they are.

```text
Artifact action="publish" url="https://claude.ai/artifact/83eeHmHAA19A6hy8kKGJBw"
         file_path="<scratch>/board.html" label="<short sha>"
```

Then record the publish in `state.page`, as below, and look at the page, per
"Confirm the page drew it" above.

#### On a publish conflict

The `Artifact` tool can refuse a publish because the live page changed after
the read, and hand back the newer content. Its general advice is to merge the
session's changes into that content. This page overrides that advice, because
`main` is its only source, and content handed back by a refusal is a version
nobody reviewed. So fetch again, read the board again, and publish from `origin/main`
by the steps above.

- Never merge the content the refusal hands back.
- Never build the published file from a copy read back from the live page.
- Never pass `force`.

If the refusal repeats, stop and tell the owner rather than forcing the publish.

#### Record what is live

A merge whose author leaves before publishing leaves the board running old code,
and nothing on the page shows it. So the data document records which commit is
live, and every sync round checks it.

After a publish, write `state.page` as the full sha of the last commit that
touched the page's source, which
`git log -1 --format=%H origin/main -- .claude/skills/sync-prune-next/board.html`
prints. Write it with an `update` pinned to the document's current version,
which the round's last read or write names. Its `data` carries `state` exactly
as read, with only `page` set. Sending the whole `state` keeps the write
correct whether `update` merges nested fields or replaces the `state` object.

```text
ArtifactData action="update" url="https://claude.ai/artifact/83eeHmHAA19A6hy8kKGJBw"
             collection="board" doc_id="data"
             data={"state": {<state as read>, "page": "<full sha>"}} if_version=<read version>
```

Each sync round compares `state.page` with what that `git log` command prints
against the freshly fetched `origin/main`. When they differ, or `state.page` is
missing, the round publishes by the steps above. That covers the first publish
after the page's source entered the repository, and any merge whose author is
gone.

The page's `usable()` check tests only the fields it needs and accepts a
document that carries more, and no render path reads `state.page`. A document
with `state.page` set draws exactly what the same document without it draws.

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
   - Pull requests that are reviewed, green at the current head as the
     `rollup` entry above defines it, and carry no `working` entry of
     `kind: "build"` on their card. When the card's `needs` names an issue
     still in `tracker`, the owner can review now but not merge, so name each
     open blocker rather than present the pull request as ready to merge.
   - `planned` entries with `ready` of `decide`.
   - Questions a session handed back.
2. **Plans ready to build with no builder.** These are `planned` entries with
   `ready` of `build`, with no `working` entry, no open pull request, and no
   `needs` entry naming an issue still in `tracker`.
3. **The MVP milestone's remaining issues**, in `next`'s order, each with the
   blockers its `needs` names. A blocked issue is not a candidate until its
   blockers close, so name what it waits on rather than recommending it.

Link every issue and pull request number, as `[#NN](...)` for an issue and
`[PR #NN](...)` for a pull request. Say which candidates only the owner can act
on, and which a new session can start without them.

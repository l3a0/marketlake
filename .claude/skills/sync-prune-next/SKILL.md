---
name: sync-prune-next
description: Sync with origin, prune the worktrees and branches whose work is finished, check the Marketlake build board, and recommend what to take next on the current MVP milestone. Use when asked to "git sync prune", to clean up worktrees or branches, to "update the build board and say what's next", or for any status round of this repo's sessions and pull requests.
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
3. Check the build board.
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
`refs/remotes/origin/` names a branch on GitHub. `origin` is this checkout's
only remote, so a ref elsewhere under `refs/remotes/` was made locally, for
example by `git fetch origin pull/N/head:refs/remotes/pr/N`, and proves
nothing. Yet `git branch -r --contains` lists it like any remote branch, so
both loops keep only that command's `origin/` lines. On 2026-10-07 this
checkout held five such refs, among them `refs/remotes/pr/492` and
`refs/remotes/audit/pr697`, and
[#758](https://github.com/l3a0/marketlake/issues/758) found 34 clean worktrees
whose only evidence was a ref like these.

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
and not a search. On 2026-10-07,
`gh pr list --state merged --search 60ef88e` returned
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
again. A count of 0 means `gh` failed, so stop the round. With no refspecs on
its input, `git fetch --stdin` falls back to the configured refspec and prints
no failure.

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
on GitHub backs it. If a second remote is ever added, exclude its prefix from
that list as well, or the round deletes refs that remote owns. This loop runs
the same three tests as the one above.

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

## 3. Check the build board

A GitHub Action rebuilds the board from GitHub and publishes it at
<https://l3a0.github.io/marketlake/>, so a round writes nothing to it. This
step checks that the Action's last run succeeded. Then it builds the same
`board.json` into a scratch directory, so step 4 reads GitHub as it stands
rather than as the last run saw it.

This prints the board workflow's newest finished run that was not cancelled.

```bash
gh run list --repo l3a0/marketlake --workflow board.yml --status completed --limit 20 --json databaseId,conclusion,createdAt,event,url --jq 'map(select(.conclusion != "cancelled"))[0]'
```

A cancelled run is normal, so the command reads past it. The workflow's
`pages` concurrency group replaces a waiting run with a newer one, and 111 of
the sibling board's last 300 runs ended `cancelled` that way. A run whose
`conclusion` is not `success` goes in the report as a link, written
`[run <databaseId>](<url>)`. Pages keeps the last good board while runs fail,
so the page then shows GitHub as it stood before the failure. An output of
`null` means every one of the last 20 finished runs was cancelled, so raise
`--limit` and run it again. A board that is stale rather than failed goes
unreported until the stale banner in
[#874](https://github.com/l3a0/marketlake/issues/874) lands.

Then build the board into scratch. Replace `<scratch>` with the round's
scratch directory.

```bash
uv run python .claude/skills/sync-prune-next/build_board.py build --repo l3a0/marketlake --out <scratch>
```

It writes `<scratch>/board.json` and prints what the read cost in GraphQL
requests and points. Run it with `uv run python`, never `python3`. macOS's
`/usr/bin/python3` is 3.9 and cannot import the script. The script reads
GitHub through the session's `gh` login and fails closed, so a failed call
stops it with a non-zero exit before it writes `board.json`. Report the
failure. Step 4 then reads the open pull requests, their checks and their
review markers straight from GitHub, as "What the generator writes" below
describes `prs`.

### The board on GitHub Pages

<https://l3a0.github.io/marketlake/> serves `board.html` beside the
`board.json` that `.github/workflows/board.yml` builds from GitHub. These
events start a run.

- An issue event, such as an issue opened, edited, labeled or closed.
- An issue comment.
- A pull request into `main` opening, updating, reopening, closing or being
  edited.
- The CI or Infra workflow finishing.
- The schedule, twice an hour.

A submitted review starts no run, and neither does a "blocked by" link or a
sub-issue, so each reaches the board with the next run. `build_board.py`,
beside this skill, is the generator, and the page fetches its output every two
minutes. The same address serves the same board on every machine, and the
workflow copies `main`'s `board.html` on every run, so a merged change to the
page goes live with the next run.

`favicon.ico` beside this skill is the page's tab icon. The page links it as
`favicon.ico`, and the workflow's "Add the page" step copies it beside
`index.html`, so renaming it means changing the link, that copy and the three
tests that check them together.

### What the generator writes

```text
{ schema: 1, state, prs, working, planned, next, tracker }
```

- **`state`** holds `main`, the default branch's head as seven characters,
  `updatedAt`, the time GitHub was read, and `issues.open`. Nothing else is in
  it. The `deployed`, `lake`, `suite`, `page` and `issues.deferred` fields that
  rounds once wrote by hand are absent, because the page draws none of them.
- **`prs`** has one entry per open pull request and open issue it names,
  keyed by `issue`, as `{issue, pr, state, review, linked, partOf, reviewed,
  rollup}`. A pull request names an issue through GitHub's closing link or
  through a `Part of #N` reference outside code, so one naming two open issues
  has two entries and one naming no open issue has none. `state` is `open`.
  `linked` is true when the pull request's `closingIssuesReferences` names the
  issue, and `partOf` is true when only a `Part of` reference does. A pull
  request from a fork gets no entry, because GitHub registers a fork's closing
  keyword, so a stranger's fork reading `Closes #640` would otherwise move
  that issue into In flight.

  A reviewed, green card lands in the owner's queue, so `reviewed` is true
  only once the review is complete. Any review comment is not enough, because
  a session can post some lenses' results while another lens still runs. That
  sent [PR #713](https://github.com/l3a0/marketlake/pull/713) to the owner
  while its mutation lens was running. A review is complete when the pull
  request carries a comment or review from the owner whose whole first line is
  `## Review complete`, posted after the head commit's `committedDate`. The
  owner's are the ones whose `authorAssociation` is `OWNER`. Anyone can
  comment on this public repository, so a stranger's marker counts for
  nothing. A heading that only starts with those words, like
  `## Review completeness check`, does not count. Neither does a marker from
  an earlier round, because the commits pushed since then are code that review
  never saw. The generator applies this rule. To check one pull request by
  hand, count its markers with
  `gh pr view <n> --json comments,reviews,commits --jq '.commits[-1].committedDate as $head | [.comments[] | select(.createdAt > $head and .authorAssociation == "OWNER") | .body] + [.reviews[] | select(.submittedAt > $head and .authorAssociation == "OWNER") | .body] | map(select(test("\\A## Review complete\\r?(\\n|\\z)"))) | length'`.
  A count above zero means reviewed. A pull request whose final review was
  posted before this rule carries no marker, so it reads `reviewed: false`
  until a session posts one, and the round reports it rather than guessing
  whether its review finished. GitHub's review decision cannot tell whether
  the review ran, so it does not decide `reviewed`. The rule for writing the
  heading lives in `CLAUDE.md`, under "Pull requests", where review sessions
  read it. `review` is one of two fixed phrases, `review complete` or
  `no Review complete marker since the head commit`, so it never names the
  lens still running.

  `rollup` is a list of `[name, conclusion]` pairs for the checks on the head
  commit, and the conclusion is one of five words.

  1. `"success"` is a check that ran and passed: a check run's or a status
     context's SUCCESS.
  2. `"skipped"` is GitHub's SKIPPED, a job whose condition kept it from
     running. It counts as settled. The infra workflow's `tofu apply (live)`
     job is skipped on every pull request, because it runs only on main.
  3. `"failure"` is a check that failed, including one that timed out or
     could not start: a check run's FAILURE, TIMED_OUT and STARTUP_FAILURE,
     and a status context's FAILURE and ERROR.
  4. `"running"` is a check that has not concluded: a check run with no
     conclusion yet, and a status context's PENDING and EXPECTED.
  5. `"neutral"` is a check that concluded without passing or failing: a
     check run's NEUTRAL, CANCELLED, STALE and ACTION_REQUIRED.

  Those cover every value of GitHub's `CheckConclusionState` and
  `StatusState`. A value GitHub adds later is written as `"neutral"` until
  this list names it. The generator leaves out the board workflow's own
  checks, `board-build` and `board-deploy`. A `pull_request_target` run
  attaches them to the pull request's head commit, so without that a reviewed
  pull request would read as still running while the board rebuilds.

  The page reads a rollup as green when every entry is `"success"` or
  `"skipped"` and at least one is `"success"`. It reads any word outside
  these five as unsettled, the same as `"neutral"`, so an unknown word keeps a
  card out of the owner's queue rather than drawing a pass.
- **`working`**, **`planned`** and **`next`** are empty until
  [#873](https://github.com/l3a0/marketlake/issues/873) moves those
  judgements onto GitHub. The page still draws each one. A `working` entry,
  `{n, kind, what}`, marks a card a session is on, and one with
  `kind: "build"` draws under Building and keeps its pull request out of the
  owner's queue. A `planned` entry, `{n, passes, ready}`, marks a card whose
  plan is finished. A `next` entry, `{issue, band, order, ready}`, ranks a
  card. With `next` empty, the page's default view draws every card in the
  chosen milestone rather than only the ranked ones.
- **`tracker`** has one card per open issue, as `{n, label, labels, ms, kind,
  needs}`. Its `ms` is the milestone's title up to its first comma, so
  "MVP 2, capture on a hosted VM" reads `"MVP 2"`, or `"no milestone"`. Its
  `kind` is `parent` while an open sub-issue exists, else `deferred` under
  that label, else `ready`. The page also draws `decision` and `data`, which
  the generator never writes. Its `needs` holds the open issues in this
  repository that a native "blocked by" link names. Dependency sections in
  issue bodies are not read, so a blocker written only in a body is not on the
  board until someone adds the link.

The generator writes no `untracked`, which the page reads as empty.

The page's `usable()` check refuses the whole document when any one of these
fails. With nothing drawn yet, the page then shows only a banner saying the
document is missing fields. With a board already drawn, it keeps that board on
screen and says live updates stopped.

1. `state.main` and `state.updatedAt` are strings, and `state.issues.open` is
   a number.
2. `tracker` is a list with at least one entry, and `prs`, `working`, `planned`
   and `next` are lists.
3. Each `tracker` entry has a number `n`, a string `label` and `ms`, a `kind`
   of `ready`, `deferred`, `parent`, `decision` or `data`, and a `needs` list
   of numbers. An `after`, when present, is a list of numbers, and `labels`,
   when present, is a list of strings.
4. Each `prs` entry has a number `pr` and `issue`, a `state` of `open`,
   `merged` or `closed`, a string `review`, a boolean `reviewed`, and a
   `rollup` list whose every entry is a pair of strings. `linked` is a boolean
   or a list of issue numbers, and `partOf`, when present, is a boolean. The
   page reads a list as linked only when it names the entry's own `issue`.
5. Each `working` entry has a number `n` and a string `what`. A `kind`, when
   present, is a string.
6. Each `planned` entry has a number `n` and a number `passes`.
7. Each `next` entry has a number `issue` and a number `band`. An `order`,
   when present, is a number.
8. Each `planned` and `next` entry has a `ready` of `build`, `decide` or
   `plan`.

The numbers and the fixed sets are checked because the page writes those
fields into links and attributes without escaping them. The booleans are
checked because the page tests them for truth, and the string `"false"` is
true. The lists of numbers are checked because the page compares them against
card numbers, and a string in `needs` matches no card and frees a blocked one.

The page filters cards by milestone, with a selector that defaults to "MVP 2".
So a card's milestone comes only from `tracker`, and an issue missing from
`tracker` never draws, whatever `next` or `prs` say about it.

### Confirm the page drew it

A green run proves the workflow published the files. It does not prove the
page drew them, because the page refuses a document its `usable()` check
rejects and then shows only a banner. So open
<https://l3a0.github.io/marketlake/> and read what it drew.

1. **Look under both selectors.** Check the cards under "MVP 2", then again
   under "All". The selector defaults to "MVP 2", and bugs that can lose a
   captured minute and bugs in an alarm carry no milestone, so they never draw
   under the default.
2. **Read the page's text.** Pages serves the board as an ordinary page rather
   than inside a frame, so a browser tool's text extraction reads it. On a
   page drawn from live data, the banner under the strip is empty and the
   footer names the `main` sha the board was built against.

### Changing the page

The page's source is `.claude/skills/sync-prune-next/board.html`, and a change
to it goes through a pull request and the same review as any other code, per
`CLAUDE.md`. The workflow publishes `main`'s copy on its next run, so nothing
else publishes the page. Before the source entered the repository, fifty-four
versions of the board went out from sessions' scratch copies with no review and
no history, and on 2026-10-06 a bug in the column logic of one of them sent an
unfinished pull request to the owner's queue.

The workflow builds `board.json` and copies the page in the same run, so a
change that makes `usable()` or a render path require a new field ships with
the generator change that writes it, in the same pull request. That pull
request also updates "What the generator writes" above.

#### Test the page before the pull request

`tests/component/test_board_page.py` runs the page's script under a
JavaScript engine, with `dom-stub.js` standing in for the browser and a fake
fetch serving each `board.json` answer in turn, and reads what each element
drew. It runs under `node` where it is installed, as on CI's runner, and under
`osascript -l JavaScript` otherwise, which ships with macOS. With neither, it
skips. CI runs it on every pull request.

```bash
uv run pytest tests/component/test_board_page.py
```

Those tests feed documents someone thought of. To see the changed page draw
the board as GitHub stands, build `board.json` from live data and serve the
page beside it, laid out as the workflow lays them out.

```bash
uv run python .claude/skills/sync-prune-next/build_board.py build --repo l3a0/marketlake --out <scratch>/site
cp .claude/skills/sync-prune-next/board.html <scratch>/site/index.html
cp .claude/skills/sync-prune-next/favicon.ico <scratch>/site/favicon.ico
uv run python -m http.server 8799 --bind 127.0.0.1 --directory <scratch>/site
```

Run the server in the background, open <http://127.0.0.1:8799/> in a browser,
and read the page under "MVP 2" and under "All". Serve `main`'s copy of the
page, from
`git show origin/main:.claude/skills/sync-prune-next/board.html`, beside the
same `board.json` and compare the two. Every difference should be one the
change intends. Stop the server when done.

## 4. Recommend what to take next

Read `<scratch>/board.json`, the file step 3 built, rather than the page,
which can be a run behind. `CLAUDE.md`'s ranking directive and its exceptions
decide the order. Read them there rather than from a copy here.

Once [#873](https://github.com/l3a0/marketlake/issues/873) moves the ranking
onto GitHub, the file's `next` stores that order, so read `next` rather than
re-deriving it. Until then `next`, `planned` and `working` are empty. So rank
the candidates from the directive itself, and report that the round had no
`working` or `planned` entries to read.

When step 3 could not build the file, read the open pull requests, their
checks and their review markers straight from GitHub, as "What the generator
writes" in step 3 describes `prs`. Pass `--limit 1000` to every `gh` list
command that feeds a candidate, because `gh` stops at its limit without a
warning and its default is 30. A cut list prints the same way a complete one
does.

The open MVP milestone tracks the work
([MVP 2, capture on a hosted VM](https://github.com/l3a0/marketlake/milestone/5),
when this was written). An issue off that path carries no milestone and the
`deferred` label. Bugs that can lose a captured minute, and bugs in an alarm,
are never deferred, so they carry neither and stay candidates even without the
milestone. That is why step 3's check of the page looks under "All" as
well.

Collect candidates from three places, and give the evidence for each one.

1. **The owner's queue.** Nothing moves until the owner answers, so these come
   first.
   - Pull requests that are reviewed, green at the current head as step 3's
     `rollup` words define it, and carry no `working` entry of
     `kind: "build"` on their card. When the card's `needs` names an issue
     still in `tracker`, the owner can review now but not merge, so name each
     open blocker rather than present the pull request as ready to merge.
   - `planned` entries with `ready` of `decide`.
   - Questions a session handed back.
2. **Plans ready to build with no builder.** These are `planned` entries with
   `ready` of `build`, with no `working` entry, no open pull request, and no
   `needs` entry naming an issue still in `tracker`.
3. **The MVP milestone's remaining issues**, in `next`'s order, or in the
   directive's while `next` is empty, each with the blockers its `needs`
   names. A blocked issue is not a candidate until its
   blockers close, so name what it waits on rather than recommending it.

Link every issue and pull request number, as `[#NN](...)` for an issue and
`[PR #NN](...)` for a pull request. Say which candidates only the owner can act
on, and which a new session can start without them.

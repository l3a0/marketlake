#!/usr/bin/env bash
# Print `stale` when a newer commit on main changes a path that starts the Infra
# workflow, and `fresh` otherwise. Both exit 0. Anything else exits non-zero and prints
# nothing on stdout.
#
# The apply job runs this after its approval. Approving two waiting runs out of order
# would otherwise apply the older commit last. A stale run skips, and that is safe only
# because the newer commit started its own run, which applies the change. So this
# compares exactly the paths in the workflow's trigger, and excludes exactly what the
# trigger excludes, which is Markdown under infra/. A newer commit that changes only
# those files starts no run, so it leaves this run fresh rather than stranding the
# change this run applies. docs/design.md carries the reasoning.
#
# main is fetched again rather than read from origin/main, because actions/checkout
# writes the triggering commit there. GITHUB_SHA names the commit this run applies.
set -euo pipefail

: "${GITHUB_SHA:?GITHUB_SHA must name the commit this run applies}"

git fetch --quiet --no-tags --depth=1 origin refs/heads/main

# git diff exits 1 for a difference, and anything above 1 is an error. Passing its
# status straight through would turn a failed comparison into a skip.
status=0
git diff --quiet "$GITHUB_SHA" FETCH_HEAD -- \
  infra/ ':(exclude,glob)infra/**/*.md' .github/workflows/infra.yml || status=$?

case "$status" in
  0) echo fresh ;;
  1) echo stale ;;
  *) exit "$status" ;;
esac

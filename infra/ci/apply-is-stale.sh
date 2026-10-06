#!/usr/bin/env bash
# Print `stale` when a newer commit on main changes infra/ or the workflow, and `fresh`
# otherwise. Both exit 0. Anything else exits non-zero and prints nothing on stdout.
#
# The apply job runs this after its approval. Approving two waiting runs out of order
# would otherwise apply the older commit last. A stale run skips, because the newer
# commit's own run applies its change. A docs-only merge after this commit leaves the
# run fresh, so no pending apply is skipped.
#
# main is fetched again rather than read from origin/main, because actions/checkout
# writes the triggering commit there. GITHUB_SHA names the commit this run applies.
set -euo pipefail

: "${GITHUB_SHA:?GITHUB_SHA must name the commit this run applies}"

git fetch --quiet --no-tags --depth=1 origin refs/heads/main

# git diff exits 1 for a difference, and anything above 1 is an error. Passing its
# status straight through would turn a failed comparison into a skip.
status=0
git diff --quiet "$GITHUB_SHA" FETCH_HEAD -- infra/ .github/workflows/infra.yml || status=$?

case "$status" in
  0) echo fresh ;;
  1) echo stale ;;
  *) exit "$status" ;;
esac

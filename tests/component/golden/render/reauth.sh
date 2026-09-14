#!/bin/bash
# Marketlake control plane: the weekly Schwab re-auth.
#
# Written by `python -m lake.control_plane render`, which never runs it. Run
# it yourself, as the owner, at a terminal on a machine with a browser. It
# needs no root and calls no sudo.
#
# Usage. It runs from anywhere, so the rendered directory can be moved:
#
#     ./reauth.sh                     # from the rendered directory
#     ~/marketlake-install/reauth.sh  # or by path, from anywhere
#
# Any arguments are passed straight to the tool, so --config and --token
# point it at a throwaway file without editing this script.
#
# Schwab's refresh token dies every seven days and an interactive browser
# login is its only renewal. So this is a standing Sunday-evening ritual,
# not a step of the first install. The Sunday 20:00 canary pages when the
# week's login has not happened, and retries every 30 minutes until 23:00.
#
# It cannot run unattended, and this comment is not what stops that. The
# tool refuses when stdin is not a terminal, which is what a launchd job
# has. Without the refusal a plist pointed here would wait five minutes for
# a callback no browser is going to send, then fail, which reads as a broken
# job rather than a misuse of one. Do not add it to a plist.
#
# It reads schwab_callback_url from config.yaml, which must match the callback
# registered on the Schwab app. That key is optional for every other job,
# because no capture path reads it, so this is the one command that refuses
# without it. The tool prints the callback back so it can be checked against
# the registration. It prints no secret.
#
# Re-authing over a token that is still valid is fine and is the point. It
# costs one login and mints a fresher token, and the Sunday assertion tests
# freshness rather than validity. The write is atomic: a temp file beside
# the token, then one rename. A failure part-way through therefore leaves
# the working token where it was.
#
# Start the login from this script or from a bookmarked Schwab URL. Never
# from a link in a notification. Pages never carry auth links, so one that
# does is not from here.
#
# It unsets MARKETLAKE_CONFIG_DIR first. That variable moves the whole config
# directory for one process so a development run cannot reach the real
# token, and this is the one run that must reach it. This script inherits
# your shell, so an export left in a shell profile would otherwise send the
# week's token to a throwaway directory while the daemon kept reading the
# real one as it expired. The tool prints the token path it wrote, so the
# sign-off block is where to check this landed where you meant.
set -euo pipefail

unset MARKETLAKE_CONFIG_DIR
cd /Users/someone/marketlake
exec /opt/py/bin/python -m lake.reauth "$@"

---
name: reauth-vm
description: Renew the Schwab token the Marketlake capture VM runs on. The owner signs in to Schwab on the laptop through the rendered reauth.sh, which writes token.json and puts it in the SSM token parameter. The session then copies the parameter onto the VM and checks that the VM holds the new token and that the Sunday canary's next attempt passes. Use when asked to "reauth the vm", "re-auth Schwab", "refresh the Schwab token", "renew the token", or when the Sunday 20:00 reminder says the token does not cover the coming week.
---

# Re-auth the VM's Schwab token

Capture on the hosted VM stops when its Schwab refresh token dies, and the refresh token dies seven days after the browser login that minted it. Only a new browser login renews it, and the VM has no browser. So the login runs on the laptop each Sunday evening, and the token travels to the VM through one SSM parameter, `/marketlake/config/schwab-oauth-token`. The design doc's `## Auth: the seven-day token, engineered around` section carries the reasoning, and the README's `## Carry the Schwab token to a hosted VM` section carries every setting and exit code.

The token travels in three steps.

1. The owner runs `reauth.sh` on the laptop and signs in to Schwab. It writes the laptop's `~/.config/marketlake/token.json` and, because the laptop's `config.yaml` sets `token_store: both`, puts the same token into the parameter. The put signs as the role `marketlake-token-writer` with the laptop's own command key, so it needs no admin session.
2. The VM copies the parameter into its own `token.json` with `python -m lake.token_store pull`. The Sunday job runs that pull before each canary attempt, and the daemon runs it while capture is down on a dead token. Running it by hand skips the wait.
3. The daemon reads `token.json` again on every cycle, so it needs no restart.

Never print, quote or paste the token, the Schwab password, the app key or the app secret. The re-auth prints a Schwab authorization URL whose `client_id` is the app key, so quote only its closing sign-off block. Never put the VM's address, its instance id or the account id on GitHub. They go in chat only.

**Table:** Who runs each step

| Step | Who | Why |
| --- | --- | --- |
| 1. Check whether a re-auth is due | Session | Reads a mint time and no secret |
| 2. Run `reauth.sh` and sign in to Schwab | Owner | The Schwab sign-in is the owner's |
| 3. Sign in to AWS | Owner | `aws login` opens the owner's browser |
| 4. Find the VM, and start it if it is stopped | Session, on the owner's AWS session | Asking for the re-auth to reach the VM is asking for the VM to be up |
| 5. Pull on the VM and check its token | Session | Runs over the VM's SSH key |
| 6. Confirm the canary passes | Session | Reads the Sunday job's log |

## 1. Check whether a re-auth is due

```bash
python3 .claude/skills/reauth-vm/scripts/token_mint.py
```

It prints the laptop token's mint time, its expiry seven days later, and the hours left, in UTC and in Eastern time, and nothing from the token itself. The Sunday canary asks more than validity: the mint time plus seven days must clear the coming Friday's option close. So a token minted the previous Sunday evening is due again this Sunday, even with hours left. A re-auth over a token that is still valid is safe, because the write is atomic and the login only mints a fresher token.

## 2. The owner runs the re-auth

`reauth.sh` is the script `python -m lake.control_plane render` wrote for the laptop, in the directory its `--out` named. Find it, since that directory is the owner's choice:

```bash
ls ~/*/reauth.sh
```

Give the owner its full path in its own `bash` fence, to run in the Terminal panel, and wait. The path goes in chat only. It runs the main checkout's Python, so a checkout far behind `main` runs old code. It asks for ENTER, opens the Schwab login in a browser, and then the browser warns about a self-signed certificate on the `127.0.0.1` callback. That warning is expected, and the owner checks that the address matches the callback the script printed. The login waits five minutes for the callback.

Read its result with `read_terminal`, waiting for new output. A good run ends with this block:

```text
Schwab re-auth
  callback url:  https://127.0.0.1:<port>
  token path:    <home>/.config/marketlake/token.json
  token landed:  yes (replaced the previous token)
  parameter:     /marketlake/config/schwab-oauth-token version <n>
```

The `parameter:` line is the only proof on the laptop that the parameter changed, because the laptop has no grant to read it. Run `token_mint.py` again and see the new mint time.

**Table:** What a re-auth that went wrong means

| What it printed | What it means | Next step |
| --- | --- | --- |
| Exit 2 before the browser opened | A key in `config.yaml` is missing or malformed | The README's token section lists the keys. The owner edits `config.yaml` outside a session |
| Exit 3, `token.json` written and the parameter not | The put failed | The one line names the fix. `(AssumeRole <code>)` is the command key or the role. A denied put is `token_store_role_arn` or `token_store_region`. Anything else, run it again |
| `token landed: yes` and no `parameter:` line | The laptop's `token_store` is not `both`, or the checkout is old | Check `grep '^token_store:' ~/.config/marketlake/config.yaml`, then run it again |
| `token landed: no` | The login returned without writing a token | Run it again |
| The login timed out | No callback reached the script in five minutes | Run it again and finish the login sooner |

## 3. The owner signs in to AWS

```bash
aws sts get-caller-identity --profile marketlake-admin --query Arn --output text
```

The VM's address changes at every start, and only an admin session can look it up. When this prints `Your session has expired` or any other error, give the owner this command and wait.

```bash
aws login --profile marketlake-admin
```

## 4. Find the VM

```bash
aws ec2 describe-instances --profile marketlake-admin --region us-east-1 --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[State.Name,PublicIpAddress]' --output text
```

On a Sunday the VM runs from 19:30 ET until its nightly stop after 23:30, so it is usually `running`. When it reads `stopped`, start it and wait with the guarded command the `open-vm-dashboard` skill gives, or with the two commands in `CLAUDE.md`'s `## Deployment approvals` section. A VM started by hand stays up for at least an hour. When it reads `stopping`, look again in a minute. A VM left stopped still recovers on its own, because the daemon pulls on the first minute capture finds the token dead, but that first minute is a gap.

## 5. Pull on the VM and check its token

```bash
.claude/skills/reauth-vm/scripts/vm_check.sh <address> pull
```

It runs four things over SSH, as `ubuntu`, with the VM's key `~/.ssh/marketlake_vm` and the host key stored under the alias `marketlake-vm`.

1. The pull. `wrote ... minted <new time>` means it copied the new token. `is current` means the Sunday job's pull got there first. Both are good.
2. The VM token's mint time, which must equal the laptop's from step 2.
3. The Sunday job's lines from its current run.
4. Whether the daemon and the Sunday job are running. The Sunday job reads `activating` while it runs.

Any other pull line is one of the outcomes the README's token section lists, each with its fix. `Host key verification failed` goes to the owner. Never get past it with `StrictHostKeyChecking=no` or `accept-new`, because a changed key is the owner's to judge.

## 6. Confirm the canary passes

The Sunday job retries a failed canary every 30 minutes until 23:00, and pulls the token before each attempt. The canary makes one throwaway authenticated call to Schwab and checks the mint time covers the coming week. So the proof that capture can use the new token, on a night the market is closed, is the next attempt passing. Run `vm_check.sh <address>` again, without `pull`, once that attempt has had time to run.

1. A `sunday: canary call failed: VendorAuthError` line from an attempt that started before the pull is expected. On 2026-11-08 the re-auth landed at 20:31 ET and an attempt failed at 20:32, seconds before the VM's file changed.
2. The job prints each attempt's summary only when it ends, after its scrub and restore test. The good last line is `sunday: attempts=<n> pinged=True slug=sunday`. Each attempt's `sunday: token pull:` line should name the new mint time from its second attempt on.
3. On a weekday, the daemon's next cycle reads the new file. The dashboard shows the refresh token's mint time, through the `open-vm-dashboard` skill or the README's `## Reach the dashboard on a hosted VM` section.

## Report

Tell the owner what was checked, with the values read.

1. The re-auth's `token landed` line and the parameter's version number.
2. The laptop's and the VM's mint times, which match, and the new expiry.
3. The canary's result, or when its next attempt runs if it has not run yet.

Then post nothing on GitHub unless the re-auth turned up a fault worth an issue, and leave the VM's address out of anything that is posted.

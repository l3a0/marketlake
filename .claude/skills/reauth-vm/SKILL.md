---
name: reauth-vm
description: Renew the Schwab token the Marketlake capture VM runs on. The owner signs in to Schwab on the laptop through the rendered reauth.sh, which writes token.json and puts it in the SSM token parameter. The session then copies the parameter onto the VM and checks that the VM holds the new token and that the Sunday canary's next attempt passes. Use when asked to "reauth the vm", "re-auth Schwab", "refresh the Schwab token", "renew the token", or when the Sunday 20:00 reminder says the token does not cover the coming week.
---

# Re-auth the VM's Schwab token

Capture on the hosted VM stops when its Schwab refresh token dies, and the refresh token dies seven days after the browser login that minted it. Only a new browser login renews it, and the VM has no browser. So the login runs on the laptop each Sunday evening, and the token travels to the VM through the token parameter: one AWS Systems Manager Parameter Store entry, an encrypted `SecureString` named `/marketlake/config/schwab-oauth-token`. The design doc's `## Auth: the seven-day token, engineered around` section carries the reasoning, and the README's `## Carry the Schwab token to a hosted VM` section carries every setting and exit code.

The token travels in three steps.

1. The owner runs `reauth.sh` on the laptop and signs in to Schwab. It writes the laptop's `~/.config/marketlake/token.json` and, because the laptop's `config.yaml` sets `token_store: both`, puts the same token into the parameter. The put signs as the role `marketlake-token-writer`, assumed with the access key of the laptop's IAM user `marketlake-command`, so it needs no admin session.
2. The VM copies the parameter into its own `token.json` with `python -m lake.token_store pull`. The Sunday job runs that pull before each canary attempt, and the daemon runs it while capture is down on a dead token. Running it by hand skips the wait.
3. The daemon reads `token.json` again on every cycle, so it needs no restart.

The canary is the Sunday job's check that the token works. It makes one throwaway authenticated call to Schwab, and asserts that the token's mint time plus seven days clears the coming Friday's option close.

Never print, quote or paste the token, the Schwab password, the app key or the app secret. The re-auth prints a Schwab authorization URL whose `client_id` is the app key, so never read the terminal above the closing sign-off block. Never put the VM's address, its instance id, the account id or a token path on GitHub. They go in chat only.

**Table:** Who runs each step

| Step | Who | Why |
| --- | --- | --- |
| 1. Check whether a re-auth is due | Session | Prints a mint time and no secret |
| 2. Run `reauth.sh` and sign in to Schwab | Owner | The Schwab sign-in is the owner's |
| 3. Sign in to AWS | Owner | `aws login` opens the owner's browser |
| 4. Find the VM's state and address | Session, reading with the owner's AWS session | The address changes at every start, and only an admin session can read it |
| 4. Start the VM, if it is stopped | Owner | `CLAUDE.md`'s `## Deployment approvals` section gives the start and wait commands to the owner |
| 5. Pull on the VM and check its token | Session | Runs over the VM's SSH key |
| 6. Confirm the canary passes | Session | Reads the Sunday job's log |

## 1. Check whether a re-auth is due

```bash
python3 .claude/skills/reauth-vm/scripts/token_mint.py
```

It prints the laptop token's mint time, its expiry seven days later, and the hours left, in UTC and in Eastern time, and nothing from the token itself. The canary asks more than validity, so a token minted the previous Sunday evening is due again this Sunday, even with hours left.

Run the re-auth while the market is closed, as on a Sunday evening. Whether a new login revokes the previous refresh token has not been measured, as the design's Auth section says. If it does, a re-auth during the session kills the VM's token until the VM pulls the new one, and every minute in between is a gap.

## 2. The owner runs the re-auth

`reauth.sh` is the script `python -m lake.control_plane render` wrote for the laptop, in the directory its `--out` named. Find it, since that directory is the owner's choice:

```bash
ls ~/*/reauth.sh
```

Give the owner its full path in its own `bash` fence, to run in the Terminal panel, and wait. The path goes in chat only.

The script runs the main checkout's Python, so a checkout far behind `main` runs old code. It asks for ENTER, opens the Schwab login in a browser, and then the browser warns about a self-signed certificate on the `127.0.0.1` callback. That warning is expected, and the owner checks that the address matches the callback the script printed. The login waits five minutes for the callback.

Once the owner says it finished, read only the sign-off block, with `read_terminal` and `lines: 6`, which stops below the authorization URL. A good run ends like this:

```text
Schwab re-auth
  callback url:  https://127.0.0.1:<port>
  token path:    <home>/.config/marketlake/token.json
  token landed:  yes (replaced the previous token)
  parameter:     /marketlake/config/schwab-oauth-token version <n>
```

The laptop has no grant to read the parameter, so the `parameter:` line is the laptop's only proof that the put landed. Step 5's pull line is the VM's proof. Run `token_mint.py` again and see the new mint time.

**Table:** What a re-auth that went wrong means

| What it printed | What it means | Next step |
| --- | --- | --- |
| Exit 2 before the browser opened | stdin is not a terminal, `schwab_callback_url` is missing from `config.yaml`, or a key the put needs is missing or malformed | Run it from a terminal. For a key, the README's token section lists them, and the owner edits `config.yaml` outside a session |
| Exit 3, `token.json` written and the parameter not | The put failed | The one line names the fix. `(AssumeRole <code>)` is the command key or the role. A denied put is `token_store_role_arn` or `token_store_region`. Anything else, run it again |
| Exit 3 with a line naming a config problem | The laptop's `token_store` value is mistyped, so the put was skipped | Fix the value in `config.yaml`. Running it again repeats the same failure |
| Exit 0, `token landed: yes` and no `parameter:` line | The laptop's `token_store` is not `both`, or the checkout is old | Check `grep '^token_store:' ~/.config/marketlake/config.yaml`, then run it again |
| `token landed: no` | The login returned without writing a token | Run it again |
| A traceback and no sign-off block, such as `RedirectTimeoutError` | The login timed out after five minutes, or Schwab refused the code exchange. No token was written, and the old one stands | Run it again and finish the login sooner |

## 3. The owner signs in to AWS

```bash
aws sts get-caller-identity --profile marketlake-admin --query Arn --output text
```

When this prints `Your session has expired` or any other error, give the owner this command and wait.

```bash
aws login --profile marketlake-admin
```

## 4. Find the VM

```bash
aws ec2 describe-instances --profile marketlake-admin --region us-east-1 --filters Name=tag:marketlake:host,Values=capture Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[State.Name,PublicIpAddress]' --output text
```

On a Sunday the VM runs from 19:30 ET until its nightly stop after 23:30, so it is usually `running`.

- **`running`:** go on to step 5 with its address.
- **`pending` or `stopping`:** look again in a minute.
- **`stopped`:** give the owner the start and wait commands from `CLAUDE.md`'s `## Deployment approvals` section, each in its own `bash` fence, and wait. A VM started by hand stays up for at least an hour. Run the lookup above again for its new address, since the wait prints none. A VM that reads `running` can still refuse SSH for some seconds after boot, so when step 5 exits 255 right after a start, run it again a minute later.

A VM left stopped still recovers, but later. When it was down at 20:00 on Sunday, its 07:30 Monday boot replays the Sunday job, which pulls the token before the open. Otherwise the daemon pulls on the first capture minute it finds the token dead, and it does not wait for the pull, so at least that first minute is a gap.

## 5. Pull on the VM and check its token

```bash
.claude/skills/reauth-vm/scripts/vm_check.sh <address> pull
```

It runs four things over SSH, as `ubuntu`, with the VM's key `~/.ssh/marketlake_vm` and the host key stored under the alias `marketlake-vm`.

1. The pull. `wrote ... minted <time>` means it copied the token, and `is current` means the Sunday job's pull got there first. Either is good only when the mint time it names is the laptop's new one. An `is current` with last week's time means the put never landed, so go back to step 2's sign-off.
2. The VM token's mint time, which must equal the laptop's.
3. The Sunday job's lines from its current run, each with its Eastern time. `(this run has printed no sunday: line yet)` is normal for a run still going.
4. Whether the daemon and the Sunday job are running. The Sunday job reads `activating` while it runs.

It exits 0 when every step worked, 1 when the pull or the mint read failed, and 255 when ssh could not reach the VM. Exit 255 usually means the VM stopped or its address changed, so look again with step 4. Any other pull line is one of the outcomes the README's token section lists, each with its fix. `Host key verification failed` goes to the owner. Never get past it with `StrictHostKeyChecking=no` or `accept-new`, because a changed key is the owner's to judge.

## 6. Confirm the canary passes

The Sunday job starts at 20:00 ET and retries a failed attempt every 30 minutes, pulling the token before each one. No attempt may start after 23:00, and the job starts a few seconds after 20:00, so the last attempt runs at about 22:30. On a night the market is closed, the proof that capture can use the new token is an attempt after the pull that passes. Run `vm_check.sh <address>` again, without `pull`, once the next attempt has had time to run.

- **What prints when:** a failed canary call prints `sunday: canary call failed: <error>` at once. Every other line, each attempt's `token pull:` line and the closing `sunday: attempts=<n> pinged=<bool> slug=sunday` among them, prints together only when the whole job ends.
- **Before the pull:** a `canary call failed: VendorAuthError` or `canary call returned http 401` from an attempt that started before the pull is expected. Compare its time with the pull's. On 2026-11-08 the re-auth landed at 20:31 ET, and the attempt that started at about 20:30 failed at 20:32.
- **The token passed:** no attempt after the pull prints `canary call failed`, `canary call returned http`, or `token mint plus seven days does not clear the coming week`.
- **`pinged=False` with none of those lines:** the token is fine and something else in the Sunday job failed, such as the scrub, the backup or the restore test. The job's own lines name it.
- **After the last attempt:** no attempt is left to prove the token. Step 5's mint times are the evidence, the Sunday check stays failed until a run pings it, and whether to start `com.marketlake.sunday.service` by hand is the owner's decision. That run makes one attempt, with a full scrub.
- **On a weekday:** the daemon's next cycle reads the new file. The dashboard shows the refresh token's mint time, through the README's `## Reach the dashboard on a hosted VM` section.

## Report

Tell the owner what was checked, with the values read.

1. The re-auth's `token landed` line and the parameter's version number.
2. The laptop's and the VM's mint times, which match, and the new expiry.
3. The canary's result, or when its next attempt runs if it has not run yet.

Post nothing on GitHub unless the re-auth turned up a fault worth an issue, and leave the VM's address and the token paths out of anything that is posted.

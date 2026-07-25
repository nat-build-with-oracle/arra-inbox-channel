---
name: configure
description: Set up and inspect the arra-inbox channel — choose the watched directory, mute noisy message types, replay recent history, and check what the channel is currently tailing. Use when the user asks to configure the inbox channel, asks "what am I watching", "why didn't I get that message", wants to replay backlog, or wants channel status.
user-invocable: true
allowed-tools:
  - Read
  - Write
  - Edit
  - Bash(ls *)
  - Bash(cat *)
  - Bash(mkdir *)
  - Bash(chmod *)
  - Bash(tail *)
  - Bash(wc *)
  - Bash(rg *)
---

# arra-inbox: configure

The inbox channel's wire is a **directory of append-only JSONL files**. There is no token, no
broker, no URL — configuration is just "which directory, and which lines do I care about."

## Where things live

| what | path |
|---|---|
| watched directory | `$INBOX_DIR` — default `~/.maw/inbox` |
| channel state | `$INBOX_STATE_DIR` — default `~/.claude/channels/inbox` |
| read position | `<state>/cursor.json` — byte offset per file |
| replies | `<state>/outbox/<room>.jsonl` |
| debug log | `<state>/inbox-channel.log` |
| optional env | `<state>/.env` (real env always wins) |

A **room** is a file basename: `~/.maw/inbox/digger-oracle.jsonl` → room `digger-oracle`.

## Steps

1. **Report current state.** Read `<state>/cursor.json` and list `$INBOX_DIR/*.jsonl` with line
   counts. Show which rooms exist, which have a cursor (being tailed), and how far behind each is.
   A room with no cursor entry has never been seen — that is normal until the server next scans.

2. **Check the log before theorising.** `tail -40 <state>/inbox-channel.log` answers most
   "why didn't I get that message" questions directly: it records every notification, every
   rotation reset, and every rejected line.

3. **Write settings to `<state>/.env`** (one `KEY=value` per line), then tell the user the channel
   must be restarted for them to take effect — the server reads `.env` once at boot.

## Settings

| var | default | meaning |
|---|---|---|
| `INBOX_DIR` | `~/.maw/inbox` | directory to tail |
| `INBOX_EXT` | `.jsonl` | file extension that counts as a room |
| `INBOX_POLL_MS` | `1000` | poll floor; `fs.watch` delivers faster when it works |
| `INBOX_REPLAY` | `0` | `0` = start at EOF, `N` = replay last N lines per file, `all` = full backfill |
| `INBOX_ROOMS` | (all) | comma-separated allowlist of room names |
| `INBOX_IGNORE_ROOMS` | — | comma-separated denylist |
| `INBOX_IGNORE_TYPES` | — | mute record types, e.g. `done` to silence maw worktree churn |
| `INBOX_IGNORE_FROM` | — | mute senders by `from` field |
| `INBOX_REPLY_MODE` | `outbox` | `outbox` = local file only; `peer` = ALSO append to `<INBOX_DIR>/<chat_id>.jsonl` |
| `INBOX_SELF` | `assistant` | the `from` name stamped on outbound records |
| `INBOX_MAX_PER_TICK` | `20` | burst ceiling per file per tick (excess is deferred, never dropped) |

### The two settings people actually want

- **Too noisy?** Real maw inboxes are ~100% `type:"done"` worktree notifications. Set
  `INBOX_IGNORE_TYPES=done` and only human-ish traffic reaches the session.
- **Missed something while detached?** Set `INBOX_REPLAY=20` for one session to backfill the last
  20 lines per room, then set it back to `0`. Replayed messages carry `meta.replayed = "true"`.

## Safety notes worth saying out loud

- **Filesystem permissions are the entire access boundary.** Anything that can append to a file in
  `INBOX_DIR` can put words into the session. The server refuses to boot if `INBOX_DIR` is
  world-writable; do not work around that by chmod'ing it back.
- `INBOX_REPLY_MODE=peer` writes into *another* oracle's inbox file. That is a side effect on
  shared state — confirm with the user before enabling it.
- Never set `INBOX_DIR` to a directory synced from an untrusted or shared source.

## Sending a test message

```bash
echo '{"ts":"'"$(date -u +%FT%TZ)"'","from":"nat","type":"msg","msg":"ping"}' >> ~/.maw/inbox/test.jsonl
```

A bare line works too — `echo "ping" >> ~/.maw/inbox/test.jsonl` — it arrives as `user: inbox`.

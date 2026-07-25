# arra-inbox-channel

A Claude Code **channel** whose wire is the local filesystem.

```bash
echo "the build finished" >> ~/.maw/inbox/ci.jsonl
```

That line becomes a conversation turn in a running Claude Code session. No HTTP. No broker.
No sockets. No daemon to keep alive. The transport is `>>`.

```
┌─────────────┐   append    ┌──────────────────────┐   tail    ┌──────────────┐
│ any process │ ──────────▶ │ ~/.maw/inbox/*.jsonl │ ────────▶ │ Claude Code  │
│ echo · cron │             │  (append-only)       │           │   session    │
│ maw hey     │             └──────────────────────┘           └──────┬───────┘
└─────────────┘                                                       │ reply
                            ┌──────────────────────┐                  │
                            │ <state>/outbox/*.jsonl│ ◀───────────────┘
                            └──────────────────────┘
```

## Why

Every other channel needs something listening on a port — a Discord gateway, an MQTT broker, a
webhook worker. That is a lot of moving infrastructure for "tell me when the thing finishes."

A file is already there. It survives reboots, it's greppable, it works offline, on a plane, inside
a locked-down VM, and its access control is `chmod`. If a process on your machine can write a file,
it can talk to Claude — and if it can't, it can't.

The default wire is [maw](https://github.com/Soul-Brews-Studio/maw-js)'s federation inbox
(`~/.maw/inbox/<oracle>.jsonl`), so an existing oracle fleet becomes reachable with zero changes to
the senders. But any directory of JSONL works, and so does any directory of plain text lines.

## Install

```bash
claude plugin marketplace add nat-build-with-oracle/arra-channels
claude plugin install arra-inbox@arra-channels
claude --channels plugin:arra-inbox@arra-channels
```

During the channels research preview, custom channels also need
`--dangerously-load-development-channels server:inbox` **or** an `allowedChannelPlugins` entry in
settings, unless the plugin is on Anthropic's built-in allowlist.

## The wire format

A room is a file. The room name is the basename: `~/.maw/inbox/digger-oracle.jsonl` → room
`digger-oracle`. Each line is one message. Three shapes are accepted:

```jsonl
{"ts":"2026-07-25T20:00:00.000Z","from":"digger-buddy","type":"msg","msg":"vault index finished"}
{"from":"ci","text":"build #412 green"}
just a bare line of text
```

The first is maw's native record and round-trips exactly. The second covers other producers
(`text` / `content` / `message` are all read). The third means `echo` is a valid client.

Inbound becomes a standard channel notification:

```jsonc
{
  "method": "notifications/claude/channel",
  "params": {
    "content": "vault index finished",
    "meta": {
      "chat_id": "digger-oracle",      // the room = the file basename
      "message_id": "digger-oracle-a1b2c3d4e5f6",  // sha256 of the line — stable across restarts
      "user": "digger-buddy",          // the `from` field
      "ts": "2026-07-25T20:00:00.000Z",
      "inbox_type": "msg"              // the `type` field
    }
  }
}
```

Provenance lives in `meta`, never in `content` — an in-content annotation would be forgeable by
anyone who can type that string into the file.

## Replies

`reply` appends a maw-shaped record to `<state>/outbox/<room>.jsonl`. That is inert by design:
Claude answering does not write into anyone else's mailbox unless you ask for it.

Set `INBOX_REPLY_MODE=peer` and replies *also* append to `<INBOX_DIR>/<chat_id>.jsonl`, which is a
peer oracle's inbox — two Claude sessions watching the same directory then hold a conversation
through nothing but files.

`edit_message` appends an `type:"edit"` record referencing the original id. The outbox is
append-only; nothing is ever rewritten in place.

## Behaviour that took thought

**It starts at EOF.** Attaching the channel does not dump your backlog into the session. On this
machine that would have been 354 historical records across 30 rooms, at 3am. Files that appear
*after* boot start at offset 0 instead — a new room has no history, so its first message (the one
that matters most) is delivered in full.

**It never loses a line to a burst.** The per-tick notification cap defers rather than drops: the
cursor advances only over bytes actually handled, so a 37-line atomic append with a cap of 5
arrives as 37 messages across 8 ticks, in order. (Tested.)

**It never delivers half a line.** Reads stop at the last newline in the window. A writer caught
mid-append leaves its bytes unread until the newline lands. This is also why cutting at a newline
means a multibyte character can never be split — Thai and emoji round-trip intact.

**It never re-delivers on restart.** The byte offset per file is persisted to `cursor.json` via
write-temp-then-rename, so a crash mid-write cannot leave a truncated cursor that silently replays
or skips. Truncation and inode changes are detected and reset the offset.

**Poll is the floor, `fs.watch` is the accelerator.** `fs.watch` is unreliable across network
filesystems, editors that write-and-rename, and some platforms. It is used to make delivery feel
instant; a 1s poll guarantees delivery even where it fails entirely.

**Byte accounting never touches decoded text.** Line lengths are measured on the raw buffer.
Decoding first and measuring the string would be a slow-acting corruption bug: `toString('utf8')`
maps every invalid byte to `U+FFFD`, which re-encodes to *three* bytes, so a single stray `0x80`
inflates the count by two — and an offset pushed past EOF trips the rotation check, which resets
to zero and re-delivers the whole file, forever. Cutting at a newline prevents a mid-rune split;
it does not make the bytes valid UTF-8.

**One bad file cannot starve the others.** Scanning is isolated per file, not per tick. In a
shared federation inbox a `chmod 000` file from another user is routine, and a single try/catch
around the whole loop would let it silently starve every alphabetically-later room for as long as
it sat there.

**A rewrite in place is detected too.** `>file` followed by *more* content than before leaves the
size above our offset and the inode unchanged, so neither the shrink nor the inode check fires.
The byte before the cursor must be the newline we stopped at; when it isn't, the file was
replaced underneath us and the cursor resets.

**A slow writer is not mistaken for an over-long line.** In a single sample, "a 300KB record still
being written" and "a line past the cap" look identical. The skip only fires once the file has
stopped growing.

## Access control

**Filesystem permissions are the entire boundary** — the same delegation `arra-mqtt` makes to
broker auth and topic namespaces. Anything that can append to a file in `INBOX_DIR` can speak into
the session.

The server therefore **refuses to boot** if `INBOX_DIR` is world-writable, and `reply`'s
model-supplied `chat_id` is validated against `[A-Za-z0-9._-]` with no leading dot before it is
ever joined into a path.

**Symlinked rooms are refused in both directions.** `appendFileSync` follows symlinks, so
`evil.jsonl -> ~/.ssh/authorized_keys` would turn a reply into an append to `authorized_keys`; the
read side is worse, since a room symlinked at `~/.ssh/id_rsa` would stream a private key into the
conversation line by line. `lstat` — which does not follow — refuses both.

**Anyone who can write to the wire can claim any identity.** `meta.user` comes from the record's
`from` field, so a writer can put any name on a message. That is inherent to the transport, not a
gap in it: write access to `INBOX_DIR` is full trust, exactly as broker credentials are for
`arra-mqtt`. Treat `meta.user` as a label, never as authentication.

Keep `INBOX_DIR` local, private, and not synced from anywhere you don't control.

## Configuration

All optional. Real environment wins over `<state>/.env`.

| var | default | meaning |
|---|---|---|
| `INBOX_DIR` | `~/.maw/inbox` | directory to tail |
| `INBOX_STATE_DIR` | `~/.claude/channels/inbox` | cursor, outbox, log |
| `INBOX_EXT` | `.jsonl` | extension that counts as a room |
| `INBOX_POLL_MS` | `1000` | poll floor in ms |
| `INBOX_REPLAY` | `0` | `0` = EOF, `N` = last N lines per file, `all` = everything |
| `INBOX_ROOMS` | (all) | allowlist of room names |
| `INBOX_IGNORE_ROOMS` | — | denylist of room names |
| `INBOX_IGNORE_TYPES` | — | mute record types — `done` silences maw worktree churn |
| `INBOX_IGNORE_FROM` | — | mute senders |
| `INBOX_REPLY_MODE` | `outbox` | `outbox` or `peer` |
| `INBOX_SELF` | `assistant` | `from` name on outbound records |
| `INBOX_MAX_PER_TICK` | `20` | burst ceiling per file per tick (deferred, not dropped) |
| `INBOX_MAX_LINE_KB` | `256` | a single line larger than this is skipped |
| `INBOX_MAX_READ_KB` | `1024` | max bytes consumed per file per tick |

`/arra-inbox:configure` walks through all of it and reports what is currently being tailed.

## Recipes

```bash
# a long build tells you when it's done
(make -j8 && echo '{"from":"make","type":"msg","msg":"build green"}' \
  || echo '{"from":"make","type":"msg","msg":"BUILD FAILED"}') >> ~/.maw/inbox/build.jsonl

# tail a log into the session, filtered
tail -F /var/log/app.log | rg --line-buffered ERROR >> ~/.maw/inbox/errors.jsonl

# cron, with no network anywhere in the path
*/30 * * * * /usr/local/bin/backup.sh >> ~/.maw/inbox/cron.jsonl 2>&1
```

Anything that can append to a file is now a Claude Code client. That is the whole idea.

## Development

```bash
bun install
bun --env-file=/dev/null run test-e2e.ts   # 92 e2e assertions, real stdio, real files
```

The suite drives the actual server over real JSON-RPC stdio against real temp directories —
nothing is mocked. It asserts the "no network" promise structurally (no net imports, no `fetch`,
no `WebSocket`), and covers partial lines, rotation, in-place rewrite, restart resume, burst
deferral, CRLF, unicode, invalid UTF-8, over-long lines, unreadable files, path traversal,
symlink refusal, self-echo, garbage config, cursor namespacing, and the world-writable refusal.

Most of those cases exist because a 5-lens adversarial audit found them in an earlier version and
proved each one against the running server. The findings, and the reasoning that killed the false
ones, are worth reading before changing the tailing logic.

> `--env-file=/dev/null` matters: bun autoloads `cwd/.env` before the server runs, which would be
> indistinguishable from real environment. The flag must be on the **innermost** bun, because
> `bun run <script>` re-spawns bun and autoloads again.

## License

Apache-2.0

/**
 * inbox-channel — a Claude Code channel plugin whose inbound wire is the LOCAL FILESYSTEM.
 *
 * No HTTP. No broker. No sockets. No network of any kind. The wire is a directory of
 * append-only JSONL files; a line appended to a file becomes a conversation turn.
 *
 * Same channel contract as arra-discord / arra-mqtt:
 *   inbound : new line in <INBOX_DIR>/<room>.jsonl
 *             → notification { method: 'notifications/claude/channel', params: { content, meta } }
 *   outbound: reply / edit_message tool → append a line to <STATE_DIR>/outbox/<room>.jsonl
 *
 * Default wire is maw's federation inbox (~/.maw/inbox/<oracle>.jsonl), whose records look like
 *   {"ts":"2026-05-25T15:36:17.810Z","from":"digger-buddy-team","type":"done","msg":"…","thread":null}
 * but ANY directory of JSONL — or of plain text lines — works. `echo hi >> $INBOX_DIR/room.jsonl`
 * is a complete, valid client.
 *
 * ACCESS CONTROL: filesystem permissions ARE the boundary. Anything that can append to a file in
 * INBOX_DIR can speak into the session — exactly like arra-mqtt delegates to broker auth + topic
 * namespace. Keep INBOX_DIR on a local, non-shared, non-world-writable path. The server refuses
 * to run against a world-writable directory (see assertInboxDirSane).
 */
// Low-level Server is REQUIRED for channels: the high-level McpServer wrapper does not
// propagate the experimental `claude/channel` capability, so Claude Code never routes
// inbound notifications as channel messages. The SDK sanctions Server for this advanced case.
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import {
  appendFileSync, chmodSync, closeSync, mkdirSync, openSync, readFileSync,
  readSync, readdirSync, realpathSync, renameSync, statSync, watch, writeFileSync,
} from 'node:fs'
import { basename, join, sep } from 'node:path'
import { homedir } from 'node:os'
import { createHash } from 'node:crypto'

const VERSION = '0.1.0'

const STATE_DIR = process.env.INBOX_STATE_DIR ?? join(homedir(), '.claude', 'channels', 'inbox')
mkdirSync(STATE_DIR, { recursive: true })

// debug log to BOTH stderr and a file (subprocess stderr is hard to inspect live)
const LOG_FILE = join(STATE_DIR, 'inbox-channel.log')
const dlog = (m: string) => {
  const line = `[${new Date().toISOString()}] ${m}`
  process.stderr.write(line)
  try { appendFileSync(LOG_FILE, line) } catch {}
}

// Load STATE_DIR/.env — real env wins (same idiom as arra-discord / arra-mqtt).
// FOOTGUN: bun autoloads cwd/.env BEFORE this file runs, so a .env sitting in the plugin
// cache would be indistinguishable from real env and win here. Every bun invocation of this
// server must pass --env-file=/dev/null — and the flag must be on the INNERMOST bun, because
// `bun run <script>` re-spawns bun, which autoloads again.
const ENV_FILE = join(STATE_DIR, '.env')
try {
  chmodSync(ENV_FILE, 0o600)
  for (const line of readFileSync(ENV_FILE, 'utf8').split('\n')) {
    const m = line.match(/^(\w+)=(.*)$/)
    if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2]
  }
} catch {}

const INBOX_DIR = process.env.INBOX_DIR ?? join(homedir(), '.maw', 'inbox')
const POLL_MS = Math.max(100, Number(process.env.INBOX_POLL_MS ?? '1000'))
const EXT = process.env.INBOX_EXT ?? '.jsonl'
// Replay: 0 = start at EOF (default — a fresh attach must never dump months of backlog into
// the session), N = emit the last N lines of each file on boot, 'all' = full backfill.
const REPLAY_RAW = (process.env.INBOX_REPLAY ?? '0').trim().toLowerCase()
const REPLAY_ALL = REPLAY_RAW === 'all'
const REPLAY_N = REPLAY_ALL ? Infinity : Math.max(0, Number(REPLAY_RAW) || 0)
// Guard against a pathological single line (a runaway writer) eating memory.
const MAX_LINE_BYTES = Number(process.env.INBOX_MAX_LINE_KB ?? '256') * 1024
// Per-tick read ceiling: a huge append is consumed across several ticks rather than in one gulp.
const MAX_READ_BYTES = Number(process.env.INBOX_MAX_READ_KB ?? '1024') * 1024
// Burst guard: if a writer floods, don't fire thousands of notifications into the session.
const MAX_NOTIFY_PER_TICK = Math.max(1, Number(process.env.INBOX_MAX_PER_TICK ?? '20'))

const csv = (v: string | undefined) =>
  (v ?? '').split(',').map(s => s.trim()).filter(Boolean)

const ONLY_ROOMS = new Set(csv(process.env.INBOX_ROOMS))          // allowlist of room names
const IGNORE_ROOMS = new Set(csv(process.env.INBOX_IGNORE_ROOMS)) // denylist of room names
const IGNORE_TYPES = new Set(csv(process.env.INBOX_IGNORE_TYPES)) // e.g. "done" to mute worktree churn
const IGNORE_FROM = new Set(csv(process.env.INBOX_IGNORE_FROM))
// Reply delivery. 'outbox' (default) = append to our own outbox file only — inert and loop-safe.
// 'peer' = ALSO append a record into <INBOX_DIR>/<peer>.jsonl so another oracle's inbox channel
// picks it up. Opt-in, because writing into someone else's inbox is a side effect on shared state.
const REPLY_MODE = (process.env.INBOX_REPLY_MODE ?? 'outbox').trim().toLowerCase()
const SELF = process.env.INBOX_SELF ?? '' // our own room name; used to tag outbound `from`

const OUTBOX_DIR = join(STATE_DIR, 'outbox')
const CURSOR_FILE = join(STATE_DIR, 'cursor.json')

/**
 * A world-writable inbox dir means any local user can inject a conversation turn. Since the
 * filesystem IS the access boundary here, that is the one misconfiguration worth refusing to
 * boot on rather than logging past.
 */
function assertInboxDirSane(): void {
  try {
    const st = statSync(INBOX_DIR)
    if (!st.isDirectory()) {
      dlog(`inbox channel: FATAL — INBOX_DIR is not a directory: ${INBOX_DIR}\n`)
      process.exit(1)
    }
    // 0o002 = world-writable. Sticky bit does not help: we care about appends to existing files.
    if (process.platform !== 'win32' && st.mode & 0o002) {
      dlog(
        `inbox channel: FATAL — INBOX_DIR is world-writable (mode ${(st.mode & 0o777).toString(8)}): ${INBOX_DIR}\n` +
        'Filesystem permissions are this channel\'s ONLY access boundary; a world-writable\n' +
        'directory lets any local process speak into the session. Fix with:\n' +
        `  chmod o-w ${INBOX_DIR}\n`,
      )
      process.exit(1)
    }
  } catch {
    // Not existing yet is fine — we create it and poll an empty dir.
    try { mkdirSync(INBOX_DIR, { recursive: true, mode: 0o700 }) } catch {}
  }
}
assertInboxDirSane()

// --- cursor: byte offset per file, so a restart resumes exactly where it stopped ---
type Cursor = { offset: number; size: number; ino: number }
let cursors: Record<string, Cursor> = {}
try { cursors = JSON.parse(readFileSync(CURSOR_FILE, 'utf8')) } catch {}

let cursorDirty = false
function saveCursors(): void {
  if (!cursorDirty) return
  // atomic: write a temp file in the same dir, then rename. A crash mid-write can never
  // leave a truncated cursor.json that would silently replay or skip messages.
  const tmp = `${CURSOR_FILE}.${process.pid}.tmp`
  try {
    writeFileSync(tmp, JSON.stringify(cursors, null, 2))
    renameSync(tmp, CURSOR_FILE)
    cursorDirty = false
  } catch (err) {
    dlog(`inbox channel: cursor save failed: ${err instanceof Error ? err.message : err}\n`)
  }
}

// --- room naming: a file basename is the room; never let it escape the directory ---
function roomOf(file: string): string {
  return basename(file, EXT)
}
/** Room names come from OUR readdir, but reply's chat_id comes from the model. Sanitize hard. */
function assertSafeRoom(room: string): void {
  if (!room || room.length > 128) throw new Error(`invalid room name: ${JSON.stringify(room)}`)
  if (!/^[A-Za-z0-9._-]+$/.test(room) || room === '.' || room === '..' || room.startsWith('.')) {
    throw new Error(`invalid room name (expected [A-Za-z0-9._-], no leading dot): ${JSON.stringify(room)}`)
  }
}

function roomAllowed(room: string): boolean {
  if (IGNORE_ROOMS.has(room)) return false
  if (ONLY_ROOMS.size && !ONLY_ROOMS.has(room)) return false
  return true
}

// --- the MCP server ---
const mcp = new Server(
  { name: 'inbox-channel', version: VERSION },
  {
    capabilities: { tools: {}, experimental: { 'claude/channel': {} } },
    instructions:
      'This channel\'s wire is the local filesystem — a directory of append-only JSONL files. ' +
      'The sender is reading those files, not this session: anything you want them to see must go ' +
      'through the reply tool. chat_id is the room, which is the inbox file\'s basename (for the ' +
      'default maw wire, that is an oracle name). Replies are appended to a local outbox file; ' +
      'no network is involved anywhere in this channel.',
  },
)

let seq = 0
const nextId = () => `i${seq++}-${Math.round(performance.now())}`

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'reply',
      description:
        'Send a message to an inbox room. chat_id = room (the inbox file basename), text = body, ' +
        'optional reply_to = message id being answered. The reply is appended as a JSONL record to ' +
        'the local outbox; nothing is sent over a network.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string' },
          text: { type: 'string' },
          reply_to: { type: 'string' },
        },
        required: ['chat_id', 'text'],
      },
    },
    {
      name: 'edit_message',
      description:
        'Amend a message previously sent to an inbox room. Appends an edit record referencing the ' +
        'original message_id (the outbox is append-only, so nothing is rewritten in place).',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string' },
          message_id: { type: 'string' },
          text: { type: 'string' },
        },
        required: ['chat_id', 'message_id', 'text'],
      },
    },
  ],
}))

// --- inbound: a new line in a watched file → notifications/claude/channel ---

/** Stable id for a record, so the same line never notifies twice across a restart+replay. */
function messageIdFor(room: string, line: string): string {
  return `${room}-${createHash('sha256').update(line).digest('hex').slice(0, 12)}`
}

type Parsed = { content: string; user: string; type: string; ts: string; thread: string | null }

/**
 * A record is either a maw-shaped JSON object, some other JSON object, or a bare text line.
 * All three are legitimate wire formats — `echo hi >> room.jsonl` must work.
 */
function parseLine(line: string): Parsed | null {
  const now = new Date().toISOString()
  let j: Record<string, unknown> | null = null
  try {
    const v = JSON.parse(line)
    if (v && typeof v === 'object' && !Array.isArray(v)) j = v as Record<string, unknown>
    else return { content: String(v), user: 'inbox', type: 'msg', ts: now, thread: null }
  } catch {
    return { content: line, user: 'inbox', type: 'msg', ts: now, thread: null } // bare text line
  }
  // maw uses `msg`; other producers commonly use `text`/`content`/`message`.
  const content = String(j.msg ?? j.text ?? j.content ?? j.message ?? '')
  if (!content) return null
  return {
    content,
    user: String(j.from ?? j.user ?? 'inbox'),
    type: String(j.type ?? 'msg'),
    ts: typeof j.ts === 'string' ? j.ts : now,
    thread: j.thread == null ? null : String(j.thread),
  }
}

function notify(room: string, line: string, p: Parsed, replayed: boolean): void {
  void mcp.notification({
    method: 'notifications/claude/channel',
    params: {
      content: p.content,
      meta: {
        chat_id: room,
        message_id: messageIdFor(room, line),
        user: p.user,
        ts: p.ts,
        // Provenance lives in meta, never in content: an in-content annotation is forgeable by
        // any writer who types that string (same reasoning as the official discord channel).
        inbox_type: p.type,
        inbox_room: room,
        ...(p.thread ? { thread: p.thread } : {}),
        ...(replayed ? { replayed: 'true' } : {}),
      },
    },
  })
}

/**
 * Read [from, to) of a file and return whole lines plus the offset actually consumed.
 * A partial trailing line (writer mid-append, non-atomic write) is NOT consumed — its bytes
 * stay unread until the newline lands. This is what makes concurrent appends safe.
 */
function readLines(path: string, from: number, to: number): { lines: string[]; consumed: number } {
  const want = Math.min(to - from, MAX_READ_BYTES)
  if (want <= 0) return { lines: [], consumed: 0 }
  const buf = Buffer.allocUnsafe(want)
  let fd: number | undefined
  let got = 0
  try {
    fd = openSync(path, 'r')
    got = readSync(fd, buf, 0, want, from)
  } finally {
    if (fd !== undefined) try { closeSync(fd) } catch {}
  }
  if (got <= 0) return { lines: [], consumed: 0 }
  const chunk = buf.subarray(0, got)
  const lastNl = chunk.lastIndexOf(0x0a)
  if (lastNl < 0) {
    // No newline in the whole window. Either a writer is mid-line, or one line exceeds our
    // ceiling. Only the latter needs intervention — otherwise we would stall forever.
    if (got >= MAX_LINE_BYTES) {
      dlog(`inbox channel: line > ${MAX_LINE_BYTES}B in ${basename(path)} — skipping ${got}B\n`)
      return { lines: [], consumed: got }
    }
    return { lines: [], consumed: 0 }
  }
  // Cutting at a newline guarantees the slice is whole UTF-8 — a multibyte character can never
  // straddle the boundary, so no replacement chars from a mid-rune split.
  const text = chunk.subarray(0, lastNl + 1).toString('utf8')
  // Keep empty lines in the array: byte accounting in scanFile depends on the array being a
  // faithful 1:1 image of the consumed bytes. They are skipped at emit time instead.
  const lines = text.split('\n')
  lines.pop() // trailing '' after the final newline — not a record
  return { lines, consumed: lastNl + 1 }
}

function listInboxFiles(): string[] {
  try {
    return readdirSync(INBOX_DIR)
      .filter(f => f.endsWith(EXT))
      .filter(f => roomAllowed(roomOf(f)))
      .sort()
  } catch {
    return []
  }
}

let booted = false

function scanFile(file: string): number {
  const path = join(INBOX_DIR, file)
  const room = roomOf(file)
  let st: ReturnType<typeof statSync>
  try { st = statSync(path) } catch { return 0 }
  if (!st.isFile()) return 0

  let cur = cursors[file]

  if (!cur) {
    // First sight of this file.
    //
    // At BOOT, start at EOF: attaching a channel must not dump months of history into the
    // session (30 maw inboxes × hundreds of lines). REPLAY opens a bounded window into that past.
    //
    // AFTER boot, start at 0: a file that did not exist when we attached has no "history" —
    // every byte in it arrived while we were watching, so all of it is new mail. Applying
    // EOF-start here would silently swallow the first message of every new room, which is
    // exactly the message that matters most (it's someone opening a conversation).
    let start = booted ? 0 : st.size
    if (!booted && REPLAY_N > 0) {
      if (REPLAY_ALL) start = 0
      else {
        // Walk back from EOF to find the start of the last N lines, without reading the
        // whole file: scan a bounded tail window.
        const window = Math.min(st.size, MAX_READ_BYTES)
        const { lines } = readLines(path, st.size - window, st.size)
        const tail = lines.slice(-REPLAY_N)
        const bytes = tail.reduce((n, l) => n + Buffer.byteLength(l) + 1, 0)
        start = Math.max(0, st.size - bytes)
      }
    }
    cur = { offset: start, size: st.size, ino: st.ino }
    cursors[file] = cur
    cursorDirty = true
  }

  // Rotation / truncation: the file shrank, or the inode changed under a same-named path.
  // Either way our offset is meaningless — restart from the beginning of the new file.
  if (st.size < cur.offset || (cur.ino && st.ino && st.ino !== cur.ino)) {
    dlog(`inbox channel: ${file} rotated/truncated (size ${cur.offset}→${st.size}, ino ${cur.ino}→${st.ino}) — resetting cursor\n`)
    cur.offset = 0
    cur.ino = st.ino
    cursorDirty = true
  }

  if (st.size <= cur.offset) { cur.size = st.size; return 0 }

  const { lines, consumed } = readLines(path, cur.offset, st.size)
  if (!consumed) return 0

  // The cursor advances only over lines we actually HANDLED. Advancing over the whole read
  // window up front would mean the burst cap silently ate mail: the un-notified remainder
  // would sit behind the cursor and never be read again. Instead we stop at the cap and leave
  // the offset pointing at the first unhandled byte, so the next tick resumes exactly there.
  let bytesHandled = 0
  let sent = 0
  let capped = false
  for (const line of lines) {
    if (sent >= MAX_NOTIFY_PER_TICK) { capped = true; break }
    bytesHandled += Buffer.byteLength(line, 'utf8') + 1 // +1 for the '\n' we split on
    const record = line.endsWith('\r') ? line.slice(0, -1) : line // tolerate CRLF writers
    if (!record) continue // blank separator line — consumed, but not a message
    const p = parseLine(record)
    if (!p) continue
    if (IGNORE_TYPES.has(p.type)) continue
    if (IGNORE_FROM.has(p.user)) continue
    notify(room, record, p, !booted && REPLAY_N > 0)
    sent++
  }

  cur.offset += capped ? bytesHandled : consumed
  cur.size = st.size
  cur.ino = st.ino
  cursorDirty = true

  if (capped) {
    dlog(`inbox channel: burst cap ${MAX_NOTIFY_PER_TICK} on ${file} — ${lines.length - sent} line(s) deferred to next tick (offset ${cur.offset})\n`)
  }
  if (sent) dlog(`inbox channel: → ${sent} notification(s) from ${file} (offset now ${cur.offset})\n`)
  return sent
}

function scanAll(): void {
  try {
    for (const f of listInboxFiles()) scanFile(f)
  } catch (err) {
    dlog(`inbox channel: scan error: ${err instanceof Error ? err.message : err}\n`)
  }
  saveCursors()
}

// --- outbound: reply / edit_message → append to the local outbox ---
function appendJsonl(path: string, rec: Record<string, unknown>): void {
  mkdirSync(join(path, '..'), { recursive: true })
  appendFileSync(path, JSON.stringify(rec) + '\n')
}

/**
 * Peer delivery writes into ANOTHER room's inbox file. Only ever inside INBOX_DIR, only ever a
 * sanitized room name, and only when explicitly enabled.
 */
function deliverToPeer(room: string, rec: Record<string, unknown>): string | null {
  if (REPLY_MODE !== 'peer') return null
  const target = join(INBOX_DIR, `${room}${EXT}`)
  // Belt and braces: even after assertSafeRoom, verify the resolved path stays in INBOX_DIR.
  const dirReal = realpathSync(INBOX_DIR)
  if (!join(dirReal, `${room}${EXT}`).startsWith(dirReal + sep)) {
    throw new Error(`refusing to write outside INBOX_DIR: ${target}`)
  }
  appendJsonl(target, rec)
  return target
}

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const a = (req.params.arguments ?? {}) as Record<string, unknown>
  try {
    const room = String(a.chat_id ?? '')
    assertSafeRoom(room)

    if (req.params.name === 'reply') {
      const id = nextId()
      const rec = {
        ts: new Date().toISOString(),
        from: SELF || 'assistant',
        type: 'msg',
        msg: String(a.text ?? ''),
        thread: null,
        id,
        ...(a.reply_to ? { replyTo: String(a.reply_to) } : {}),
      }
      appendJsonl(join(OUTBOX_DIR, `${room}${EXT}`), rec)
      const peer = deliverToPeer(room, rec)
      return {
        content: [{
          type: 'text',
          text: `sent (id: ${id}) → outbox/${room}${EXT}${peer ? ` + peer inbox ${peer}` : ''}`,
        }],
      }
    }

    if (req.params.name === 'edit_message') {
      const rec = {
        ts: new Date().toISOString(),
        from: SELF || 'assistant',
        type: 'edit',
        msg: String(a.text ?? ''),
        thread: null,
        id: String(a.message_id ?? ''),
      }
      appendJsonl(join(OUTBOX_DIR, `${room}${EXT}`), rec)
      deliverToPeer(room, rec)
      return { content: [{ type: 'text', text: `edited (id: ${a.message_id})` }] }
    }

    return { content: [{ type: 'text', text: `unknown tool: ${req.params.name}` }], isError: true }
  } catch (err) {
    return {
      content: [{ type: 'text', text: `${req.params.name} failed: ${err instanceof Error ? err.message : String(err)}` }],
      isError: true,
    }
  }
})

await mcp.connect(new StdioServerTransport())

dlog(
  `inbox channel v${VERSION}: watching ${INBOX_DIR}/*${EXT} — poll ${POLL_MS}ms, replay ${REPLAY_RAW}, ` +
  `reply-mode ${REPLY_MODE}, state ${STATE_DIR}\n`,
)

// Boot scan establishes cursors (and emits the replay window, if any).
scanAll()
booted = true

// Poll is the reliable floor: it survives editors that write-and-rename, network filesystems,
// and platforms where fs.watch drops events. The watcher below is only an accelerator.
const timer = setInterval(scanAll, POLL_MS)
timer.unref?.()

// fs.watch on the DIRECTORY catches both new files and appends, and costs one descriptor.
// Coalesce bursts into a single scan on the next tick of the event loop.
let watchPending = false
try {
  const w = watch(INBOX_DIR, () => {
    if (watchPending) return
    watchPending = true
    setTimeout(() => { watchPending = false; scanAll() }, 50).unref?.()
  })
  w.on('error', err => dlog(`inbox channel: watch error (poll still active): ${err}\n`))
  w.unref?.()
} catch (err) {
  dlog(`inbox channel: fs.watch unavailable (poll still active): ${err instanceof Error ? err.message : err}\n`)
}

// stdin EOF → graceful shutdown (mirrors arra-discord / arra-mqtt).
let ending = false
function shutdown(): void {
  if (ending) return
  ending = true
  saveCursors() // never lose our place on exit
  dlog('inbox channel: shutdown, cursors saved\n')
  process.exit(0)
}
process.stdin.on('end', shutdown)
process.stdin.on('close', shutdown)
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

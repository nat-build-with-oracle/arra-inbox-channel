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
  appendFileSync, chmodSync, closeSync, lstatSync, mkdirSync, openSync, readFileSync,
  readSync, readdirSync, renameSync, statSync, watch, writeFileSync,
} from 'node:fs'
import { basename, join } from 'node:path'
import { homedir } from 'node:os'
import { createHash } from 'node:crypto'

const VERSION = '0.3.0'

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

/**
 * Every numeric knob goes through here. `Number('abc')` is NaN, and NaN propagates silently into
 * places that fail in spectacular ways: setInterval(NaN) degenerates to a ~1ms timer (hundreds of
 * directory scans per second), and Buffer.allocUnsafe(NaN) throws on every tick, which — before
 * per-file isolation — killed the entire scan loop. A typo in .env should not do either.
 */
function num(name: string, fallback: number, min: number): number {
  const raw = process.env[name]
  if (raw === undefined || raw.trim() === '') return fallback
  const v = Number(raw)
  if (!Number.isFinite(v)) {
    dlog(`inbox channel: ${name}=${JSON.stringify(raw)} is not a number — using ${fallback}\n`)
    return fallback
  }
  return Math.max(min, Math.floor(v))
}

const INBOX_DIR = process.env.INBOX_DIR ?? join(homedir(), '.maw', 'inbox')
const POLL_MS = num('INBOX_POLL_MS', 1000, 100)
const EXT = process.env.INBOX_EXT ?? '.jsonl'
// Replay: 0 = start at EOF (default — a fresh attach must never dump months of backlog into
// the session), N = emit the last N lines of each file on boot, 'all' = full backfill.
const REPLAY_RAW = (process.env.INBOX_REPLAY ?? '0').trim().toLowerCase()
const REPLAY_ALL = REPLAY_RAW === 'all'
const REPLAY_N = REPLAY_ALL ? Infinity : Math.max(0, Number(REPLAY_RAW) || 0)
// Guard against a pathological single line (a runaway writer) eating memory.
const MAX_LINE_BYTES = num('INBOX_MAX_LINE_KB', 256, 1) * 1024
// Per-tick read ceiling: a huge append is consumed across several ticks rather than in one gulp.
//
// It must never be smaller than MAX_LINE_BYTES. "No newline in a full window" is the ONLY
// evidence we have that a line is over-long, and it only proves the line is at least one window
// wide. With a window narrower than the cap that proof can never be reached, so an over-long line
// would consume 0 bytes every tick and stall the room permanently. Clamping is safer than
// trusting the operator to keep two independent knobs consistent.
const RAW_MAX_READ_BYTES = num('INBOX_MAX_READ_KB', 1024, 1) * 1024
const MAX_READ_BYTES = Math.max(RAW_MAX_READ_BYTES, MAX_LINE_BYTES)
// Burst guard: if a writer floods, don't fire thousands of notifications into the session.
const MAX_NOTIFY_PER_TICK = num('INBOX_MAX_PER_TICK', 20, 1)

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
// The cursor file is namespaced by WHICH directory it describes. STATE_DIR defaults to one fixed
// path, but INBOX_DIR is per-session — and cursors are keyed by bare basename. Sharing one
// cursor.json across two different inbox directories means a `notes.jsonl` in dir A inherits the
// offset of an unrelated `notes.jsonl` in dir B: either a chunk of history dumped into the
// session, or messages skipped outright. The directory is part of the identity of a cursor.
const CURSOR_FILE = join(
  STATE_DIR,
  `cursor-${createHash('sha256').update(INBOX_DIR).digest('hex').slice(0, 12)}.json`,
)

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
type Cursor = { offset: number; size: number; ino: number; skipping?: boolean }
let cursors: Record<string, Cursor> = {}
try { cursors = JSON.parse(readFileSync(CURSOR_FILE, 'utf8')) } catch {}

let cursorDirty = false
function saveCursors(): void {
  if (!cursorDirty) return
  // atomic: write a temp file in the same dir, then rename. A crash mid-write can never
  // leave a truncated cursor.json that would silently replay or skip messages. The temp name
  // carries our pid so two servers never collide on the same partial file.
  const tmp = `${CURSOR_FILE}.${process.pid}.tmp`
  try {
    // Two Claude sessions can run this plugin against the same STATE_DIR. Each keeps its own
    // in-memory position (so each session gets every message), but the file is shared — a blind
    // overwrite would let the slower writer rewind the saved position and re-deliver old mail
    // into the NEXT session. Merge by furthest-read instead: last-writer-wins is the bug.
    let onDisk: Record<string, Cursor> = {}
    try { onDisk = JSON.parse(readFileSync(CURSOR_FILE, 'utf8')) } catch {}
    const merged: Record<string, Cursor> = { ...onDisk }
    for (const [file, cur] of Object.entries(cursors)) {
      const other = merged[file]
      // Only take the max when both refer to the SAME file incarnation; a rotation (new inode)
      // legitimately moves the offset backwards and must not be clobbered by a stale high value.
      merged[file] = other && other.ino === cur.ino && other.offset > cur.offset ? other : cur
    }
    writeFileSync(tmp, JSON.stringify(merged, null, 2))
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

type Parsed = { content: string; user: string; type: string; ts: string; thread: string | null; origin?: string }

// Stamped on every record we write. In peer mode our reply lands in a file we are also tailing,
// so without a marker the channel reads its own output straight back into the session — an
// endless self-conversation. Keyed to this process, so a DIFFERENT server still sees the message
// normally; only the author ignores it.
const INSTANCE = `${process.pid}-${createHash('sha256').update(`${STATE_DIR}|${process.pid}`).digest('hex').slice(0, 8)}`

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
    origin: typeof j.origin === 'string' ? j.origin : undefined,
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
type Line = { text: string; bytes: number }

function readLines(path: string, from: number, to: number, skipping = false, stable = true): { lines: Line[]; consumed: number; skipping: boolean } {
  const want = Math.min(to - from, MAX_READ_BYTES)
  if (want <= 0) return { lines: [], consumed: 0, skipping }
  const buf = Buffer.allocUnsafe(want)
  let fd: number | undefined
  let got = 0
  try {
    fd = openSync(path, 'r')
    got = readSync(fd, buf, 0, want, from)
  } finally {
    if (fd !== undefined) try { closeSync(fd) } catch {}
  }
  if (got <= 0) return { lines: [], consumed: 0, skipping }
  const chunk = buf.subarray(0, got)

  // Mid-skip of an over-long line: discard bytes until the line ENDS. Resuming at a fixed byte
  // offset instead would hand the tail of that line to the parser as if it were a fresh record,
  // emitting a garbage fragment into the session.
  if (skipping) {
    const nl = chunk.indexOf(0x0a)
    if (nl < 0) return { lines: [], consumed: got, skipping: true } // still inside the monster line
    // The line ended. Hand back only the newline itself; the whole lines that follow are read on
    // the next tick by the normal path, which keeps byte accounting in exactly one place.
    return { lines: [], consumed: nl + 1, skipping: false }
  }

  const lastNl = chunk.lastIndexOf(0x0a)
  if (lastNl < 0) {
    // No newline in the whole window. Either a writer is mid-line, or one line exceeds our
    // ceiling. Only the latter needs intervention — otherwise we would stall forever.
    // `stable` means the file did not grow since the previous tick. Without that check a writer
    // legitimately streaming a large record gets its data discarded mid-flight: the reader sees
    // a full window with no newline yet and declares the line over-long, when in truth the
    // newline simply had not been written. A big record and a slow writer look identical in a
    // single sample; only time distinguishes them.
    if (got >= MAX_LINE_BYTES && stable) {
      dlog(`inbox channel: line > ${MAX_LINE_BYTES}B in ${basename(path)} — skipping to end of line\n`)
      return { lines: [], consumed: got, skipping: true }
    }
    return { lines: [], consumed: 0, skipping: false }
  }
  // Split in the BYTE domain and record each line's true byte length.
  //
  // Decoding first and measuring the string with Buffer.byteLength would be a latent corruption
  // bug: toString('utf8') maps every invalid byte to U+FFFD, which re-encodes to THREE bytes, so
  // one stray 0x80 on the wire inflates the count by two. That inflated count feeds the cursor on
  // a capped tick, and an offset past EOF trips the rotation check — which resets to 0 and
  // re-delivers the whole file, forever. Cutting at a newline prevents a mid-rune split; it does
  // not make the bytes valid UTF-8. Only the raw buffer knows how long a line really is.
  const lines: Line[] = []
  let start = 0
  while (start <= lastNl) {
    const nl = chunk.indexOf(0x0a, start)
    if (nl < 0 || nl > lastNl) break
    lines.push({ text: chunk.subarray(start, nl).toString('utf8'), bytes: nl - start + 1 })
    start = nl + 1
  }
  return { lines, consumed: lastNl + 1, skipping: false }
}

/** Read the single byte before `offset` and report whether it is the newline we stopped at. */
function endsWithNewlineAt(path: string, offset: number): boolean {
  let fd: number | undefined
  try {
    fd = openSync(path, 'r')
    const b = Buffer.allocUnsafe(1)
    const got = readSync(fd, b, 0, 1, offset - 1)
    return got === 1 && b[0] === 0x0a
  } catch {
    return true // unreadable for now — let the normal path handle and log it
  } finally {
    if (fd !== undefined) try { closeSync(fd) } catch {}
  }
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
let replaying = false
// Log a refused symlink / failing file once per cause, not once per poll tick (1/s forever).
const warnedSymlink = new Set<string>()
const warnedScan = new Set<string>()

function scanFile(file: string): number {
  const path = join(INBOX_DIR, file)
  const room = roomOf(file)
  let st: ReturnType<typeof statSync>
  try { st = statSync(path) } catch { return 0 }
  if (!st.isFile()) return 0
  // A symlinked room would let anyone who can write INBOX_DIR point the channel at an arbitrary
  // file and have its contents read aloud into the session, line by line.
  try {
    assertNotSymlink(path, 'read')
  } catch (err) {
    if (!warnedSymlink.has(file)) {
      warnedSymlink.add(file)
      dlog(`inbox channel: ${err instanceof Error ? err.message : err} — room ${room} ignored\n`)
    }
    return 0
  }

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
        // Walk back to the start of the last N lines without reading the whole file: scan a
        // bounded tail window.
        const from = Math.max(0, st.size - MAX_READ_BYTES)
        const { lines, consumed } = readLines(path, from, st.size)
        // Anchor on the last COMPLETE line, not on st.size. readLines deliberately stops at the
        // final newline, so a file with a trailing partial line (a writer mid-append) makes
        // st.size overshoot the end of whole lines — and the cursor would land inside a record,
        // delivering its tail as a bogus first message.
        const endOfWholeLines = from + consumed
        const tail = lines.slice(-REPLAY_N)
        const bytes = tail.reduce((n, l) => n + l.bytes, 0)
        start = Math.max(0, endOfWholeLines - bytes)
      }
    }
    cur = { offset: start, size: st.size, ino: st.ino }
    cursors[file] = cur
    cursorDirty = true
  }

  // Rotation / truncation: the file shrank, or the inode changed under a same-named path.
  // Either way our offset is meaningless — restart from the beginning of the new file.
  let rotated = st.size < cur.offset || (cur.ino !== 0 && st.ino !== 0 && st.ino !== cur.ino)

  // Same-inode rewrite: `>file` then writing MORE than before leaves size >= offset and the inode
  // unchanged, so neither check above fires and we would resume reading from the middle of new
  // content — emitting a fragment. The byte just before our offset must be the newline we stopped
  // at; if it is not, the bytes underneath us were replaced. Only checked when the file changed,
  // so the steady state costs nothing.
  if (!rotated && cur.offset > 0 && st.size !== cur.size && !endsWithNewlineAt(path, cur.offset)) {
    dlog(`inbox channel: ${file} rewritten in place (byte at ${cur.offset - 1} is not a newline) — resetting cursor\n`)
    rotated = true
  }

  if (rotated) {
    dlog(`inbox channel: ${file} rotated/truncated (offset ${cur.offset}, size ${st.size}, ino ${cur.ino}→${st.ino}) — resetting cursor\n`)
    cur.offset = 0
    cur.ino = st.ino
    cur.skipping = false
    cursorDirty = true
  }

  if (st.size <= cur.offset) { cur.size = st.size; return 0 }

  const { lines, consumed, skipping } = readLines(path, cur.offset, st.size, cur.skipping, st.size === cur.size)
  cur.skipping = skipping
  if (!consumed) { cur.size = st.size; cursorDirty = true; return 0 }

  // The cursor advances only over lines we actually HANDLED. Advancing over the whole read
  // window up front would mean the burst cap silently ate mail: the un-notified remainder
  // would sit behind the cursor and never be read again. Instead we stop at the cap and leave
  // the offset pointing at the first unhandled byte, so the next tick resumes exactly there.
  let bytesHandled = 0
  let sent = 0
  let capped = false
  for (const line of lines) {
    if (sent >= MAX_NOTIFY_PER_TICK) { capped = true; break }
    bytesHandled += line.bytes // exact, measured on the raw buffer — never re-encoded
    const record = line.text.endsWith('\r') ? line.text.slice(0, -1) : line.text // tolerate CRLF
    if (!record) continue // blank separator line — consumed, but not a message
    const p = parseLine(record)
    if (!p) continue
    if (p.origin && p.origin === INSTANCE) continue // our own peer-mode reply, echoed back
    if (IGNORE_TYPES.has(p.type)) continue
    if (IGNORE_FROM.has(p.user)) continue
    notify(room, record, p, replaying)
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
  let files: string[] = []
  try {
    files = listInboxFiles()
  } catch (err) {
    dlog(`inbox channel: cannot list ${INBOX_DIR}: ${err instanceof Error ? err.message : err}\n`)
    return
  }
  for (const f of files) {
    // Per-FILE isolation, not per-tick. listInboxFiles() returns sorted names, so a single
    // unreadable file (a chmod 000 drop from another user — routine in a shared federation
    // inbox) would otherwise throw out of the loop and starve every alphabetically-later room
    // for as long as it sits there. Silently, and forever.
    try {
      scanFile(f)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      if (!warnedScan.has(f + msg)) {
        warnedScan.add(f + msg)
        dlog(`inbox channel: ${f} skipped this tick: ${msg}\n`)
      }
    }
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
 *
 * assertSafeRoom already blocks traversal in the NAME, but a name-only check is not enough: a
 * symlink planted at <INBOX_DIR>/<room>.jsonl redirects the write wherever it points, and
 * appendFileSync follows symlinks. `evil.jsonl -> ~/.ssh/authorized_keys` would turn a reply into
 * an append to authorized_keys. lstat (which does NOT follow) is the check that actually holds.
 */
function deliverToPeer(room: string, rec: Record<string, unknown>): string | null {
  if (REPLY_MODE !== 'peer') return null
  const target = join(INBOX_DIR, `${room}${EXT}`)
  assertNotSymlink(target, 'write')
  appendJsonl(target, rec)
  return target
}

/**
 * Refuse to read or write through a symlink inside INBOX_DIR.
 *
 * Reads matter as much as writes: a room symlinked at ~/.ssh/id_rsa would stream a private key
 * into the conversation line by line. Since filesystem permissions are this channel's entire
 * access boundary, anything that can plant a symlink in INBOX_DIR could otherwise redirect the
 * channel at arbitrary files. A symlinked room file has no legitimate use here.
 */
function assertNotSymlink(path: string, mode: 'read' | 'write'): void {
  let st: ReturnType<typeof lstatSync>
  try { st = lstatSync(path) } catch { return } // does not exist yet — appendJsonl will create it
  if (st.isSymbolicLink()) {
    throw new Error(`refusing to ${mode} through a symlink: ${path}`)
  }
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
        origin: INSTANCE, // so we don't re-ingest our own reply in peer mode
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
        origin: INSTANCE,
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

// The boot scan (and any replay) must not run until the client has finished initializing.
// Notifications emitted before `notifications/initialized` are discarded by the peer, so the
// very first messages — including the entire replay window someone explicitly asked for — would
// vanish into the void with no error anywhere. Waiting costs one round trip.
let startScanning: () => void
const initialized = new Promise<void>(resolve => { startScanning = () => resolve() })
mcp.oninitialized = () => startScanning()

await mcp.connect(new StdioServerTransport())

dlog(
  `inbox channel v${VERSION}: watching ${INBOX_DIR}/*${EXT} — poll ${POLL_MS}ms, replay ${REPLAY_RAW}, ` +
  `reply-mode ${REPLY_MODE}, state ${STATE_DIR}\n`,
)
if (MAX_READ_BYTES !== RAW_MAX_READ_BYTES) {
  dlog(
    `inbox channel: INBOX_MAX_READ_KB (${RAW_MAX_READ_BYTES / 1024}) was below INBOX_MAX_LINE_KB ` +
    `(${MAX_LINE_BYTES / 1024}) — raised to match, otherwise an over-long line stalls its room forever\n`,
  )
}

// A client that connects but never initializes must not wedge the channel forever.
await Promise.race([initialized, new Promise<void>(r => setTimeout(r, 10_000).unref?.())])

// Boot scan establishes cursors (and emits the replay window, if any).
replaying = REPLAY_N > 0
scanAll()
booted = true
replaying = false

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

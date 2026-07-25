/**
 * End-to-end test: drives the real server over real stdio JSON-RPC against a real temp
 * directory. Nothing is mocked — if this passes, the channel works.
 *
 *   bun --env-file=/dev/null run test-e2e.ts
 */
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { appendFileSync, mkdtempSync, mkdirSync, readFileSync, existsSync, writeFileSync, statSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

const SERVER = join(import.meta.dirname, 'server.ts')

let pass = 0
let fail = 0
const failures: string[] = []
function check(name: string, cond: boolean, detail = ''): void {
  if (cond) { pass++; console.log(`  ✓ ${name}`) }
  else { fail++; failures.push(name); console.log(`  ✗ ${name}${detail ? ` — ${detail}` : ''}`) }
}

type Msg = Record<string, any>

// Every spawned server, so a forgotten kill() can never hang the suite: a live child keeps the
// event loop alive forever, which looks like a mysterious freeze rather than a test failure.
const spawned: Harness[] = []

class Harness {
  proc: ChildProcessWithoutNullStreams
  notifications: Msg[] = []
  responses = new Map<number, Msg>()
  stderr = ''
  private buf = ''
  private id = 0

  constructor(public dir: string, public state: string, env: Record<string, string> = {}) {
    this.proc = spawn('bun', ['--env-file=/dev/null', 'run', SERVER], {
      env: { ...process.env, INBOX_DIR: dir, INBOX_STATE_DIR: state, INBOX_POLL_MS: '100', ...env },
      stdio: ['pipe', 'pipe', 'pipe'],
    }) as ChildProcessWithoutNullStreams
    this.proc.stdout.on('data', d => {
      this.buf += d.toString()
      let i: number
      while ((i = this.buf.indexOf('\n')) >= 0) {
        const line = this.buf.slice(0, i).trim()
        this.buf = this.buf.slice(i + 1)
        if (!line) continue
        try {
          const m = JSON.parse(line) as Msg
          if (m.id !== undefined && m.method === undefined) this.responses.set(m.id, m)
          else if (m.method) this.notifications.push(m)
        } catch {}
      }
    })
    this.proc.stderr.on('data', d => { this.stderr += d.toString() })
    spawned.push(this)
  }

  send(m: Msg): void { this.proc.stdin.write(JSON.stringify(m) + '\n') }

  async request(method: string, params: unknown = {}, timeoutMs = 8000): Promise<Msg> {
    const id = ++this.id
    this.send({ jsonrpc: '2.0', id, method, params })
    const t0 = Date.now()
    while (Date.now() - t0 < timeoutMs) {
      const r = this.responses.get(id)
      if (r) return r
      await sleep(20)
    }
    throw new Error(`timeout waiting for ${method}`)
  }

  async initialize(): Promise<Msg> {
    const r = await this.request('initialize', {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: { name: 'e2e', version: '0' },
    })
    this.send({ jsonrpc: '2.0', method: 'notifications/initialized' })
    return r
  }

  channelMsgs(): Msg[] {
    return this.notifications.filter(n => n.method === 'notifications/claude/channel')
  }

  /** Wait until at least n channel notifications have arrived (or time out and return what we have). */
  async waitForChannel(n: number, timeoutMs = 6000): Promise<Msg[]> {
    const t0 = Date.now()
    while (Date.now() - t0 < timeoutMs) {
      if (this.channelMsgs().length >= n) break
      await sleep(50)
    }
    return this.channelMsgs()
  }

  kill(): void { try { this.proc.kill('SIGKILL') } catch {} }
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

function tmp(): { dir: string; state: string; root: string } {
  const root = mkdtempSync(join(tmpdir(), 'inbox-e2e-'))
  const dir = join(root, 'inbox')
  const state = join(root, 'state')
  mkdirSync(dir, { recursive: true, mode: 0o700 })
  mkdirSync(state, { recursive: true, mode: 0o700 })
  return { dir, state, root }
}

const cleanup: string[] = []

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n1. handshake + capability + tools')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  const init = await h.initialize()
  const caps = init.result?.capabilities
  check('initialize succeeds', !!init.result, JSON.stringify(init).slice(0, 200))
  check('declares experimental claude/channel', !!caps?.experimental?.['claude/channel'],
    JSON.stringify(caps))
  check('server name is inbox-channel', init.result?.serverInfo?.name === 'inbox-channel')

  const tools = await h.request('tools/list')
  const names = (tools.result?.tools ?? []).map((t: Msg) => t.name).sort()
  check('tools = [edit_message, reply]', JSON.stringify(names) === '["edit_message","reply"]',
    JSON.stringify(names))
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n2. inbound: maw-shaped JSONL → channel notification')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400) // let the boot scan establish cursors

  appendFileSync(join(dir, 'digger-oracle.jsonl'), JSON.stringify({
    ts: '2026-07-25T20:00:00.000Z', from: 'digger-buddy', type: 'msg',
    msg: 'the vault index finished', thread: null,
  }) + '\n')

  const msgs = await h.waitForChannel(1)
  check('one notification arrived', msgs.length === 1, `got ${msgs.length}; stderr=${h.stderr.slice(-300)}`)
  const p = msgs[0]?.params
  check('content is the msg field', p?.content === 'the vault index finished', JSON.stringify(p?.content))
  check('chat_id is the file basename', p?.meta?.chat_id === 'digger-oracle', JSON.stringify(p?.meta))
  check('user is the from field', p?.meta?.user === 'digger-buddy')
  check('ts preserved from record', p?.meta?.ts === '2026-07-25T20:00:00.000Z')
  check('inbox_type in meta', p?.meta?.inbox_type === 'msg')
  check('message_id present', typeof p?.meta?.message_id === 'string' && p.meta.message_id.length > 0)
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n3. inbound: bare text line (echo hi >> room.jsonl)')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400)
  appendFileSync(join(dir, 'plain.jsonl'), 'hello from a bare echo\n')
  const msgs = await h.waitForChannel(1)
  check('bare text becomes a turn', msgs[0]?.params?.content === 'hello from a bare echo',
    JSON.stringify(msgs[0]?.params))
  check('bare text room is correct', msgs[0]?.params?.meta?.chat_id === 'plain')
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n4. EOF-start: pre-existing history is NOT replayed by default')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  for (let i = 0; i < 50; i++) {
    appendFileSync(join(dir, 'old.jsonl'), JSON.stringify({ from: 'x', type: 'done', msg: `old ${i}` }) + '\n')
  }
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(800)
  check('50 historical lines produced 0 notifications', h.channelMsgs().length === 0,
    `got ${h.channelMsgs().length}`)

  appendFileSync(join(dir, 'old.jsonl'), JSON.stringify({ from: 'x', type: 'msg', msg: 'brand new' }) + '\n')
  const msgs = await h.waitForChannel(1)
  check('a NEW line after EOF still arrives', msgs.length === 1 && msgs[0].params.content === 'brand new',
    `got ${msgs.length}`)
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n5. INBOX_REPLAY=3 backfills exactly the last 3 lines')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  for (let i = 0; i < 10; i++) {
    appendFileSync(join(dir, 'hist.jsonl'), JSON.stringify({ from: 'x', type: 'msg', msg: `line ${i}` }) + '\n')
  }
  const h = new Harness(dir, state, { INBOX_REPLAY: '3' })
  await h.initialize()
  const msgs = await h.waitForChannel(3)
  check('exactly 3 replayed', msgs.length === 3, `got ${msgs.length}`)
  check('replayed the LAST 3', msgs.map(m => m.params.content).join(',') === 'line 7,line 8,line 9',
    msgs.map(m => m.params.content).join(','))
  check('replayed messages are tagged in meta', msgs[0]?.params?.meta?.replayed === 'true')
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n6. cursor persistence: restart resumes, does not re-deliver')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h1 = new Harness(dir, state)
  await h1.initialize()
  await sleep(400)
  appendFileSync(join(dir, 'resume.jsonl'), JSON.stringify({ from: 'a', type: 'msg', msg: 'first' }) + '\n')
  await h1.waitForChannel(1)
  check('first message delivered', h1.channelMsgs().length === 1)
  h1.proc.stdin.end() // graceful: triggers cursor save
  await sleep(500)
  h1.kill()

  check('cursor.json written', existsSync(join(state, 'cursor.json')))

  const h2 = new Harness(dir, state)
  await h2.initialize()
  await sleep(700)
  check('restart does NOT re-deliver old line', h2.channelMsgs().length === 0,
    `got ${h2.channelMsgs().length}`)
  appendFileSync(join(dir, 'resume.jsonl'), JSON.stringify({ from: 'a', type: 'msg', msg: 'second' }) + '\n')
  const m2 = await h2.waitForChannel(1)
  check('new line after restart delivered', m2.length === 1 && m2[0].params.content === 'second')
  h2.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n7. partial-line safety: no newline yet → no delivery')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400)
  const f = join(dir, 'partial.jsonl')
  appendFileSync(f, '{"from":"a","type":"msg","msg":"half wri')
  await sleep(500)
  check('partial line not delivered', h.channelMsgs().length === 0, `got ${h.channelMsgs().length}`)
  appendFileSync(f, 'tten"}\n')
  const msgs = await h.waitForChannel(1)
  check('completed line delivered whole', msgs.length === 1 && msgs[0].params.content === 'half written',
    JSON.stringify(msgs[0]?.params?.content))
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n8. truncation/rotation resets the cursor')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400)
  const f = join(dir, 'rot.jsonl')
  appendFileSync(f, JSON.stringify({ from: 'a', type: 'msg', msg: 'before rotate' }) + '\n')
  await h.waitForChannel(1)
  writeFileSync(f, JSON.stringify({ from: 'a', type: 'msg', msg: 'after rotate' }) + '\n') // truncate + rewrite
  const msgs = await h.waitForChannel(2)
  check('post-rotation line delivered', msgs.length === 2 && msgs[1].params.content === 'after rotate',
    msgs.map(m => m.params.content).join(','))
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n9. outbound: reply appends to the local outbox')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  const r = await h.request('tools/call', {
    name: 'reply', arguments: { chat_id: 'digger-oracle', text: 'ack — indexing done', reply_to: 'i0-1' },
  })
  check('reply returns success', !r.result?.isError, JSON.stringify(r).slice(0, 200))
  const out = join(state, 'outbox', 'digger-oracle.jsonl')
  check('outbox file created', existsSync(out), out)
  if (existsSync(out)) {
    const rec = JSON.parse(readFileSync(out, 'utf8').trim().split('\n')[0])
    check('outbox record has the text', rec.msg === 'ack — indexing done', JSON.stringify(rec))
    check('outbox record carries replyTo', rec.replyTo === 'i0-1')
    check('outbox record is maw-shaped', 'ts' in rec && 'from' in rec && 'type' in rec)
  }

  const e = await h.request('tools/call', {
    name: 'edit_message', arguments: { chat_id: 'digger-oracle', message_id: 'i0-1', text: 'ack (edited)' },
  })
  check('edit_message succeeds', !e.result?.isError)
  const lines = readFileSync(out, 'utf8').trim().split('\n')
  check('edit appended, not rewritten', lines.length === 2 && JSON.parse(lines[1]).type === 'edit')
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n10. path traversal in chat_id is rejected')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state, { INBOX_REPLY_MODE: 'peer' })
  await h.initialize()
  for (const bad of ['../evil', '../../etc/passwd', '/abs/path', 'a/b', '..', '.hidden', '']) {
    const r = await h.request('tools/call', { name: 'reply', arguments: { chat_id: bad, text: 'pwn' } })
    check(`rejects chat_id ${JSON.stringify(bad)}`, r.result?.isError === true,
      JSON.stringify(r.result).slice(0, 160))
  }
  check('no file escaped the temp root', !existsSync(join(root, 'evil.jsonl')))
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n11. filters: INBOX_IGNORE_TYPES / INBOX_ROOMS')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state, { INBOX_IGNORE_TYPES: 'done', INBOX_ROOMS: 'keep' })
  await h.initialize()
  await sleep(400)
  appendFileSync(join(dir, 'keep.jsonl'), JSON.stringify({ from: 'a', type: 'done', msg: 'noise' }) + '\n')
  appendFileSync(join(dir, 'drop.jsonl'), JSON.stringify({ from: 'a', type: 'msg', msg: 'wrong room' }) + '\n')
  appendFileSync(join(dir, 'keep.jsonl'), JSON.stringify({ from: 'a', type: 'msg', msg: 'signal' }) + '\n')
  const msgs = await h.waitForChannel(1)
  check('only the allowed room + non-ignored type survives', msgs.length === 1,
    msgs.map(m => m.params.content).join(','))
  check('the survivor is the signal', msgs[0]?.params?.content === 'signal')
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n12. peer delivery mode writes into the peer inbox')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state, { INBOX_REPLY_MODE: 'peer', INBOX_SELF: 'nh-oracle' })
  await h.initialize()
  const r = await h.request('tools/call', { name: 'reply', arguments: { chat_id: 'peer-oracle', text: 'hi peer' } })
  check('peer reply succeeds', !r.result?.isError, JSON.stringify(r.result).slice(0, 200))
  const peerFile = join(dir, 'peer-oracle.jsonl')
  check('peer inbox file written', existsSync(peerFile), peerFile)
  if (existsSync(peerFile)) {
    const rec = JSON.parse(readFileSync(peerFile, 'utf8').trim())
    check('peer record from = INBOX_SELF', rec.from === 'nh-oracle', JSON.stringify(rec))
    check('peer record readable by maw (ts/from/type/msg/thread)',
      ['ts', 'from', 'type', 'msg', 'thread'].every(k => k in rec))
  }
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n13. the "no network" promise is structurally true')
{
  const src = readFileSync(SERVER, 'utf8')
  // Strip comments/strings-in-docs is overkill; assert on import statements only.
  const imports = [...src.matchAll(/^import[\s\S]*?from\s+'([^']+)'/gm)].map(m => m[1])
  const netish = imports.filter(i =>
    /^(node:)?(http|https|net|tls|dgram|http2|ws|dns)$/.test(i) || /^(axios|node-fetch|undici|mqtt|discord\.js|ws)$/.test(i))
  check('no network modules imported', netish.length === 0, netish.join(','))
  check('no fetch( call in source', !/\bfetch\s*\(/.test(src))
  check('no WebSocket in source', !/\bWebSocket\b/.test(src))
  check('imports are only sdk + node builtins',
    imports.every(i => i.startsWith('node:') || i.startsWith('@modelcontextprotocol/')), imports.join(','))
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n14. world-writable INBOX_DIR is refused')
{
  const { state, root } = tmp(); cleanup.push(root)
  const open = join(root, 'open-inbox')
  mkdirSync(open, { recursive: true })
  const { chmodSync } = await import('node:fs')
  chmodSync(open, 0o777)
  const h = new Harness(open, state)
  await sleep(1200)
  check('server exited on world-writable dir', h.proc.exitCode !== null, `exitCode=${h.proc.exitCode}`)
  check('fatal message explains why', h.stderr.includes('world-writable'), h.stderr.slice(-200))
  h.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
console.log('\n15. burst cap defers, never drops (no silent mail loss)')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state, { INBOX_MAX_PER_TICK: '5', INBOX_POLL_MS: '100' })
  await h.initialize()
  await sleep(400)
  // 37 lines in one atomic append, with a cap of 5 per tick: all 37 must still arrive.
  const burst = Array.from({ length: 37 }, (_, i) =>
    JSON.stringify({ from: 'flood', type: 'msg', msg: `burst ${i}` })).join('\n') + '\n'
  appendFileSync(join(dir, 'flood.jsonl'), burst)
  const msgs = await h.waitForChannel(37, 12000)
  check('all 37 burst lines delivered', msgs.length === 37, `got ${msgs.length}`)
  check('delivered in order, none skipped',
    msgs.map(m => m.params.content).join(',') === Array.from({ length: 37 }, (_, i) => `burst ${i}`).join(','),
    msgs.slice(0, 8).map(m => m.params.content).join(','))
  h.kill()
}

console.log('\n16. CRLF writers and blank lines are tolerated')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400)
  appendFileSync(join(dir, 'crlf.jsonl'),
    JSON.stringify({ from: 'a', type: 'msg', msg: 'windows line' }) + '\r\n' +
    '\n' + // blank separator
    JSON.stringify({ from: 'a', type: 'msg', msg: 'after blank' }) + '\n')
  const msgs = await h.waitForChannel(2)
  check('CRLF record parsed (not mangled by \\r)', msgs[0]?.params?.content === 'windows line',
    JSON.stringify(msgs[0]?.params?.content))
  check('blank line skipped, next record still delivered',
    msgs.length === 2 && msgs[1].params.content === 'after blank', `got ${msgs.length}`)
  h.kill()
}

console.log('\n17. unicode is never split mid-rune')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const h = new Harness(dir, state)
  await h.initialize()
  await sleep(400)
  const thai = 'ทดสอบภาษาไทย — เขียนไฟล์ลง inbox แล้วเด้งเข้ามาเป็นข้อความ 🎉'
  appendFileSync(join(dir, 'uni.jsonl'), JSON.stringify({ from: 'nat', type: 'msg', msg: thai }) + '\n')
  const msgs = await h.waitForChannel(1)
  check('Thai + emoji round-trip intact', msgs[0]?.params?.content === thai,
    JSON.stringify(msgs[0]?.params?.content))
  check('no replacement characters', !String(msgs[0]?.params?.content ?? '').includes('�'))
  h.kill()
}

console.log('\n18. symlinked rooms are refused in BOTH directions')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const { symlinkSync } = await import('node:fs')

  // A "secret" outside the inbox, as if it were ~/.ssh/id_rsa.
  const secret = join(root, 'secret.txt')
  writeFileSync(secret, 'PRIVATE KEY MATERIAL\nsecond secret line\n')
  // …and a victim file a peer-mode write must never be redirected into.
  const victim = join(root, 'authorized_keys')
  writeFileSync(victim, 'ssh-rsa LEGITIMATE\n')

  symlinkSync(secret, join(dir, 'leak.jsonl'))   // read side
  symlinkSync(victim, join(dir, 'pwn.jsonl'))    // write side

  const h = new Harness(dir, state, { INBOX_REPLY_MODE: 'peer' })
  await h.initialize()
  await sleep(1200)

  const leaked = h.channelMsgs().map(m => String(m.params.content)).join('\n')
  check('symlinked room is NOT read into the session', h.channelMsgs().length === 0,
    `leaked: ${leaked.slice(0, 120)}`)
  check('no secret material reached the channel', !leaked.includes('PRIVATE KEY MATERIAL'))
  check('refusal is logged', h.stderr.includes('symlink'), h.stderr.slice(-200))

  const r = await h.request('tools/call', { name: 'reply', arguments: { chat_id: 'pwn', text: 'ssh-rsa ATTACKER' } })
  check('peer write through a symlink is refused', r.result?.isError === true,
    JSON.stringify(r.result).slice(0, 200))
  check('victim file was NOT appended to',
    readFileSync(victim, 'utf8') === 'ssh-rsa LEGITIMATE\n', JSON.stringify(readFileSync(victim, 'utf8')))

  // A normal room in the same directory still works — the guard is targeted, not a blanket ban.
  appendFileSync(join(dir, 'normal.jsonl'), JSON.stringify({ from: 'a', type: 'msg', msg: 'still fine' }) + '\n')
  const msgs = await h.waitForChannel(1)
  check('non-symlink rooms are unaffected', msgs.length === 1 && msgs[0].params.content === 'still fine',
    `got ${msgs.length}`)
  h.kill()
}

console.log('\n19. an over-long line is skipped WHOLE, not fragmented into the session')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  // 8KB cap, 4KB read window: the monster line is far bigger than both, so the skip must span
  // several ticks and still land exactly on the line's end.
  const h = new Harness(dir, state, { INBOX_MAX_LINE_KB: '8', INBOX_MAX_READ_KB: '4' })
  await h.initialize()
  await sleep(400)
  const f = join(dir, 'huge.jsonl')
  appendFileSync(f, JSON.stringify({ from: 'a', type: 'msg', msg: 'X'.repeat(200_000) }) + '\n')
  appendFileSync(f, JSON.stringify({ from: 'a', type: 'msg', msg: 'after the monster' }) + '\n')
  const msgs = await h.waitForChannel(1, 12000)
  check('exactly one message survives', msgs.length === 1, `got ${msgs.length}: ` +
    msgs.map(m => String(m.params.content).slice(0, 30)).join(' | '))
  check('it is the line AFTER the monster', msgs[0]?.params?.content === 'after the monster',
    String(msgs[0]?.params?.content).slice(0, 60))
  check('no fragment of the monster leaked', !msgs.some(m => String(m.params.content).includes('XXXX')))
  check('narrow read window was clamped, not left to stall', h.stderr.includes('raised to match'),
    h.stderr.slice(-200))
  h.kill()
}

console.log('\n20. two servers sharing a STATE_DIR do not rewind each other')
{
  const { dir, state, root } = tmp(); cleanup.push(root)
  const f = join(dir, 'shared.jsonl')

  // A reads 3 lines and saves. B starts later, sees only 1 more line, and saves too.
  const a = new Harness(dir, state)
  await a.initialize()
  await sleep(400)
  for (let i = 0; i < 3; i++) appendFileSync(f, JSON.stringify({ from: 'x', type: 'msg', msg: `m${i}` }) + '\n')
  await a.waitForChannel(3)
  check('server A saw all 3', a.channelMsgs().length === 3, `got ${a.channelMsgs().length}`)

  const b = new Harness(dir, state)
  await b.initialize()
  await sleep(600)
  appendFileSync(f, JSON.stringify({ from: 'x', type: 'msg', msg: 'm3' }) + '\n')
  await b.waitForChannel(1)
  await a.waitForChannel(4)
  check('both servers received the new line', a.channelMsgs().length === 4 && b.channelMsgs().length === 1,
    `A=${a.channelMsgs().length} B=${b.channelMsgs().length}`)

  a.proc.stdin.end(); b.proc.stdin.end()
  await sleep(700)
  a.kill(); b.kill()

  const saved = JSON.parse(readFileSync(join(state, 'cursor.json'), 'utf8'))
  const size = statSync(f).size
  check('merged cursor is at EOF, not rewound', saved['shared.jsonl']?.offset === size,
    `offset=${saved['shared.jsonl']?.offset} size=${size}`)

  // The real consequence: a THIRD server must not re-deliver anything.
  const c = new Harness(dir, state)
  await c.initialize()
  await sleep(800)
  check('a later server replays nothing', c.channelMsgs().length === 0, `got ${c.channelMsgs().length}`)
  c.kill()
}

// ─────────────────────────────────────────────────────────────────────────────
for (const h of spawned) h.kill()
for (const d of cleanup) { try { rmSync(d, { recursive: true, force: true }) } catch {} }

console.log(`\n${'─'.repeat(60)}`)
console.log(`${pass} passed, ${fail} failed`)
if (fail) { console.log(`failures:\n  ${failures.join('\n  ')}`); process.exit(1) }
console.log('all green ✓')
process.exit(0)

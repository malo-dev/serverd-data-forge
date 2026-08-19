import express from 'express'
import cors from 'cors'
import { createServer } from 'http'
import { Server } from 'socket.io'

const PORT = process.env.PORT || 4210
const ADMIN_KEY = process.env.ADMIN_KEY || ''
const DEFAULT_TTL_MS = 30 * 60 * 1000 // 30 minutes
const MIN_TTL_MS = 1 * 60 * 1000 // 1 minute
const MAX_TTL_MS = 4 * 60 * 60 * 1000 // 4 hours

// In-memory only — cleared on server restart. Blocks apply to the collab relay only,
// never to loading the app itself (there is no way to IP-block static hosting from here).
const blockedIps = new Set()

function clientIp(socket) {
  const forwarded = socket.handshake.headers['x-forwarded-for']
  if (forwarded) return forwarded.split(',')[0].trim()
  return socket.handshake.address || 'unknown'
}

function clampTtlMs(requestedMinutes) {
  const ms = Number(requestedMinutes) > 0 ? Number(requestedMinutes) * 60 * 1000 : DEFAULT_TTL_MS
  return Math.min(MAX_TTL_MS, Math.max(MIN_TTL_MS, ms))
}

const app = express()
app.use(cors())
app.get('/', (_req, res) => res.send('JSON Workbench collab relay is running.'))
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }))

const httpServer = createServer(app)
const io = new Server(httpServer, { cors: { origin: '*' } })

// In-memory only — never persisted to disk. Rooms disappear on expiry or when empty.
const rooms = new Map() // code -> { members: Set<socketId>, expiresAt: number, timer: NodeJS.Timeout }

function generateCode() {
  let code
  do {
    code = String(Math.floor(1000 + Math.random() * 9000)) // 4-digit, never starts with 0
  } while (rooms.has(code))
  return code
}

function scheduleExpiry(code, ttlMs) {
  const room = rooms.get(code)
  if (!room) return
  clearTimeout(room.timer)
  room.timer = setTimeout(() => {
    const expired = rooms.get(code)
    if (!expired) return
    io.to(code).emit('tunnel-expired')
    for (const memberId of expired.members) {
      io.sockets.sockets.get(memberId)?.leave(code)
    }
    rooms.delete(code)
    console.log(`[collab] room ${code} expired`)
  }, ttlMs)
}

function memberList(room) {
  return [...room.members].map((id) => room.names.get(id) || 'Anonymous')
}

function requestRate(socket) {
  const buckets = socket.data.rateBuckets
  if (!buckets || !buckets.size) return 0
  const nowSec = Math.floor(Date.now() / 1000)
  let total = 0
  let windowSeconds = 0
  for (const [bucketSec, count] of buckets) {
    if (nowSec - bucketSec <= 4) {
      total += count
      windowSeconds++
    }
  }
  return windowSeconds ? Math.round((total / windowSeconds) * 10) / 10 : 0
}

io.on('connection', (socket) => {
  const ip = clientIp(socket)
  socket.data.ip = ip

  if (blockedIps.has(ip)) {
    socket.emit('ip-blocked')
    socket.disconnect(true)
    return
  }

  // Rolling request-rate tracking for the admin panel: counts inbound events per
  // socket in 1s buckets, keeping only the last few seconds for a live req/s reading.
  socket.data.rateBuckets = new Map() // secondEpoch -> count

  // Defense in depth: also gate every inbound event, not just the connection handshake —
  // covers the race where a block lands after connect but before the socket disconnects.
  socket.use((_packet, next) => {
    if (blockedIps.has(socket.data.ip) && !socket.data.isAdmin) {
      socket.emit('ip-blocked')
      socket.disconnect(true)
      return
    }
    const sec = Math.floor(Date.now() / 1000)
    socket.data.rateBuckets.set(sec, (socket.data.rateBuckets.get(sec) || 0) + 1)
    for (const bucketSec of socket.data.rateBuckets.keys()) {
      if (sec - bucketSec > 5) socket.data.rateBuckets.delete(bucketSec)
    }
    next()
  })

  socket.on('create-tunnel', (payload, ack) => {
    if (blockedIps.has(socket.data.ip)) return ack?.({ ok: false, error: 'Access denied.' })
    const code = generateCode()
    const ttlMs = clampTtlMs(payload?.ttlMinutes)
    const names = new Map([[socket.id, String(payload?.name || 'Anonymous').slice(0, 24)]])
    const ips = new Map([[socket.id, socket.data.ip]])
    rooms.set(code, { members: new Set([socket.id]), names, ips, ttlMs, expiresAt: Date.now() + ttlMs })
    scheduleExpiry(code, ttlMs)
    socket.join(code)
    socket.data.roomCode = code
    ack?.({ ok: true, code, expiresAt: rooms.get(code).expiresAt })
    console.log(`[collab] room ${code} created by ${socket.id} (ttl ${Math.round(ttlMs / 60000)}min)`)
  })

  socket.on('join-tunnel', (payload, ack) => {
    if (blockedIps.has(socket.data.ip)) return ack?.({ ok: false, error: 'Access denied.' })
    const normalized = String(payload?.code || payload || '').trim()
    const room = rooms.get(normalized)
    if (!room) {
      ack?.({ ok: false, error: 'This code is invalid or has expired.' })
      return
    }
    room.members.add(socket.id)
    room.names.set(socket.id, String(payload?.name || 'Anonymous').slice(0, 24))
    room.ips.set(socket.id, socket.data.ip)
    socket.join(normalized)
    socket.data.roomCode = normalized
    ack?.({ ok: true, code: normalized, expiresAt: room.expiresAt, peerCount: room.members.size, peerNames: memberList(room) })
    socket.to(normalized).emit('peer-joined', { peerCount: room.members.size, peerNames: memberList(room) })
    console.log(`[collab] ${socket.id} joined room ${normalized} (${room.members.size} members)`)
  })

  // Silent reconnection attempt after a page reload — same behavior as join,
  // but the caller doesn't treat "not found" as an error worth showing.
  socket.on('rejoin-tunnel', (payload, ack) => {
    if (blockedIps.has(socket.data.ip)) return ack?.({ ok: false })
    const normalized = String(payload?.code || payload || '').trim()
    const room = rooms.get(normalized)
    if (!room) {
      ack?.({ ok: false })
      return
    }
    room.members.add(socket.id)
    room.names.set(socket.id, String(payload?.name || 'Anonymous').slice(0, 24))
    room.ips.set(socket.id, socket.data.ip)
    socket.join(normalized)
    socket.data.roomCode = normalized
    ack?.({ ok: true, code: normalized, expiresAt: room.expiresAt, peerCount: room.members.size, peerNames: memberList(room) })
    socket.to(normalized).emit('peer-joined', { peerCount: room.members.size, peerNames: memberList(room) })
    console.log(`[collab] ${socket.id} rejoined room ${normalized} (${room.members.size} members)`)
  })

  socket.on('state-update', (payload) => {
    const code = socket.data.roomCode
    if (!code || !rooms.has(code)) return
    socket.to(code).emit('state-update', payload)
  })

  // Manual fallback: ask everyone else in the room to re-broadcast their full state now.
  socket.on('request-sync', () => {
    const code = socket.data.roomCode
    if (!code || !rooms.has(code)) return
    socket.to(code).emit('sync-requested')
  })

  // Ephemeral session chat — never stored, gone the moment the tunnel closes.
  socket.on('chat-message', (text) => {
    const code = socket.data.roomCode
    const room = rooms.get(code)
    if (!code || !room) return
    const message = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      name: room.names.get(socket.id) || 'Anonymous',
      text: String(text || '').slice(0, 1000),
      at: Date.now(),
    }
    io.to(code).emit('chat-message', message)
  })

  // File is relayed in-memory only, straight through to peers — never written to disk here.
  socket.on('file-share', (file) => {
    const code = socket.data.roomCode
    const room = rooms.get(code)
    if (!code || !room) return
    if (!file?.dataUrl || typeof file.dataUrl !== 'string' || file.dataUrl.length > 15 * 1024 * 1024) return
    const shared = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      name: String(file.name || 'file').slice(0, 200),
      mimeType: String(file.mimeType || 'application/octet-stream').slice(0, 100),
      size: Number(file.size) || 0,
      dataUrl: file.dataUrl,
      sharedBy: room.names.get(socket.id) || 'Anonymous',
      at: Date.now(),
    }
    io.to(code).emit('file-shared', shared)
    console.log(`[collab] ${socket.id} shared file "${shared.name}" in room ${code}`)
  })

  socket.on('leave-tunnel', () => {
    leaveCurrentRoom(socket)
  })

  socket.on('disconnect', () => {
    leaveCurrentRoom(socket)
  })

  function leaveCurrentRoom(socket) {
    const code = socket.data.roomCode
    if (!code) return
    const room = rooms.get(code)
    if (room) {
      room.members.delete(socket.id)
      room.names.delete(socket.id)
      room.ips.delete(socket.id)
      socket.to(code).emit('peer-left', { peerCount: room.members.size, peerNames: memberList(room) })
      if (room.members.size === 0) {
        clearTimeout(room.timer)
        rooms.delete(code)
        console.log(`[collab] room ${code} closed (empty)`)
      }
    }
    socket.data.roomCode = null
  }

  // --- Admin: view active tunnels/IPs, block/unblock IPs from the collab relay ---
  socket.on('admin-auth', (payload, ack) => {
    const ok = Boolean(ADMIN_KEY) && payload?.key === ADMIN_KEY
    socket.data.isAdmin = ok
    ack?.({ ok })
  })

  function adminState() {
    return {
      blockedIps: [...blockedIps],
      rooms: [...rooms.entries()].map(([code, room]) => ({
        code,
        expiresAt: room.expiresAt,
        members: [...room.members].map((id) => {
          const memberSocket = io.sockets.sockets.get(id)
          return {
            socketId: id,
            name: room.names.get(id) || 'Anonymous',
            ip: room.ips.get(id) || 'unknown',
            reqPerSec: memberSocket ? requestRate(memberSocket) : 0,
          }
        }),
      })),
    }
  }

  socket.on('admin-get-state', (_payload, ack) => {
    if (!socket.data.isAdmin) return ack?.({ ok: false, error: 'Not authorized.' })
    ack?.({ ok: true, state: adminState() })
  })

  socket.on('admin-block-ip', (payload, ack) => {
    if (!socket.data.isAdmin) return ack?.({ ok: false, error: 'Not authorized.' })
    const ip = String(payload?.ip || '').trim()
    if (!ip) return ack?.({ ok: false, error: 'No IP given.' })
    // Never let an admin lock themselves out — an admin session at this IP is exempt,
    // even if it's also sitting in a room being moderated.
    if (ip === socket.data.ip) return ack?.({ ok: false, error: "You can't block your own IP." })
    blockedIps.add(ip)
    for (const [, s] of io.sockets.sockets) {
      if (s.data.ip === ip && !s.data.isAdmin) {
        s.emit('ip-blocked')
        leaveCurrentRoom(s)
        s.disconnect(true)
      }
    }
    console.log(`[admin] blocked ${ip}`)
    ack?.({ ok: true, state: adminState() })
  })

  socket.on('admin-unblock-ip', (payload, ack) => {
    if (!socket.data.isAdmin) return ack?.({ ok: false, error: 'Not authorized.' })
    const ip = String(payload?.ip || '').trim()
    blockedIps.delete(ip)
    console.log(`[admin] unblocked ${ip}`)
    ack?.({ ok: true, state: adminState() })
  })
})

httpServer.listen(PORT, () => {
  console.log(`[collab] relay server listening on http://localhost:${PORT}`)
})

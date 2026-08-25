/**
 * Data Forge relay server.
 *
 * Everything here is in-memory only — nothing is ever written to disk or a
 * database. Rooms, devices, and control sessions all disappear on server
 * restart or when they become empty.
 *
 * - Collab relay: browser tabs join a 4-digit tunnel code and get document
 *   edits, chat, and shared files relayed between them (`rooms`).
 * - Native agents: a separate presence system (`devices`) for the Data Forge
 *   Native Agent — a desktop companion that can join the same tunnel code and
 *   be asked (by a browser tab or another agent) for a remote-control session.
 *   Every remote-control action requires the target device's explicit,
 *   per-session consent; approved sessions are tracked in
 *   `activeControlSessions` and every terminal/mouse/keyboard/screen event is
 *   routed only between that session's controller and target — never broadcast.
 * - IP blocking is defense-in-depth: checked at connect, and again on every
 *   inbound packet via `socket.use`, since a block can land mid-session.
 * - `maxHttpBufferSize` is raised above Engine.IO's ~1MB default so it doesn't
 *   silently kill the socket on a file share near our own 15MB cap.
 */

import express from 'express'
import cors from 'cors'
import { createServer } from 'http'
import { Server } from 'socket.io'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const PUBLIC_DIR = path.join(__dirname, 'public')

const PORT = process.env.PORT || 4210
const ADMIN_KEY = process.env.ADMIN_KEY || ''
const DEFAULT_TTL_MS = 30 * 60 * 1000 // 30 minutes
const MIN_TTL_MS = 1 * 60 * 1000 // 1 minute
const MAX_TTL_MS = 4 * 60 * 60 * 1000 // 4 hours

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

const SERVER_STARTED_AT = Date.now()

const app = express()
app.use(cors())
app.use('/static', express.static(path.join(PUBLIC_DIR, 'static')))
app.get('/', (_req, res) => res.sendFile(path.join(PUBLIC_DIR, 'status.html')))
app.get('/health', (req, res) => {
  const healthData = { ok: true, rooms: rooms.size, devices: devices.size, uptimeMs: Date.now() - SERVER_STARTED_AT }
  if (req.accepts(['json', 'html']) === 'html') {
    res.sendFile(path.join(PUBLIC_DIR, 'health.html'))
  } else {
    res.json(healthData)
  }
})

const httpServer = createServer(app)
// Engine.IO's default maxPayload (~1MB) is far below our own file-share cap (15MB of
// base64) — without raising it, a large share gets killed at the transport level with
// an abrupt disconnect, before the 'file-share' handler's own size check ever runs.
const io = new Server(httpServer, { cors: { origin: '*' }, maxHttpBufferSize: 20 * 1024 * 1024 })

// In-memory only — never persisted to disk. Rooms disappear on expiry or when empty.
const rooms = new Map() // code -> { members: Set<socketId>, expiresAt: number, timer: NodeJS.Timeout }

// Native agents (Data Forge Native Agent), separate from the browser-side collab
// members above. An agent registers itself here on connect, independent of any
// tunnel, and additionally joins a room's `devices` set once it's told to join a
// tunnel by code — that's what lets a browser tab in the same room see it and
// address a remote-control request to it by deviceId.
const devices = new Map() // deviceId -> { socketId, hostname, os, osVersion, arch, agentVersion, status, roomCode }
const pendingControlRequests = new Map() // requestId -> { fromSocketId, toDeviceId, tunnelCode }
// Populated once a control request is approved, cleared when the session ends —
// this is what lets terminal/mouse/keyboard/screen events be routed to exactly the
// controller<->target pair for that session, and rejected from anyone else.
const activeControlSessions = new Map() // requestId -> { controllerSocketId, targetDeviceId, targetSocketId }

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
    rooms.set(code, { members: new Set([socket.id]), names, ips, deviceIds: new Set(), ttlMs, expiresAt: Date.now() + ttlMs })
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
  socket.on('file-share', (file, ack) => {
    const code = socket.data.roomCode
    const room = rooms.get(code)
    if (!code || !room) return ack?.({ ok: false, error: 'You are not in an active tunnel on the server. Try reconnecting.' })
    if (!file?.dataUrl || typeof file.dataUrl !== 'string') return ack?.({ ok: false, error: 'Invalid file.' })
    if (file.dataUrl.length > 15 * 1024 * 1024) return ack?.({ ok: false, error: 'File too large.' })
    const shared = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      name: String(file.name || 'file').slice(0, 200),
      mimeType: String(file.mimeType || 'application/octet-stream').slice(0, 100),
      size: Number(file.size) || 0,
      gzipped: Boolean(file.gzipped),
      dataUrl: file.dataUrl,
      sharedBy: room.names.get(socket.id) || 'Anonymous',
      at: Date.now(),
    }
    io.to(code).emit('file-shared', shared)
    console.log(`[collab] ${socket.id} shared file "${shared.name}" in room ${code}`)
    ack?.({ ok: true })
  })

  socket.on('leave-tunnel', () => {
    leaveCurrentRoom(socket)
  })

  socket.on('disconnect', () => {
    leaveCurrentRoom(socket)
    leaveDeviceTunnel(socket)
    removeDevice(socket)
    for (const [requestId, session] of activeControlSessions) {
      if (session.controllerSocketId === socket.id || session.targetSocketId === socket.id) {
        endControlSession(requestId, 'participant disconnected')
      }
    }
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
      if (room.members.size === 0 && room.deviceIds.size === 0) {
        clearTimeout(room.timer)
        rooms.delete(code)
        console.log(`[collab] room ${code} closed (empty)`)
      }
    }
    socket.data.roomCode = null
  }

  // --- Native agents (Data Forge Native Agent): presence, tunnel membership, and
  // remote-control request relaying between a device and either a browser tab or
  // another device sharing the same tunnel code. ---

  function deviceListForRoom(code) {
    const room = rooms.get(code)
    if (!room) return []
    return [...room.deviceIds]
      .map((id) => devices.get(id))
      .filter(Boolean)
      .map((d) => ({ deviceId: d.deviceId, hostname: d.hostname, os: d.os, arch: d.arch, agentVersion: d.agentVersion, status: d.status }))
  }

  socket.on('device-register', (payload) => {
    if (blockedIps.has(socket.data.ip)) return
    const deviceId = String(payload?.deviceId || '').trim()
    if (!deviceId) return
    socket.data.deviceId = deviceId
    devices.set(deviceId, {
      deviceId,
      socketId: socket.id,
      hostname: String(payload?.hostname || 'unknown').slice(0, 100),
      os: String(payload?.os || 'unknown').slice(0, 50),
      osVersion: String(payload?.osVersion || '').slice(0, 100),
      arch: String(payload?.arch || 'unknown').slice(0, 20),
      agentVersion: String(payload?.agentVersion || '0.0.0').slice(0, 20),
      status: 'online',
      roomCode: null,
      lastSeen: Date.now(),
    })
    console.log(`[agent] device ${deviceId} (${payload?.hostname}) registered`)
  })

  socket.on('device-heartbeat', (payload) => {
    const deviceId = socket.data.deviceId || payload?.deviceId
    const device = deviceId && devices.get(deviceId)
    if (device) device.lastSeen = Date.now()
  })

  socket.on('device-join-tunnel', (payload, ack) => {
    if (blockedIps.has(socket.data.ip)) return ack?.({ ok: false, error: 'Access denied.' })
    const deviceId = socket.data.deviceId
    const device = deviceId && devices.get(deviceId)
    if (!device) return ack?.({ ok: false, error: 'Device not registered yet.' })
    const code = String(payload?.code || '').trim()
    const room = rooms.get(code)
    if (!room) return ack?.({ ok: false, error: 'This code is invalid or has expired.' })

    room.deviceIds.add(deviceId)
    device.roomCode = code
    socket.join(code)
    socket.data.roomCode = code
    socket.to(code).emit('device-joined', { deviceId, hostname: device.hostname, os: device.os })
    ack?.({ ok: true, code, expiresAt: room.expiresAt })
    console.log(`[agent] device ${deviceId} joined tunnel ${code}`)
  })

  socket.on('device-leave-tunnel', () => {
    leaveDeviceTunnel(socket)
  })

  function leaveDeviceTunnel(socket) {
    const deviceId = socket.data.deviceId
    const device = deviceId && devices.get(deviceId)
    if (!device || !device.roomCode) return
    const code = device.roomCode
    const room = rooms.get(code)
    if (room) {
      room.deviceIds.delete(deviceId)
      socket.to(code).emit('device-left', { deviceId })
      if (room.members.size === 0 && room.deviceIds.size === 0) {
        clearTimeout(room.timer)
        rooms.delete(code)
        console.log(`[collab] room ${code} closed (empty)`)
      }
    }
    device.roomCode = null
  }

  function removeDevice(socket) {
    const deviceId = socket.data.deviceId
    if (!deviceId) return
    devices.delete(deviceId)
    console.log(`[agent] device ${deviceId} disconnected`)
  }

  // List devices present in the caller's current tunnel — what JSON-Workbench's UI
  // calls to populate "machines connected to this tunnel."
  socket.on('device-list', (_payload, ack) => {
    const code = socket.data.roomCode
    if (!code) return ack?.({ ok: false, error: 'You are not in a tunnel.' })
    ack?.({ ok: true, devices: deviceListForRoom(code) })
  })

  // A browser tab (or another device) in the tunnel asks to control a specific
  // device by id. The request is relayed to that device's agent — nothing here
  // grants control; the target agent's own consent prompt decides.
  socket.on('device-control-request', (payload, ack) => {
    const code = socket.data.roomCode
    if (!code) return ack?.({ ok: false, error: 'You are not in a tunnel.' })
    const targetDeviceId = String(payload?.deviceId || '').trim()
    const target = devices.get(targetDeviceId)
    if (!target || target.roomCode !== code) return ack?.({ ok: false, error: 'That device is not in this tunnel.' })

    const targetSocket = io.sockets.sockets.get(target.socketId)
    if (!targetSocket) return ack?.({ ok: false, error: 'That device is not reachable right now.' })

    const requestId = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
    pendingControlRequests.set(requestId, { fromSocketId: socket.id, toDeviceId: targetDeviceId, tunnelCode: code })
    targetSocket.emit('device-control-request', {
      requestId,
      fromLabel: String(payload?.fromLabel || 'A tunnel participant').slice(0, 100),
      tunnelCode: code,
    })
    ack?.({ ok: true, requestId })
    console.log(`[agent] control request ${requestId} sent to device ${targetDeviceId} in tunnel ${code}`)
  })

  // The target agent's own consent decision comes back here, and we relay it to
  // whoever originally asked. On approval, the pair is promoted into
  // activeControlSessions so terminal/input/screen events can be routed and
  // authorized for the lifetime of this specific session.
  socket.on('device-control-response', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    const pending = pendingControlRequests.get(requestId)
    if (!pending) return
    pendingControlRequests.delete(requestId)
    const approved = Boolean(payload?.approved)
    if (approved) {
      activeControlSessions.set(requestId, {
        controllerSocketId: pending.fromSocketId,
        targetDeviceId: pending.toDeviceId,
        targetSocketId: socket.id,
      })
    }
    const requester = io.sockets.sockets.get(pending.fromSocketId)
    requester?.emit('device-control-response', { requestId, approved, deviceId: pending.toDeviceId })
    console.log(`[agent] control request ${requestId} ${approved ? 'approved' : 'denied'}`)
  })

  function endControlSession(requestId, reason) {
    const session = activeControlSessions.get(requestId)
    if (!session) return
    activeControlSessions.delete(requestId)
    const controller = io.sockets.sockets.get(session.controllerSocketId)
    const target = io.sockets.sockets.get(session.targetSocketId)
    controller?.emit('device-control-session-ended', { requestId, reason })
    target?.emit('device-control-session-ended', { requestId, reason })
    console.log(`[agent] control session ${requestId} ended (${reason})`)
  }

  // Either side can end an active session at any time — the controller giving up,
  // or the target agent's own "Disconnect / Stop Session" control.
  socket.on('device-control-end', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    const session = activeControlSessions.get(requestId)
    if (!session) return
    if (socket.id !== session.controllerSocketId && socket.id !== session.targetSocketId) return
    endControlSession(requestId, 'ended by participant')
  })

  function isSessionController(requestId, socketId) {
    const session = activeControlSessions.get(requestId)
    return Boolean(session && session.controllerSocketId === socketId)
  }

  function isSessionTarget(requestId, socketId) {
    const session = activeControlSessions.get(requestId)
    return Boolean(session && session.targetSocketId === socketId)
  }

  // --- Remote terminal: strictly scoped to an active, approved control session.
  // The controller's shell input is relayed to the target agent, which is the only
  // thing that ever actually spawns a shell and runs commands. ---
  socket.on('terminal-input', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionController(requestId, socket.id)) return
    const session = activeControlSessions.get(requestId)
    const target = io.sockets.sockets.get(session.targetSocketId)
    target?.emit('terminal-input', { requestId, data: String(payload?.data || '') })
  })

  socket.on('terminal-output', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionTarget(requestId, socket.id)) return
    const session = activeControlSessions.get(requestId)
    const controller = io.sockets.sockets.get(session.controllerSocketId)
    controller?.emit('terminal-output', { requestId, data: String(payload?.data || '') })
  })

  socket.on('terminal-resize', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionController(requestId, socket.id)) return
    const session = activeControlSessions.get(requestId)
    const target = io.sockets.sockets.get(session.targetSocketId)
    const cols = Number(payload?.cols) || 80
    const rows = Number(payload?.rows) || 24
    target?.emit('terminal-resize', { requestId, cols, rows })
  })

  // --- Remote mouse/keyboard: same strict routing as the terminal above — only
  // the approved controller of an active session can send input, and it's relayed
  // only to that session's target agent, never broadcast. ---
  function relayInputEvent(eventName) {
    socket.on(eventName, (payload) => {
      const requestId = String(payload?.requestId || '').trim()
      if (!isSessionController(requestId, socket.id)) return
      const session = activeControlSessions.get(requestId)
      const target = io.sockets.sockets.get(session.targetSocketId)
      target?.emit(eventName, payload)
    })
  }
  relayInputEvent('remote-mouse-move')
  relayInputEvent('remote-mouse-click')
  relayInputEvent('remote-mouse-scroll')
  relayInputEvent('remote-key-press')
  relayInputEvent('remote-type-text')

  // --- Screen sharing (Socket.IO periodic-JPEG version — see screenCapture.ts on
  // the agent side; a WebRTC-based version can replace this later without
  // changing the consent/session model). Controller asks the target to start/stop;
  // frames flow target -> controller only, strictly scoped to the active session. ---
  socket.on('screen-share-start', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionController(requestId, socket.id)) return
    const session = activeControlSessions.get(requestId)
    const target = io.sockets.sockets.get(session.targetSocketId)
    target?.emit('screen-share-start', { requestId })
  })

  socket.on('screen-share-stop', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionController(requestId, socket.id)) return
    const session = activeControlSessions.get(requestId)
    const target = io.sockets.sockets.get(session.targetSocketId)
    target?.emit('screen-share-stop', { requestId })
  })

  socket.on('screen-frame', (payload) => {
    const requestId = String(payload?.requestId || '').trim()
    if (!isSessionTarget(requestId, socket.id)) return
    if (!payload?.dataUrl || typeof payload.dataUrl !== 'string') return
    // Frames are relayed live, never buffered/stored server-side — same in-memory-
    // only posture as the rest of this relay. A large-but-bounded cap protects
    // against a runaway payload without needing per-frame disk or memory tracking.
    if (payload.dataUrl.length > 4 * 1024 * 1024) return
    const session = activeControlSessions.get(requestId)
    const controller = io.sockets.sockets.get(session.controllerSocketId)
    controller?.emit('screen-frame', {
      requestId,
      dataUrl: payload.dataUrl,
      width: Number(payload.width) || 0,
      height: Number(payload.height) || 0,
      capturedAt: Number(payload.capturedAt) || Date.now(),
    })
  })

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
      // Every socket currently connected to the relay, whether or not it's sitting in a
      // tunnel — so the admin can see people who just have the app open, not in a session.
      allConnections: [...io.sockets.sockets.values()].map((s) => ({
        socketId: s.id,
        ip: s.data.ip || 'unknown',
        roomCode: s.data.roomCode || null,
        isAdmin: Boolean(s.data.isAdmin),
        reqPerSec: requestRate(s),
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

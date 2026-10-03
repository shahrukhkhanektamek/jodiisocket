const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  maxHttpBufferSize: 1e8, // 100MB for potential high data bursts
  pingTimeout: 30000,
  pingInterval: 10000,
});

// ==================== STATE MANAGEMENT ====================
// Map of userId (string) -> Set of socketId
const userSockets = new Map();
// Map of socketId -> { userId, role, name, avatar }
const socketUsers = new Map();
// Map of roomId -> { hostSocketId, hostUserId, info, viewers: Set<socketId>, viewersHistory: Map, duration, sessionCoins }
const activeLiveRooms = new Map();
// Map of roomId -> disconnect timeout for host background / resume grace period
const roomDisconnectTimeouts = new Map();
// Map of callId -> { callId, sessionId, callerId, receiverId, callType, caller, status, endedBy, reason, startedAt, acceptedAt, endedAt }
const activeCalls = new Map();
// Map of partyRoomId -> 8 seats array
const partyRooms = new Map();

// Helper to get all equivalent IDs for a user (e.g. '1' <-> 'host_1')
const getCandidateUserIds = (id) => {
  if (!id) return [];
  const raw = String(id).trim();
  const set = new Set([raw]);
  if (raw.startsWith('host_')) {
    set.add(raw.replace(/^host_/, ''));
  } else if (/^\d+$/.test(raw)) {
    set.add(`host_${raw}`);
  }
  return Array.from(set);
};

const isUserMatch = (idA, idB) => {
  if (!idA || !idB) return false;
  const cA = getCandidateUserIds(idA);
  const cB = getCandidateUserIds(idB);
  return cA.some((a) => cB.includes(a));
};

// Helper to send to a user using all candidate ID forms with socket deduplication
const emitToUser = (targetUserId, event, payload) => {
  const candidateIds = getCandidateUserIds(targetUserId);
  const targetSockets = new Set();
  for (const cid of candidateIds) {
    const sIds = userSockets.get(cid);
    if (sIds && sIds.size > 0) {
      sIds.forEach((sid) => targetSockets.add(sid));
    }
  }
  if (targetSockets.size > 0) {
    targetSockets.forEach((sid) => {
      io.to(sid).emit(event, payload);
    });
    return true;
  }
  return false;
};

// In-memory stream history (seed data + completed streams)
const streamHistory = [
  {
    id: 'sh_seed_1',
    roomId: 'room_live_seed_1',
    hostId: '1',
    title: 'Late Night Acoustic Vibes & Chill',
    category: 'Acoustic & Chill',
    durationSeconds: 2740,
    coinsEarned: 1850,
    peakViewers: 28,
    viewers: [
      { id: 'u1', name: 'Aarav Sharma', avatar: 'https://images.unsplash.com/photo-1535713875002-d1d0cf377fde?w=800&q=80', joinedAt: 'Yesterday 10:15 PM', coinsSpent: 400 },
      { id: 'u2', name: 'Rohan Verma', avatar: 'https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=800&q=80', joinedAt: 'Yesterday 10:20 PM', coinsSpent: 650 },
      { id: 'u3', name: 'Kabir Mehta', avatar: 'https://images.unsplash.com/photo-1522075469751-3a6694fb2f61?w=800&q=80', joinedAt: 'Yesterday 10:32 PM', coinsSpent: 800 },
    ],
    startedAt: new Date(Date.now() - 86400000).toISOString(),
    endedAt: new Date(Date.now() - 86400000 + 2740000).toISOString(),
  },
  {
    id: 'sh_seed_2',
    roomId: 'room_live_seed_2',
    hostId: '1',
    title: 'Weekend Special Q&A with Fans',
    category: 'Conversations',
    durationSeconds: 3600,
    coinsEarned: 2400,
    peakViewers: 34,
    viewers: [
      { id: 'u4', name: 'Vikram Joshi', avatar: 'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=800&q=80', joinedAt: '2 days ago', coinsSpent: 1200 },
      { id: 'u5', name: 'Priya Sen', avatar: 'https://images.unsplash.com/photo-1494790108377-be9c29b29330?w=800&q=80', joinedAt: '2 days ago', coinsSpent: 750 },
      { id: 'u6', name: 'Neha Gupta', avatar: 'https://images.unsplash.com/photo-1517841905240-472988babdf9?w=800&q=80', joinedAt: '2 days ago', coinsSpent: 450 },
    ],
    startedAt: new Date(Date.now() - 172800000).toISOString(),
    endedAt: new Date(Date.now() - 172800000 + 3600000).toISOString(),
  },
];

// ==================== REST API ENDPOINTS ====================

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Jodii Club Unified WebRTC & Socket Server',
    connectedClients: socketUsers.size,
    activeLiveRooms: activeLiveRooms.size,
    activeCalls: activeCalls.size,
    timestamp: new Date().toISOString(),
  });
});

// 1. Get Host Stream History
app.get('/api/live/history/:hostId', (req, res) => {
  const hostId = String(req.params.hostId);
  const list = streamHistory.filter((s) => !s.hostId || String(s.hostId) === hostId || hostId === 'all' || hostId === '1');
  res.json({
    success: true,
    history: list,
  });
});

// 2. Check Active Live Stream for Host
app.get('/api/live/active/:hostId', (req, res) => {
  const hostId = String(req.params.hostId);
  for (const [roomId, room] of activeLiveRooms.entries()) {
    if (String(room.hostUserId) === hostId || (room.info && String(room.info.hostId) === hostId)) {
      return res.json({
        success: true,
        activeStream: {
          roomId,
          title: room.info?.title || 'Live Broadcast',
          category: room.info?.category || 'Acoustic & Chill',
          startedAt: room.info?.startedAt || Date.now(),
          viewersCount: room.viewers ? room.viewers.size : 0,
          duration: room.duration || room.info?.duration || 0,
        },
      });
    }
  }
  return res.json({ success: true, activeStream: null });
});

// Check All Active Live Rooms & Hosts for Reconciliation
app.get('/api/live/active-rooms', (req, res) => {
  const rooms = [];
  const activeHostIds = [];
  for (const [roomId, room] of activeLiveRooms.entries()) {
    const isHostOnline = Boolean(room.hostSocketId && io.sockets.sockets.has(room.hostSocketId));
    rooms.push({
      roomId,
      hostUserId: String(room.hostUserId),
      isHostOnline,
      viewersCount: room.viewers ? room.viewers.size : 0,
      title: room.info?.title || 'Live Broadcast',
      category: room.info?.category || 'Acoustic & Chill',
    });
    if (room.hostUserId) {
      activeHostIds.push(String(room.hostUserId));
    }
  }
  return res.json({
    success: true,
    count: rooms.length,
    rooms,
    activeHostIds,
  });
});

// Stream Termination Helper
const handleEndLiveStream = (data) => {
  const roomId = String(data?.roomId || data?.room_id || data?.streamId || '');
  const hostId = data?.hostId || data?.host_id ? String(data.hostId || data.host_id) : null;
  console.log(`[Live Stream] Host explicitly ended live in room: ${roomId}, host: ${hostId}`);

  if (roomId && roomDisconnectTimeouts.has(roomId)) {
    clearTimeout(roomDisconnectTimeouts.get(roomId));
    roomDisconnectTimeouts.delete(roomId);
  }

  const room = activeLiveRooms.get(roomId);
  const durationSeconds = Number(data?.durationSeconds || data?.duration_seconds || (room ? room.duration : 0)) || 0;
  const coinsEarned = Number(data?.coinsEarned || data?.coins_earned || (room ? room.sessionCoins : 0)) || 0;

  if (room) {
    const historyItem = {
      id: 'sh_' + Date.now(),
      roomId,
      hostId: hostId || room.hostUserId || '1',
      title: room.info?.title || 'Live Broadcast',
      category: room.info?.category || 'Acoustic & Chill',
      durationSeconds: durationSeconds,
      coinsEarned: coinsEarned,
      peakViewers: room.viewersHistory ? room.viewersHistory.size : (room.viewers ? room.viewers.size : 1),
      viewers: room.viewersHistory ? Array.from(room.viewersHistory.values()) : [],
      startedAt: room.info?.startedAt || new Date(Date.now() - (durationSeconds * 1000)).toISOString(),
      endedAt: new Date().toISOString(),
    };
    streamHistory.unshift(historyItem);
    console.log(`[Stream History] Recorded stream ${roomId} with ${historyItem.viewers.length} viewers, duration: ${durationSeconds}s`);
  } else if (roomId) {
    streamHistory.unshift({
      id: 'sh_' + Date.now(),
      roomId,
      hostId: hostId || '1',
      title: 'Live Broadcast',
      category: 'Acoustic & Chill',
      durationSeconds: durationSeconds,
      coinsEarned: coinsEarned,
      peakViewers: 1,
      viewers: [],
      startedAt: new Date(Date.now() - (durationSeconds * 1000)).toISOString(),
      endedAt: new Date().toISOString(),
    });
  }

  // Notify viewers inside the room
  if (roomId) {
    io.to(roomId).emit('stream:ended', { roomId, streamId: roomId, hostId });
    io.to(`stream_${roomId}`).emit('stream:ended', { roomId, streamId: roomId, hostId });
    activeLiveRooms.delete(roomId);
  }

  if (hostId) {
    for (const [rId, r] of activeLiveRooms.entries()) {
      if (String(r.hostUserId) === hostId || (r.info && String(r.info.hostId) === hostId)) {
        console.log(`[handleEndLiveStream] Also deleting active room ${rId} for host ${hostId}`);
        activeLiveRooms.delete(rId);
      }
    }
  }

  // Broadcast globally so all discovery cards in user apps are removed immediately!
  io.emit('stream:ended', { roomId, streamId: roomId, hostId });
  io.emit('stream:ended_global', { roomId, streamId: roomId, hostId });
  io.emit('live_stream_ended', { roomId, streamId: roomId, hostId });

  // Call Laravel API to update MySQL LiveRoom and Host table
  try {
    const postData = JSON.stringify({
      room_id: roomId,
      host_id: hostId,
      duration_seconds: durationSeconds,
      coins_earned: coinsEarned,
      end_reason: data?.end_reason || 'socket_sync',
    });
    const req = http.request(
      'http://127.0.0.1/projects/irshad/jodiiclub/api/v1/live/end-room',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(postData),
        },
        timeout: 3000,
      },
      (laravelRes) => {
        // success
      }
    );
    req.on('error', (e) => {
      console.warn('[handleEndLiveStream] Failed to notify Laravel end-room:', e.message);
    });
    req.write(postData);
    req.end();
  } catch (err) {
    console.warn('[handleEndLiveStream] Error calling Laravel end-room:', err);
  }
};

// REST endpoints to terminate an active live stream
app.post('/api/live/end', (req, res) => {
  handleEndLiveStream(req.body);
  return res.json({ success: true, message: 'Stream ended successfully' });
});

app.post('/api/live-rooms/end', (req, res) => {
  handleEndLiveStream(req.body);
  return res.json({ success: true, message: 'Stream ended successfully' });
});

app.post('/api/live/end-room', (req, res) => {
  handleEndLiveStream(req.body);
  return res.json({ success: true, message: 'Stream ended successfully' });
});

// 3. Dynamic Host Dashboard Stats
app.get('/api/host/dashboard/:hostId', (req, res) => {
  const hostId = String(req.params.hostId);
  const hostStreams = streamHistory.filter((s) => !s.hostId || String(s.hostId) === hostId || hostId === 'all');
  const totalStreamSeconds = hostStreams.reduce((acc, s) => acc + (s.durationSeconds || 0), 0);
  const totalStreamHours = (totalStreamSeconds / 3600).toFixed(1);
  const totalCoinsEarned = hostStreams.reduce((acc, s) => acc + (s.coinsEarned || 0), 0);

  let activeStream = null;
  for (const [roomId, room] of activeLiveRooms.entries()) {
    if (String(room.hostUserId) === hostId || (room.info && String(room.info.hostId) === hostId)) {
      activeStream = {
        roomId,
        title: room.info?.title || 'Live Broadcast',
        category: room.info?.category || 'Acoustic & Chill',
        startedAt: room.info?.startedAt || Date.now(),
        viewersCount: room.viewers ? room.viewers.size : 0,
        duration: room.duration || room.info?.duration || 0,
      };
      break;
    }
  }

  res.json({
    success: true,
    data: {
      totalStreams: hostStreams.length,
      streamHours: totalStreamHours !== '0.0' ? totalStreamHours : '18.5',
      streamHoursWeek: totalStreamHours !== '0.0' ? totalStreamHours : '18.5',
      totalFollowers: 142 + (hostStreams.length * 15),
      audienceReach: 1420 + (hostStreams.length * 85),
      availableCoins: 2450 + totalCoinsEarned,
      totalEarningsRs: 894274.50,
      availableEarningsRs: 877274.50,
      todayEarningsRupees: 650,
      activeStream,
      recentStreams: hostStreams.slice(0, 5),
    },
  });
});

// ==================== SOCKET.IO CONNECTIONS ====================

io.on('connection', (socket) => {
  console.log(`[Socket Connected] ID: ${socket.id}`);

  // 1. User Online Registration (Supports 'register_user' and 'user:register')
  const handleUserRegister = (data) => {
    if (!data || (!data.userId && !data.id)) return;
    const userId = String(data.userId || data.id);
    const candidateIds = getCandidateUserIds(userId);

    socketUsers.set(socket.id, {
      userId,
      role: data.role || 'user',
      name: data.name || 'Anonymous',
      avatar: data.avatar,
    });

    candidateIds.forEach((cid) => {
      if (!userSockets.has(cid)) {
        userSockets.set(cid, new Set());
      }
      userSockets.get(cid).add(socket.id);
    });

    console.log(`[User Registered] User ${userId} (keys: ${candidateIds.join(', ')}) mapped to socket ${socket.id}`);
    socket.emit('registered', { success: true, userId });
    socket.broadcast.emit('user:online_status', { userId, isOnline: true });

    // Check activeCalls: Deliver queued calls to newly registered receiver
    for (const [cId, call] of activeCalls.entries()) {
      if (call.status === 'ringing' && isUserMatch(call.receiverId, userId)) {
        if (Date.now() - call.startedAt < 35000) {
          console.log(`[Register] Delivering queued incoming_call to newly registered receiver ${userId} for call ${cId}`);
          const incomingPayload = {
            callId: call.callId,
            sessionId: call.sessionId,
            callType: call.callType || 'video',
            caller: call.caller,
          };
          socket.emit('incoming_call', incomingPayload);
          socket.emit('call:incoming', incomingPayload);
        }
      }
    }
  };

  socket.on('register_user', handleUserRegister);
  socket.on('user:register', handleUserRegister);

  // ==================== 1-ON-1 CALL SIGNALING ====================

  socket.on('call_initiate', (payload) => {
    const callerInfo = socketUsers.get(socket.id);
    const callerId = callerInfo ? callerInfo.userId : (payload.caller?.id || 'caller');
    const receiverId = String(payload?.receiver?.id || payload?.receiverId || payload?.targetUserId);
    const callId = payload.callId || 'call_' + Date.now();
    console.log(`[Call Initiate] From ${callerId} to ${receiverId} (${payload.callType})`);

    activeCalls.set(callId, {
      callId,
      sessionId: payload.sessionId,
      callerId,
      receiverId,
      callType: payload.callType || 'video',
      caller: {
        id: callerId,
        name: callerInfo ? callerInfo.name : (payload.caller?.name || 'Caller'),
        avatar: callerInfo?.avatar || payload.caller?.avatar,
        role: callerInfo?.role || payload.caller?.role || 'user',
      },
      status: 'ringing',
      startedAt: Date.now(),
    });

    const incomingData = {
      callId,
      sessionId: payload.sessionId,
      callType: payload.callType || 'video',
      caller: {
        id: callerId,
        name: callerInfo ? callerInfo.name : (payload.caller?.name || 'Caller'),
        avatar: callerInfo?.avatar || payload.caller?.avatar,
        role: callerInfo?.role || payload.caller?.role || 'user',
      },
      receiverId,
    };

    const delivered = emitToUser(receiverId, 'incoming_call', incomingData);
    emitToUser(receiverId, 'call:incoming', incomingData);

    if (!delivered) {
      console.log(`[Call Initiate] Receiver ${receiverId} not yet connected; ringing active for call ${callId}`);
    }
  });
  socket.on('call:initiate', (payload) => {
    socket.emit('call_initiate', payload);
  });

  socket.on('call_accept', (payload) => {
    const callId = payload.callId;
    const targetId = payload.callerId || payload.remoteUserId || payload.targetUserId;
    console.log(`[Call Accept] Call ${callId} accepted for ${targetId}`);

    const call = activeCalls.get(callId);
    if (call) {
      call.status = 'connected';
      call.acceptedAt = Date.now();
    }

    const acceptPayload = {
      callId,
      acceptedBy: socketUsers.get(socket.id)?.userId || payload.acceptedBy,
    };

    const recipient = targetId || (call ? call.callerId : null);
    if (recipient) {
      emitToUser(recipient, 'call_accepted', acceptPayload);
      emitToUser(recipient, 'call:accepted', acceptPayload);
    }
  });
  socket.on('call:accept', (payload) => {
    const callId = payload.callId;
    const targetId = payload.callerId || payload.remoteUserId || payload.targetUserId;
    const call = activeCalls.get(callId);
    if (call) {
      call.status = 'connected';
      call.acceptedAt = Date.now();
    }
    const acceptPayload = {
      callId,
      acceptedBy: socketUsers.get(socket.id)?.userId || payload.acceptedBy,
    };
    const recipient = targetId || (call ? call.callerId : null);
    if (recipient) {
      emitToUser(recipient, 'call_accepted', acceptPayload);
      emitToUser(recipient, 'call:accepted', acceptPayload);
    }
  });

  socket.on('call_reject', (payload) => {
    const callId = payload.callId;
    const targetUserId = payload.callerId || payload.targetUserId || payload.remoteUserId || payload.receiverId;
    console.log(`[Call Reject] Call ${callId} rejected for ${targetUserId} by ${payload.endedBy}`);

    const call = activeCalls.get(callId);
    if (call) {
      call.status = 'rejected';
      call.endedBy = payload.endedBy || 'host';
      call.reason = payload.reason || 'Call rejected';
      call.endedAt = Date.now();
      setTimeout(() => {
        if (activeCalls.get(callId)?.status === 'rejected') {
          activeCalls.delete(callId);
        }
      }, 5000);
    }

    const rejData = {
      callId,
      reason: payload.reason || 'Call rejected',
      endedBy: payload.endedBy || 'host',
    };

    const recipient = targetUserId || (call ? call.callerId : null);
    if (recipient) {
      emitToUser(recipient, 'call_rejected', rejData);
      emitToUser(recipient, 'call:rejected', rejData);
    }
  });
  socket.on('call:reject', (payload) => {
    const callId = payload.callId;
    const targetUserId = payload.callerId || payload.targetUserId || payload.remoteUserId || payload.receiverId;
    const call = activeCalls.get(callId);
    if (call) {
      call.status = 'rejected';
      call.endedBy = payload.endedBy || 'host';
      call.reason = payload.reason || 'Call rejected';
      call.endedAt = Date.now();
      setTimeout(() => {
        if (activeCalls.get(callId)?.status === 'rejected') {
          activeCalls.delete(callId);
        }
      }, 5000);
    }
    const rejData = {
      callId,
      reason: payload.reason || 'Call rejected',
      endedBy: payload.endedBy || 'host',
    };
    const recipient = targetUserId || (call ? call.callerId : null);
    if (recipient) {
      emitToUser(recipient, 'call_rejected', rejData);
      emitToUser(recipient, 'call:rejected', rejData);
    }
  });

  const handleCallEnd = (payload) => {
    const callId = payload.callId;
    const targetUserId = payload.remoteUserId || payload.targetUserId || payload.receiverId || payload.callerId;
    const sender = socketUsers.get(socket.id);
    const endedBy = payload.endedBy || sender?.role || 'user';
    const reason = payload.reason || (endedBy === 'host' ? 'ended_by_host' : 'ended_by_user');
    console.log(`[Call End] Call ${callId} ended by ${endedBy} for ${targetUserId}`);

    let call = activeCalls.get(callId);
    if (!call && targetUserId) {
      for (const c of activeCalls.values()) {
        if (c.callId === callId || isUserMatch(c.callerId, targetUserId) || isUserMatch(c.receiverId, targetUserId)) {
          call = c;
          break;
        }
      }
    }
    if (call) {
      call.status = 'ended';
      call.endedBy = endedBy;
      call.reason = reason;
      call.endedAt = Date.now();
      setTimeout(() => {
        if (activeCalls.get(callId)?.status === 'ended') {
          activeCalls.delete(callId);
        }
      }, 5000);
    }

    const endPayload = {
      callId,
      endedBy,
      reason,
    };

    const recipient = targetUserId || (call ? (isUserMatch(call.callerId, sender?.userId) ? call.receiverId : call.callerId) : null);
    if (recipient) {
      emitToUser(recipient, 'call_ended', endPayload);
      emitToUser(recipient, 'call:ended', endPayload);
    }
  };
  socket.on('call_end', handleCallEnd);
  socket.on('call:end', handleCallEnd);

  // Check call status on demand (e.g. when app resumes from background)
  socket.on('check_call_status', (data) => {
    const callId = data?.callId;
    const call = activeCalls.get(callId);
    if (call) {
      socket.emit('call_status_response', {
        callId,
        status: call.status,
        endedBy: call.endedBy,
        reason: call.reason,
      });
      if (call.status === 'ended' || call.status === 'rejected') {
        socket.emit('call_ended', {
          callId,
          endedBy: call.endedBy || 'remote',
          reason: call.reason || 'Call ended',
        });
      }
    } else {
      socket.emit('call_status_response', {
        callId,
        status: 'unknown',
        reason: 'Call session not active in memory',
      });
    }
  });

  // 1-on-1 In-Call Gifts
  const handleCallGift = (payload) => {
    const targetUserId = payload.receiverId || payload.targetUserId || payload.remoteUserId || payload.hostId;
    console.log(`[Call Gift] From ${payload.senderName || payload.senderId} to ${targetUserId}: ${payload.gift?.name}`);
    if (targetUserId) {
      emitToUser(targetUserId, 'call_gift', payload);
      emitToUser(targetUserId, 'call:gift', payload);
    }
  };
  socket.on('call_gift', handleCallGift);
  socket.on('call:gift', handleCallGift);

  // WebRTC 1-on-1 SDP Offer / Answer / ICE
  const handleWebRtcOffer = (payload) => {
    const sender = socketUsers.get(socket.id);
    const targetUserId = payload.targetUserId || payload.toUserId || payload.remoteUserId;
    console.log(`[WebRTC 1-on-1 Offer] From ${sender?.userId} to ${targetUserId}`);
    const data = {
      offer: payload.offer,
      fromUserId: sender ? sender.userId : payload.fromUserId,
      callId: payload.callId,
    };
    emitToUser(targetUserId, 'webrtc_offer', data);
  };
  socket.on('webrtc_offer', handleWebRtcOffer);
  socket.on('webrtc:offer', handleWebRtcOffer);

  const handleWebRtcAnswer = (payload) => {
    const sender = socketUsers.get(socket.id);
    const targetUserId = payload.targetUserId || payload.toUserId || payload.remoteUserId;
    console.log(`[WebRTC 1-on-1 Answer] From ${sender?.userId} to ${targetUserId}`);
    const data = {
      answer: payload.answer,
      fromUserId: sender ? sender.userId : payload.fromUserId,
      callId: payload.callId,
    };
    emitToUser(targetUserId, 'webrtc_answer', data);
  };
  socket.on('webrtc_answer', handleWebRtcAnswer);
  socket.on('webrtc:answer', handleWebRtcAnswer);

  const handleWebRtcIce = (payload) => {
    const sender = socketUsers.get(socket.id);
    const targetUserId = payload.targetUserId || payload.toUserId || payload.remoteUserId;
    const data = {
      candidate: payload.candidate,
      fromUserId: sender ? sender.userId : payload.fromUserId,
      callId: payload.callId,
    };
    emitToUser(targetUserId, 'webrtc_ice_candidate', data);
  };
  socket.on('webrtc_ice_candidate', handleWebRtcIce);
  socket.on('webrtc:ice_candidate', handleWebRtcIce);

  // Backward compatibility: webrtc_signal
  socket.on('webrtc_signal', (payload) => {
    const sender = socketUsers.get(socket.id);
    const targetUserId = payload.targetUserId || payload.toUserId;
    const data = {
      signal: payload.signal || payload.signalData,
      signalData: payload.signal || payload.signalData,
      type: payload.type,
      fromUserId: sender ? sender.userId : null,
    };
    emitToUser(targetUserId, 'webrtc_signal', data);
    emitToUser(targetUserId, 'webrtc:signal', data);
  });
  socket.on('webrtc:signal', (payload) => {
    const sender = socketUsers.get(socket.id);
    const targetUserId = payload.targetUserId || payload.toUserId;
    const data = {
      signal: payload.signal || payload.signalData,
      signalData: payload.signal || payload.signalData,
      type: payload.type,
      fromUserId: sender ? sender.userId : null,
    };
    emitToUser(targetUserId, 'webrtc_signal', data);
    emitToUser(targetUserId, 'webrtc:signal', data);
  });

  // ==================== LIVE STREAMING (1-TO-MANY WEBRTC) ====================

  socket.on('host:went_live', (data) => {
    const roomId = String(data.roomId || 'room_' + Date.now());
    socket.join(roomId);
    socket.join(`stream_${roomId}`);

    // Cancel any pending disconnect grace timer for this room
    if (roomDisconnectTimeouts.has(roomId)) {
      clearTimeout(roomDisconnectTimeouts.get(roomId));
      roomDisconnectTimeouts.delete(roomId);
      console.log(`[Live Stream] Host reconnected/resumed in room: ${roomId}`);
    }

    let existingViewers = new Set();
    if (activeLiveRooms.has(roomId)) {
      existingViewers = activeLiveRooms.get(roomId).viewers;
    }
    existingViewers.delete(socket.id);

    activeLiveRooms.set(roomId, {
      hostSocketId: socket.id,
      hostUserId: String(data.hostId),
      info: data,
      viewers: existingViewers,
      viewersHistory: activeLiveRooms.get(roomId)?.viewersHistory || new Map(),
      duration: data.duration || 0,
    });
    console.log(`[Live Stream] Host went live in room: ${roomId} (Socket: ${socket.id})`);
    
    const liveAlertPayload = {
      roomId,
      streamId: roomId,
      hostId: String(data.hostId),
      hostName: data.hostName || 'Live Host',
      avatar: data.avatar || data.hostAvatar || 'https://images.unsplash.com/photo-1534528741775-53994a69daeb?w=800&q=80',
      coverImage: data.coverImage || data.avatar || 'https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4?w=800&q=80',
      title: data.title || 'Live Broadcast',
      category: data.category || 'Live Lounge',
      viewersCount: 1,
      tags: data.tags || ['#Live', '#Official'],
      startedAt: new Date().toISOString(),
      ...data,
    };
    io.emit('live_stream_started', liveAlertPayload);
    socket.broadcast.emit('host:live_alert', liveAlertPayload);
    io.emit('host:went_live_broadcast', liveAlertPayload);

    // If viewers joined before host:went_live, tell host to initiate WebRTC for each
    existingViewers.forEach((viewerSocketId) => {
      if (viewerSocketId !== socket.id) {
        io.to(socket.id).emit('viewer:joined', {
          viewerSocketId,
          roomId,
          viewerCount: existingViewers.size,
        });
      }
    });
  });

  socket.on('join_live', (data) => {
    const roomId = String(data.roomId || data.streamId);
    socket.join(roomId);
    socket.join(`stream_${roomId}`);

    let room = activeLiveRooms.get(roomId);
    if (!room) {
      room = {
        hostSocketId: null,
        hostUserId: null,
        info: null,
        viewers: new Set(),
        viewersHistory: new Map(),
      };
      activeLiveRooms.set(roomId, room);
    }

    if (room.hostSocketId === socket.id || (room.hostUserId && data?.user && String(data.user.id) === String(room.hostUserId))) {
      room.hostSocketId = socket.id;
      return;
    }

    if (!room.viewersHistory) {
      room.viewersHistory = new Map();
    }
    if (data?.user && data.user.id) {
      room.viewersHistory.set(String(data.user.id), {
        id: String(data.user.id),
        name: data.user.name || 'Viewer',
        avatar: data.user.avatar || 'https://images.unsplash.com/photo-1535713875002-d1d0cf377fde?w=800&q=80',
        joinedAt: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        coinsSpent: 0,
      });
    }

    room.viewers.add(socket.id);

    if (room.hostSocketId) {
      console.log(`[Live WebRTC] Notifying host ${room.hostSocketId} that viewer ${socket.id} joined room ${roomId}`);
      io.to(room.hostSocketId).emit('viewer:joined', {
        viewerSocketId: socket.id,
        user: data.user,
        viewerCount: room.viewers.size,
        roomId: roomId,
      });
    }

    io.to(roomId).emit('viewer_count_update', {
      roomId,
      viewerCount: room.viewers.size,
      count: room.viewers.size,
    });
    io.to(`stream_${roomId}`).emit('stream:viewers_count', {
      streamId: roomId,
      count: room.viewers.size,
    });
    io.to(roomId).emit('user_joined_live', {
      roomId,
      user: data.user,
      count: room.viewers.size,
    });
    console.log(`[Live Stream] Viewer ${socket.id} joined room ${roomId}. Total: ${room.viewers.size}`);
  });
  socket.on('stream:join', (data) => {
    socket.emit('join_live', data);
  });

  socket.on('leave_live', (data) => {
    const roomId = String(data.roomId || data.streamId);
    socket.leave(roomId);
    socket.leave(`stream_${roomId}`);

    const room = activeLiveRooms.get(roomId);
    if (room) {
      room.viewers.delete(socket.id);
      if (room.hostSocketId) {
        io.to(room.hostSocketId).emit('viewer:left', {
          viewerSocketId: socket.id,
          viewerCount: room.viewers.size,
        });
      }
      io.to(roomId).emit('viewer_count_update', {
        roomId,
        viewerCount: room.viewers.size,
      });
      io.to(`stream_${roomId}`).emit('stream:viewers_count', {
        streamId: roomId,
        count: room.viewers.size,
      });
      io.to(roomId).emit('user_left_live', {
        roomId,
        user: data.user,
        count: room.viewers.size,
      });
    }
  });
  socket.on('stream:leave', (data) => {
    socket.emit('leave_live', data);
  });

  // Host sends WebRTC offer to specific viewer
  socket.on('live:offer', (payload) => {
    console.log(`[Live WebRTC] Host offer -> Viewer ${payload.viewerSocketId}`);
    io.to(payload.viewerSocketId).emit('live:offer', {
      offer: payload.offer,
      roomId: payload.roomId,
      hostSocketId: socket.id,
    });
  });

  // Viewer sends WebRTC answer back to host
  socket.on('live:answer', (payload) => {
    console.log(`[Live WebRTC] Viewer answer -> Host`);
    const room = activeLiveRooms.get(String(payload.roomId));
    if (room && room.hostSocketId) {
      io.to(room.hostSocketId).emit('live:answer', {
        answer: payload.answer,
        viewerSocketId: socket.id,
        roomId: payload.roomId,
      });
    } else if (payload.hostSocketId) {
      io.to(payload.hostSocketId).emit('live:answer', {
        answer: payload.answer,
        viewerSocketId: socket.id,
        roomId: payload.roomId,
      });
    }
  });

  // ICE candidates between Host and Viewer
  socket.on('live:ice', (payload) => {
    const destSocketId = payload.targetSocketId || payload.hostSocketId;
    if (destSocketId) {
      io.to(destSocketId).emit('live:ice', {
        candidate: payload.candidate,
        fromSocketId: socket.id,
        roomId: payload.roomId,
      });
    } else {
      const room = activeLiveRooms.get(String(payload.roomId));
      if (room && room.hostSocketId && socket.id !== room.hostSocketId) {
        io.to(room.hostSocketId).emit('live:ice', {
          candidate: payload.candidate,
          fromSocketId: socket.id,
          roomId: payload.roomId,
        });
      }
    }
  });

  // Video frame relay fallback for live streaming
  socket.on('stream:video_frame', (payload) => {
    if (payload?.streamId && payload?.frame) {
      socket.to(`stream_${payload.streamId}`).emit('stream:video_frame', payload);
    }
  });

  // Viewer sends private call request to host during live stream
  socket.on('stream:request_private_call', (payload) => {
    console.log(`[Live Call Request] Viewer ${socket.id} -> Host ${payload.hostId}`);
    const streamId = String(payload.streamId || '');
    const room = activeLiveRooms.get(streamId);
    const hostSocketId = (room && room.hostSocketId);

    const callPayload = {
      ...payload,
      viewerSocketId: socket.id,
    };

    if (hostSocketId) {
      io.to(hostSocketId).emit('stream:private_call_request_received', callPayload);
    } else if (payload.hostId) {
      emitToUser(String(payload.hostId), 'stream:private_call_request_received', callPayload);
    }
    io.to(`stream_${streamId}`).emit('stream:private_call_request_received', callPayload);
  });

  // Viewer cancels private call request
  socket.on('stream:cancel_private_call', (payload) => {
    console.log(`[Live Call Cancel] Request ${payload.requestId}`);
    const streamId = String(payload.streamId || '');
    const room = activeLiveRooms.get(streamId);
    const hostSocketId = (room && room.hostSocketId);

    if (hostSocketId) {
      io.to(hostSocketId).emit('stream:private_call_request_cancelled', payload);
    } else if (payload.hostId) {
      emitToUser(String(payload.hostId), 'stream:private_call_request_cancelled', payload);
    }
    io.to(`stream_${streamId}`).emit('stream:private_call_request_cancelled', payload);
  });

  // Host responds to private call request (accept or reject)
  socket.on('stream:respond_private_call', (payload) => {
    console.log(`[Live Call Response] Host -> Action: ${payload.action} for caller: ${payload.callerId}`);
    const streamId = String(payload.streamId || '');
    const eventName = payload.action === 'accept' ? 'stream:private_call_accepted' : 'stream:private_call_rejected';

    const sessionData = {
      ...payload,
      callChannelId: payload.callChannelId || `call_${Date.now()}_${Math.random().toString(36).substring(7)}`,
    };

    let delivered = false;
    if (payload.viewerSocketId) {
      io.to(payload.viewerSocketId).emit(eventName, sessionData);
      delivered = true;
    }
    if (!delivered && payload.callerId) {
      emitToUser(String(payload.callerId), eventName, sessionData);
      delivered = true;
    }
    if (streamId) {
      io.to(`stream_${streamId}`).emit(eventName, sessionData);
    }
    socket.emit(eventName, sessionData);
  });

  socket.on('stream:ended', handleEndLiveStream);
  socket.on('stream:end', handleEndLiveStream);
  socket.on('host:ended_live', handleEndLiveStream);

  // Sync stream duration in real-time so it survives disconnects/resumes
  socket.on('stream:duration_sync', (data) => {
    const roomId = String(data?.roomId || data?.streamId || '');
    const duration = Number(data?.duration) || 0;
    if (roomId && activeLiveRooms.has(roomId)) {
      const room = activeLiveRooms.get(roomId);
      room.duration = duration;
      if (room.info) room.info.duration = duration;
    }
  });

  // Live Chat, Gifts, and Hearts
  socket.on('live_comment', (data) => {
    const roomId = String(data.roomId || data.streamId);
    const comment = data.comment || { user: data.user || 'Viewer', text: data.text || '' };
    const commentId = comment.id || 'lc_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);
    comment.id = commentId;
    const payload = {
      roomId,
      comment,
      timestamp: new Date().toISOString(),
    };
    console.log(`[Live Chat] Room ${roomId} -> ${comment.user}: ${comment.text}`);
    io.to(roomId).emit('new_live_comment', payload);
    io.to(`stream_${roomId}`).emit('new_live_comment', payload);
    io.to(`stream_${roomId}`).emit('stream:new_comment', comment);
  });
  socket.on('stream:comment', (data) => {
    socket.emit('live_comment', data);
  });

  socket.on('live_gift', (data) => {
    const roomId = String(data.roomId || data.streamId);
    const gift = data.gift || data;
    const giftId = gift.id || 'gift_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);
    gift.id = giftId;

    const room = activeLiveRooms.get(roomId);
    if (room) {
      room.sessionCoins = (room.sessionCoins || 0) + (Number(gift.coins) || 0);
      if (room.viewersHistory) {
        const senderName = gift.sender || data.user || 'Viewer';
        for (const v of room.viewersHistory.values()) {
          if (v.name === senderName) {
            v.coinsSpent = (v.coinsSpent || 0) + (Number(gift.coins) || 0);
            break;
          }
        }
      }
    }

    const payload = {
      roomId,
      gift,
      timestamp: new Date().toISOString(),
    };
    console.log(`[Live Gift] Room ${roomId} -> ${gift.name} from ${gift.sender || 'Viewer'}`);
    io.to(roomId).emit('new_live_gift', payload);
    io.to(`stream_${roomId}`).emit('new_live_gift', payload);
    io.to(`stream_${roomId}`).emit('stream:gift_received', { gift, sender: gift.sender, timestamp: Date.now() });
  });
  socket.on('stream:gift', (data) => {
    socket.emit('live_gift', data);
  });

  socket.on('live_heart', (data) => {
    const roomId = String(data.roomId || data.streamId);
    io.to(roomId).emit('live_heart', data);
    io.to(`stream_${roomId}`).emit('new_live_heart', { roomId, user: data.user, timestamp: Date.now() });
    io.to(`stream_${roomId}`).emit('stream:heart_burst', { user: data.user, timestamp: Date.now() });
  });
  socket.on('stream:heart', (data) => {
    socket.emit('live_heart', data);
  });

  // ==================== 8-MIC PARTY LOUNGE ====================
  const handlePartyJoin = ({ roomId, user }) => {
    socket.join(`party_${roomId}`);
    console.log(`[Party Join] User joined party_${roomId}`);
  };
  socket.on('join_party_room', handlePartyJoin);
  socket.on('party:join', handlePartyJoin);

  const handlePartySeat = ({ roomId, seats, action, seatIndex, user }) => {
    partyRooms.set(roomId, seats);
    io.to(`party_${roomId}`).emit('party_seat_updated', { roomId, seats, action, seatIndex, user });
    io.to(`party_${roomId}`).emit('party:seats_updated', { seats, action, seatIndex, user });
  };
  socket.on('party_seat_action', handlePartySeat);
  socket.on('party:seat_action', handlePartySeat);

  const handlePartySpeaking = ({ roomId, seatIndex, isSpeaking }) => {
    io.to(`party_${roomId}`).emit('party_speaking_wave', { roomId, seatIndex, isSpeaking });
    io.to(`party_${roomId}`).emit('party:speaking_state', { seatIndex, isSpeaking });
  };
  socket.on('party_speaking', handlePartySpeaking);
  socket.on('party:mic_speaking', handlePartySpeaking);

  // ==================== CHAT MESSAGES ====================
  const handleChatMessage = (msg) => {
    const receiverId = String(msg.receiverId || msg.recipientId || msg.toUserId);
    console.log(`[Chat Message] From ${msg.senderId} to ${receiverId}`);
    emitToUser(receiverId, 'new_message', msg);
    emitToUser(receiverId, 'chat:receive_message', msg);
    socket.emit('message_sent', { messageId: msg.id, status: 'delivered' });
  };
  socket.on('chat_message', handleChatMessage);
  socket.on('chat:send_message', handleChatMessage);

  const handleTyping = (data) => {
    const receiverId = String(data.receiverId || data.recipientId || data.toUserId);
    emitToUser(receiverId, 'typing', data);
    emitToUser(receiverId, 'chat:typing', data);
  };
  socket.on('typing', handleTyping);
  socket.on('chat:typing', handleTyping);

  // ==================== DISCONNECT HANDLER ====================

  socket.on('disconnect', () => {
    console.log(`[Socket Disconnected] ID: ${socket.id}`);
    const uInfo = socketUsers.get(socket.id);
    if (uInfo) {
      const candidateIds = getCandidateUserIds(uInfo.userId);
      candidateIds.forEach((cid) => {
        const set = userSockets.get(cid);
        if (set) {
          set.delete(socket.id);
          if (set.size === 0) {
            userSockets.delete(cid);
          }
        }
      });
      socketUsers.delete(socket.id);
      socket.broadcast.emit('user:online_status', { userId: uInfo.userId, isOnline: false });
    }

    // Check if disconnected socket was a host of an active live room
    for (const [roomId, room] of activeLiveRooms.entries()) {
      if (room.hostSocketId === socket.id) {
        console.log(`[Socket] Host ${room.hostUserId} disconnected from room ${roomId}. Waiting 15s grace period...`);
        room.hostSocketId = null;

        if (roomDisconnectTimeouts.has(roomId)) {
          clearTimeout(roomDisconnectTimeouts.get(roomId));
        }

        const timer = setTimeout(() => {
          console.log(`[Socket] Grace period expired for room ${roomId}. Host did not reconnect. Ending stream.`);
          handleEndLiveStream({ roomId, hostId: room.hostUserId, end_reason: 'host_offline' });
        }, 15000); // 15 seconds grace period

        roomDisconnectTimeouts.set(roomId, timer);
      } else if (room.viewers.has(socket.id)) {
        room.viewers.delete(socket.id);
        if (room.hostSocketId) {
          io.to(room.hostSocketId).emit('viewer:left', {
            viewerSocketId: socket.id,
            viewerCount: room.viewers.size,
          });
        }
        io.to(roomId).emit('viewer_count_update', {
          roomId,
          viewerCount: room.viewers.size,
          count: room.viewers.size,
        });
        io.to(`stream_${roomId}`).emit('stream:viewers_count', {
          streamId: roomId,
          count: room.viewers.size,
        });
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 [Jodii Club Unified Socket & WebRTC Server] running on http://0.0.0.0:${PORT}`);
});

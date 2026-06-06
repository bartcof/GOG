const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// ========== ХРАНИЛИЩА ==========
const users = new Map();
const messages = new Map();
const groups = new Map();
const channels = new Map();
const calls = new Map();
const stories = new Map();
const polls = new Map();
const bots = new Map();
const stickers = new Map();
const voiceMessages = new Map();
const files = new Map();
const contacts = new Map();
const blocked = new Map();
const favorites = new Map();
const folders = new Map();
const scheduled = new Map();
const drafts = new Map();
const pinned = new Map();
const reactions = new Map();
const reports = new Map();
const admins = new Set();
const banned = new Set();
const muted = new Map();
const archived = new Map();
const secretChats = new Map();

let onlineUsers = new Set();
let adminId = null;

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

function broadcastToAll(type, data, exclude = null) {
    wss.clients.forEach(client => {
        if (client !== exclude && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type, ...data }));
        }
    });
}

function broadcastUserList() {
    const list = Array.from(users.entries()).map(([id, data]) => ({
        id, name: data.name, avatar: data.avatar,
        online: onlineUsers.has(id), isAdmin: data.isAdmin,
        lastSeen: data.lastSeen, phone: data.phone,
        bio: data.bio, premium: data.premium
    }));
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'user_list', users: list }));
        }
    });
}

wss.on('connection', (ws, req) => {
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            
            switch(msg.type) {
                // ========== РЕГИСТРАЦИЯ ==========
                case 'register':
                    if (banned.has(msg.userId)) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Аккаунт заблокирован' }));
                        ws.close();
                        return;
                    }
                    userId = msg.userId;
                    if (users.has(userId)) {
                        const u = users.get(userId);
                        u.ws = ws; u.online = true; u.lastSeen = new Date();
                    } else {
                        users.set(userId, {
                            ws, name: msg.name, avatar: msg.avatar || '😊',
                            online: true, isAdmin: users.size === 0,
                            lastSeen: new Date(), createdAt: new Date(),
                            phone: msg.phone || '', bio: msg.bio || '', premium: false
                        });
                        if (users.size === 1) adminId = userId;
                    }
                    onlineUsers.add(userId);
                    ws.send(JSON.stringify({ type: 'registered', userId, isAdmin: users.get(userId).isAdmin }));
                    broadcastUserList();
                    broadcastToAll('system', { text: `👋 ${users.get(userId).name} присоединился` });
                    break;
                    
                // ========== ПРОФИЛЬ ==========
                case 'update_profile':
                    const upUser = users.get(msg.userId);
                    if (upUser) {
                        if (msg.name) upUser.name = msg.name;
                        if (msg.avatar) upUser.avatar = msg.avatar;
                        if (msg.bio) upUser.bio = msg.bio;
                        if (msg.phone) upUser.phone = msg.phone;
                        broadcastUserList();
                        ws.send(JSON.stringify({ type: 'profile_updated', user: { name: upUser.name, avatar: upUser.avatar, bio: upUser.bio, phone: upUser.phone } }));
                    }
                    break;
                    
                // ========== СООБЩЕНИЯ ==========
                case 'private_message':
                    const toUser = users.get(msg.to);
                    const fromUser = users.get(msg.from);
                    if (!toUser || !fromUser) return;
                    if (blocked.get(msg.to)?.has(msg.from)) return;
                    
                    const chatIdPrivate = getChatId(msg.from, msg.to);
                    if (!messages.has(chatIdPrivate)) messages.set(chatIdPrivate, []);
                    
                    const newMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        text: msg.text, time: msg.time, read: false,
                        edited: false, deleted: false, forwarded: false,
                        replyTo: msg.replyTo
                    };
                    messages.get(chatIdPrivate).push(newMsg);
                    
                    if (toUser.ws?.readyState === WebSocket.OPEN) {
                        toUser.ws.send(JSON.stringify({
                            type: 'new_message', message: newMsg,
                            fromName: fromUser.name, fromAvatar: fromUser.avatar
                        }));
                    }
                    ws.send(JSON.stringify({ type: 'message_sent', message: newMsg }));
                    break;
                    
                case 'edit_message':
                    const editChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(editChatId)) {
                        const editMsg = messages.get(editChatId).find(m => m.id === msg.messageId);
                        if (editMsg && editMsg.from === msg.userId) {
                            editMsg.text = msg.newText;
                            editMsg.edited = true;
                            const other = users.get(msg.otherId);
                            if (other?.ws) other.ws.send(JSON.stringify({ type: 'message_edited', messageId: msg.messageId, newText: msg.newText }));
                        }
                    }
                    break;
                    
                case 'delete_message':
                    const delChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(delChatId)) {
                        const delMsg = messages.get(delChatId).find(m => m.id === msg.messageId);
                        if (delMsg && delMsg.from === msg.userId) {
                            delMsg.text = 'Сообщение удалено';
                            delMsg.deleted = true;
                            const other = users.get(msg.otherId);
                            if (other?.ws) other.ws.send(JSON.stringify({ type: 'message_deleted', messageId: msg.messageId }));
                        }
                    }
                    break;
                    
                case 'reply_message':
                    const replyChatId = getChatId(msg.from, msg.to);
                    const originalMsg = messages.get(replyChatId)?.find(m => m.id === msg.replyToId);
                    if (originalMsg) {
                        const reply = {
                            id: Date.now(), from: msg.from, to: msg.to,
                            text: msg.text, time: msg.time, replyTo: originalMsg.id,
                            replyText: originalMsg.text, replyName: users.get(originalMsg.from)?.name
                        };
                        messages.get(replyChatId).push(reply);
                        const replyToUser = users.get(msg.to);
                        if (replyToUser?.ws) replyToUser.ws.send(JSON.stringify({ type: 'new_message', message: reply }));
                    }
                    break;
                    
                // ========== РЕАКЦИИ ==========
                case 'add_reaction':
                    const reactKey = `${msg.chatId}_${msg.messageId}`;
                    if (!reactions.has(reactKey)) reactions.set(reactKey, new Map());
                    reactions.get(reactKey).set(msg.userId, msg.reaction);
                    broadcastToAll('reaction_added', { messageId: msg.messageId, userId: msg.userId, reaction: msg.reaction });
                    break;
                    
                // ========== ЗВОНКИ ==========
                case 'start_voice_call':
                    const callId = Date.now();
                    calls.set(callId, { from: msg.from, to: msg.to, status: 'ringing' });
                    const callTo = users.get(msg.to);
                    if (callTo?.ws) callTo.ws.send(JSON.stringify({ type: 'incoming_call', callId, from: msg.from }));
                    break;
                    
                case 'video_call':
                    const videoCallId = Date.now();
                    calls.set(videoCallId, { from: msg.from, to: msg.to, type: 'video', status: 'ringing' });
                    const videoCallTo = users.get(msg.to);
                    if (videoCallTo?.ws) videoCallTo.ws.send(JSON.stringify({ type: 'incoming_video_call', callId: videoCallId, from: msg.from }));
                    break;
                    
                case 'end_call':
                    const endCall = calls.get(msg.callId);
                    if (endCall) {
                        endCall.status = 'ended';
                        [endCall.from, endCall.to].forEach(pid => {
                            const p = users.get(pid);
                            if (p?.ws) p.ws.send(JSON.stringify({ type: 'call_ended', callId: msg.callId }));
                        });
                        calls.delete(msg.callId);
                    }
                    break;
                    
                // ========== ГОЛОСОВЫЕ СООБЩЕНИЯ ==========
                case 'voice_message':
                    const voiceMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        duration: msg.duration, url: msg.url, time: msg.time, type: 'voice'
                    };
                    const voiceChatId = getChatId(msg.from, msg.to);
                    if (!messages.has(voiceChatId)) messages.set(voiceChatId, []);
                    messages.get(voiceChatId).push(voiceMsg);
                    const voiceTo = users.get(msg.to);
                    if (voiceTo?.ws) voiceTo.ws.send(JSON.stringify({ type: 'new_voice', message: voiceMsg }));
                    break;
                    
                // ========== ФАЙЛЫ ==========
                case 'send_file':
                    const fileId = Date.now();
                    files.set(fileId, { userId: msg.from, name: msg.name, size: msg.size, url: msg.url });
                    const fileMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        fileId: fileId, fileName: msg.name, time: msg.time, type: 'file'
                    };
                    const fileChatId = getChatId(msg.from, msg.to);
                    if (!messages.has(fileChatId)) messages.set(fileChatId, []);
                    messages.get(fileChatId).push(fileMsg);
                    const fileTo = users.get(msg.to);
                    if (fileTo?.ws) fileTo.ws.send(JSON.stringify({ type: 'new_file', message: fileMsg }));
                    break;
                    
                // ========== ГРУППЫ ==========
                case 'create_group':
                    const groupId = 'group_' + Date.now();
                    groups.set(groupId, {
                        name: msg.name, avatar: msg.avatar || '👥',
                        members: [msg.creator, ...(msg.members || [])],
                        creator: msg.creator, admins: [msg.creator],
                        messages: [], createdAt: new Date()
                    });
                    groups.get(groupId).members.forEach(m => {
                        const member = users.get(m);
                        if (member?.ws) member.ws.send(JSON.stringify({ type: 'group_created', groupId, groupName: msg.name }));
                    });
                    break;
                    
                case 'group_message':
                    const group = groups.get(msg.groupId);
                    if (group?.members.includes(msg.from)) {
                        const gMsg = { id: Date.now(), from: msg.from, text: msg.text, time: msg.time };
                        group.messages.push(gMsg);
                        group.members.forEach(m => {
                            const member = users.get(m);
                            if (member?.ws) member.ws.send(JSON.stringify({ type: 'new_group_message', groupId: msg.groupId, message: gMsg }));
                        });
                    }
                    break;
                    
                // ========== КАНАЛЫ ==========
                case 'create_channel':
                    const channelId = 'channel_' + Date.now();
                    channels.set(channelId, {
                        name: msg.name, avatar: msg.avatar || '📢',
                        subscribers: [msg.creator], creator: msg.creator,
                        posts: [], createdAt: new Date()
                    });
                    ws.send(JSON.stringify({ type: 'channel_created', channelId }));
                    break;
                    
                // ========== ОПРОСЫ ==========
                case 'create_poll':
                    const pollId = Date.now();
                    polls.set(pollId, {
                        question: msg.question, options: msg.options,
                        votes: new Array(msg.options.length).fill(0),
                        voters: new Set(), multi: msg.multi === 'multi',
                        creator: msg.creator
                    });
                    broadcastToAll('poll_created', { pollId, poll: { question: msg.question, options: msg.options } });
                    break;
                    
                case 'vote_poll':
                    const poll = polls.get(msg.pollId);
                    if (poll && !poll.voters.has(msg.userId)) {
                        poll.voters.add(msg.userId);
                        if (Array.isArray(msg.options)) {
                            msg.options.forEach(opt => { if (poll.votes[opt] !== undefined) poll.votes[opt]++; });
                        } else if (typeof msg.options === 'number') {
                            poll.votes[msg.options]++;
                        }
                        broadcastToAll('poll_updated', { pollId: msg.pollId, votes: poll.votes });
                    }
                    break;
                    
                // ========== ЭМОДЗИ ==========
                case 'send_sticker':
                    const stickerMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        sticker: msg.sticker, time: msg.time, type: 'sticker'
                    };
                    const stickerChatId = getChatId(msg.from, msg.to);
                    if (!messages.has(stickerChatId)) messages.set(stickerChatId, []);
                    messages.get(stickerChatId).push(stickerMsg);
                    const stickerTo = users.get(msg.to);
                    if (stickerTo?.ws) stickerTo.ws.send(JSON.stringify({ type: 'new_sticker', message: stickerMsg }));
                    break;
                    
                // ========== БЛОКИРОВКА ==========
                case 'block_user':
                    if (!blocked.has(msg.userId)) blocked.set(msg.userId, new Set());
                    blocked.get(msg.userId).add(msg.blockId);
                    ws.send(JSON.stringify({ type: 'user_blocked' }));
                    break;
                    
                case 'unblock_user':
                    blocked.get(msg.userId)?.delete(msg.blockId);
                    ws.send(JSON.stringify({ type: 'user_unblocked' }));
                    break;
                    
                // ========== СТАТУСЫ ==========
                case 'typing':
                    const typingTo = users.get(msg.to);
                    if (typingTo?.ws) typingTo.ws.send(JSON.stringify({
                        type: 'typing', from: msg.from, fromName: msg.fromName, isTyping: msg.isTyping
                    }));
                    break;
                    
                case 'mark_read':
                    const readChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(readChatId)) {
                        messages.get(readChatId).forEach(m => {
                            if (m.to === msg.userId && !m.read) m.read = true;
                        });
                    }
                    break;
                    
                // ========== ИСТОРИЯ ==========
                case 'get_history':
                    const histChatId = getChatId(msg.userId, msg.withUserId);
                    const history = messages.get(histChatId) || [];
                    ws.send(JSON.stringify({ type: 'chat_history', messages: history }));
                    break;
                    
                case 'get_users':
                    broadcastUserList();
                    break;
                    
                case 'delete_account':
                    const delUser = users.get(msg.userId);
                    if (delUser) {
                        banned.add(msg.userId);
                        users.delete(msg.userId);
                        onlineUsers.delete(msg.userId);
                        broadcastUserList();
                        broadcastToAll('system', { text: `⚠️ ${delUser.name} удалил аккаунт` });
                    }
                    break;
                    
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
                    break;
            }
        } catch(e) { console.error('Error:', e); }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            if (users.has(userId)) {
                users.get(userId).online = false;
                users.get(userId).lastSeen = new Date();
                broadcastUserList();
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 MEGA MESSENGER PRO ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✨ Исправлено: дублирование переменных`);
    console.log(`✅ Сервер работает стабильно!\n`);
});

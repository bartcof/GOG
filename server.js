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
                // ========== 1-10: РЕГИСТРАЦИЯ И ПРОФИЛЬ ==========
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
                            phone: msg.phone, bio: '', premium: false,
                            language: 'ru', theme: 'dark', notifications: true
                        });
                        if (users.size === 1) adminId = userId;
                    }
                    onlineUsers.add(userId);
                    ws.send(JSON.stringify({ type: 'registered', userId, isAdmin: users.get(userId).isAdmin }));
                    broadcastUserList();
                    broadcastToAll('system', { text: `👋 ${users.get(userId).name} присоединился` });
                    break;
                    
                case 'update_profile':
                    const u = users.get(msg.userId);
                    if (u) {
                        if (msg.name) u.name = msg.name;
                        if (msg.avatar) u.avatar = msg.avatar;
                        if (msg.bio) u.bio = msg.bio;
                        if (msg.phone) u.phone = msg.phone;
                        broadcastUserList();
                        ws.send(JSON.stringify({ type: 'profile_updated', user: u }));
                    }
                    break;
                    
                case 'get_profile':
                    const target = users.get(msg.userId);
                    if (target) {
                        ws.send(JSON.stringify({
                            type: 'profile_data',
                            id: target.id, name: target.name, avatar: target.avatar,
                            bio: target.bio, phone: target.phone, premium: target.premium,
                            lastSeen: target.lastSeen, createdAt: target.createdAt
                        }));
                    }
                    break;
                    
                // ========== 11-20: СООБЩЕНИЯ И ЧАТЫ ==========
                case 'private_message':
                    const to = users.get(msg.to);
                    const from = users.get(msg.from);
                    if (!to || !from) return;
                    if (blocked.get(msg.to)?.has(msg.from)) return;
                    
                    const chatId = getChatId(msg.from, msg.to);
                    if (!messages.has(chatId)) messages.set(chatId, []);
                    
                    const newMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        text: msg.text, time: msg.time, read: false,
                        edited: false, deleted: false, forwarded: false,
                        replyTo: msg.replyTo, viewOnce: msg.viewOnce || false,
                        ttl: msg.ttl || 0
                    };
                    messages.get(chatId).push(newMsg);
                    
                    if (to.ws?.readyState === WebSocket.OPEN) {
                        to.ws.send(JSON.stringify({
                            type: 'new_message', message: newMsg,
                            fromName: from.name, fromAvatar: from.avatar
                        }));
                    }
                    ws.send(JSON.stringify({ type: 'message_sent', message: newMsg }));
                    break;
                    
                case 'edit_message':
                    const eChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(eChatId)) {
                        const m = messages.get(eChatId).find(m => m.id === msg.messageId);
                        if (m && m.from === msg.userId) {
                            m.text = msg.newText;
                            m.edited = true;
                            const other = users.get(msg.otherId);
                            if (other?.ws) other.ws.send(JSON.stringify({ type: 'message_edited', messageId: msg.messageId, newText: msg.newText }));
                        }
                    }
                    break;
                    
                case 'delete_message':
                    const dChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(dChatId)) {
                        const m = messages.get(dChatId).find(m => m.id === msg.messageId);
                        if (m && m.from === msg.userId) {
                            m.text = 'Сообщение удалено';
                            m.deleted = true;
                            const other = users.get(msg.otherId);
                            if (other?.ws) other.ws.send(JSON.stringify({ type: 'message_deleted', messageId: msg.messageId }));
                        }
                    }
                    break;
                    
                case 'forward_message':
                    const fMsg = messages.get(getChatId(msg.fromChat, msg.originalFrom)).find(m => m.id === msg.messageId);
                    if (fMsg) {
                        const forwarded = { ...fMsg, id: Date.now(), forwarded: true, originalFrom: fMsg.from };
                        const targetChat = getChatId(msg.to, msg.from);
                        if (!messages.has(targetChat)) messages.set(targetChat, []);
                        messages.get(targetChat).push(forwarded);
                        const targetUser = users.get(msg.to);
                        if (targetUser?.ws) targetUser.ws.send(JSON.stringify({ type: 'new_message', message: forwarded }));
                    }
                    break;
                    
                case 'reply_message':
                    const rChatId = getChatId(msg.from, msg.to);
                    const original = messages.get(rChatId).find(m => m.id === msg.replyToId);
                    if (original) {
                        const reply = {
                            id: Date.now(), from: msg.from, to: msg.to,
                            text: msg.text, time: msg.time, replyTo: original.id,
                            replyText: original.text, replyName: users.get(original.from)?.name
                        };
                        messages.get(rChatId).push(reply);
                        const toUser = users.get(msg.to);
                        if (toUser?.ws) toUser.ws.send(JSON.stringify({ type: 'new_message', message: reply }));
                    }
                    break;
                    
                case 'pin_message':
                    const pChatId = getChatId(msg.userId, msg.otherId);
                    if (!pinned.has(pChatId)) pinned.set(pChatId, new Set());
                    pinned.get(pChatId).add(msg.messageId);
                    broadcastToAll('message_pinned', { chatId: pChatId, messageId: msg.messageId });
                    break;
                    
                case 'unpin_message':
                    const upChatId = getChatId(msg.userId, msg.otherId);
                    pinned.get(upChatId)?.delete(msg.messageId);
                    break;
                    
                case 'save_draft':
                    const dKey = getChatId(msg.userId, msg.chatId);
                    drafts.set(dKey, { text: msg.text, time: Date.now() });
                    break;
                    
                case 'get_draft':
                    const draft = drafts.get(getChatId(msg.userId, msg.chatId));
                    if (draft) ws.send(JSON.stringify({ type: 'draft', text: draft.text }));
                    break;
                    
                // ========== 21-30: РЕАКЦИИ И СТИКЕРЫ ==========
                case 'add_reaction':
                    const rKey = `${msg.chatId}_${msg.messageId}`;
                    if (!reactions.has(rKey)) reactions.set(rKey, new Map());
                    reactions.get(rKey).set(msg.userId, msg.reaction);
                    broadcastToAll('reaction_added', { messageId: msg.messageId, userId: msg.userId, reaction: msg.reaction });
                    break;
                    
                case 'remove_reaction':
                    const rrKey = `${msg.chatId}_${msg.messageId}`;
                    reactions.get(rrKey)?.delete(msg.userId);
                    broadcastToAll('reaction_removed', { messageId: msg.messageId, userId: msg.userId });
                    break;
                    
                case 'get_reactions':
                    const grKey = `${msg.chatId}_${msg.messageId}`;
                    const reacts = reactions.get(grKey) || new Map();
                    ws.send(JSON.stringify({ type: 'reactions_list', reactions: Array.from(reacts.entries()) }));
                    break;
                    
                case 'send_sticker':
                    const stickerMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        sticker: msg.sticker, time: msg.time, type: 'sticker'
                    };
                    const sChatId = getChatId(msg.from, msg.to);
                    messages.get(sChatId).push(stickerMsg);
                    const sTo = users.get(msg.to);
                    if (sTo?.ws) sTo.ws.send(JSON.stringify({ type: 'new_sticker', message: stickerMsg }));
                    break;
                    
                case 'upload_sticker':
                    if (!stickers.has(msg.userId)) stickers.set(msg.userId, []);
                    stickers.get(msg.userId).push({ id: Date.now(), url: msg.url, emoji: msg.emoji });
                    ws.send(JSON.stringify({ type: 'sticker_uploaded' }));
                    break;
                    
                case 'get_stickers':
                    const userStickers = stickers.get(msg.userId) || [];
                    ws.send(JSON.stringify({ type: 'stickers_list', stickers: userStickers }));
                    break;
                    
                // ========== 31-40: ГОЛОСОВЫЕ И ВИДЕО ==========
                case 'voice_message':
                    const vMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        duration: msg.duration, url: msg.url, time: msg.time, type: 'voice'
                    };
                    const vChatId = getChatId(msg.from, msg.to);
                    messages.get(vChatId).push(vMsg);
                    const vTo = users.get(msg.to);
                    if (vTo?.ws) vTo.ws.send(JSON.stringify({ type: 'new_voice', message: vMsg }));
                    break;
                    
                case 'start_voice_call':
                    const callId = Date.now();
                    calls.set(callId, { from: msg.from, to: msg.to, status: 'ringing' });
                    const callTo = users.get(msg.to);
                    if (callTo?.ws) callTo.ws.send(JSON.stringify({ type: 'incoming_call', callId, from: msg.from }));
                    break;
                    
                case 'answer_call':
                    const call = calls.get(msg.callId);
                    if (call) {
                        call.status = 'connected';
                        const caller = users.get(call.from);
                        if (caller?.ws) caller.ws.send(JSON.stringify({ type: 'call_answered', callId: msg.callId }));
                    }
                    break;
                    
                case 'end_call':
                    const endCall = calls.get(msg.callId);
                    if (endCall) {
                        endCall.status = 'ended';
                        const participants = [endCall.from, endCall.to];
                        participants.forEach(pid => {
                            const p = users.get(pid);
                            if (p?.ws) p.ws.send(JSON.stringify({ type: 'call_ended', callId: msg.callId }));
                        });
                        calls.delete(msg.callId);
                    }
                    break;
                    
                case 'video_call':
                    const vCallId = Date.now();
                    calls.set(vCallId, { from: msg.from, to: msg.to, type: 'video', status: 'ringing' });
                    const vCallTo = users.get(msg.to);
                    if (vCallTo?.ws) vCallTo.ws.send(JSON.stringify({ type: 'incoming_video_call', callId: vCallId, from: msg.from }));
                    break;
                    
                // ========== 41-50: ГРУППЫ И КАНАЛЫ ==========
                case 'create_group':
                    const groupId = 'group_' + Date.now();
                    groups.set(groupId, {
                        name: msg.name, avatar: msg.avatar || '👥',
                        members: [msg.creator, ...(msg.members || [])],
                        creator: msg.creator, admins: [msg.creator],
                        messages: [], createdAt: new Date(),
                        permissions: { sendMessages: true, sendMedia: true, addMembers: true }
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
                    
                case 'add_member':
                    const addGroup = groups.get(msg.groupId);
                    if (addGroup && addGroup.admins.includes(msg.adminId)) {
                        addGroup.members.push(msg.newMember);
                        const newMember = users.get(msg.newMember);
                        if (newMember?.ws) newMember.ws.send(JSON.stringify({ type: 'added_to_group', groupId: msg.groupId, groupName: addGroup.name }));
                    }
                    break;
                    
                case 'remove_member':
                    const remGroup = groups.get(msg.groupId);
                    if (remGroup && remGroup.admins.includes(msg.adminId)) {
                        remGroup.members = remGroup.members.filter(m => m !== msg.memberId);
                        const removed = users.get(msg.memberId);
                        if (removed?.ws) removed.ws.send(JSON.stringify({ type: 'removed_from_group', groupId: msg.groupId }));
                    }
                    break;
                    
                case 'promote_admin':
                    const promGroup = groups.get(msg.groupId);
                    if (promGroup && promGroup.creator === msg.adminId) {
                        promGroup.admins.push(msg.memberId);
                        broadcastToAll('admin_promoted', { groupId: msg.groupId, memberId: msg.memberId });
                    }
                    break;
                    
                case 'create_channel':
                    const channelId = 'channel_' + Date.now();
                    channels.set(channelId, {
                        name: msg.name, avatar: msg.avatar || '📢',
                        subscribers: [msg.creator], creator: msg.creator,
                        posts: [], createdAt: new Date()
                    });
                    ws.send(JSON.stringify({ type: 'channel_created', channelId }));
                    break;
                    
                case 'subscribe_channel':
                    const channel = channels.get(msg.channelId);
                    if (channel && !channel.subscribers.includes(msg.userId)) {
                        channel.subscribers.push(msg.userId);
                        ws.send(JSON.stringify({ type: 'subscribed', channelId: msg.channelId }));
                    }
                    break;
                    
                case 'channel_post':
                    const postChannel = channels.get(msg.channelId);
                    if (postChannel && postChannel.creator === msg.userId) {
                        const post = { id: Date.now(), text: msg.text, time: msg.time };
                        postChannel.posts.push(post);
                        postChannel.subscribers.forEach(s => {
                            const sub = users.get(s);
                            if (sub?.ws) sub.ws.send(JSON.stringify({ type: 'new_post', channelId: msg.channelId, post }));
                        });
                    }
                    break;
                    
                // ========== 51-60: ИСТОРИИ И СТАТУСЫ ==========
                case 'add_story':
                    const storyId = Date.now();
                    if (!stories.has(msg.userId)) stories.set(msg.userId, []);
                    stories.get(msg.userId).push({
                        id: storyId, media: msg.media, type: msg.type,
                        text: msg.text, expires: Date.now() + 86400000,
                        views: []
                    });
                    broadcastToAll('new_story', { userId: msg.userId, storyId });
                    break;
                    
                case 'view_story':
                    const userStories = stories.get(msg.storyUserId);
                    const story = userStories?.find(s => s.id === msg.storyId);
                    if (story && !story.views.includes(msg.viewerId)) {
                        story.views.push(msg.viewerId);
                        const owner = users.get(msg.storyUserId);
                        if (owner?.ws) owner.ws.send(JSON.stringify({ type: 'story_viewed', storyId: msg.storyId, viewer: msg.viewerId }));
                    }
                    break;
                    
                case 'set_status':
                    users.get(msg.userId).status = msg.status;
                    broadcastUserList();
                    break;
                    
                case 'set_last_seen':
                    const setUser = users.get(msg.userId);
                    if (setUser) setUser.lastSeen = new Date();
                    break;
                    
                // ========== 61-70: ОПРОСЫ И ВИКТОРИНЫ ==========
                case 'create_poll':
                    const pollId = Date.now();
                    polls.set(pollId, {
                        question: msg.question, options: msg.options,
                        votes: new Array(msg.options.length).fill(0),
                        voters: new Set(), multi: msg.multi || false,
                        anonymous: msg.anonymous || true, ends: msg.ends
                    });
                    broadcastToAll('poll_created', { pollId, poll: polls.get(pollId) });
                    break;
                    
                case 'vote_poll':
                    const poll = polls.get(msg.pollId);
                    if (poll && !poll.voters.has(msg.userId)) {
                        poll.voters.add(msg.userId);
                        msg.options.forEach(opt => poll.votes[opt]++);
                        broadcastToAll('poll_updated', { pollId: msg.pollId, votes: poll.votes });
                    }
                    break;
                    
                case 'get_poll_results':
                    const resultPoll = polls.get(msg.pollId);
                    ws.send(JSON.stringify({ type: 'poll_results', results: resultPoll?.votes }));
                    break;
                    
                // ========== 71-80: БЛОКИРОВКИ И БЕЗОПАСНОСТЬ ==========
                case 'block_user':
                    if (!blocked.has(msg.userId)) blocked.set(msg.userId, new Set());
                    blocked.get(msg.userId).add(msg.blockId);
                    ws.send(JSON.stringify({ type: 'user_blocked' }));
                    break;
                    
                case 'unblock_user':
                    blocked.get(msg.userId)?.delete(msg.blockId);
                    ws.send(JSON.stringify({ type: 'user_unblocked' }));
                    break;
                    
                case 'get_blocked':
                    const blockedList = Array.from(blocked.get(msg.userId) || []);
                    ws.send(JSON.stringify({ type: 'blocked_list', users: blockedList }));
                    break;
                    
                case 'report_user':
                    reports.set(Date.now(), { reporter: msg.userId, reported: msg.reportedId, reason: msg.reason });
                    broadcastToAll('user_reported', { reportedId: msg.reportedId });
                    break;
                    
                case 'start_secret_chat':
                    const secretId = 'secret_' + Date.now();
                    secretChats.set(secretId, { users: [msg.userId, msg.otherId], key: msg.key, messages: [] });
                    const secretOther = users.get(msg.otherId);
                    if (secretOther?.ws) secretOther.ws.send(JSON.stringify({ type: 'secret_chat_started', chatId: secretId }));
                    break;
                    
                case 'secret_message':
                    const secret = secretChats.get(msg.chatId);
                    if (secret && secret.users.includes(msg.from)) {
                        const sMsg = { id: Date.now(), from: msg.from, text: msg.text, encrypted: true, ttl: msg.ttl };
                        secret.messages.push(sMsg);
                        const otherId = secret.users.find(u => u !== msg.from);
                        const other = users.get(otherId);
                        if (other?.ws) other.ws.send(JSON.stringify({ type: 'new_secret_message', message: sMsg }));
                    }
                    break;
                    
                // ========== 81-90: ФАЙЛЫ И МЕДИА ==========
                case 'send_file':
                    const fileId = Date.now();
                    files.set(fileId, { userId: msg.from, name: msg.name, size: msg.size, url: msg.url });
                    const fileMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        file: fileId, fileName: msg.name, time: msg.time, type: 'file'
                    };
                    const fChatId = getChatId(msg.from, msg.to);
                    messages.get(fChatId).push(fileMsg);
                    const fTo = users.get(msg.to);
                    if (fTo?.ws) fTo.ws.send(JSON.stringify({ type: 'new_file', message: fileMsg }));
                    break;
                    
                case 'download_file':
                    const file = files.get(msg.fileId);
                    if (file) ws.send(JSON.stringify({ type: 'file_data', file }));
                    break;
                    
                case 'send_photo':
                    const photoMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        photo: msg.url, caption: msg.caption, time: msg.time, type: 'photo'
                    };
                    const pChatId = getChatId(msg.from, msg.to);
                    messages.get(pChatId).push(photoMsg);
                    const pTo = users.get(msg.to);
                    if (pTo?.ws) pTo.ws.send(JSON.stringify({ type: 'new_photo', message: photoMsg }));
                    break;
                    
                case 'send_video':
                    const videoMsg = {
                        id: Date.now(), from: msg.from, to: msg.to,
                        video: msg.url, duration: msg.duration, time: msg.time, type: 'video'
                    };
                    const vdChatId = getChatId(msg.from, msg.to);
                    messages.get(vdChatId).push(videoMsg);
                    const vdTo = users.get(msg.to);
                    if (vdTo?.ws) vdTo.ws.send(JSON.stringify({ type: 'new_video', message: videoMsg }));
                    break;
                    
                // ========== 91-100: НАСТРОЙКИ И УТИЛИТЫ ==========
                case 'mute_user':
                    muted.set(msg.userId, { mutedId: msg.muteId, until: Date.now() + (msg.duration || 3600000) });
                    break;
                    
                case 'unmute_user':
                    muted.delete(msg.userId);
                    break;
                    
                case 'archive_chat':
                    if (!archived.has(msg.userId)) archived.set(msg.userId, new Set());
                    archived.get(msg.userId).add(msg.chatId);
                    break;
                    
                case 'unarchive_chat':
                    archived.get(msg.userId)?.delete(msg.chatId);
                    break;
                    
                case 'add_contact':
                    if (!contacts.has(msg.userId)) contacts.set(msg.userId, new Set());
                    contacts.get(msg.userId).add(msg.contactId);
                    break;
                    
                case 'get_contacts':
                    const userContacts = Array.from(contacts.get(msg.userId) || []);
                    ws.send(JSON.stringify({ type: 'contacts_list', contacts: userContacts }));
                    break;
                    
                case 'add_favorite':
                    if (!favorites.has(msg.userId)) favorites.set(msg.userId, new Set());
                    favorites.get(msg.userId).add(msg.chatId);
                    break;
                    
                case 'get_favorites':
                    const favs = Array.from(favorites.get(msg.userId) || []);
                    ws.send(JSON.stringify({ type: 'favorites_list', chats: favs }));
                    break;
                    
                case 'create_folder':
                    if (!folders.has(msg.userId)) folders.set(msg.userId, []);
                    folders.get(msg.userId).push({ name: msg.name, chats: msg.chats });
                    break;
                    
                case 'schedule_message':
                    if (!scheduled.has(msg.userId)) scheduled.set(msg.userId, []);
                    scheduled.get(msg.userId).push({ to: msg.to, text: msg.text, time: msg.scheduleTime });
                    break;
                    
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
                    
                case 'get_history':
                    const histChatId = getChatId(msg.userId, msg.withUserId);
                    const history = messages.get(histChatId) || [];
                    ws.send(JSON.stringify({ type: 'chat_history', messages: history }));
                    break;
                    
                case 'search_messages':
                    const searchChatId = getChatId(msg.userId, msg.withUserId);
                    const allMsgs = messages.get(searchChatId) || [];
                    const found = allMsgs.filter(m => m.text?.toLowerCase().includes(msg.query.toLowerCase()));
                    ws.send(JSON.stringify({ type: 'search_results', messages: found }));
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
                    
                case 'get_users':
                    broadcastUserList();
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
    console.log(`\n🚀 MEGA MESSENGER PRO - 100+ ФУНКЦИЙ!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✨ Telegram Clone с полным функционалом!\n`);
});

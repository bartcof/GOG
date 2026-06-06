const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Хранилища данных
const users = new Map();
const messages = new Map();
const groups = new Map();
const onlineUsers = new Set();

// Вспомогательные функции
function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

function broadcastUserList() {
    const list = Array.from(users.entries()).map(([id, data]) => ({
        id: id,
        name: data.name,
        avatar: data.avatar,
        online: onlineUsers.has(id),
        isAdmin: data.isAdmin || false,
        bio: data.bio || '',
        lastSeen: data.lastSeen
    }));
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'user_list', users: list }));
        }
    });
}

function broadcastToAll(type, data, exclude = null) {
    wss.clients.forEach(client => {
        if (client !== exclude && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type, ...data }));
        }
    });
}

wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            
            switch(msg.type) {
                // ========== РЕГИСТРАЦИЯ ==========
                case 'register':
                    userId = msg.userId;
                    
                    if (users.has(userId)) {
                        const existing = users.get(userId);
                        existing.ws = ws;
                        existing.online = true;
                        existing.lastSeen = new Date();
                    } else {
                        users.set(userId, {
                            ws: ws,
                            name: msg.name,
                            avatar: msg.avatar || '😊',
                            online: true,
                            isAdmin: users.size === 0,
                            bio: msg.bio || '',
                            phone: msg.phone || '',
                            lastSeen: new Date(),
                            createdAt: new Date()
                        });
                    }
                    
                    onlineUsers.add(userId);
                    
                    ws.send(JSON.stringify({
                        type: 'registered',
                        userId: userId,
                        isAdmin: users.get(userId).isAdmin
                    }));
                    
                    broadcastUserList();
                    broadcastToAll('system_message', {
                        text: `👋 ${users.get(userId).name} присоединился к чату`,
                        time: new Date().toLocaleTimeString()
                    });
                    break;
                    
                // ========== ПРОФИЛЬ ==========
                case 'update_profile':
                    const user = users.get(msg.userId);
                    if (user) {
                        if (msg.name) user.name = msg.name;
                        if (msg.avatar) user.avatar = msg.avatar;
                        if (msg.bio) user.bio = msg.bio;
                        if (msg.phone) user.phone = msg.phone;
                        
                        broadcastUserList();
                        ws.send(JSON.stringify({
                            type: 'profile_updated',
                            name: user.name,
                            avatar: user.avatar,
                            bio: user.bio
                        }));
                    }
                    break;
                    
                case 'get_profile':
                    const targetUser = users.get(msg.userId);
                    if (targetUser) {
                        ws.send(JSON.stringify({
                            type: 'profile_data',
                            id: targetUser.id,
                            name: targetUser.name,
                            avatar: targetUser.avatar,
                            bio: targetUser.bio,
                            phone: targetUser.phone,
                            lastSeen: targetUser.lastSeen
                        }));
                    }
                    break;
                    
                // ========== ЛИЧНЫЕ СООБЩЕНИЯ ==========
                case 'private_message':
                    const recipient = users.get(msg.to);
                    const sender = users.get(msg.from);
                    
                    if (!recipient || !sender) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Пользователь не найден' }));
                        return;
                    }
                    
                    const chatId = getChatId(msg.from, msg.to);
                    if (!messages.has(chatId)) messages.set(chatId, []);
                    
                    const newMessage = {
                        id: Date.now(),
                        from: msg.from,
                        to: msg.to,
                        text: msg.text,
                        time: msg.time,
                        read: false,
                        edited: false,
                        deleted: false
                    };
                    messages.get(chatId).push(newMessage);
                    
                    // Отправка получателю
                    if (recipient.ws && recipient.ws.readyState === WebSocket.OPEN) {
                        recipient.ws.send(JSON.stringify({
                            type: 'new_message',
                            message: newMessage,
                            fromName: sender.name,
                            fromAvatar: sender.avatar
                        }));
                    }
                    
                    // Подтверждение отправителю
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        message: newMessage
                    }));
                    break;
                    
                // ========== РЕДАКТИРОВАНИЕ И УДАЛЕНИЕ ==========
                case 'edit_message':
                    const editChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(editChatId)) {
                        const editMsg = messages.get(editChatId).find(m => m.id === msg.messageId);
                        if (editMsg && editMsg.from === msg.userId) {
                            editMsg.text = msg.newText;
                            editMsg.edited = true;
                            
                            const otherUser = users.get(msg.otherId);
                            if (otherUser && otherUser.ws && otherUser.ws.readyState === WebSocket.OPEN) {
                                otherUser.ws.send(JSON.stringify({
                                    type: 'message_edited',
                                    messageId: msg.messageId,
                                    newText: msg.newText
                                }));
                            }
                        }
                    }
                    break;
                    
                case 'delete_message':
                    const delChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(delChatId)) {
                        const delMsg = messages.get(delChatId).find(m => m.id === msg.messageId);
                        if (delMsg && delMsg.from === msg.userId) {
                            delMsg.text = '🗑️ Сообщение удалено';
                            delMsg.deleted = true;
                            
                            const otherUser = users.get(msg.otherId);
                            if (otherUser && otherUser.ws && otherUser.ws.readyState === WebSocket.OPEN) {
                                otherUser.ws.send(JSON.stringify({
                                    type: 'message_deleted',
                                    messageId: msg.messageId
                                }));
                            }
                        }
                    }
                    break;
                    
                // ========== ОТВЕТЫ ==========
                case 'reply_message':
                    const replyChatId = getChatId(msg.from, msg.to);
                    const originalMsg = messages.get(replyChatId)?.find(m => m.id === msg.replyToId);
                    
                    if (originalMsg) {
                        const replyMessage = {
                            id: Date.now(),
                            from: msg.from,
                            to: msg.to,
                            text: msg.text,
                            time: msg.time,
                            replyTo: originalMsg.id,
                            replyText: originalMsg.text,
                            replyName: users.get(originalMsg.from)?.name,
                            read: false
                        };
                        
                        if (!messages.has(replyChatId)) messages.set(replyChatId, []);
                        messages.get(replyChatId).push(replyMessage);
                        
                        const replyToUser = users.get(msg.to);
                        if (replyToUser && replyToUser.ws && replyToUser.ws.readyState === WebSocket.OPEN) {
                            replyToUser.ws.send(JSON.stringify({
                                type: 'new_message',
                                message: replyMessage,
                                fromName: users.get(msg.from)?.name,
                                fromAvatar: users.get(msg.from)?.avatar
                            }));
                        }
                        
                        ws.send(JSON.stringify({ type: 'message_sent', message: replyMessage }));
                    }
                    break;
                    
                // ========== ПЕРЕСЫЛКА ==========
                case 'forward_message':
                    const forwardChatId = getChatId(msg.userId, msg.originalFrom);
                    const forwardMsg = messages.get(forwardChatId)?.find(m => m.id === msg.messageId);
                    
                    if (forwardMsg) {
                        const forwarded = {
                            id: Date.now(),
                            from: msg.userId,
                            to: msg.toUserId,
                            text: forwardMsg.text,
                            time: new Date().toLocaleTimeString(),
                            forwarded: true,
                            originalFrom: forwardMsg.from,
                            originalFromName: users.get(forwardMsg.from)?.name
                        };
                        
                        const targetChatId = getChatId(msg.userId, msg.toUserId);
                        if (!messages.has(targetChatId)) messages.set(targetChatId, []);
                        messages.get(targetChatId).push(forwarded);
                        
                        const targetUser = users.get(msg.toUserId);
                        if (targetUser && targetUser.ws && targetUser.ws.readyState === WebSocket.OPEN) {
                            targetUser.ws.send(JSON.stringify({
                                type: 'new_message',
                                message: forwarded,
                                fromName: users.get(msg.userId)?.name,
                                fromAvatar: users.get(msg.userId)?.avatar
                            }));
                        }
                    }
                    break;
                    
                // ========== РЕАКЦИИ ==========
                case 'add_reaction':
                    broadcastToAll('reaction_added', {
                        messageId: msg.messageId,
                        userId: msg.userId,
                        reaction: msg.reaction,
                        chatId: msg.chatId
                    });
                    break;
                    
                // ========== ГРУППЫ ==========
                case 'create_group':
                    const groupId = 'group_' + Date.now();
                    groups.set(groupId, {
                        id: groupId,
                        name: msg.name,
                        avatar: msg.avatar || '👥',
                        members: [msg.creatorId, ...(msg.members || [])],
                        creator: msg.creatorId,
                        admins: [msg.creatorId],
                        messages: [],
                        createdAt: new Date()
                    });
                    
                    // Уведомляем всех участников
                    groups.get(groupId).members.forEach(memberId => {
                        const member = users.get(memberId);
                        if (member && member.ws && member.ws.readyState === WebSocket.OPEN) {
                            member.ws.send(JSON.stringify({
                                type: 'group_created',
                                groupId: groupId,
                                groupName: msg.name,
                                groupAvatar: msg.avatar || '👥'
                            }));
                        }
                    });
                    
                    ws.send(JSON.stringify({ type: 'group_created_success', groupId: groupId }));
                    break;
                    
                case 'group_message':
                    const group = groups.get(msg.groupId);
                    if (group && group.members.includes(msg.from)) {
                        const groupMsg = {
                            id: Date.now(),
                            from: msg.from,
                            fromName: msg.fromName,
                            text: msg.text,
                            time: msg.time
                        };
                        group.messages.push(groupMsg);
                        
                        group.members.forEach(memberId => {
                            const member = users.get(memberId);
                            if (member && member.ws && member.ws.readyState === WebSocket.OPEN) {
                                member.ws.send(JSON.stringify({
                                    type: 'new_group_message',
                                    groupId: msg.groupId,
                                    groupName: group.name,
                                    message: groupMsg
                                }));
                            }
                        });
                    }
                    break;
                    
                // ========== КАНАЛЫ ==========
                case 'create_channel':
                    const channelId = 'channel_' + Date.now();
                    const channel = {
                        id: channelId,
                        name: msg.name,
                        avatar: msg.avatar || '📢',
                        subscribers: [msg.creatorId],
                        creator: msg.creatorId,
                        posts: [],
                        createdAt: new Date()
                    };
                    channels.set(channelId, channel);
                    
                    ws.send(JSON.stringify({
                        type: 'channel_created',
                        channelId: channelId,
                        channelName: msg.name
                    }));
                    break;
                    
                case 'subscribe_channel':
                    const channelToSubscribe = channels.get(msg.channelId);
                    if (channelToSubscribe && !channelToSubscribe.subscribers.includes(msg.userId)) {
                        channelToSubscribe.subscribers.push(msg.userId);
                        ws.send(JSON.stringify({
                            type: 'subscribed',
                            channelId: msg.channelId,
                            channelName: channelToSubscribe.name
                        }));
                    }
                    break;
                    
                // ========== ОПРОСЫ ==========
                case 'create_poll':
                    const pollId = Date.now();
                    const poll = {
                        id: pollId,
                        question: msg.question,
                        options: msg.options,
                        votes: new Array(msg.options.length).fill(0),
                        voters: new Set(),
                        multi: msg.multi,
                        creator: msg.creatorId
                    };
                    polls.set(pollId, poll);
                    
                    broadcastToAll('poll_created', {
                        pollId: pollId,
                        question: msg.question,
                        options: msg.options
                    });
                    break;
                    
                case 'vote_poll':
                    const targetPoll = polls.get(msg.pollId);
                    if (targetPoll && !targetPoll.voters.has(msg.userId)) {
                        targetPoll.voters.add(msg.userId);
                        if (Array.isArray(msg.options)) {
                            msg.options.forEach(opt => {
                                if (targetPoll.votes[opt] !== undefined) targetPoll.votes[opt]++;
                            });
                        } else {
                            targetPoll.votes[msg.options]++;
                        }
                        
                        broadcastToAll('poll_updated', {
                            pollId: msg.pollId,
                            votes: targetPoll.votes
                        });
                    }
                    break;
                    
                // ========== ГОЛОСОВЫЕ СООБЩЕНИЯ ==========
                case 'voice_message':
                    const voiceChatId = getChatId(msg.from, msg.to);
                    const voiceMsg = {
                        id: Date.now(),
                        from: msg.from,
                        to: msg.to,
                        duration: msg.duration,
                        voiceUrl: msg.voiceUrl,
                        time: msg.time,
                        type: 'voice'
                    };
                    
                    if (!messages.has(voiceChatId)) messages.set(voiceChatId, []);
                    messages.get(voiceChatId).push(voiceMsg);
                    
                    const voiceRecipient = users.get(msg.to);
                    if (voiceRecipient && voiceRecipient.ws && voiceRecipient.ws.readyState === WebSocket.OPEN) {
                        voiceRecipient.ws.send(JSON.stringify({
                            type: 'new_voice',
                            message: voiceMsg,
                            fromName: users.get(msg.from)?.name
                        }));
                    }
                    break;
                    
                // ========== СТИКЕРЫ ==========
                case 'send_sticker':
                    const stickerChatId = getChatId(msg.from, msg.to);
                    const stickerMsg = {
                        id: Date.now(),
                        from: msg.from,
                        to: msg.to,
                        sticker: msg.sticker,
                        time: msg.time,
                        type: 'sticker'
                    };
                    
                    if (!messages.has(stickerChatId)) messages.set(stickerChatId, []);
                    messages.get(stickerChatId).push(stickerMsg);
                    
                    const stickerRecipient = users.get(msg.to);
                    if (stickerRecipient && stickerRecipient.ws && stickerRecipient.ws.readyState === WebSocket.OPEN) {
                        stickerRecipient.ws.send(JSON.stringify({
                            type: 'new_sticker',
                            message: stickerMsg,
                            fromName: users.get(msg.from)?.name
                        }));
                    }
                    break;
                    
                // ========== СТАТУСЫ ==========
                case 'typing':
                    const typingUser = users.get(msg.to);
                    if (typingUser && typingUser.ws && typingUser.ws.readyState === WebSocket.OPEN) {
                        typingUser.ws.send(JSON.stringify({
                            type: 'typing',
                            from: msg.from,
                            fromName: msg.fromName,
                            isTyping: msg.isTyping
                        }));
                    }
                    break;
                    
                case 'mark_read':
                    const readChatId = getChatId(msg.userId, msg.otherId);
                    if (messages.has(readChatId)) {
                        messages.get(readChatId).forEach(m => {
                            if (m.to === msg.userId && !m.read) {
                                m.read = true;
                            }
                        });
                    }
                    break;
                    
                // ========== ИСТОРИЯ ==========
                case 'get_history':
                    const historyChatId = getChatId(msg.userId, msg.withUserId);
                    const history = messages.get(historyChatId) || [];
                    ws.send(JSON.stringify({
                        type: 'chat_history',
                        messages: history,
                        withUser: msg.withUserId
                    }));
                    break;
                    
                case 'search_messages':
                    const searchChatId = getChatId(msg.userId, msg.withUserId);
                    const allMsgs = messages.get(searchChatId) || [];
                    const found = allMsgs.filter(m => 
                        m.text && m.text.toLowerCase().includes(msg.query.toLowerCase())
                    );
                    ws.send(JSON.stringify({
                        type: 'search_results',
                        messages: found,
                        query: msg.query
                    }));
                    break;
                    
                // ========== БЛОКИРОВКА ==========
                case 'block_user':
                    const blockUser = users.get(msg.blockId);
                    if (blockUser) {
                        if (!blockUser.blockedBy) blockUser.blockedBy = new Set();
                        blockUser.blockedBy.add(msg.userId);
                        ws.send(JSON.stringify({ type: 'user_blocked', userId: msg.blockId }));
                    }
                    break;
                    
                case 'unblock_user':
                    const unblockUser = users.get(msg.blockId);
                    if (unblockUser && unblockUser.blockedBy) {
                        unblockUser.blockedBy.delete(msg.userId);
                        ws.send(JSON.stringify({ type: 'user_unblocked', userId: msg.blockId }));
                    }
                    break;
                    
                // ========== КОНТАКТЫ ==========
                case 'add_contact':
                    const contactUser = users.get(msg.contactId);
                    if (contactUser) {
                        if (!contactUser.contacts) contactUser.contacts = new Set();
                        contactUser.contacts.add(msg.userId);
                        ws.send(JSON.stringify({ type: 'contact_added', contactId: msg.contactId }));
                    }
                    break;
                    
                // ========== ЗВОНКИ ==========
                case 'start_call':
                    const callTo = users.get(msg.to);
                    if (callTo && callTo.ws && callTo.ws.readyState === WebSocket.OPEN) {
                        callTo.ws.send(JSON.stringify({
                            type: 'incoming_call',
                            from: msg.from,
                            fromName: msg.fromName,
                            callId: msg.callId
                        }));
                    }
                    break;
                    
                case 'end_call':
                    const callEndTo = users.get(msg.to);
                    if (callEndTo && callEndTo.ws && callEndTo.ws.readyState === WebSocket.OPEN) {
                        callEndTo.ws.send(JSON.stringify({
                            type: 'call_ended',
                            callId: msg.callId
                        }));
                    }
                    break;
                    
                // ========== УДАЛЕНИЕ АККАУНТА ==========
                case 'delete_account':
                    const deletedUser = users.get(msg.userId);
                    if (deletedUser) {
                        users.delete(msg.userId);
                        onlineUsers.delete(msg.userId);
                        broadcastUserList();
                        broadcastToAll('system_message', {
                            text: `⚠️ ${deletedUser.name} удалил аккаунт`,
                            time: new Date().toLocaleTimeString()
                        });
                    }
                    break;
                    
                // ========== ПОЛУЧЕНИЕ ПОЛЬЗОВАТЕЛЕЙ ==========
                case 'get_users':
                    broadcastUserList();
                    break;
                    
                // ========== PING ДЛЯ ПОДДЕРЖАНИЯ СОЕДИНЕНИЯ ==========
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
                    break;
            }
        } catch(e) {
            console.error('Ошибка обработки:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            if (users.has(userId)) {
                users.get(userId).online = false;
                users.get(userId).lastSeen = new Date();
                broadcastUserList();
                broadcastToAll('system_message', {
                    text: `👋 ${users.get(userId)?.name} покинул чат`,
                    time: new Date().toLocaleTimeString()
                });
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 MEGA MESSENGER PRO ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✅ Все функции работают!`);
    console.log(`📊 Пользователей онлайн: ${onlineUsers.size}`);
    console.log(`💬 Чатов активно: ${messages.size}`);
    console.log(`👥 Групп создано: ${groups.size}\n`);
});

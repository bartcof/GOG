const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Хранилище данных
const users = new Map();
const messages = new Map();
const groups = new Map();
const deletedUsers = new Set();
let onlineUsers = new Set();
let adminId = null;

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

function broadcastToAll(type, data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type, ...data }));
        }
    });
}

function broadcastUserList() {
    const userList = Array.from(users.entries()).map(([id, data]) => ({
        id: id,
        name: data.name,
        avatar: data.avatar,
        online: onlineUsers.has(id),
        isAdmin: data.isAdmin || false,
        isBanned: data.isBanned || false,
        lastSeen: data.lastSeen
    }));
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({
                type: 'user_list',
                users: userList
            }));
        }
    });
}

wss.on('connection', (ws, req) => {
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            
            switch(message.type) {
                case 'register':
                    if (deletedUsers.has(message.userId)) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Аккаунт заблокирован' }));
                        return;
                    }
                    
                    userId = message.userId;
                    
                    // Обновляем существующего пользователя вместо создания нового
                    if (users.has(userId)) {
                        const existingUser = users.get(userId);
                        existingUser.name = message.name;
                        existingUser.avatar = message.avatar;
                        existingUser.ws = ws;
                        existingUser.online = true;
                        existingUser.lastSeen = new Date().toISOString();
                    } else {
                        users.set(userId, {
                            ws: ws,
                            name: message.name,
                            avatar: message.avatar,
                            online: true,
                            isAdmin: users.size === 0,
                            isBanned: false,
                            createdAt: new Date().toISOString(),
                            lastSeen: new Date().toISOString()
                        });
                    }
                    
                    if (users.size === 1) adminId = userId;
                    onlineUsers.add(userId);
                    
                    ws.send(JSON.stringify({
                        type: 'registered',
                        userId: userId,
                        isAdmin: users.get(userId).isAdmin,
                        userName: users.get(userId).name
                    }));
                    
                    broadcastUserList();
                    broadcastToAll('system_message', {
                        text: `👋 ${users.get(userId).name} присоединился`,
                        time: new Date().toLocaleTimeString()
                    });
                    break;
                    
                case 'update_profile':
                    if (users.has(message.userId)) {
                        const user = users.get(message.userId);
                        const oldName = user.name;
                        user.name = message.name;
                        user.avatar = message.avatar;
                        
                        broadcastUserList();
                        broadcastToAll('system_message', {
                            text: `✏️ ${oldName} изменил имя на ${message.name}`,
                            time: new Date().toLocaleTimeString()
                        });
                        
                        ws.send(JSON.stringify({
                            type: 'profile_updated',
                            name: message.name,
                            avatar: message.avatar
                        }));
                    }
                    break;
                    
                case 'delete_user':
                    const admin = users.get(message.adminId);
                    if (admin && admin.isAdmin && users.has(message.userId)) {
                        const targetUser = users.get(message.userId);
                        if (targetUser.ws && targetUser.ws.readyState === WebSocket.OPEN) {
                            targetUser.ws.send(JSON.stringify({
                                type: 'account_deleted',
                                reason: message.reason || 'Аккаунт удалён администратором'
                            }));
                            setTimeout(() => targetUser.ws.close(), 1000);
                        }
                        deletedUsers.add(message.userId);
                        users.delete(message.userId);
                        onlineUsers.delete(message.userId);
                        broadcastUserList();
                        broadcastToAll('system_message', {
                            text: `⚠️ ${targetUser.name} был удалён`,
                            time: new Date().toLocaleTimeString()
                        });
                    }
                    break;
                    
                case 'create_group':
                    const groupId = 'group_' + Date.now();
                    groups.set(groupId, {
                        name: message.groupName,
                        avatar: message.groupAvatar || '👥',
                        members: [message.creatorId, ...(message.members || [])],
                        creator: message.creatorId,
                        messages: [],
                        createdAt: new Date().toISOString()
                    });
                    
                    groups.get(groupId).members.forEach(memberId => {
                        const member = users.get(memberId);
                        if (member && member.ws && member.ws.readyState === WebSocket.OPEN) {
                            member.ws.send(JSON.stringify({
                                type: 'group_created',
                                groupId: groupId,
                                groupName: message.groupName,
                                groupAvatar: message.groupAvatar || '👥'
                            }));
                        }
                    });
                    break;
                    
                case 'group_message':
                    const group = groups.get(message.groupId);
                    if (group && group.members.includes(message.from)) {
                        const newMessage = {
                            id: Date.now(),
                            from: message.from,
                            fromName: message.fromName,
                            fromAvatar: message.fromAvatar,
                            text: message.text,
                            time: message.time,
                            type: 'group'
                        };
                        group.messages.push(newMessage);
                        
                        group.members.forEach(memberId => {
                            const member = users.get(memberId);
                            if (member && member.ws && member.ws.readyState === WebSocket.OPEN) {
                                member.ws.send(JSON.stringify({
                                    type: 'new_group_message',
                                    groupId: message.groupId,
                                    groupName: group.name,
                                    groupAvatar: group.avatar,
                                    message: newMessage
                                }));
                            }
                        });
                    }
                    break;
                    
                case 'private_message':
                    const recipient = users.get(message.to);
                    const sender = users.get(message.from);
                    
                    const chatId = getChatId(message.from, message.to);
                    if (!messages.has(chatId)) messages.set(chatId, []);
                    
                    const newMsg = {
                        id: Date.now(),
                        from: message.from,
                        to: message.to,
                        text: message.text,
                        time: message.time,
                        read: false
                    };
                    messages.get(chatId).push(newMsg);
                    
                    if (recipient && recipient.ws && recipient.ws.readyState === WebSocket.OPEN && !recipient.isBanned) {
                        recipient.ws.send(JSON.stringify({
                            type: 'new_message',
                            messageId: newMsg.id,
                            from: message.from,
                            fromName: message.fromName,
                            fromAvatar: message.fromAvatar,
                            text: message.text,
                            time: message.time
                        }));
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        messageId: newMsg.id,
                        to: message.to,
                        text: message.text,
                        time: message.time
                    }));
                    break;
                    
                case 'typing':
                    const typingRecipient = users.get(message.to);
                    if (typingRecipient && typingRecipient.ws && typingRecipient.ws.readyState === WebSocket.OPEN) {
                        typingRecipient.ws.send(JSON.stringify({
                            type: 'typing',
                            from: message.from,
                            fromName: message.fromName,
                            isTyping: message.isTyping
                        }));
                    }
                    break;
                    
                case 'mark_read':
                    const chatIdRead = getChatId(message.userId, message.otherId);
                    if (messages.has(chatIdRead)) {
                        messages.get(chatIdRead).forEach(msg => {
                            if (msg.to === message.userId && !msg.read) {
                                msg.read = true;
                            }
                        });
                    }
                    break;
                    
                case 'get_users':
                    broadcastUserList();
                    break;
                    
                case 'get_history':
                    const historyChatId = getChatId(message.userId, message.withUserId);
                    const history = messages.get(historyChatId) || [];
                    ws.send(JSON.stringify({
                        type: 'chat_history',
                        messages: history,
                        withUser: message.withUserId
                    }));
                    break;
            }
        } catch(e) {
            console.error('Error:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            if (users.has(userId)) {
                users.get(userId).online = false;
                users.get(userId).lastSeen = new Date().toISOString();
                broadcastUserList();
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 TELEGRAM CLONE ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✨ Полный функционал Telegram!\n`);
});

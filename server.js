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
        isBanned: data.isBanned || false
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
                        ws.send(JSON.stringify({ type: 'error', text: 'Этот ID заблокирован' }));
                        return;
                    }
                    
                    userId = message.userId;
                    users.set(userId, {
                        ws: ws,
                        name: message.name,
                        avatar: message.avatar,
                        online: true,
                        isAdmin: users.size === 0,
                        isBanned: false,
                        createdAt: new Date().toISOString()
                    });
                    
                    if (users.size === 1) adminId = userId;
                    onlineUsers.add(userId);
                    
                    ws.send(JSON.stringify({
                        type: 'registered',
                        userId: userId,
                        isAdmin: users.get(userId).isAdmin
                    }));
                    
                    broadcastUserList();
                    broadcastToAll('system_message', {
                        text: `👋 ${message.name} присоединился к чату!`,
                        time: new Date().toLocaleTimeString()
                    });
                    break;
                    
                case 'delete_user':
                    const admin = users.get(message.adminId);
                    if (admin && admin.isAdmin && users.has(message.userId)) {
                        const targetUser = users.get(message.userId);
                        if (targetUser.ws && targetUser.ws.readyState === WebSocket.OPEN) {
                            targetUser.ws.send(JSON.stringify({
                                type: 'account_deleted',
                                reason: message.reason || 'Ваш аккаунт был удалён администратором'
                            }));
                            setTimeout(() => targetUser.ws.close(), 1000);
                        }
                        deletedUsers.add(message.userId);
                        users.delete(message.userId);
                        onlineUsers.delete(message.userId);
                        broadcastUserList();
                        broadcastToAll('system_message', {
                            text: `⚠️ Пользователь ${targetUser.name} был удалён из чата`,
                            time: new Date().toLocaleTimeString()
                        });
                    }
                    break;
                    
                case 'ban_user':
                    const adminBan = users.get(message.adminId);
                    if (adminBan && adminBan.isAdmin && users.has(message.userId)) {
                        const bannedUser = users.get(message.userId);
                        bannedUser.isBanned = true;
                        if (bannedUser.ws && bannedUser.ws.readyState === WebSocket.OPEN) {
                            bannedUser.ws.send(JSON.stringify({
                                type: 'banned',
                                reason: message.reason
                            }));
                        }
                        broadcastUserList();
                    }
                    break;
                    
                case 'create_group':
                    const groupId = 'group_' + Date.now();
                    groups.set(groupId, {
                        name: message.groupName,
                        avatar: message.groupAvatar || '👥',
                        members: [message.creatorId, ...message.members],
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
                                    message: newMessage
                                }));
                            }
                        });
                    }
                    break;
                    
                case 'private_message':
                    const recipient = users.get(message.to);
                    const sender = users.get(message.from);
                    
                    if (recipient && recipient.ws && recipient.ws.readyState === WebSocket.OPEN && !recipient.isBanned) {
                        recipient.ws.send(JSON.stringify({
                            type: 'new_message',
                            from: message.from,
                            fromName: message.fromName,
                            fromAvatar: message.fromAvatar,
                            text: message.text,
                            time: message.time
                        }));
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'message_sent',
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
                    
                case 'edit_message':
                    // Редактирование сообщения
                    broadcastToAll('message_edited', {
                        messageId: message.messageId,
                        newText: message.newText,
                        chatId: message.chatId
                    });
                    break;
                    
                case 'delete_message':
                    broadcastToAll('message_deleted', {
                        messageId: message.messageId,
                        chatId: message.chatId
                    });
                    break;
                    
                case 'get_users':
                    broadcastUserList();
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
    console.log(`\n🚀 MEGA MESSENGER ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✨ Полный функционал как в Telegram!\n`);
});

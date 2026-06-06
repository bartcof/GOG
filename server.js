const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Хранилище данных (сохраняем между перезагрузками)
const users = new Map(); // userId -> {ws, name, avatar, online, lastSeen, isAdmin}
const messages = new Map(); // chatId -> [messages]
const groups = new Map();
const bannedUsers = new Set();
let onlineUsers = new Set();

// Функция для генерации ID чата
function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

// Отправка всем пользователям
function broadcastToAll(type, data, excludeWs = null) {
    wss.clients.forEach(client => {
        if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type, ...data }));
        }
    });
}

// Отправка списка пользователей
function broadcastUserList() {
    const userList = Array.from(users.entries()).map(([id, data]) => ({
        id: id,
        name: data.name,
        avatar: data.avatar,
        online: onlineUsers.has(id),
        isAdmin: data.isAdmin || false,
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
    
    ws.on('message', async (data) => {
        try {
            const message = JSON.parse(data);
            
            switch(message.type) {
                case 'register':
                    // Проверка на бан
                    if (bannedUsers.has(message.userId)) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Ваш аккаунт заблокирован' }));
                        ws.close();
                        return;
                    }
                    
                    userId = message.userId;
                    
                    // ПОЧИНЕНО: Обновляем существующего пользователя, а не создаём нового
                    if (users.has(userId)) {
                        const existingUser = users.get(userId);
                        existingUser.ws = ws;
                        existingUser.online = true;
                        existingUser.lastSeen = new Date().toISOString();
                        // Не меняем имя и аватар, если они не были переданы
                        if (message.name && message.name !== 'undefined') {
                            existingUser.name = message.name;
                        }
                        if (message.avatar && message.avatar !== 'undefined') {
                            existingUser.avatar = message.avatar;
                        }
                    } else {
                        // Новый пользователь
                        users.set(userId, {
                            ws: ws,
                            name: message.name || 'User_' + Math.floor(Math.random() * 1000),
                            avatar: message.avatar || '😊',
                            online: true,
                            isAdmin: users.size === 0,
                            lastSeen: new Date().toISOString(),
                            createdAt: new Date().toISOString()
                        });
                    }
                    
                    onlineUsers.add(userId);
                    
                    // Отправляем подтверждение
                    ws.send(JSON.stringify({
                        type: 'registered',
                        userId: userId,
                        isAdmin: users.get(userId).isAdmin,
                        userName: users.get(userId).name
                    }));
                    
                    // Отправляем историю сообщений
                    const userMessages = [];
                    for (let [chatId, msgs] of messages) {
                        if (chatId.includes(userId)) {
                            userMessages.push({ chatId, messages: msgs });
                        }
                    }
                    ws.send(JSON.stringify({
                        type: 'chat_history_all',
                        histories: userMessages
                    }));
                    
                    broadcastUserList();
                    broadcastToAll('system_message', {
                        text: `👋 ${users.get(userId).name} присоединился к чату`,
                        time: new Date().toLocaleTimeString()
                    }, ws);
                    break;
                    
                case 'update_profile':
                    if (users.has(message.userId)) {
                        const user = users.get(message.userId);
                        const oldName = user.name;
                        user.name = message.name;
                        user.avatar = message.avatar;
                        user.lastSeen = new Date().toISOString();
                        
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
                    
                case 'private_message':
                    const recipient = users.get(message.to);
                    const sender = users.get(message.from);
                    
                    if (!recipient) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Пользователь не найден' }));
                        return;
                    }
                    
                    if (bannedUsers.has(message.from) || bannedUsers.has(message.to)) {
                        ws.send(JSON.stringify({ type: 'error', text: 'Вы заблокированы' }));
                        return;
                    }
                    
                    const chatId = getChatId(message.from, message.to);
                    if (!messages.has(chatId)) messages.set(chatId, []);
                    
                    const newMessage = {
                        id: Date.now(),
                        from: message.from,
                        to: message.to,
                        text: message.text,
                        time: message.time,
                        read: false,
                        edited: false,
                        deleted: false
                    };
                    messages.get(chatId).push(newMessage);
                    
                    // Отправляем получателю
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
                    
                case 'edit_message':
                    const editChatId = getChatId(message.userId, message.otherId);
                    if (messages.has(editChatId)) {
                        const msgs = messages.get(editChatId);
                        const msgIndex = msgs.findIndex(m => m.id === message.messageId);
                        if (msgIndex !== -1 && msgs[msgIndex].from === message.userId) {
                            msgs[msgIndex].text = message.newText;
                            msgs[msgIndex].edited = true;
                            
                            // Уведомляем собеседника
                            const otherUser = users.get(message.otherId);
                            if (otherUser && otherUser.ws && otherUser.ws.readyState === WebSocket.OPEN) {
                                otherUser.ws.send(JSON.stringify({
                                    type: 'message_edited',
                                    messageId: message.messageId,
                                    newText: message.newText
                                }));
                            }
                        }
                    }
                    break;
                    
                case 'delete_message':
                    const deleteChatId = getChatId(message.userId, message.otherId);
                    if (messages.has(deleteChatId)) {
                        const msgs = messages.get(deleteChatId);
                        const msgIndex = msgs.findIndex(m => m.id === message.messageId);
                        if (msgIndex !== -1 && msgs[msgIndex].from === message.userId) {
                            msgs[msgIndex].deleted = true;
                            msgs[msgIndex].text = 'Сообщение удалено';
                            
                            const otherUser = users.get(message.otherId);
                            if (otherUser && otherUser.ws && otherUser.ws.readyState === WebSocket.OPEN) {
                                otherUser.ws.send(JSON.stringify({
                                    type: 'message_deleted',
                                    messageId: message.messageId
                                }));
                            }
                        }
                    }
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
                    const readChatId = getChatId(message.userId, message.otherId);
                    if (messages.has(readChatId)) {
                        messages.get(readChatId).forEach(msg => {
                            if (msg.to === message.userId && !msg.read) {
                                msg.read = true;
                            }
                        });
                    }
                    break;
                    
                case 'delete_account':
                    if (users.has(message.userId)) {
                        const user = users.get(message.userId);
                        bannedUsers.add(message.userId);
                        users.delete(message.userId);
                        onlineUsers.delete(message.userId);
                        
                        broadcastUserList();
                        broadcastToAll('system_message', {
                            text: `⚠️ ${user.name} удалил аккаунт`,
                            time: new Date().toLocaleTimeString()
                        });
                    }
                    break;
                    
                case 'get_users':
                    broadcastUserList();
                    break;
                    
                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
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
    console.log(`\n🚀 MESSENGER V3 ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`✅ Баг с дублированием пользователей ИСПРАВЛЕН!`);
    console.log(`✨ Добавлены новые функции!\n`);
});

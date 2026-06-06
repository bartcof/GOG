const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Файлы для сохранения данных
const DATA_FILE = './data.json';

// Загрузка сохранённых данных
let savedData = { users: {}, messages: {} };
if (fs.existsSync(DATA_FILE)) {
    try {
        savedData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    } catch(e) { console.log('Ошибка загрузки данных'); }
}

// Хранилища
const users = new Map();
const messages = new Map();
const onlineUsers = new Set();

// Загружаем сохранённых пользователей
for (let [id, data] of Object.entries(savedData.users || {})) {
    users.set(id, {
        name: data.name,
        avatar: data.avatar,
        bio: data.bio || '',
        phone: data.phone || '',
        createdAt: data.createdAt,
        isAdmin: data.isAdmin || false
    });
}

// Загружаем сохранённые сообщения
for (let [chatId, msgs] of Object.entries(savedData.messages || {})) {
    messages.set(chatId, msgs);
}

// Функция сохранения данных
function saveData() {
    const toSave = {
        users: {},
        messages: {}
    };
    
    for (let [id, user] of users) {
        toSave.users[id] = {
            name: user.name,
            avatar: user.avatar,
            bio: user.bio,
            phone: user.phone,
            createdAt: user.createdAt,
            isAdmin: user.isAdmin
        };
    }
    
    for (let [chatId, msgs] of messages) {
        toSave.messages[chatId] = msgs;
    }
    
    fs.writeFileSync(DATA_FILE, JSON.stringify(toSave, null, 2));
    console.log('💾 Данные сохранены');
}

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

function broadcastUsers() {
    const list = [];
    for (let [id, user] of users) {
        list.push({
            id: id,
            name: user.name,
            avatar: user.avatar,
            online: onlineUsers.has(id),
            bio: user.bio,
            isAdmin: user.isAdmin
        });
    }
    
    for (let [id, user] of users) {
        if (user.ws && user.ws.readyState === WebSocket.OPEN) {
            user.ws.send(JSON.stringify({
                type: 'users',
                users: list
            }));
        }
    }
}

wss.on('connection', (ws) => {
    console.log('📱 Новое подключение');
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log('📨 Получено:', msg.type);
            
            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;
                
                if (users.has(userId)) {
                    const existing = users.get(userId);
                    existing.ws = ws;
                    existing.online = true;
                } else {
                    users.set(userId, {
                        ws: ws,
                        name: msg.name,
                        avatar: msg.avatar || '😊',
                        online: true,
                        bio: msg.bio || '',
                        phone: msg.phone || '',
                        createdAt: new Date().toISOString(),
                        isAdmin: users.size === 0
                    });
                    saveData();
                }
                
                onlineUsers.add(userId);
                
                ws.send(JSON.stringify({
                    type: 'registered',
                    userId: userId,
                    isAdmin: users.get(userId).isAdmin
                }));
                
                // Отправляем историю сообщений
                const userMessages = [];
                for (let [chatId, msgs] of messages) {
                    if (chatId.includes(userId)) {
                        const otherId = chatId.replace(userId, '').replace('_', '');
                        if (otherId && otherId !== userId) {
                            userMessages.push({
                                withUser: otherId,
                                messages: msgs
                            });
                        }
                    }
                }
                ws.send(JSON.stringify({
                    type: 'chat_history',
                    histories: userMessages
                }));
                
                broadcastUsers();
            }
            
            // ОТПРАВКА СООБЩЕНИЯ
            if (msg.type === 'message') {
                const toUser = users.get(msg.to);
                const fromUser = users.get(msg.from);
                
                if (toUser && fromUser) {
                    const chatId = getChatId(msg.from, msg.to);
                    if (!messages.has(chatId)) messages.set(chatId, []);
                    
                    const newMsg = {
                        id: Date.now(),
                        from: msg.from,
                        to: msg.to,
                        text: msg.text,
                        time: msg.time,
                        read: false,
                        date: new Date().toISOString()
                    };
                    messages.get(chatId).push(newMsg);
                    saveData();
                    
                    // Отправляем получателю
                    if (toUser.ws && toUser.ws.readyState === WebSocket.OPEN) {
                        toUser.ws.send(JSON.stringify({
                            type: 'message',
                            from: msg.from,
                            text: msg.text,
                            time: msg.time,
                            fromName: fromUser.name
                        }));
                    }
                    
                    // Подтверждение отправителю
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        message: newMsg
                    }));
                }
            }
            
            // ПОЛУЧЕНИЕ ИСТОРИИ
            if (msg.type === 'get_history') {
                const chatId = getChatId(msg.userId, msg.withUserId);
                const history = messages.get(chatId) || [];
                ws.send(JSON.stringify({
                    type: 'history_data',
                    messages: history,
                    withUser: msg.withUserId
                }));
            }
            
            // УДАЛЕНИЕ СООБЩЕНИЯ
            if (msg.type === 'delete_message') {
                const chatId = getChatId(msg.userId, msg.otherId);
                if (messages.has(chatId)) {
                    const msgs = messages.get(chatId);
                    const index = msgs.findIndex(m => m.id === msg.messageId);
                    if (index !== -1 && msgs[index].from === msg.userId) {
                        msgs[index].text = '🗑️ Сообщение удалено';
                        msgs[index].deleted = true;
                        saveData();
                        
                        const otherUser = users.get(msg.otherId);
                        if (otherUser && otherUser.ws) {
                            otherUser.ws.send(JSON.stringify({
                                type: 'message_deleted',
                                messageId: msg.messageId,
                                chatId: chatId
                            }));
                        }
                        ws.send(JSON.stringify({
                            type: 'message_deleted_confirm',
                            messageId: msg.messageId
                        }));
                    }
                }
            }
            
            // УДАЛЕНИЕ АККАУНТА
            if (msg.type === 'delete_account') {
                if (users.has(msg.userId)) {
                    const deletedUser = users.get(msg.userId);
                    
                    // Удаляем все сообщения пользователя
                    for (let [chatId, msgs] of messages) {
                        const filtered = msgs.filter(m => m.from !== msg.userId && m.to !== msg.userId);
                        if (filtered.length === 0) {
                            messages.delete(chatId);
                        } else {
                            messages.set(chatId, filtered);
                        }
                    }
                    
                    users.delete(msg.userId);
                    onlineUsers.delete(msg.userId);
                    saveData();
                    
                    broadcastUsers();
                    
                    // Уведомляем всех
                    for (let [id, user] of users) {
                        if (user.ws && user.ws.readyState === WebSocket.OPEN) {
                            user.ws.send(JSON.stringify({
                                type: 'user_deleted',
                                userId: msg.userId,
                                name: deletedUser.name
                            }));
                        }
                    }
                    
                    if (deletedUser.ws) {
                        deletedUser.ws.send(JSON.stringify({
                            type: 'account_deleted',
                            message: 'Ваш аккаунт был удалён'
                        }));
                    }
                }
            }
            
            // ОЧИСТКА ИСТОРИИ
            if (msg.type === 'clear_history') {
                const chatId = getChatId(msg.userId, msg.withUserId);
                if (messages.has(chatId)) {
                    messages.delete(chatId);
                    saveData();
                    
                    const otherUser = users.get(msg.withUserId);
                    if (otherUser && otherUser.ws) {
                        otherUser.ws.send(JSON.stringify({
                            type: 'history_cleared',
                            withUser: msg.userId
                        }));
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'history_cleared_confirm',
                        withUser: msg.withUserId
                    }));
                }
            }
            
            // ОБНОВЛЕНИЕ ПРОФИЛЯ
            if (msg.type === 'update_profile') {
                const user = users.get(msg.userId);
                if (user) {
                    if (msg.name) user.name = msg.name;
                    if (msg.avatar) user.avatar = msg.avatar;
                    if (msg.bio) user.bio = msg.bio;
                    if (msg.phone) user.phone = msg.phone;
                    saveData();
                    
                    ws.send(JSON.stringify({
                        type: 'profile_updated',
                        name: user.name,
                        avatar: user.avatar,
                        bio: user.bio
                    }));
                    broadcastUsers();
                }
            }
            
            // СТАТУС ПЕЧАТИ
            if (msg.type === 'typing') {
                const toUser = users.get(msg.to);
                if (toUser && toUser.ws) {
                    toUser.ws.send(JSON.stringify({
                        type: 'typing',
                        from: msg.from,
                        isTyping: msg.isTyping,
                        fromName: msg.fromName
                    }));
                }
            }
            
            // ОТМЕТКА О ПРОЧТЕНИИ
            if (msg.type === 'mark_read') {
                const chatId = getChatId(msg.userId, msg.otherId);
                if (messages.has(chatId)) {
                    let updated = false;
                    messages.get(chatId).forEach(m => {
                        if (m.to === msg.userId && !m.read) {
                            m.read = true;
                            updated = true;
                        }
                    });
                    if (updated) saveData();
                }
            }
            
        } catch(e) {
            console.log('❌ Ошибка:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            if (users.has(userId)) {
                users.get(userId).online = false;
                broadcastUsers();
            }
            console.log('👋 Пользователь отключился');
        }
    });
});

// Автосохранение каждые 30 секунд
setInterval(saveData, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 СЕРВЕР ЗАПУЩЕН на порту ${PORT}`);
    console.log(`📱 Открой: http://localhost:${PORT}`);
    console.log(`💾 Сохранено пользователей: ${users.size}`);
    console.log(`💬 Сохранено чатов: ${messages.size}\n`);
});

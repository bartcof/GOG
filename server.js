const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// СОХРАНЕНИЕ В ФАЙЛ
const DATA_FILE = './data.json';

// Загрузка сохранённых данных
let savedData = { users: {}, messages: {} };
if (fs.existsSync(DATA_FILE)) {
    try {
        const fileContent = fs.readFileSync(DATA_FILE, 'utf8');
        savedData = JSON.parse(fileContent);
        console.log('✅ Загружена история из data.json');
    } catch(e) { 
        console.log('❌ Ошибка загрузки data.json, создаю новый'); 
    }
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
        createdAt: data.createdAt,
        isAdmin: data.isAdmin || false
    });
}

// Загружаем сохранённые сообщения
for (let [chatId, msgs] of Object.entries(savedData.messages || {})) {
    messages.set(chatId, msgs);
    console.log(`📁 Загружен чат ${chatId}: ${msgs.length} сообщений`);
}

// Функция сохранения
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
            createdAt: user.createdAt,
            isAdmin: user.isAdmin
        };
    }
    
    for (let [chatId, msgs] of messages) {
        toSave.messages[chatId] = msgs;
    }
    
    fs.writeFileSync(DATA_FILE, JSON.stringify(toSave, null, 2));
    console.log('💾 Данные сохранены в data.json');
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
            bio: user.bio
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
                        createdAt: new Date().toISOString(),
                        isAdmin: users.size === 0
                    });
                    saveData();
                }
                
                onlineUsers.add(userId);
                
                ws.send(JSON.stringify({
                    type: 'registered',
                    userId: userId
                }));
                
                // ОТПРАВЛЯЕМ ВСЮ ИСТОРИЮ пользователю
                const userHistory = [];
                for (let [chatId, msgs] of messages) {
                    if (chatId.includes(userId)) {
                        const parts = chatId.split('_');
                        const otherId = parts[0] === userId ? parts[1] : parts[0];
                        if (otherId && otherId !== userId) {
                            userHistory.push({
                                withUser: otherId,
                                messages: msgs
                            });
                        }
                    }
                }
                
                ws.send(JSON.stringify({
                    type: 'full_history',
                    histories: userHistory
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
                        timestamp: Date.now()
                    };
                    messages.get(chatId).push(newMsg);
                    saveData(); // СОХРАНЯЕМ СРАЗУ
                    
                    // Отправляем получателю
                    if (toUser.ws && toUser.ws.readyState === WebSocket.OPEN) {
                        toUser.ws.send(JSON.stringify({
                            type: 'new_message',
                            message: newMsg,
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
            
            // ЗАПРОС ИСТОРИИ
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
                                messageId: msg.messageId
                            }));
                        }
                    }
                }
            }
            
            // ОЧИСТКА ВСЕЙ ИСТОРИИ ЧАТА
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
                        type: 'history_cleared_confirm'
                    }));
                }
            }
            
            // УДАЛЕНИЕ АККАУНТА
            if (msg.type === 'delete_account') {
                if (users.has(msg.userId)) {
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
                    
                    if (users.get(msg.userId)?.ws) {
                        users.get(msg.userId).ws.send(JSON.stringify({
                            type: 'account_deleted'
                        }));
                    }
                }
            }
            
            // ОБНОВЛЕНИЕ ПРОФИЛЯ
            if (msg.type === 'update_profile') {
                const user = users.get(msg.userId);
                if (user) {
                    if (msg.name) user.name = msg.name;
                    if (msg.avatar) user.avatar = msg.avatar;
                    if (msg.bio) user.bio = msg.bio;
                    saveData();
                    
                    ws.send(JSON.stringify({
                        type: 'profile_updated',
                        name: user.name,
                        avatar: user.avatar
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

// Автосохранение каждые 10 секунд
setInterval(saveData, 10000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 СЕРВЕР ЗАПУЩЕН на порту ${PORT}`);
    console.log(`📱 Открой: http://localhost:${PORT}`);
    console.log(`💾 Сохранено пользователей: ${users.size}`);
    console.log(`💬 Сохранено чатов: ${messages.size}`);
    console.log(`📁 Файл истории: ${DATA_FILE}\n`);
});

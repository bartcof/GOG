const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Файл для сохранения истории (в persistent volume)
const DATA_DIR = process.env.DATA_DIR || './data';
const DATA_FILE = path.join(DATA_DIR, 'history.json');

// Создаём директорию если её нет
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Загружаем сохранённую историю
let savedHistory = { messages: {} };
if (fs.existsSync(DATA_FILE)) {
    try {
        savedHistory = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
        console.log('📂 Загружена история из файла');
    } catch(e) { console.log('Новый файл истории'); }
}

const users = new Map();
const messages = new Map();
const onlineUsers = new Set();

// Загружаем сообщения из файла
for (let [chatId, msgs] of Object.entries(savedHistory.messages || {})) {
    messages.set(chatId, msgs);
}

// Сохранение в файл
function saveToFile() {
    const toSave = { messages: {} };
    for (let [chatId, msgs] of messages) {
        toSave.messages[chatId] = msgs;
    }
    fs.writeFileSync(DATA_FILE, JSON.stringify(toSave, null, 2));
    console.log('💾 История сохранена');
}

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

// Отправка списка пользователей
function broadcastUsers() {
    const list = [];
    for (let [id, user] of users) {
        list.push({
            id: id,
            name: user.name,
            avatar: user.avatar,
            online: onlineUsers.has(id)
        });
    }
    
    for (let [id, user] of users) {
        if (user.ws && user.ws.readyState === WebSocket.OPEN) {
            user.ws.send(JSON.stringify({ type: 'users', users: list }));
        }
    }
}

wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            
            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;
                
                users.set(userId, {
                    ws: ws,
                    name: msg.name,
                    avatar: msg.avatar || '😊',
                    online: true
                });
                onlineUsers.add(userId);
                
                ws.send(JSON.stringify({ type: 'registered', userId: userId }));
                
                // ОТПРАВЛЯЕМ ВСЮ ИСТОРИЮ пользователю
                const userChats = [];
                for (let [chatId, msgs] of messages) {
                    if (chatId.includes(userId)) {
                        const otherId = chatId.replace(userId, '').replace('_', '');
                        if (otherId && otherId !== userId) {
                            userChats.push({
                                withUser: otherId,
                                messages: msgs
                            });
                        }
                    }
                }
                
                ws.send(JSON.stringify({
                    type: 'all_history',
                    chats: userChats
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
                        date: new Date().toISOString()
                    };
                    messages.get(chatId).push(newMsg);
                    saveToFile(); // СОХРАНЯЕМ В ФАЙЛ
                    
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
            
            // ЗАПРОС ИСТОРИИ ЧАТА
            if (msg.type === 'get_chat_history') {
                const chatId = getChatId(msg.userId, msg.withUserId);
                const history = messages.get(chatId) || [];
                ws.send(JSON.stringify({
                    type: 'chat_history',
                    messages: history,
                    withUser: msg.withUserId
                }));
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
            
        } catch(e) { console.log('Ошибка:', e); }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            if (users.has(userId)) {
                users.get(userId).online = false;
                broadcastUsers();
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 Сервер запущен на порту ${PORT}`);
    console.log(`💾 История сохранена: ${messages.size} чатов\n`);
});


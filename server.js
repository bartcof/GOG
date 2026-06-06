const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Простые хранилища
const users = new Map();
const messages = new Map();
const onlineUsers = new Set();

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

wss.on('connection', (ws) => {
    console.log('Новое подключение');
    let userId = null;
    
    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log('Получено:', msg.type);
            
            if (msg.type === 'register') {
                userId = msg.userId;
                users.set(userId, {
                    ws: ws,
                    name: msg.name,
                    avatar: msg.avatar || '😊',
                    online: true
                });
                onlineUsers.add(userId);
                
                // Отправляем подтверждение
                ws.send(JSON.stringify({ 
                    type: 'registered', 
                    userId: userId 
                }));
                
                // Отправляем список пользователей всем
                const userList = [];
                for (let [id, user] of users) {
                    userList.push({
                        id: id,
                        name: user.name,
                        avatar: user.avatar,
                        online: onlineUsers.has(id)
                    });
                }
                
                // Отправляем список всем
                for (let [id, user] of users) {
                    if (user.ws && user.ws.readyState === WebSocket.OPEN) {
                        user.ws.send(JSON.stringify({
                            type: 'users',
                            users: userList
                        }));
                    }
                }
            }
            
            if (msg.type === 'message') {
                const toUser = users.get(msg.to);
                if (toUser && toUser.ws) {
                    toUser.ws.send(JSON.stringify({
                        type: 'message',
                        from: msg.from,
                        text: msg.text,
                        time: msg.time,
                        fromName: msg.fromName
                    }));
                }
            }
            
            if (msg.type === 'typing') {
                const toUser = users.get(msg.to);
                if (toUser && toUser.ws) {
                    toUser.ws.send(JSON.stringify({
                        type: 'typing',
                        from: msg.from,
                        isTyping: msg.isTyping
                    }));
                }
            }
            
        } catch(e) {
            console.log('Ошибка:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            console.log('Пользователь отключился');
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n✅ СЕРВЕР ЗАПУЩЕН на порту ${PORT}`);
    console.log(`📱 Открой: http://localhost:${PORT}\n`);
});

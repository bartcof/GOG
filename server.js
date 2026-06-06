const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// Хранилище пользователей
const users = new Map(); // userId -> {ws, name, avatar, online}
let allUsers = new Map(); // userId -> {name, avatar} для истории
let onlineUsers = new Set();

// Генерация ID чата
function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

wss.on('connection', (ws, req) => {
    let userId = null;
    
    // Отправка списка ВСЕХ пользователей (онлайн и оффлайн)
    function broadcastUserList() {
        // Собираем всех пользователей
        const userList = Array.from(allUsers.entries()).map(([id, data]) => ({
            id: id,
            name: data.name,
            avatar: data.avatar,
            online: onlineUsers.has(id)
        }));
        
        // Отправляем каждому клиенту
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({
                    type: 'user_list',
                    users: userList,
                    currentUserId: userId
                }));
            }
        });
    }
    
    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data);
            
            switch(message.type) {
                case 'register':
                    userId = message.userId;
                    // Сохраняем пользователя
                    users.set(userId, {
                        ws: ws,
                        name: message.name,
                        avatar: message.avatar,
                        online: true
                    });
                    allUsers.set(userId, {
                        name: message.name,
                        avatar: message.avatar
                    });
                    onlineUsers.add(userId);
                    
                    // Отправляем подтверждение
                    ws.send(JSON.stringify({
                        type: 'registered',
                        userId: userId
                    }));
                    
                    // Рассылаем обновлённый список всем
                    broadcastUserList();
                    console.log(`✅ Пользователь ${message.name} подключился (${userId})`);
                    console.log(`👥 Онлайн: ${onlineUsers.size} человек`);
                    break;
                    
                case 'get_users':
                    // Запрос списка пользователей
                    const userList = Array.from(allUsers.entries()).map(([id, data]) => ({
                        id: id,
                        name: data.name,
                        avatar: data.avatar,
                        online: onlineUsers.has(id)
                    }));
                    ws.send(JSON.stringify({
                        type: 'user_list',
                        users: userList,
                        currentUserId: userId
                    }));
                    break;
                    
                case 'private_message':
                    // Отправка личного сообщения
                    const recipient = users.get(message.to);
                    if (recipient && recipient.ws.readyState === WebSocket.OPEN) {
                        recipient.ws.send(JSON.stringify({
                            type: 'new_message',
                            from: message.from,
                            fromName: message.fromName,
                            fromAvatar: message.fromAvatar,
                            text: message.text,
                            time: message.time
                        }));
                    }
                    
                    // Подтверждение отправителю
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        to: message.to,
                        text: message.text,
                        time: message.time
                    }));
                    break;
                    
                case 'typing':
                    const typingRecipient = users.get(message.to);
                    if (typingRecipient && typingRecipient.ws.readyState === WebSocket.OPEN) {
                        typingRecipient.ws.send(JSON.stringify({
                            type: 'typing',
                            from: message.from,
                            fromName: message.fromName,
                            isTyping: message.isTyping
                        }));
                    }
                    break;
            }
        } catch(e) {
            console.error('❌ Ошибка:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            broadcastUserList();
            console.log(`❌ Пользователь ${allUsers.get(userId)?.name} отключился`);
            console.log(`👥 Онлайн: ${onlineUsers.size} человек`);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 МЕССЕНДЖЕР ЗАПУЩЕН!`);
    console.log(`📱 Открой: https://gog-production-2083.up.railway.app`);
    console.log(`👥 Жди подключения других пользователей...\n`);
});

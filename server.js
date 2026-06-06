const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

const users = new Map();
const messages = new Map();
let onlineUsers = new Set();

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

wss.on('connection', (ws, req) => {
    let userId = null;
    
    function broadcastOnlineUsers() {
        const onlineList = Array.from(onlineUsers).map(id => ({
            id: id,
            name: users.get(id)?.name,
            avatar: users.get(id)?.avatar
        }));
        
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({
                    type: 'online_users',
                    users: onlineList
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
                    users.set(userId, {
                        ws: ws,
                        name: message.name,
                        avatar: message.avatar,
                        online: true
                    });
                    onlineUsers.add(userId);
                    broadcastOnlineUsers();
                    
                    ws.send(JSON.stringify({
                        type: 'init',
                        userId: userId,
                        messages: Array.from(messages.entries())
                    }));
                    break;
                    
                case 'private_message':
                    const chatId = getChatId(message.from, message.to);
                    if (!messages.has(chatId)) {
                        messages.set(chatId, []);
                    }
                    
                    const newMessage = {
                        id: Date.now(),
                        from: message.from,
                        to: message.to,
                        text: message.text,
                        time: message.time,
                        read: false
                    };
                    
                    messages.get(chatId).push(newMessage);
                    
                    const recipient = users.get(message.to);
                    if (recipient && recipient.ws.readyState === WebSocket.OPEN) {
                        recipient.ws.send(JSON.stringify({
                            type: 'new_message',
                            message: newMessage,
                            fromUser: {
                                id: message.from,
                                name: users.get(message.from)?.name,
                                avatar: users.get(message.from)?.avatar
                            }
                        }));
                    }
                    
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        message: newMessage
                    }));
                    break;
                    
                case 'typing':
                    const typingTo = users.get(message.to);
                    if (typingTo && typingTo.ws.readyState === WebSocket.OPEN) {
                        typingTo.ws.send(JSON.stringify({
                            type: 'typing',
                            from: message.from,
                            isTyping: message.isTyping
                        }));
                    }
                    break;
            }
        } catch(e) {
            console.error('Error:', e);
        }
    });
    
    ws.on('close', () => {
        if (userId) {
            onlineUsers.delete(userId);
            broadcastOnlineUsers();
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`✅ Сервер работает на порту ${PORT}`);
});

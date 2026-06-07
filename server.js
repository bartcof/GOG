const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// PostgreSQL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/messenger',
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// СОЗДАНИЕ ТАБЛИЦ
async function initDB() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id VARCHAR(100) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            avatar VARCHAR(10) DEFAULT '😊',
            bio TEXT,
            online BOOLEAN DEFAULT FALSE,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id BIGINT PRIMARY KEY,
            from_user VARCHAR(100) REFERENCES users(id),
            to_user VARCHAR(100) REFERENCES users(id),
            text TEXT,
            time VARCHAR(20),
            read BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    
    console.log('✅ База данных готова');
}

initDB();

// Хранилище активных соединений
const activeUsers = new Map();

// Функция отправки списка ВСЕХ пользователей
async function sendUserList() {
    const result = await pool.query('SELECT id, name, avatar, bio, online FROM users ORDER BY name');
    const userList = result.rows;
    
    for (let [userId, ws] of activeUsers) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
                type: 'users_list',
                users: userList,
                currentUserId: userId
            }));
        }
    }
}

wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);
            
            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;
                
                // Сохраняем пользователя
                const exists = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                if (exists.rows.length === 0) {
                    await pool.query(
                        'INSERT INTO users (id, name, avatar, bio, online) VALUES ($1, $2, $3, $4, $5)',
                        [userId, msg.name, msg.avatar || '😊', msg.bio || '', true]
                    );
                } else {
                    await pool.query('UPDATE users SET online = TRUE, last_seen = NOW() WHERE id = $1', [userId]);
                }
                
                activeUsers.set(userId, ws);
                
                ws.send(JSON.stringify({ type: 'registered', userId: userId }));
                
                // Отправляем список ВСЕХ пользователей
                await sendUserList();
                
                // Отправляем историю сообщений
                const history = await pool.query(
                    `SELECT * FROM messages 
                     WHERE from_user = $1 OR to_user = $1 
                     ORDER BY created_at ASC`,
                    [userId]
                );
                
                ws.send(JSON.stringify({
                    type: 'chat_history',
                    messages: history.rows
                }));
            }
            
            // ПОЛУЧИТЬ ПОЛЬЗОВАТЕЛЕЙ
            if (msg.type === 'get_users') {
                await sendUserList();
            }
            
            // ОТПРАВКА СООБЩЕНИЯ
            if (msg.type === 'message') {
                const messageId = Date.now();
                await pool.query(
                    'INSERT INTO messages (id, from_user, to_user, text, time) VALUES ($1, $2, $3, $4, $5)',
                    [messageId, msg.from, msg.to, msg.text, msg.time]
                );
                
                // Отправляем получателю
                const toWs = activeUsers.get(msg.to);
                if (toWs && toWs.readyState === WebSocket.OPEN) {
                    toWs.send(JSON.stringify({
                        type: 'new_message',
                        message: { id: messageId, from: msg.from, text: msg.text, time: msg.time },
                        fromName: msg.fromName
                    }));
                }
                
                // Подтверждение отправителю
                ws.send(JSON.stringify({
                    type: 'message_sent',
                    messageId: messageId
                }));
            }
            
            // ПОИСК ПОЛЬЗОВАТЕЛЕЙ
            if (msg.type === 'search_users') {
                const searchTerm = `%${msg.query}%`;
                const result = await pool.query(
                    `SELECT id, name, avatar, bio FROM users 
                     WHERE name ILIKE $1 AND id != $2
                     LIMIT 20`,
                    [searchTerm, msg.userId]
                );
                ws.send(JSON.stringify({
                    type: 'search_results',
                    users: result.rows,
                    query: msg.query
                }));
            }
            
        } catch(e) {
            console.error('Ошибка:', e);
        }
    });
    
    ws.on('close', async () => {
        if (userId) {
            await pool.query('UPDATE users SET online = FALSE, last_seen = NOW() WHERE id = $1', [userId]);
            activeUsers.delete(userId);
            await sendUserList();
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 Сервер запущен на порту ${PORT}`);
    console.log(`👥 Активных пользователей: ${activeUsers.size}\n`);
});

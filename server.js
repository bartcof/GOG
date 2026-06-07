const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Увеличиваем лимит для фото
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static('public'));

// Подключение к БД
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// СОЗДАНИЕ ТАБЛИЦ
async function initDB() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                avatar TEXT DEFAULT '😊',
                avatar_type VARCHAR(20) DEFAULT 'emoji',
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
    } catch(e) {
        console.error('❌ Ошибка БД:', e.message);
    }
}

initDB();

// Хранилище активных соединений
const activeUsers = new Map();

// Отправка списка пользователей
async function broadcastUserList() {
    try {
        const result = await pool.query('SELECT id, name, avatar, avatar_type, online, last_seen FROM users ORDER BY name');
        
        for (let [userId, ws] of activeUsers) {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'users_list',
                    users: result.rows,
                    currentUserId: userId
                }));
            }
        }
    } catch(e) {
        console.error('Ошибка отправки списка:', e.message);
    }
}

// Отправка истории сообщений
async function sendHistory(ws, userId) {
    try {
        const result = await pool.query(
            `SELECT * FROM messages 
             WHERE from_user = $1 OR to_user = $1 
             ORDER BY created_at ASC`,
            [userId]
        );
        
        ws.send(JSON.stringify({
            type: 'chat_history',
            messages: result.rows,
            userId: userId
        }));
    } catch(e) {
        console.error('Ошибка отправки истории:', e.message);
    }
}

// Обновление профиля
async function updateUserProfile(userId, name, avatar, avatarType) {
    await pool.query(
        'UPDATE users SET name = $1, avatar = $2, avatar_type = $3, last_seen = NOW() WHERE id = $4',
        [name, avatar, avatarType, userId]
    );
}

wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);
            
            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;
                
                // Проверяем, существует ли пользователь с таким ID
                const exists = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                
                if (exists.rows.length === 0) {
                    // Новый пользователь
                    await pool.query(
                        'INSERT INTO users (id, name, avatar, avatar_type, online) VALUES ($1, $2, $3, $4, $5)',
                        [userId, msg.name, msg.avatar || '😊', msg.avatarType || 'emoji', true]
                    );
                    console.log(`✅ Новый пользователь: ${msg.name}`);
                } else {
                    // Существующий пользователь - просто обновляем статус
                    await pool.query('UPDATE users SET online = TRUE, last_seen = NOW() WHERE id = $1', [userId]);
                    console.log(`✅ Пользователь вернулся: ${exists.rows[0].name}`);
                }
                
                activeUsers.set(userId, ws);
                
                ws.send(JSON.stringify({ 
                    type: 'registered', 
                    userId: userId,
                    userData: exists.rows[0] || { name: msg.name, avatar: msg.avatar }
                }));
                
                // Отправляем историю
                await sendHistory(ws, userId);
                
                // Отправляем список пользователей
                await broadcastUserList();
            }
            
            // ОБНОВЛЕНИЕ ПРОФИЛЯ (никнейм и аватар)
            if (msg.type === 'update_profile') {
                await updateUserProfile(msg.userId, msg.name, msg.avatar, msg.avatarType);
                
                // Оповещаем всех пользователей об обновлении
                await broadcastUserList();
                
                ws.send(JSON.stringify({ type: 'profile_updated', success: true }));
                console.log(`📝 Профиль обновлён: ${msg.name}`);
            }
            
            // ПОЛУЧИТЬ ПОЛЬЗОВАТЕЛЕЙ
            if (msg.type === 'get_users') {
                await broadcastUserList();
            }
            
            // ПОИСК ПОЛЬЗОВАТЕЛЕЙ
            if (msg.type === 'search_users') {
                const searchTerm = `%${msg.query}%`;
                const result = await pool.query(
                    `SELECT id, name, avatar, avatar_type, online FROM users 
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
            
            // ОТПРАВКА СООБЩЕНИЯ
            if (msg.type === 'message') {
                const messageId = Date.now();
                
                await pool.query(
                    'INSERT INTO messages (id, from_user, to_user, text, time) VALUES ($1, $2, $3, $4, $5)',
                    [messageId, msg.from, msg.to, msg.text, msg.time]
                );
                
                const toWs = activeUsers.get(msg.to);
                if (toWs && toWs.readyState === WebSocket.OPEN) {
                    toWs.send(JSON.stringify({
                        type: 'new_message',
                        message: { id: messageId, from: msg.from, text: msg.text, time: msg.time },
                        fromName: msg.fromName,
                        fromAvatar: msg.fromAvatar
                    }));
                }
                
                ws.send(JSON.stringify({
                    type: 'message_sent',
                    messageId: messageId,
                    to: msg.to,
                    text: msg.text,
                    time: msg.time
                }));
            }
            
        } catch(e) {
            console.error('Ошибка:', e.message);
        }
    });
    
    ws.on('close', async () => {
        if (userId) {
            await pool.query('UPDATE users SET online = FALSE, last_seen = NOW() WHERE id = $1', [userId]);
            activeUsers.delete(userId);
            await broadcastUserList();
            console.log(`👋 Пользователь отключился`);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 СЕРВЕР ЗАПУЩЕН на порту ${PORT}`);
    console.log(`💾 Сохранение в PostgreSQL`);
    console.log(`👥 Активных пользователей: ${activeUsers.size}\n`);
});

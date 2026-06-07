const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// ========== ПОДКЛЮЧЕНИЕ К БД ==========
const pool = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/messenger',
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// ========== СОЗДАНИЕ ТАБЛИЦ ==========
async function initDB() {
    try {
        // Таблица пользователей
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                avatar VARCHAR(10) DEFAULT '😊',
                bio TEXT,
                phone VARCHAR(20),
                is_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        // Таблица сообщений
        await pool.query(`
            CREATE TABLE IF NOT EXISTS messages (
                id BIGINT PRIMARY KEY,
                from_user VARCHAR(100) REFERENCES users(id),
                to_user VARCHAR(100) REFERENCES users(id),
                text TEXT,
                time VARCHAR(20),
                read BOOLEAN DEFAULT FALSE,
                edited BOOLEAN DEFAULT FALSE,
                deleted BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        // Индексы для быстрого поиска
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_from_to ON messages(from_user, to_user)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_to_read ON messages(to_user, read)`);
        
        console.log('✅ База данных инициализирована');
    } catch(e) {
        console.error('❌ Ошибка БД:', e);
    }
}

initDB();

// ========== ХРАНИЛИЩА ДЛЯ WS ==========
const users = new Map();
const onlineUsers = new Set();

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

// ========== ОТПРАВКА СПИСКА ПОЛЬЗОВАТЕЛЕЙ ==========
async function broadcastUsers() {
    const result = await pool.query(
        'SELECT id, name, avatar, is_admin, last_seen FROM users ORDER BY name'
    );
    const list = result.rows.map(user => ({
        ...user,
        online: onlineUsers.has(user.id)
    }));
    
    for (let [id, user] of users) {
        if (user.ws && user.ws.readyState === WebSocket.OPEN) {
            user.ws.send(JSON.stringify({ type: 'users', users: list }));
        }
    }
}

// ========== СОХРАНЕНИЕ СООБЩЕНИЯ В БД ==========
async function saveMessage(messageId, from, to, text, time) {
    await pool.query(
        `INSERT INTO messages (id, from_user, to_user, text, time) 
         VALUES ($1, $2, $3, $4, $5)`,
        [messageId, from, to, text, time]
    );
}

// ========== ПОЛУЧЕНИЕ ИСТОРИИ ==========
async function getChatHistory(userId, otherId) {
    const result = await pool.query(
        `SELECT * FROM messages 
         WHERE (from_user = $1 AND to_user = $2) 
            OR (from_user = $2 AND to_user = $1)
         ORDER BY created_at ASC
         LIMIT 500`,
        [userId, otherId]
    );
    return result.rows;
}

// ========== ОТМЕТКА О ПРОЧТЕНИИ ==========
async function markAsRead(userId, otherId) {
    await pool.query(
        `UPDATE messages SET read = TRUE 
         WHERE to_user = $1 AND from_user = $2 AND read = FALSE`,
        [userId, otherId]
    );
}

// ========== ОБНОВЛЕНИЕ ПРОФИЛЯ ==========
async function updateProfile(userId, name, avatar, bio, phone) {
    await pool.query(
        `UPDATE users SET 
            name = COALESCE($1, name),
            avatar = COALESCE($2, avatar),
            bio = COALESCE($3, bio),
            phone = COALESCE($4, phone),
            last_seen = CURRENT_TIMESTAMP
         WHERE id = $5`,
        [name, avatar, bio, phone, userId]
    );
}

// ========== УДАЛЕНИЕ СООБЩЕНИЯ ==========
async function deleteMessage(messageId, userId) {
    await pool.query(
        `UPDATE messages SET deleted = TRUE, text = '🗑️ Сообщение удалено' 
         WHERE id = $1 AND from_user = $2`,
        [messageId, userId]
    );
}

// ========== УДАЛЕНИЕ АККАУНТА ==========
async function deleteAccount(userId) {
    await pool.query(`DELETE FROM messages WHERE from_user = $1 OR to_user = $1`, [userId]);
    await pool.query(`DELETE FROM users WHERE id = $1`, [userId]);
}

// ========== ОЧИСТКА ИСТОРИИ ==========
async function clearHistory(userId, otherId) {
    await pool.query(
        `DELETE FROM messages 
         WHERE (from_user = $1 AND to_user = $2) 
            OR (from_user = $2 AND to_user = $1)`,
        [userId, otherId]
    );
}

// ========== ОБНОВЛЕНИЕ LAST SEEN ==========
async function updateLastSeen(userId) {
    await pool.query(`UPDATE users SET last_seen = CURRENT_TIMESTAMP WHERE id = $1`, [userId]);
}

// ========== WEB SOCKET СОЕДИНЕНИЕ ==========
wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);
            
            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;
                
                // Проверяем, существует ли пользователь
                const existing = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                
                if (existing.rows.length === 0) {
                    await pool.query(
                        `INSERT INTO users (id, name, avatar, bio, phone, is_admin) 
                         VALUES ($1, $2, $3, $4, $5, $6)`,
                        [userId, msg.name, msg.avatar || '😊', msg.bio || '', msg.phone || '', false]
                    );
                } else {
                    await updateLastSeen(userId);
                }
                
                users.set(userId, { ws, name: msg.name });
                onlineUsers.add(userId);
                
                ws.send(JSON.stringify({ type: 'registered', userId: userId }));
                
                // Отправляем историю чатов
                const userChats = [];
                const allUsers = await pool.query('SELECT id FROM users WHERE id != $1', [userId]);
                
                for (let user of allUsers.rows) {
                    const history = await getChatHistory(userId, user.id);
                    if (history.length > 0) {
                        userChats.push({
                            withUser: user.id,
                            messages: history
                        });
                    }
                }
                
                ws.send(JSON.stringify({ type: 'all_history', chats: userChats }));
                await broadcastUsers();
            }
            
            // ОТПРАВКА СООБЩЕНИЯ
            if (msg.type === 'message') {
                const messageId = Date.now();
                await saveMessage(messageId, msg.from, msg.to, msg.text, msg.time);
                
                const toUser = users.get(msg.to);
                if (toUser && toUser.ws && toUser.ws.readyState === WebSocket.OPEN) {
                    toUser.ws.send(JSON.stringify({
                        type: 'new_message',
                        message: { id: messageId, from: msg.from, text: msg.text, time: msg.time },
                        fromName: msg.fromName
                    }));
                }
                
                ws.send(JSON.stringify({ type: 'message_sent', messageId: messageId }));
            }
            
            // ПОЛУЧЕНИЕ ИСТОРИИ
            if (msg.type === 'get_history') {
                const history = await getChatHistory(msg.userId, msg.withUserId);
                ws.send(JSON.stringify({
                    type: 'history_data',
                    messages: history,
                    withUser: msg.withUserId
                }));
            }
            
            // ОТМЕТКА О ПРОЧТЕНИИ
            if (msg.type === 'mark_read') {
                await markAsRead(msg.userId, msg.otherId);
            }
            
            // УДАЛЕНИЕ СООБЩЕНИЯ
            if (msg.type === 'delete_message') {
                await deleteMessage(msg.messageId, msg.userId);
                
                const otherUser = users.get(msg.otherId);
                if (otherUser && otherUser.ws) {
                    otherUser.ws.send(JSON.stringify({
                        type: 'message_deleted',
                        messageId: msg.messageId
                    }));
                }
            }
            
            // ОЧИСТКА ИСТОРИИ
            if (msg.type === 'clear_history') {
                await clearHistory(msg.userId, msg.withUserId);
                ws.send(JSON.stringify({ type: 'history_cleared' }));
            }
            
            // ОБНОВЛЕНИЕ ПРОФИЛЯ
            if (msg.type === 'update_profile') {
                await updateProfile(msg.userId, msg.name, msg.avatar, msg.bio, msg.phone);
                await broadcastUsers();
                ws.send(JSON.stringify({ type: 'profile_updated' }));
            }
            
            // УДАЛЕНИЕ АККАУНТА
            if (msg.type === 'delete_account') {
                await deleteAccount(msg.userId);
                onlineUsers.delete(msg.userId);
                users.delete(msg.userId);
                await broadcastUsers();
                ws.send(JSON.stringify({ type: 'account_deleted' }));
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
            
        } catch(e) { console.error('Ошибка:', e); }
    });
    
    ws.on('close', async () => {
        if (userId) {
            onlineUsers.delete(userId);
            await updateLastSeen(userId);
            await broadcastUsers();
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 СЕРВЕР ЗАПУЩЕН на порту ${PORT}`);
    console.log(`💾 База данных: PostgreSQL`);
});

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

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
                invite_code VARCHAR(50) UNIQUE,
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
        
        await pool.query(`
            CREATE TABLE IF NOT EXISTS friends (
                user_id VARCHAR(100) REFERENCES users(id),
                friend_id VARCHAR(100) REFERENCES users(id),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, friend_id)
            )
        `);
        
        console.log('✅ База данных готова');
    } catch(e) {
        console.error('❌ Ошибка БД:', e.message);
    }
}

initDB();

const activeUsers = new Map();

// Генерация кода приглашения
function generateInviteCode() {
    return Math.random().toString(36).substring(2, 10).toUpperCase();
}

async function broadcastUserList() {
    try {
        const result = await pool.query('SELECT id, name, avatar, avatar_type, online, invite_code FROM users ORDER BY name');
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
        console.error('Ошибка:', e.message);
    }
}

async function sendHistory(ws, userId) {
    try {
        const result = await pool.query(
            `SELECT * FROM messages WHERE from_user = $1 OR to_user = $1 ORDER BY created_at ASC`,
            [userId]
        );
        ws.send(JSON.stringify({ type: 'chat_history', messages: result.rows }));
    } catch(e) {
        console.error('Ошибка:', e.message);
    }
}

async function addFriend(userId, friendId) {
    try {
        // Проверяем, не друзья ли уже
        const existing = await pool.query(
            'SELECT * FROM friends WHERE (user_id = $1 AND friend_id = $2) OR (user_id = $2 AND friend_id = $1)',
            [userId, friendId]
        );
        if (existing.rows.length === 0) {
            await pool.query(
                'INSERT INTO friends (user_id, friend_id, status) VALUES ($1, $2, $3)',
                [userId, friendId, 'accepted']
            );
            await pool.query(
                'INSERT INTO friends (user_id, friend_id, status) VALUES ($1, $2, $3)',
                [friendId, userId, 'accepted']
            );
            return true;
        }
        return false;
    } catch(e) {
        console.error('Ошибка добавления друга:', e.message);
        return false;
    }
}

async function getFriends(userId) {
    const result = await pool.query(
        `SELECT u.id, u.name, u.avatar, u.avatar_type, u.online 
         FROM friends f JOIN users u ON f.friend_id = u.id 
         WHERE f.user_id = $1 AND f.status = 'accepted'`,
        [userId]
    );
    return result.rows;
}

wss.on('connection', (ws) => {
    let userId = null;
    
    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);
            
            if (msg.type === 'register') {
                userId = msg.userId;
                
                const exists = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                if (exists.rows.length === 0) {
                    const inviteCode = generateInviteCode();
                    await pool.query(
                        'INSERT INTO users (id, name, avatar, avatar_type, online, invite_code) VALUES ($1, $2, $3, $4, $5, $6)',
                        [userId, msg.name, msg.avatar || '😊', msg.avatarType || 'emoji', true, inviteCode]
                    );
                } else {
                    await pool.query('UPDATE users SET online = TRUE, last_seen = NOW() WHERE id = $1', [userId]);
                }
                
                activeUsers.set(userId, ws);
                
                // Отправляем данные пользователя
                const userData = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                ws.send(JSON.stringify({ 
                    type: 'registered', 
                    userId: userId,
                    userData: userData.rows[0],
                    inviteLink: `${msg.referrer || ''}?ref=${userData.rows[0].invite_code}`
                }));
                
                // Обработка реферальной ссылки
                if (msg.refCode) {
                    const referrer = await pool.query('SELECT id FROM users WHERE invite_code = $1', [msg.refCode]);
                    if (referrer.rows.length > 0 && referrer.rows[0].id !== userId) {
                        await addFriend(referrer.rows[0].id, userId);
                    }
                }
                
                await sendHistory(ws, userId);
                await broadcastUserList();
            }
            
            if (msg.type === 'update_profile') {
                await pool.query(
                    'UPDATE users SET name = $1, avatar = $2, avatar_type = $3, bio = $4 WHERE id = $5',
                    [msg.name, msg.avatar, msg.avatarType, msg.bio || '', msg.userId]
                );
                await broadcastUserList();
                ws.send(JSON.stringify({ type: 'profile_updated' }));
            }
            
            if (msg.type === 'get_invite_link') {
                const user = await pool.query('SELECT invite_code FROM users WHERE id = $1', [msg.userId]);
                if (user.rows.length > 0) {
                    ws.send(JSON.stringify({
                        type: 'invite_link',
                        link: `${msg.baseUrl}?ref=${user.rows[0].invite_code}`
                    }));
                }
            }
            
            if (msg.type === 'add_friend') {
                const success = await addFriend(msg.userId, msg.friendId);
                ws.send(JSON.stringify({ type: 'friend_added', success: success, friendId: msg.friendId }));
                await broadcastUserList();
            }
            
            if (msg.type === 'get_friends') {
                const friends = await getFriends(msg.userId);
                ws.send(JSON.stringify({ type: 'friends_list', friends: friends }));
            }
            
            if (msg.type === 'get_users') {
                await broadcastUserList();
            }
            
            if (msg.type === 'search_users') {
                const searchTerm = `%${msg.query.toLowerCase()}%`;
                const result = await pool.query(
                    `SELECT id, name, avatar, avatar_type, online FROM users 
                     WHERE LOWER(name) LIKE $1 AND id != $2
                     LIMIT 50`,
                    [searchTerm, msg.userId]
                );
                ws.send(JSON.stringify({
                    type: 'search_results',
                    users: result.rows,
                    query: msg.query
                }));
            }
            
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
                ws.send(JSON.stringify({ type: 'message_sent', messageId: messageId }));
            }
            
        } catch(e) {
            console.error('Ошибка:', e.message);
        }
    });
    
    ws.on('close', async () => {
        if (userId) {
            await pool.query('UPDATE users SET online = FALSE WHERE id = $1', [userId]);
            activeUsers.delete(userId);
            await broadcastUserList();
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n🚀 СЕРВЕР ЗАПУЩЕН на порту ${PORT}\n`);
});

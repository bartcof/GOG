const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static('public'));

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// ПЕРЕСОЗДАНИЕ ТАБЛИЦ
async function initDB() {
    try {
        await pool.query('DROP TABLE IF EXISTS friends CASCADE');
        await pool.query('DROP TABLE IF EXISTS messages CASCADE');
        await pool.query('DROP TABLE IF EXISTS users CASCADE');
        
        await pool.query(`
            CREATE TABLE users (
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
            CREATE TABLE messages (
                id BIGINT PRIMARY KEY,
                from_user VARCHAR(100) REFERENCES users(id) ON DELETE CASCADE,
                to_user VARCHAR(100) REFERENCES users(id) ON DELETE CASCADE,
                text TEXT,
                time VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);
        
        await pool.query(`
            CREATE TABLE friends (
                user_id VARCHAR(100) REFERENCES users(id) ON DELETE CASCADE,
                friend_id VARCHAR(100) REFERENCES users(id) ON DELETE CASCADE,
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

function generateInviteCode() {
    return Math.random().toString(36).substring(2, 10).toUpperCase();
}

async function broadcastUserList() {
    try {
        const result = await pool.query('SELECT id, name, avatar, avatar_type, online, bio FROM users ORDER BY name');
        for (let [userId, ws] of activeUsers) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'users_list',
                    users: result.rows
                }));
            }
        }
    } catch(e) {
        console.error('Ошибка broadcast:', e.message);
    }
}

// Оповещение конкретного пользователя об изменениях другого пользователя
async function notifyUserAboutProfileChange(targetUserId, changedUser) {
    const ws = activeUsers.get(targetUserId);
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: 'user_profile_updated',
            user: changedUser
        }));
    }
}

async function sendHistory(ws, userId) {
    try {
        const result = await pool.query(
            `SELECT * FROM messages WHERE from_user = $1 OR to_user = $1 ORDER BY created_at ASC LIMIT 500`,
            [userId]
        );
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'chat_history', messages: result.rows }));
        }
    } catch(e) {
        console.error('Ошибка истории:', e.message);
    }
}

async function addFriend(userId, friendId) {
    try {
        if (userId === friendId) return false;
        const existing = await pool.query('SELECT * FROM friends WHERE user_id = $1 AND friend_id = $2', [userId, friendId]);
        if (existing.rows.length === 0) {
            await pool.query('INSERT INTO friends (user_id, friend_id) VALUES ($1, $2)', [userId, friendId]);
            await pool.query('INSERT INTO friends (user_id, friend_id) VALUES ($1, $2)', [friendId, userId]);
            return true;
        }
        return false;
    } catch(e) {
        return false;
    }
}

async function getFriends(userId) {
    try {
        const result = await pool.query(
            `SELECT u.id, u.name, u.avatar, u.avatar_type, u.online, u.bio 
             FROM friends f JOIN users u ON f.friend_id = u.id WHERE f.user_id = $1`,
            [userId]
        );
        return result.rows;
    } catch(e) {
        return [];
    }
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
                        'INSERT INTO users (id, name, avatar, avatar_type, online, invite_code, bio) VALUES ($1, $2, $3, $4, $5, $6, $7)',
                        [userId, msg.name, msg.avatar || '😊', msg.avatarType || 'emoji', true, inviteCode, msg.bio || '']
                    );
                } else {
                    await pool.query('UPDATE users SET online = TRUE, last_seen = NOW() WHERE id = $1', [userId]);
                }
                
                activeUsers.set(userId, ws);
                const userData = await pool.query('SELECT * FROM users WHERE id = $1', [userId]);
                
                ws.send(JSON.stringify({ type: 'registered', userId, userData: userData.rows[0] }));
                
                if (msg.refCode && msg.refCode !== 'null') {
                    const referrer = await pool.query('SELECT id FROM users WHERE invite_code = $1', [msg.refCode]);
                    if (referrer.rows.length > 0 && referrer.rows[0].id !== userId) {
                        await addFriend(referrer.rows[0].id, userId);
                        const referrerWs = activeUsers.get(referrer.rows[0].id);
                        if (referrerWs && referrerWs.readyState === WebSocket.OPEN) {
                            referrerWs.send(JSON.stringify({
                                type: 'friend_added_notify',
                                friend: userData.rows[0]
                            }));
                        }
                    }
                }
                
                await sendHistory(ws, userId);
                const friendsList = await getFriends(userId);
                ws.send(JSON.stringify({ type: 'friends_list', friends: friendsList }));
                await broadcastUserList();
            }
            
            if (msg.type === 'update_profile') {
                await pool.query('UPDATE users SET name = $1, bio = $2 WHERE id = $3', [msg.name, msg.bio || '', msg.userId]);
                // Отправить обновлённые данные всем
                const updatedUser = await pool.query('SELECT id, name, avatar, avatar_type, online, bio FROM users WHERE id = $1', [msg.userId]);
                await broadcastUserList();
                ws.send(JSON.stringify({ type: 'profile_updated' }));
            }
            
            if (msg.type === 'update_avatar') {
                await pool.query('UPDATE users SET avatar = $1, avatar_type = $2 WHERE id = $3', [msg.avatar, msg.avatarType, msg.userId]);
                await broadcastUserList();
                ws.send(JSON.stringify({ type: 'avatar_updated' }));
            }
            
            if (msg.type === 'get_invite_link') {
                const user = await pool.query('SELECT invite_code FROM users WHERE id = $1', [msg.userId]);
                if (user.rows.length > 0) {
                    ws.send(JSON.stringify({ type: 'invite_link', link: `${msg.baseUrl}?ref=${user.rows[0].invite_code}` }));
                }
            }
            
            if (msg.type === 'add_friend') {
                await addFriend(msg.userId, msg.friendId);
                ws.send(JSON.stringify({ type: 'friend_added', success: true }));
                const friendsList = await getFriends(msg.userId);
                ws.send(JSON.stringify({ type: 'friends_list', friends: friendsList }));
                const friendWs = activeUsers.get(msg.friendId);
                if (friendWs && friendWs.readyState === WebSocket.OPEN) {
                    const friendFriendsList = await getFriends(msg.friendId);
                    friendWs.send(JSON.stringify({ type: 'friends_list', friends: friendFriendsList }));
                }
                await broadcastUserList();
            }
            
            if (msg.type === 'get_friends') {
                const friendsList = await getFriends(msg.userId);
                ws.send(JSON.stringify({ type: 'friends_list', friends: friendsList }));
            }
            
            if (msg.type === 'get_users') {
                const result = await pool.query('SELECT id, name, avatar, avatar_type, online, bio FROM users WHERE id != $1', [msg.userId || userId]);
                ws.send(JSON.stringify({ type: 'users_list', users: result.rows }));
            }
            
            if (msg.type === 'search_users') {
                const searchTerm = `%${msg.query.toLowerCase()}%`;
                const result = await pool.query(
                    `SELECT id, name, avatar, avatar_type, online, bio FROM users 
                     WHERE LOWER(name) LIKE $1 AND id != $2 LIMIT 50`,
                    [searchTerm, msg.userId]
                );
                ws.send(JSON.stringify({ type: 'search_results', users: result.rows, query: msg.query }));
            }
            
            if (msg.type === 'message') {
                const messageId = Date.now();
                const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                await pool.query('INSERT INTO messages (id, from_user, to_user, text, time) VALUES ($1, $2, $3, $4, $5)',
                    [messageId, msg.from, msg.to, msg.text, time]);
                
                const toWs = activeUsers.get(msg.to);
                if (toWs && toWs.readyState === WebSocket.OPEN) {
                    toWs.send(JSON.stringify({
                        type: 'new_message',
                        message: { id: messageId, from: msg.from, to: msg.to, text: msg.text, time },
                        fromName: msg.fromName
                    }));
                }
                ws.send(JSON.stringify({ type: 'message_sent' }));
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

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Сервер на порту ${PORT}`);
});

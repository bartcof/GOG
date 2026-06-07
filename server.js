const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static('public'));

// PostgreSQL connection pool
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('sslmode=disable')
        ? false
        : { rejectUnauthorized: false }
});

// Create tables and indexes on startup
async function initDb() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            avatar TEXT NOT NULL DEFAULT '😊',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);

    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id BIGINT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            from_user TEXT NOT NULL,
            to_user TEXT NOT NULL,
            text TEXT NOT NULL,
            time TEXT NOT NULL,
            date TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages (chat_id)
    `);

    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages (date)
    `);

    console.log('🗄️  База данных инициализирована');
}

function getChatId(user1, user2) {
    return [user1, user2].sort().join('_');
}

// In-memory map of connected users (ws + metadata)
const users = new Map();
const onlineUsers = new Set();

// Broadcast the full user list to every connected client
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

    ws.on('message', async (data) => {
        try {
            const msg = JSON.parse(data);

            // РЕГИСТРАЦИЯ
            if (msg.type === 'register') {
                userId = msg.userId;

                // Upsert user into the database
                await pool.query(
                    `INSERT INTO users (id, name, avatar)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, avatar = EXCLUDED.avatar`,
                    [userId, msg.name, msg.avatar || '😊']
                );

                users.set(userId, {
                    ws: ws,
                    name: msg.name,
                    avatar: msg.avatar || '😊',
                    online: true
                });
                onlineUsers.add(userId);

                ws.send(JSON.stringify({ type: 'registered', userId: userId }));

                // Load full message history for this user from the database
                const historyResult = await pool.query(
                    `SELECT * FROM messages
                     WHERE from_user = $1 OR to_user = $1
                     ORDER BY date ASC`,
                    [userId]
                );

                // Group messages by chat partner
                const chatMap = new Map();
                for (const row of historyResult.rows) {
                    const otherId = row.from_user === userId ? row.to_user : row.from_user;
                    if (!chatMap.has(otherId)) chatMap.set(otherId, []);
                    chatMap.get(otherId).push({
                        id: Number(row.id),
                        from: row.from_user,
                        to: row.to_user,
                        text: row.text,
                        time: row.time,
                        date: row.date
                    });
                }

                const userChats = [];
                for (let [otherId, msgs] of chatMap) {
                    userChats.push({ withUser: otherId, messages: msgs });
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

                    const newMsg = {
                        id: Date.now(),
                        from: msg.from,
                        to: msg.to,
                        text: msg.text,
                        time: msg.time,
                        date: new Date().toISOString()
                    };

                    // Persist message to PostgreSQL
                    await pool.query(
                        `INSERT INTO messages (id, chat_id, from_user, to_user, text, time, date)
                         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                        [newMsg.id, chatId, newMsg.from, newMsg.to, newMsg.text, newMsg.time, newMsg.date]
                    );

                    // Deliver to recipient if online
                    if (toUser.ws && toUser.ws.readyState === WebSocket.OPEN) {
                        toUser.ws.send(JSON.stringify({
                            type: 'new_message',
                            message: newMsg,
                            fromName: fromUser.name
                        }));
                    }

                    // Confirm delivery to sender
                    ws.send(JSON.stringify({
                        type: 'message_sent',
                        message: newMsg
                    }));
                }
            }

            // ЗАПРОС ИСТОРИИ ЧАТА
            if (msg.type === 'get_chat_history') {
                const chatId = getChatId(msg.userId, msg.withUserId);

                const result = await pool.query(
                    `SELECT * FROM messages WHERE chat_id = $1 ORDER BY date ASC`,
                    [chatId]
                );

                const history = result.rows.map(row => ({
                    id: Number(row.id),
                    from: row.from_user,
                    to: row.to_user,
                    text: row.text,
                    time: row.time,
                    date: row.date
                }));

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

initDb()
    .then(() => {
        server.listen(PORT, '0.0.0.0', () => {
            console.log(`\n🚀 Сервер запущен на порту ${PORT}\n`);
        });
    })
    .catch(err => {
        console.error('❌ Ошибка инициализации базы данных:', err);
        process.exit(1);
    });

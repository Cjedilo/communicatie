import asyncpg
import hashlib
import logging

class Postgres:
    def __init__(self):
        self.connection = None

    async def init(self):       
        self.connection = await asyncpg.connect('postgresql://communicatie:communicatie@localhost/communicatie')
    
    async def get_channels(self):
        rows = await self.connection.fetch("SELECT id, name FROM channels")
        channels = []
        for row in rows:
            channels.append({
                'name': row['name'],
                'id': row['id']
            })

        return channels

    async def get_users(self):
        rows = await self.connection.fetch("SELECT id, name FROM users")
        users = []
        for row in rows:
            users.append({
                'name': row['name'],
                'id': row['id']
            })

        return users
    
    async def create_channel(self, group_name):
        try:
            group_id = await self.connection.fetchval('''
                INSERT INTO channels (name) VALUES($1) RETURNING id
            ''', group_name)

            return {
                "group_name": group_name,
                "id": group_id
            }
        except asyncpg.exceptions.UniqueViolationError:
            return {
                "error": "Name '{group_name}' does already exist."
            }

    async def create_user(self, user_name, password):
        try:
            user_id = await self.connection.fetchval('''
                INSERT INTO users (name, password) VALUES($1, $2) RETURNING id
            ''', user_name, 'fake_password')
            logging.info(password + str(user_id)) 
            await self.connection.execute('''
                UPDATE users set password = $1 WHERE id = $2
            ''', hashlib.md5((password + str(user_id)).encode()).hexdigest(), user_id
            )

            return {
                "user_name": user_name,
                "id": user_id
            }
        except asyncpg.exceptions.UniqueViolationError:
            return {
                "error": "Name '{user_name}' does already exist."
            }
    
    async def login(self, user_name, password):
        user_id = await self.connection.fetchval('''
            SELECT id FROM users where name = $1 and password = MD5($2 || "id")
        ''', user_name, password)
        logging.info(f'{user_id=}')
        return {
            "user_id": user_id
        }


    async def delete_channel(self, channel_id):
        await self.connection.execute("DELETE FROM channels where id = $1", channel_id)
        return channel_id

    async def delete_user(self, user_id):
        await self.connection.execute("DELETE FROM users where id = $1", user_id)
        return user_id

    async def channel(self, chat_id):
        name = await self.connection.fetchval("SELECT name FROM channels where id = $1", chat_id)
        return {
            "chat_name": name,
            "messages" : [row["text"] for row in reversed(await self.connection.fetch("SELECT text FROM messages where channel = $1 ORDER By created DESC LIMIT 25", chat_id))]
        }

    async def message(self, message, channel, user_id):
        await self.connection.execute("INSERT INTO messages (channel, send_by, text) VALUES($1, $2, $3)", channel, user_id, message)
        return {
            "message": message,
            "user_id": user_id
        }
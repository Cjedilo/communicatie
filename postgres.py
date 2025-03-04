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
    
    async def create_channel(self, channel_name):
        try:
            channel_id = await self.connection.fetchval('''
                INSERT INTO channels (name) VALUES($1) RETURNING id
            ''', channel_name)

            return {
                "name": channel_name,
                "id": channel_id
            }
        except asyncpg.exceptions.UniqueViolationError:
            return {
                "error": "Channel '{channel_name}' does already exist."
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

    async def read_profiles(self, profiles):
        result = await self.connection.fetch("SELECT name, avatar, id FROM users WHERE id = ANY($1::uuid[])", profiles)
        response = [{"name": row["name"], "avatar": row["avatar"], "id": row["id"]} for row in result]
        return response

    async def delete_channel(self, channel_id):
        await self.connection.execute("DELETE FROM channels where id = $1", channel_id)
        return channel_id

    async def delete_user(self, user_id):
        await self.connection.execute("DELETE FROM users where id = $1", user_id)
        return user_id

    async def channel(self, chat_id):
        response = {
            "messages" : [
                {"id": row["id"], "message" : row["text"], "send_by": row["send_by"], "date": row["created"]}
                    for row in reversed(await self.connection.fetch(
                        "SELECT id, text, send_by, created FROM messages where channel = $1 ORDER By created DESC LIMIT 25", chat_id
                    ))
            ]
        }
        name = await self.connection.fetchval("SELECT name FROM channels where id = $1", chat_id)
        if name:
            response["channel_name"] = name
        else:
            response["parent"] = chat_id

        return response

    async def message(self, message, channel, user_id):
        message_id, created = await self.connection.fetchval("INSERT INTO messages (channel, send_by, text) VALUES($1, $2, $3) RETURNING id, created", channel, user_id, message)
        return {
            "message": message,
            "id": message_id,
            "send_by": user_id,
            "parent": channel,
            "date": created
        }
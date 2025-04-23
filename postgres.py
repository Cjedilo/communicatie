import json
import asyncpg
import hashlib
import logging

class Postgres:
    def __init__(self):
        self.connection = None

    async def init(self):
        if self.connection == None:       
            self.connection = await asyncpg.connect('postgresql://communicatie:communicatie@localhost/communicatie')
    
    async def get_channels(self, private_id):
        rows = await self.connection.fetch("SELECT id, text, send_by, properties FROM messages where channel is null and (properties->>'public')::boolean IS TRUE")
        channels = []
        for row in rows:
            channels.append({
                'name': row['text'],
                'id': row['id'],
                'owner': row['send_by'],
                'properties': json.loads(row['properties']),
            })

        rows = await self.connection.fetch("SELECT id, text, send_by, properties FROM messages JOIN channel_member on channel_member.channel = messages.id where channel_member.member = (SELECT public_id from users where private_id = $1) and messages.channel is null and (messages.properties->>'public')::boolean IS FALSE", private_id)
        for row in rows:
            channels.append({
                'name': row['text'],
                'id': row['id'],
                'owner': row['send_by'],
                'properties': row['properties'],
            })
        
        return channels

    async def get_users(self):
        rows = await self.connection.fetch("SELECT public_id, name FROM users ORDER BY created")
        users = []
        for row in rows:
            users.append({
                'name': row['name'],
                'id': row['public_id'],
            })

        return users
    
    async def get_user(self, user_id):
        user_row = await self.connection.fetchrow("SELECT name, avatar from users where public_id = $1", user_id)
        nr_messages = await self.connection.fetchval("SELECT COUNT(text) from messages where send_by = $1", user_id)

        return {
            "name": user_row['name'],
            "avatar": user_row['avatar'],
            "nr_messages": nr_messages,
        }

    async def create_channel(self, channel_name, public, private_id):
        try:
            result = await self.connection.fetchrow('''
                INSERT INTO messages (channel, send_by, text, properties) VALUES(null, (SELECT public_id from users where private_id = $3), $1, $2::jsonb) RETURNING id, send_by, properties
            ''', channel_name, json.dumps({"public": public}), private_id)
            if not public:
                await self.connection.execute('''
                    INSERT INTO channel_member (channel, member) VALUES($1, (SELECT public_id from users where private_id = $2))
                ''', result["id"], private_id)
            return {
                "name": channel_name,
                "id": result["id"],
                "owner": result["send_by"],
                "properties": result["properties"],                
            }
        except asyncpg.exceptions.UniqueViolationError:
            return {
                "error": "Channel '{channel_name}' does already exist."
            }

    async def create_user(self, user_name, password):
        try:
            user_id = await self.connection.fetchval('''
                INSERT INTO users (name, password) VALUES($1, $2) RETURNING private_id
            ''', user_name, 'fake_password')
            logging.info(password + str(user_id)) 
            await self.connection.execute('''
                UPDATE users set password = $1 WHERE id = $2
            ''', hashlib.md5((password + str(user_id)).encode()).hexdigest(), user_id
            )

            return {
                "user_name": user_name,
                "id": user_id,
            }
        except asyncpg.exceptions.UniqueViolationError:
            return {
                "error": "Name '{user_name}' does already exist."
            }
    
    async def login(self, user_name, password):
        row = await self.connection.fetchrow('''
            SELECT private_id, public_id FROM users where name = $1 and password = MD5($2 || "public_id")
        ''', user_name, password)
        return {
            "private_id": row["private_id"] if row else None,
            "public_id": row["public_id"] if row else None,
        }

    async def read_profiles(self, profiles):
        result = await self.connection.fetch("SELECT name, avatar, public_id FROM users WHERE public_id = ANY($1::uuid[])", profiles)
        response = [{"name": row["name"], "avatar": row["avatar"], "id": row["public_id"]} for row in result]
        return response

    async def delete_channel(self, channel_id, private_id):
        result = await self.connection.execute("DELETE FROM messages where id = $1 AND send_by = (SELECT public_id from users where private_id = $2)", channel_id, private_id)
        logging.info(result)
        return channel_id if result == "DELETE 1" else None

    async def delete_user(self, user_id, private_id):
        result = await self.connection.execute("DELETE FROM users where public_id = $1 AND private_id = $2", user_id, private_id)
        return user_id if result == "DELETE 1" else None

    async def channel(self, chat_id):
        response = {
            "messages" : [
                {"id": row["id"], "message" : row["text"], "image" : row["image"], "send_by": row["send_by"], "date": row["created"]}
                    for row in reversed(await self.connection.fetch(
                        "SELECT id, text, image, send_by, created FROM messages where channel = $1 ORDER By created DESC LIMIT 25", chat_id
                    ))
            ]
        }
        name = await self.connection.fetchval("SELECT text FROM messages where id = $1 and channel is null", chat_id)
        if name:
            response["channel_name"] = name
        else:
            response["parent"] = chat_id

        return response

    async def message(self, message, image, channel, user_id):
        result = await self.connection.fetchrow("INSERT INTO messages (channel, send_by, text, image) VALUES($1, (SELECT public_id from users where private_id = $2), $3, $4) RETURNING id, send_by, created", channel, user_id, message, image)
        return {
            "message": message,
            "image": image,
            "id": result["id"],
            "send_by": result["send_by"],
            "parent": channel,
            "date": result["created"]
        }
    
    async def set_avatar(self, private_id, file):
         await self.init()
         await self.connection.execute("UPDATE users set avatar = $2 where private_id = $1", private_id, file)
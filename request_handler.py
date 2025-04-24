import os
import uuid
import postgres
import json
import logging

from datetime import datetime
from uuid import UUID

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%m/%d/%Y, %H:%M:%S")
        return json.JSONEncoder.default(self, obj)
    
class RequestHandler:
    def __init__(self):
        self.database = postgres.Postgres()

    async def handle(self, request):
            await self.database.init()

            data = json.loads(request)
            logging.info(f'Received: {data}')
            params = data.get('parameters')
            private_id = data['private_id']
            match data["request"]:
                case "read_channels":
                    response = await self.database.get_channels(private_id)
                case "read_users":
                    response = await self.database.get_users()
                case "read_user":
                    response = await self.database.get_user(params['id'])
                case "create_user":
                    response = await self.database.create_user(params['user_name'], params['password'])
                case "create_channel":
                    response = await self.database.create_channel(params['channel_name'], params['public'], private_id)
                case "delete_channel":
                    response = await self.database.delete_channel(params['id'], private_id)
                case "delete_user":
                    response = await self.database.delete_user(params['id'], private_id)
                case "message":
                    response = await self.database.message(params["message"], params.get("image"), params["channel"], params["user"])
                case "read_channel":
                    response = await self.database.channel(params["id"])
                case "login":
                    response = await self.database.login(params["user_name"], params["password"])
                case "read_profiles":
                    response = await self.database.read_profiles(params)
                case "read_members":
                    response = await self.database.read_members(params)
                case "set_member":
                    response = await self.database.set_member(params["channel"], params["user"], params["is_member"], private_id)
                case _:
                    response = {
                        "error": "request not suported"
                    }
            return json.dumps({"response": data["request"], "value": response}, cls=JSONEncoder)

    async def set_avatar(self, image, private_id):
        img_content = image.file.read()
        img_extention = os.path.splitext(image.filename)[1]
        img_name = str(uuid.uuid4())
        full_name = f"img/{img_name}{img_extention}"
        with open(full_name, "wb") as file:
            file.write(img_content)
        
        await self.database.set_avatar(private_id, full_name)

    async def upload(self, image, private_id):
        img_content = image.file.read()
        img_extention = os.path.splitext(image.filename)[1]
        img_name = str(uuid.uuid4())
        full_name = f"img/{img_name}{img_extention}"
        with open(full_name, "wb") as file:
            file.write(img_content)
        
        return json.dumps(full_name)


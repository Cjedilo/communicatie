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
            match data["request"]:
                case "read_channels":
                    response = await self.database.get_channels()
                case "read_users":
                    response = await self.database.get_users()
                case "read_user":
                    response = await self.database.get_user(params['id'])
                case "create_user":
                    response = await self.database.create_user(params['user_name'], params['password'])
                case "create_channel":
                    response = await self.database.create_channel(params['channel_name'])
                case "delete_channel":
                    response = await self.database.delete_channel(params['id'])
                case "delete_user":
                    response = await self.database.delete_user(params['id'])
                case "message":
                    response = await self.database.message(params["message"], params["channel"], params["user"])
                case "read_channel":
                    response = await self.database.channel(params["id"])
                case "login":
                    response = await self.database.login(params["user_name"], params["password"])
                case "read_profiles":
                    response = await self.database.read_profiles(params)
                case _:
                    response = {
                        "error": "request not suported"
                    }
            return json.dumps({"response": data["request"], "value": response}, cls=JSONEncoder)

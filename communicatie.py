#!venv/bin/python

import asyncio
import json
import ssl
import logging
import postgres
from datetime import datetime
from websockets.asyncio.server import serve
from uuid import UUID


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.strftime("%m/%d/%Y, %H:%M:%S")
        return json.JSONEncoder.default(self, obj)



async def handle(websocket):
    database = postgres.Postgres()
    await database.init()
    async for message in websocket:
        try:
            data = json.loads(message)
            logging.info(f'Received: {data}')
            params = data.get('parameters')
            match data["request"]:
                case "read_channels":
                    response = await database.get_channels()
                case "read_users":
                    response = await database.get_users()
                case "read_user":
                    response = await database.get_user(params['id'])
                case "create_user":
                    response = await database.create_user(params['user_name'], params['password'])
                case "create_channel":
                    response = await database.create_channel(params['channel_name'])
                case "delete_channel":
                    response = await database.delete_channel(params['id'])
                case "delete_user":
                    response = await database.delete_user(params['id'])
                case "message":
                    response = await database.message(params["message"], params["channel"], params["user"])
                case "read_channel":
                    response = await database.channel(params["id"])
                case "login":
                    response = await database.login(params["user_name"], params["password"])
                case "read_profiles":
                    response = await database.read_profiles(params)
                case _:
                    response = {
                        "error": "request not suported"
                    }
            response_string = json.dumps({"response": data["request"], "value": response}, cls=UUIDEncoder)
            logging.info(f"sending: {response_string}")
            await websocket.send(response_string)
        except json.JSONDecodeError as e:
            logging.error(f"Fout bij JSON decoderen! {e=}")


def make_ssl_context():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_cert = "/etc/letsencrypt/live/felix-goedhart.nl/fullchain.pem"
    ssl_key = "/etc/letsencrypt/live/felix-goedhart.nl/privkey.pem"
    ssl_context.load_cert_chain(ssl_cert, keyfile=ssl_key)
    logging.info(f"SSL geladen: {ssl_cert}, {ssl_key}")

    return ssl_context

async def main():
    logging.basicConfig(level=logging.INFO)
    async with serve(handle, "0.0.0.0", 8181, ssl=make_ssl_context()) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
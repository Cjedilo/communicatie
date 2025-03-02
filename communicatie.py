#!venv/bin/python

import asyncio
import json
import ssl
import logging
import postgres
from websockets.asyncio.server import serve
from uuid import UUID


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return str(obj)
        return json.JSONEncoder.default(self, obj)



async def echo(websocket):
    database = postgres.Postgres()
    await database.init()
    async for message in websocket:
        try:
            data = json.loads(message)
            logging.info(f'Ontvangen: {data}')
            response = {
                "error": "request not suported"
            }
            if data.get("request") == "channels":
                response = {'channels': await database.get_channels()}
            elif data.get("request") == "users":
                response = {'users': await database.get_users()}
            elif data.get("request") == "new_user":
                response = {'new_user': await database.create_user(data['user_name'], data['password'])}
            elif data.get("request") == "new_channel":
                response = {'new_channel': await database.create_channel(data['channel_name'])}
            elif data.get("delete") == "channel":
                response = {'delete_channel': await database.delete_channel(data['id'])}
            elif data.get("delete") == "user":
                response = {'delete_user': await database.delete_user(data['id'])}
            elif data.get("message"):
                response = await database.message(data["message"], data["channel"], data["user"])
            elif data.get("channel"):
                response = await database.channel(data["channel"])
            elif data.get("login"):
                response = await database.login(data["login"], data["password"])

            logging.info(f"sending: {json.dumps(response, cls=UUIDEncoder)}")
            await websocket.send(json.dumps(response, cls=UUIDEncoder))
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
    async with serve(echo, "0.0.0.0", 8181, ssl=make_ssl_context()) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
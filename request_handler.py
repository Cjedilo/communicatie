from collections import defaultdict
import common
import os
import ssl
import uuid
import peers
import postgres
import json
import logging
import aiohttp

from websockets.asyncio.client import connect


class RequestHandler:
    subscribed = defaultdict(set)

    def __init__(self, database: postgres.Postgres, peers: peers.Peers):
        self.database = database
        self.peers = peers
        
    async def subscribe(self, channel_id, ws):
        RequestHandler.subscribed[channel_id].add(ws)

    async def handle(self, request, ws):
            data = json.loads(request)
            logging.info(f'Received: {data}')
            params = data.get('parameters')
            private_id = data.get('private_id')
            match data["request"]:
                case "read_channels":
                    response = {
                        "local": await self.database.get_channels(private_id),
                        "remote": await self.peers.get_channels(private_id),
                    }
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
                    listeners = RequestHandler.subscribed[params["channel"]].copy()
                    for listener in listeners:
                        if listener != ws:
                            try:
                                await listener.send_str(json.dumps({"response": "message", "value": response}, cls=common.JSONEncoder))    
                            except aiohttp.client_exceptions.ClientConnectionResetError as e:
                                RequestHandler.subscribed[params["channel"]].remove(listener)

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
                case "subscribe":
                    await self.subscribe(params["channel"], ws)
                    response = await self.database.read_messages(params["channel"])
                case "unsubscribe_all":
                    for channel in RequestHandler.subscribed:
                        if ws in RequestHandler.subscribed[channel]:
                            RequestHandler.subscribed[channel].remove(ws)
                    response = True
                case "read_peers":
                    response = await self.peers.get_peers()
                case "set_peer_name":
                    response = await self.database.set_peer_name(params, private_id)
                case "set_peer_address":
                    response = await self.database.set_peer_address(params, private_id)
                case "add_peer":
                    response = await self.peers.add(params)
                case _:
                    response = {
                        "error": "request not suported"
                    }
            return json.dumps({"response": data["request"], "value": response}, cls=common.JSONEncoder)

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


class PeerHandler:
    def __init__(self, database, peers, handler):
        self.database = database
        self.peers = peers
        self.clients = {}

    async def handle(self, request, ws):
        await self.database.init()

        data = json.loads(request)
        logging.info(f'Received: {data}')
        params = data.get('parameters')
        match data["request"]:
            case "peer_details":
                response = (await self.peers.get_peers())["me"]
            case "read_channels":
                response = await self.database.get_channels_for_peer(self.clients[ws], params["public_id"])
            case "authenticate":
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                async with connect(params["address"], ssl=ctx) as websocket:
                    await websocket.send(json.dumps({"request": "verify", "request_id": str(uuid.uuid4()), "parameters": {"address": await self.database.get_peer_address() or await self.peers.guess_peer_address()}}))
                    response = json.loads(await websocket.recv())
                    logging.info(f"checking: {response} {params}")
                    if response["value"] == params["secret"]:
                        self.clients[ws] = await self.database.get_peer_id(params["address"])
                        response = True
                    else:
                        response = False
            case "verify":
                response = self.peers.get_secret(params["address"])
            case _:
                response = {
                    "error": "request not suported"
                }
        return json.dumps({"response": data["request"], "request_id": data["request_id"], "value": response}, cls=common.JSONEncoder)
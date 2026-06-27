import asyncio
import common
import json
import logging
import uuid
import socket
import ssl

from collections import defaultdict
from requests import get
from postgres import Postgres
from setting import Settings
from websockets.asyncio.client import connect


class Peer:
    def __init__(self, data):
        self.id = str(data["id"])
        self.name = data["name"]
        self.address = data["address"]
        self.subscribed = data["subscribed"]
        self.connected = False
        self.connected_event = asyncio.Event()

        
class Peers:
    def __init__(self, database: Postgres):
        self.database = database
        self.peers = {}
        self.peer_listeners = {}
        self.answers = defaultdict(lambda: defaultdict(dict))
        self.waiting = defaultdict(lambda: defaultdict(asyncio.Event))
        self.secrets = {}

    async def add_peer(self, peer_row):
        async def update_peer(peer):
            try:
                peer_update = await self.request(peer.id, {"request": "peer_details"})
                logging.info(f"Updating peer {peer.id} ({peer.name}) with {peer_update}")
                update_name = peer_update["value"]["name"]
                update_address = peer_update["value"]["address"]
                if peer.name != update_name or peer.address != update_address:
                    peer.name = update_name
                    peer.address = update_address
                    await self.database.update_peer(peer)
            except Exception as e:
                logging.exception(f"Error updating peer {peer.id}: {e}")

        peer = self.peers.setdefault(str(peer_row["id"]), Peer(peer_row))
       
        logging.info(f"Adding peer {peer.id} ({peer.name})")
        tasks = []
        if peer.subscribed:
            task = asyncio.create_task(self.peer_listener(peer))
            self.peer_listeners[peer.id] = task
            task.add_done_callback(self.peer_listener_done)
            await peer.connected_event.wait()
            tasks.append(asyncio.create_task(update_peer(peer)))

        return tasks

    async def init(self):
        peers = await self.database.read_peers()
        tasks = []
        for db_peer in peers:
            tasks = await self.add_peer(db_peer)

        await asyncio.gather(*tasks)

    def peer_listener_done(self, task):
        if task.cancelled():
            logging.info("Peer listener cancelled")
        elif task.exception():
            logging.error(f"Peer listener exception: {task.exception()}")
        else:
            logging.info("Peer listener completed successfully")
        
        self.peer_listeners.pop(task.result().id, None)

    async def guess_peer_address(self):
        ip = get('https://api.ipify.org').content.decode('utf8')
        name = socket.gethostbyaddr(ip)

        return f"wss://{name[0] if name[0] else ip}{f':{Settings.port}' if Settings.port != 80 else ''}/peer"
    
    async def get_local_address(self):
        address = await self.database.get_peer_address()
        if not address:
            address = await self.guess_peer_address()
        return address
    
    async def get_peers(self):
        name = await self.database.get_peer_name()
        address = await self.get_local_address() 
        return {
            "me": {
                "name": name,
                "address": address,
            },
            "others": [{"address": peer.address, "name": peer.name, "connected": peer.connected} for peer in self.peers.values()]
        }
    
    def get_secret(self, address):
        logging.info(f"Getting secret for address {address}")
        return self.secrets.get(address, None)
    
    async def authenticate_peer(self, peer, websocket):
        my_address = await self.get_local_address()
        secret = str(uuid.uuid4())
        logging.info(f"Generated secret for peer {peer.address}: {secret}")
        self.secrets[peer.address] = secret
        await websocket.send(json.dumps({"request": "authenticate", "request_id": str(uuid.uuid4()), "parameters": {"address": my_address, "secret": secret}}))
        response = await websocket.recv()
        response = json.loads(response)
        if response["response"] != "authenticate":
            raise Exception("Authentication failed")
        if not response["value"]:
            raise Exception("Authentication failed")

    async def peer_listener(self, peer):
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            logging.info(f"Connecting to peer {peer.id} ({peer.name}) at {peer.address}")
            async with connect(peer.address, ssl=ctx) as websocket:
                await self.authenticate_peer(peer, websocket)
                peer.connected = True
                peer.connected_event.set()
                peer.socket = websocket
                while response := await websocket.recv():
                    response = json.loads(response)
                    self.answers[peer.id][response["request_id"]] = response
                    self.waiting[peer.id][response["request_id"]].set()
        except Exception as e:
            logging.exception(f"Peer gone {repr(e)}")
        
        peer.connected = False
        peer.connected_event.clear()
        peer.socket = None

        self.answers.pop(peer.id, None)
        self.waiting.pop(peer.id, None)
        self.peer_listeners.pop(peer.id, None)

        return peer

    async def request(self, peer_id, request):
        if peer_id not in self.peers:
            logging.error(f"Peer {peer_id} not found")
            return

        peer = self.peers[peer_id]
        if peer.connected:
            request["request_id"] = str(uuid.uuid4())
            logging.info(f"Sending request to {peer_id}: {request}")
            await peer.socket.send(json.dumps(request, cls=common.JSONEncoder))
        
            try:
                await self.waiting[peer.id][request["request_id"]].wait()
                response = self.answers[peer.id].pop(request["request_id"])
                return response
            except asyncio.TimeoutError:
                logging.error(f"Timeout waiting for response from peer {peer_id}")

        return None
    
    async def add(self, address):
        id = await self.database.add_peer(address)
        await self.add_peer({"address": address, "name": address, "id": id})

        return await self.request(id, {"request": "peer_details"})

    async def get_channels(self, private_id):
        async def fetch_channel(peer, public_id):
            return {
                "peer_id": peer.id,
                "peer_name": peer.name,
                "channels": (await self.request(peer.id, {"request": "read_channels", "parameters": {"public_id": public_id}})).get("value", [])
            } if peer.connected else {
                "peer_id": peer.id,
                "peer_name": peer.name,
                "channels": []
            }

        public_id = await self.database.get_public_id(private_id)
        if not public_id:
            logging.error(f"Public ID not found for private ID {private_id}")
            return None

        tasks = []
        for peer in self.peers.values():
            tasks.append(asyncio.create_task(fetch_channel(peer, public_id)))
        await asyncio.gather(*tasks)            

        return [result.result() for result in tasks]
    
    async def get_channel(self, peer_id, channel_id):
        if peer_id not in self.peers:
            logging.error(f"Peer {peer_id} not found")
            return None

        peer = self.peers[peer_id]
        if peer.connected:
            request = {"request": "read_channel", "parameters": {"id": channel_id}}
            logging.info(f"Sending request to {peer_id}: {request}")
            response = await self.request(peer_id, request)
            if response and response.get("response") == "read_channel":
                return response.get("value")
            else:
                logging.error(f"Error getting channel {channel_id} from peer {peer_id}: {response}")
        return None
    

#!venv/bin/python

import argparse
import asyncio
import logging
import mimetypes
import pathlib
import ssl

import aiohttp
from peers import Peers
import postgres
import request_handler
import jinja2
import aiohttp_jinja2
import os

from aiohttp import web
from setting import Settings

routes = web.RouteTableDef()
handler = None
peer_handler = None

@routes.get('/')
@routes.get('/index.html')
async def get_index(request):
    return aiohttp_jinja2.render_template("index.html", request, context={})

@routes.get('/communicatie.css')
async def get_stylesheet(request):
    response = aiohttp_jinja2.render_template("communicatie.css", request, context={})
    response.headers['content-type'] = 'text/css'
    return response 

@routes.get('/communicatie.js')
async def get_javascript(request):
    response = aiohttp_jinja2.render_template("communicatie.js", request, context={"ws_address": "ws"})
    response.headers['content-type'] = 'text/javascript'
    return response

@routes.get('/chat.html')
async def get_chat(request):
    return aiohttp_jinja2.render_template("chat.html", request, context={})

@routes.get('/login.html')
async def get_login(request):
    return aiohttp_jinja2.render_template("login.html", request, context={})

@routes.get('/user.html')
async def get_user(request):
    return aiohttp_jinja2.render_template("user.html", request, context={})

@routes.get('/channels.html')
async def get_channels(request):
    return aiohttp_jinja2.render_template("channels.html", request, context={})

@routes.get('/img/{tail:.*}')
async def image(request):
    data = pathlib.Path(request.url.path.strip('/')).read_bytes()
    return web.Response(body=data, content_type=mimetypes.guess_type(request.url.path)[0])

@routes.post("/set_avatar")
async def set_avatar(request):
    post = await request.post()
    image = post.get("file")
    private_id = post.get("private_id")
    logging.info(image)
    await handler.set_avatar(image, private_id)
    
    return web.Response(body="ok")

@routes.post("/upload")
async def upload(request):
    post = await request.post()
    image = post.get("file")
    private_id = post.get("private_id")
    logging.info(image)
    
    return web.Response(body=await handler.upload(image, private_id))

@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:  
            response_string = await handler.handle(msg.data, ws)
            logging.info(f"sending: {response_string}")
            await ws.send_str(response_string)
    return ws

@routes.get('/peer')
async def peer_websocket(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            response_string = await peer_handler.handle(msg.data, ws)
            logging.info(f"sending: {response_string}")
            await ws.send_str(response_string)
    return ws

def make_ssl_context():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_cert = "/etc/letsencrypt/live/felix-goedhart.nl/fullchain.pem"
    ssl_key = "/etc/letsencrypt/live/felix-goedhart.nl/privkey.pem"
    ssl_context.load_cert_chain(ssl_cert, keyfile=ssl_key)
    logging.info(f"SSL geladen: {ssl_cert}, {ssl_key}")

    return ssl_context

def args_parse():
    parser = argparse.ArgumentParser(
                    prog='communication',
                    description='Talk to others, keep your data.',
                    epilog='Written by Bas')
    parser.add_argument('-p', '--port', default=8181, type=int)
    parser.add_argument('-d', '--database', default="postgresql://communicatie:communicatie@localhost/communicatie", type=str)
    args = parser.parse_args()

    Settings.port = args.port
    Settings.db_connect = args.database
    
async def main():
    global handler
    global peer_handler
    global routes

    logging.basicConfig(level=logging.INFO)
    args_parse()

    database = postgres.Postgres()
    await database.init()
    peers = Peers(database)
    
    handler = request_handler.RequestHandler(database, peers)
    peer_handler = request_handler.PeerHandler(database, peers, handler)

    app = web.Application()
    app.add_routes(routes)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(os.path.join(os.getcwd(), "templates")))

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, port=Settings.port, ssl_context=make_ssl_context())    
    await site.start()



    await peers.init()

    # wait forever
    await asyncio.Event().wait()
    


if __name__ == "__main__":
    asyncio.run(main())

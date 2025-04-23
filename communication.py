#!venv/bin/python

import logging
import mimetypes
import pathlib
import ssl
import request_handler
import jinja2
import aiohttp_jinja2
import os

from aiohttp import web

routes = web.RouteTableDef()
handler = request_handler.RequestHandler()

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
    response = aiohttp_jinja2.render_template("communicatie.js", request, context={})
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
            response_string = await handler.handle(msg.data)
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

def main():
    logging.basicConfig(level=logging.INFO)

    app = web.Application()
    app.add_routes(routes)
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader(os.path.join(os.getcwd(), "templates")))
    web.run_app(app, port=8181, ssl_context=make_ssl_context())


if __name__ == "__main__":
    main()
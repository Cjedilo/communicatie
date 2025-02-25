#!venv/bin/python

import asyncio
import json
import ssl
import logging
import psycopg2

from websockets.asyncio.server import serve

def get_channels():
    # https://magicstack.github.io/asyncpg/current/usage.html
    conn = psycopg2.connect(
        database="communicatie", user='communicatie', password='communicatie', host='127.0.0.1', port= '5432'
    )
    cur = conn.cursor()
    cur.execute("SELECT name FROM channels;")
    channels = []
    while row := cur.fetchone():
        channels.append(row)
    cur.close()
    conn.close()
    return channels


async def echo(websocket):
    async for message in websocket:
        try:
            data = json.loads(message)
            logging.info(f'Ontvangen: {data}')
            if data.get("request") == "channels":
                await websocket.send(json.dumps({'channels': get_channels()}))
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